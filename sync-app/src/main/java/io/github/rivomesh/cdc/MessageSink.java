package io.github.rivomesh.cdc;

import java.nio.charset.StandardCharsets;
import java.util.*;

/** Confirmed at-least-once events with durable receipts, never a cross-broker atomic-transaction claim. */
public final class MessageSink implements SinkAdapter {
    public interface Factory {ConfirmedPublisher open()throws Exception;}
    private final String id,generation;
    private final Map<String,String> mappings;
    private final PublicationStore store;
    private final Factory factory;
    private final Map<String,TableSchema> schemas=new HashMap<>();
    private final Set<String> destinations=new TreeSet<>();
    private final Set<String> established=new TreeSet<>();
    private final int maximumMessageBytes;
    private ConfirmedPublisher publisher;
    private String transportProgress;
    private long activeEpoch;
    public MessageSink(String id,Map<String,String> mappings,PublicationStore store,Factory factory,int maximumMessageBytes)throws Exception{
        if(id==null||mappings==null||mappings.isEmpty()||maximumMessageBytes<1024)throw new IllegalArgumentException("MESSAGE_SINK_CONFIG_INVALID");
        this.id=id;this.mappings=new TreeMap<>(mappings);this.store=store;this.factory=factory;this.maximumMessageBytes=maximumMessageBytes;generation=store.generation();
    }
    @Override public String id(){return id;}
    @Override public boolean includes(TableSchema table){return mappings.containsKey(table.database);}
    @Override public Consistency consistency(){return Consistency.AT_LEAST_ONCE_EVENTS;}
    @Override public Checkpoint activate(long epoch)throws Exception{Checkpoint checkpoint=store.activate(epoch);activeEpoch=epoch;return checkpoint;}
    @Override public Checkpoint checkpoint()throws Exception{return store.checkpoint();}
    private ConfirmedPublisher publisher()throws Exception{if(publisher==null){ConfirmedPublisher opened=factory.open();try{String identity=opened.identity();if(identity!=null)store.identity(activeEpoch,identity);transportProgress=store.progress();if(opened.requiresProgress()&&transportProgress==null&&(!established.isEmpty()||store.checkpoint().sequence>0))throw new IllegalStateException("PUBLICATION_TRANSPORT_UPGRADE_REQUIRES_PLAN");opened.resumeProgress(transportProgress);if(!established.isEmpty())opened.require(established);Set<String> fresh=new TreeSet<>(destinations);fresh.removeAll(established);if(!fresh.isEmpty())opened.prepare(fresh);String preparedIdentity=opened.identity();if(preparedIdentity!=null)store.identity(activeEpoch,preparedIdentity);publisher=opened;}catch(Exception failure){try{opened.close();}catch(Exception close){failure.addSuppressed(close);}throw failure;}}return publisher;}
    private MessageEvent event(String kind,String namespace,String logicalId){MessageEvent e=new MessageEvent();e.pipelineId=store.pipelineId();e.subscriptionId=id;e.generation=generation;e.kind=kind;e.namespace=namespace;e.id=e.pipelineId+"/"+generation+"/"+Codec.sha256(id+":"+logicalId);return e;}
    private String namespace(TableSchema table){String namespace=mappings.get(table.database);if(namespace==null)throw new IllegalArgumentException("DATABASE_MAPPING_MISSING");return namespace;}
    /** Reserve control channels and encode names containing broker routing syntax. */
    public static String channel(String namespace,TableSchema table){String name=table.name;if(!name.matches("[A-Za-z0-9_]{1,128}")||name.startsWith("__")||name.startsWith("_rm_"))name="_rm_"+Codec.sha256(table.id);return namespace+"."+name;}
    private ConfirmedPublisher.Message message(MessageEvent event,String channel,String key,long epoch){
        ConfirmedPublisher.Message message=new ConfirmedPublisher.Message();message.destination=channel;message.key=key;message.id=event.id;message.body=Codec.encode(event).getBytes(StandardCharsets.UTF_8);
        if(message.body.length>maximumMessageBytes)throw new IllegalStateException("MESSAGE_TOO_LARGE");message.headers.put("rivomesh_event_id",event.id);message.headers.put("rivomesh_epoch",Long.toString(epoch));message.headers.put("rivomesh_kind",event.kind);return message;
    }
    private void send(long epoch,List<ConfirmedPublisher.Message> messages)throws Exception{
        store.check(epoch);
        try{ConfirmedPublisher active=publisher();active.publish(messages);String confirmed=active.confirmedProgress();if(confirmed!=null){store.progress(epoch,transportProgress,confirmed);transportProgress=confirmed;}else if(active.requiresProgress())throw new IllegalStateException("PUBLICATION_TRANSPORT_PROGRESS_MISSING");}catch(Exception failure){reset();throw failure;}
    }
    private void reset(){if(publisher!=null)try{publisher.close();}catch(Exception ignored){/* publication receipt has not advanced */}finally{publisher=null;}}
    @Override public synchronized void prepare(long epoch,TableSchema table)throws Exception{
        table.validate();String namespace=namespace(table);store.check(epoch);
        boolean prepared=store.schemaPrepared(table);Set<String> required=new TreeSet<>(Arrays.asList(namespace+".__schema",namespace+".__transactions",channel(namespace,table)));destinations.addAll(required);if(prepared)established.addAll(required);
        try{if(prepared)publisher().require(required);else publisher().prepare(required);}
        catch(Exception failure){reset();throw failure;}
        if(!prepared){
            MessageEvent event=event("SCHEMA",namespace,"schema:"+table.id+":"+table.version);event.tableId=table.id;event.schemaVersion=table.version;event.schema=table;
            send(epoch,Collections.singletonList(message(event,namespace+".__schema",table.id,epoch)));store.schemaPrepared(epoch,table);
        }established.addAll(required);schemas.put(table.id,Codec.decode(Codec.encode(table),TableSchema.class));
    }
    private String key(TableSchema table,List<Cell> row,String rowId){
        if(row==null)return null;if(row.size()!=table.columns.size())throw new IllegalArgumentException("ROW_WIDTH_INVALID");
        if(table.keyless()){if(rowId==null||!rowId.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("ROW_IDENTITY_REQUIRED");return table.id+":"+rowId;}
        List<String> names=table.primaryKey.isEmpty()?table.identityKey:table.primaryKey;TableSchema keys=new TableSchema();List<Cell> values=new ArrayList<>();
        for(String name:names){int column=table.column(name);Cell cell=row.get(column);if(cell==null||!cell.present||cell.value==null)throw new IllegalArgumentException("IDENTITY_VALUE_MISSING");keys.columns.add(table.columns.get(column));values.add(cell);}
        return table.id+":"+Codec.sha256(JdbcNormalization.canonical(keys,values));
    }
    @Override public synchronized void apply(long epoch,long sequence,SourceTransaction tx)throws Exception{
        tx.validate();if(!generation.equals(tx.streamGeneration))throw new IllegalStateException("STREAM_GENERATION_MISMATCH");String hash=Codec.sha256(Codec.encode(tx));Checkpoint cp=store.checkpoint();
        if(cp.sequence==sequence&&tx.id.equals(cp.transactionId)&&hash.equals(cp.transactionHash)){store.check(epoch);return;}
        if(sequence!=cp.sequence+1)throw new IllegalStateException("PUBLICATION_SEQUENCE_GAP");
        Set<String> namespaces=new TreeSet<>(mappings.values());
        // Validate the whole transaction before publishing any BEGIN or row record.
        int validatedOrdinal=0;Set<String> checkedTables=new HashSet<>();
        for(Change change:tx.changes){if(change.operation!=Change.Operation.BARRIER){
            if(change.operation==Change.Operation.DDL)throw new IllegalStateException("DDL_PLAN_REQUIRED");
            TableSchema table=schemas.get(change.tableId);if(table==null||table.version!=change.schemaVersion)throw new IllegalStateException("PUBLICATION_SCHEMA_NOT_PREPARED");if(checkedTables.add(table.id)){SnapshotCheckpoint snapshot=store.snapshot(table.id);if(snapshot==null||!"COMPLETE".equals(snapshot.state))throw new IllegalStateException("PUBLICATION_SNAPSHOT_INCOMPLETE");}
            rowMessage(epoch,sequence,tx,change,validatedOrdinal);
        }validatedOrdinal++;}
        for(String namespace:namespaces){MessageEvent begin=transactionEvent("BEGIN",namespace,tx,sequence);send(epoch,Collections.singletonList(message(begin,namespace+".__transactions",tx.id,epoch)));}
        List<ConfirmedPublisher.Message> batch=new ArrayList<>();int ordinal=0;
        for(Change change:tx.changes){
            if(change.operation==Change.Operation.BARRIER){ordinal++;continue;}
            batch.add(rowMessage(epoch,sequence,tx,change,ordinal++));if(batch.size()==100){send(epoch,batch);batch=new ArrayList<>();}
        }if(!batch.isEmpty())send(epoch,batch);
        for(String namespace:namespaces){MessageEvent commit=transactionEvent("COMMIT",namespace,tx,sequence);commit.contentHash=hash;send(epoch,Collections.singletonList(message(commit,namespace+".__transactions",tx.id,epoch)));}
        store.committed(epoch,sequence,tx.id,hash);
    }
    private MessageEvent transactionEvent(String kind,String namespace,SourceTransaction tx,long sequence){MessageEvent event=event(kind,namespace,"tx:"+tx.id+":"+namespace+":"+kind);event.transactionId=tx.id;event.transactionKind=tx.kind.name();event.attributes=new TreeMap<>(tx.attributes);event.sequence=sequence;event.position=tx.position;for(String mapped:new TreeSet<>(mappings.values()))event.expectedChanges.put(mapped,0);int ordinal=0;for(Change change:tx.changes){if(change.operation==Change.Operation.BARRIER)event.controls.put(ordinal,change);else{String mapped=namespace(schemas.get(change.tableId));event.expectedChanges.put(mapped,event.expectedChanges.get(mapped)+1);event.totalChanges++;}ordinal++;}return event;}
    private ConfirmedPublisher.Message rowMessage(long epoch,long sequence,SourceTransaction tx,Change change,int ordinal){
        TableSchema table=schemas.get(change.tableId);String namespace=namespace(table);MessageEvent event=event("ROW",namespace,"row:"+change.eventId);event.transactionId=tx.id;event.sequence=sequence;event.position=tx.position;event.ordinal=ordinal;event.tableId=table.id;event.schemaVersion=table.version;event.change=change;event.beforeKey=key(table,change.before,change.rowId);event.afterKey=key(table,change.after,change.rowId);
        return message(event,channel(namespace,table),event.afterKey==null?event.beforeKey:event.afterKey,epoch);
    }
    private void manifest(MessageEvent event,TableSchema table,Journal.SnapshotState snapshot){event.tableId=table.id;event.schemaVersion=table.version;event.snapshotId=snapshot.snapshotId;event.position=snapshot.baseline;event.rows=snapshot.rows;event.parts=snapshot.nextPart;}
    @Override public synchronized SnapshotCheckpoint beginSnapshot(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception{
        if(!"COMPLETE".equals(snapshot.state)||!table.id.equals(snapshot.tableId)||snapshot.schemaVersion!=table.version||!store.schemaPrepared(table))throw new IllegalArgumentException("SNAPSHOT_NOT_COMPLETE_OR_PREPARED");
        SnapshotCheckpoint cp=store.snapshot(table.id);if(cp==null){String namespace=namespace(table);MessageEvent begin=event("SNAPSHOT_BEGIN",namespace,"snapshot:"+snapshot.snapshotId+":begin");manifest(begin,table,snapshot);send(epoch,Collections.singletonList(message(begin,namespace+".__transactions",table.id,epoch)));store.begin(epoch,table,snapshot);cp=store.snapshot(table.id);}
        if(!snapshot.snapshotId.equals(cp.snapshotId))throw new IllegalStateException("PUBLICATION_SNAPSHOT_REBASE_REQUIRES_PLAN");return cp;
    }
    @Override public synchronized void snapshotPart(long epoch,TableSchema table,String snapshotId,Journal.SnapshotPart part)throws Exception{
        SnapshotCheckpoint cp=store.snapshot(table.id);if(cp==null||!snapshotId.equals(cp.snapshotId))throw new IllegalStateException("PUBLICATION_SNAPSHOT_CHANGED");
        if(part==null||part.rows==null||part.rows.isEmpty()||part.part<0)throw new IllegalArgumentException("PUBLICATION_PART_INVALID");
        String hash=Codec.sha256(Codec.encode(part.rows));if(part.part<cp.nextPart){store.part(epoch,table,snapshotId,part.part,part.rows.size(),hash);return;}
        if(!"BUILDING".equals(cp.state)||part.part!=cp.nextPart)throw new IllegalStateException("PUBLICATION_PART_GAP");
        List<ConfirmedPublisher.Message> batch=new ArrayList<>();int ordinal=0;String namespace=namespace(table);
        for(List<Cell> row:part.rows){
            String rowId=Codec.sha256(snapshotId+":"+part.part+":"+ordinal);MessageEvent event=event("SNAPSHOT_ROW",namespace,"snapshot:"+snapshotId+":"+part.part+":"+ordinal);event.tableId=table.id;event.schemaVersion=table.version;event.snapshotId=snapshotId;event.part=part.part;event.ordinal=ordinal++;
            Change change=new Change();change.operation=Change.Operation.SNAPSHOT;change.eventId=event.id;change.tableId=table.id;change.schemaVersion=table.version;change.rowId=table.keyless()?rowId:null;change.after=row;change.validate();event.change=change;event.afterKey=key(table,row,change.rowId);
            batch.add(message(event,channel(namespace,table),event.afterKey,epoch));if(batch.size()==100){send(epoch,batch);batch=new ArrayList<>();}
        }if(!batch.isEmpty())send(epoch,batch);store.part(epoch,table,snapshotId,part.part,part.rows.size(),hash);
    }
    @Override public synchronized void finishSnapshot(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception{
        SnapshotCheckpoint cp=store.snapshot(table.id);if(cp==null||!snapshot.snapshotId.equals(cp.snapshotId)||cp.nextPart!=snapshot.nextPart||cp.rows!=snapshot.rows)throw new IllegalStateException("PUBLICATION_SNAPSHOT_MANIFEST_MISMATCH");
        if("COMPLETE".equals(cp.state)){store.check(epoch);return;}
        String namespace=namespace(table);MessageEvent end=event("SNAPSHOT_END",namespace,"snapshot:"+snapshot.snapshotId+":end");manifest(end,table,snapshot);send(epoch,Collections.singletonList(message(end,namespace+".__transactions",table.id,epoch)));store.finish(epoch,table,snapshot);
    }
    @Override public Map<String,Object> diagnostics()throws Exception{Map<String,Object> result=new LinkedHashMap<>();result.put("id",id);result.put("consistency",consistency().name());result.put("checkpoint",checkpoint());return result;}
    @Override public synchronized void close(){reset();}
}
