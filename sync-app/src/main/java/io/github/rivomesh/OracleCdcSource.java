package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.PipelineFailure;
import io.github.rivomesh.cdc.SourceAdapter;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

/** A single-primary Oracle PDB source with Flashback snapshots and archived LogMiner delivery. */
final class OracleCdcSource implements SourceAdapter {
    private final Config.Source config;
    private final Config.OracleSource options;
    private final String generation,pdbIncarnation;
    private final int maxTransactionRows;
    private final OracleSourceProbe.Authority authority;
    private final Map<String,TableSchema> tables=new LinkedHashMap<>();
    private SourcePosition snapshot;

    static void validate(PipelineConfig pipeline){
        Config.Source source=pipeline.source;
        Config.OracleSource options=source.oracle;
        if(options==null||options.root==null||options.pdb==null||!endpoint(options.root)||!endpoint(options.pdb)
                ||source.database==null||!source.database.matches("[A-Z][A-Z0-9_]{0,127}")
                ||source.clusterId==null||!source.clusterId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                ||options.archivePollMillis<250||options.archivePollMillis>30000||pipeline.maxTransactionRows<1)
            throw new IllegalArgumentException("ORACLE_SOURCE_CONFIG_INVALID");
        if(source.nodes==null||!source.nodes.isEmpty()||!Arrays.asList("AUTO","SINGLE").contains(source.topology))
            throw new IllegalArgumentException("ORACLE_SINGLE_PRIMARY_REQUIRED");
        if(source.expectedVersion!=null&&!source.expectedVersion.matches("23\\.26(?:\\.[0-9]+\\.[0-9]+\\.[0-9]+)?"))
            throw new IllegalArgumentException("SOURCE_VERSION_PROFILE_INVALID");
        Set<String> owners=new TreeSet<>();
        for(String table:OracleSourceProbe.selectedTables(options.tables))owners.add(table.substring(0,table.indexOf('.')));
        if(!owners.equals(new TreeSet<>(pipeline.databases)))throw new IllegalArgumentException("ORACLE_SOURCE_SCOPE_MISMATCH");
        for(PipelineConfig.Target target:pipeline.targets)
            if(target!=null&&target.product!=TargetProduct.POSTGRESQL)
                throw new IllegalArgumentException("ORACLE_SOURCE_TARGET_PROFILE_UNAVAILABLE");
    }

    private static boolean endpoint(Config.OracleEndpoint value){
        return value.url!=null&&value.url.startsWith("jdbc:oracle:thin:@//")
                &&value.username!=null&&!value.username.trim().isEmpty();
    }

    OracleCdcSource(PipelineConfig pipeline,String generation)throws Exception{
        validate(pipeline);
        config=pipeline.source;
        options=config.oracle;
        this.generation=generation;
        maxTransactionRows=pipeline.maxTransactionRows;
        try(Connection root=options.root.connect();Connection pdb=options.pdb.connect()){
            authority=OracleSourceProbe.authority(root,config.database);
            OracleSourceProbe.verifyPdb(pdb,config.database,authority);
            pdbIncarnation=pdbIncarnation(root,authority.containerId);
            if(config.expectedVersion!=null&&!config.expectedVersion.equals("23.26")
                    &&!config.expectedVersion.equals(authority.version))throw invalid("ORACLE_SOURCE_VERSION_UNSUPPORTED");
            OracleArchiveWindow.latestEnd(root,authority.resetlogsScn);
        }
    }

    @Override public String type(){return "ORACLE";}

    @Override public List<TableSchema> discover()throws Exception{
        try(Connection pdb=options.pdb.connect()){
            OracleSourceProbe.verifyPdb(pdb,config.database,authority);
            tables.clear();
            for(String qualified:options.tables){
                OracleSourceProbe.verifyFlashback(pdb,qualified,
                        snapshot==null?authority.currentScn:snapshot.getPayload().get("scn"));
                TableSchema table=OracleSourceProbe.redoSchema(pdb,config.database,qualified,options.allowVirtualNumericColumns);
                requireSupplementalImages(pdb,table);
                tables.put(qualified,table);
            }
            return new ArrayList<>(tables.values());
        }
    }

    private static void requireSupplementalImages(Connection pdb,TableSchema table)throws Exception{
        try(PreparedStatement query=pdb.prepareStatement("SELECT 1 FROM all_log_groups WHERE owner=? AND table_name=? "
                +"AND log_group_type='ALL COLUMN LOGGING' AND always='ALWAYS'")){
            query.setQueryTimeout(20);
            query.setString(1,table.database);
            query.setString(2,table.name);
            try(ResultSet row=query.executeQuery()){
                if(!row.next())throw invalid("ORACLE_ALL_COLUMN_LOGGING_REQUIRED");
            }
        }
    }

    private OracleSourceProbe.Authority current(Connection root)throws Exception{
        OracleSourceProbe.Authority observed=OracleSourceProbe.authority(root,config.database);
        if(!authority.same(observed)||!pdbIncarnation.equals(pdbIncarnation(root,observed.containerId)))
            throw invalid("ORACLE_SOURCE_AUTHORITY_CHANGED");
        return observed;
    }

    private static String pdbIncarnation(Connection root,int container)throws Exception{
        try(PreparedStatement query=root.prepareStatement("SELECT pdb_incarnation#,begin_resetlogs_scn "
                +"FROM v$pdb_incarnation WHERE con_id=? AND status='CURRENT'")){
            query.setQueryTimeout(20);
            query.setInt(1,container);
            try(ResultSet row=query.executeQuery()){
                if(!row.next())throw invalid("ORACLE_PDB_INCARNATION_UNAVAILABLE");
                String result=OracleArchiveWindow.scn(row.getString(1))+":"+OracleArchiveWindow.scn(row.getString(2));
                if(row.next())throw invalid("ORACLE_PDB_INCARNATION_UNAVAILABLE");
                return result;
            }
        }
    }

    private static final class Floor {
        final BigInteger observed,floor;
        Floor(BigInteger observed,BigInteger floor){this.observed=observed;this.floor=floor;}
    }

    private Floor safeFloor(Connection root,OracleSourceProbe.Authority first)throws Exception{
        BigInteger earliest=OracleArchiveWindow.scn(first.currentScn);
        // Transactions active throughout this sample can predate the current SCN.
        // Transactions that finish during the sample are covered by the later observed SCN.
        try(PreparedStatement query=root.prepareStatement("SELECT COUNT(*),COUNT(start_scn),MIN(start_scn) "
                +"FROM v$transaction WHERE con_id=?")){
            query.setQueryTimeout(20);
            query.setInt(1,authority.containerId);
            try(ResultSet row=query.executeQuery()){
                if(!row.next()||row.getLong(1)!=row.getLong(2))throw invalid("ORACLE_ACTIVE_TRANSACTION_SCN_UNAVAILABLE");
                if(row.getLong(1)>0){
                    BigInteger active=OracleArchiveWindow.scn(row.getString(3));
                    if(active.signum()==0)throw invalid("ORACLE_ACTIVE_TRANSACTION_SCN_UNAVAILABLE");
                    earliest=earliest.min(active);
                }
            }
        }
        BigInteger observed=OracleArchiveWindow.scn(current(root).currentScn);
        if(observed.compareTo(earliest)<0)throw invalid("ORACLE_SOURCE_SCN_REWOUND");
        return new Floor(observed,earliest);
    }

    @Override public SourcePosition position()throws Exception{
        try(Connection root=options.root.connect()){
            Floor anchor=safeFloor(root,current(root));
            snapshot=positionAt(anchor.observed,anchor.floor);
            return snapshot;
        }
    }

    private SourcePosition positionAt(BigInteger scn,BigInteger start){
        SourcePosition point=authority.at(config.clusterId,scn.toString());
        Map<String,String> fields=new LinkedHashMap<>(point.getPayload());
        fields.put("archiveStartScn",start.toString());
        fields.put("pdbIncarnation",pdbIncarnation);
        return new SourcePosition(1,type(),config.clusterId,fields);
    }

    private BigInteger scn(SourcePosition point){
        if(point==null||point.getFormatVersion()!=1||!type().equals(point.getSourceType())
                ||!config.clusterId.equals(point.getSourceId()))throw invalid("ORACLE_POSITION_IDENTITY_MISMATCH");
        Map<String,String> fields=point.getPayload();
        if(!authority.databaseId.equals(fields.get("databaseId"))||!authority.resetlogsScn.equals(fields.get("resetlogsScn"))
                ||!authority.pdbGuid.equals(fields.get("pdbGuid"))||!authority.pdbUid.equals(fields.get("pdbUid"))
                ||!pdbIncarnation.equals(fields.get("pdbIncarnation")))throw invalid("ORACLE_SOURCE_AUTHORITY_CHANGED");
        BigInteger value=OracleArchiveWindow.scn(fields.get("scn"));
        if(floor(point).compareTo(value)>0)throw invalid("ORACLE_POSITION_FLOOR_INVALID");
        return value;
    }

    private BigInteger floor(SourcePosition point){
        return OracleArchiveWindow.scn(point.getPayload().get("archiveStartScn"));
    }

    @Override public void validateFailover(SourcePosition captured,List<SourcePosition> dependencies)throws Exception{
        try(Connection root=options.root.connect()){
            OracleSourceProbe.Authority live=current(root);
            if(captured==null)return;
            BigInteger durable=scn(captured),now=OracleArchiveWindow.scn(live.currentScn);
            if(now.compareTo(durable)<0)throw invalid("ORACLE_SOURCE_SCN_REWOUND");
            for(SourcePosition dependency:dependencies)
                if(scn(dependency).compareTo(now)>0)throw invalid("ORACLE_SOURCE_SCN_REWOUND");
            BigInteger archiveEnd=OracleArchiveWindow.latestEnd(root,authority.resetlogsScn).min(now);
            if(archiveEnd.subtract(BigInteger.ONE).compareTo(durable)>0)
                coverage(root,floor(captured),archiveEnd,now,false);
            snapshot=positionAt(now,floor(captured));
        }
    }

    @Override public void checkTopology(SourcePosition captured,List<SourcePosition> dependencies)throws Exception{
        try(Connection root=options.root.connect()){
            BigInteger now=OracleArchiveWindow.scn(current(root).currentScn);
            if(scn(captured).compareTo(now)>0)throw invalid("ORACLE_SOURCE_SCN_REWOUND");
            for(SourcePosition dependency:dependencies)
                if(scn(dependency).compareTo(now)>0)throw invalid("ORACLE_SOURCE_SCN_REWOUND");
        }
    }

    @Override public Snapshot snapshot(TableSchema table)throws Exception{
        if(snapshot==null)throw invalid("ORACLE_SNAPSHOT_ANCHOR_REQUIRED");
        Connection read=options.pdb.connect();
        try{
            OracleSourceProbe.verifyPdb(read,config.database,authority);
            if(table.columns.stream().anyMatch(OracleDate::isColumn))OracleDate.prepareSession(read);
            StringBuilder sql=new StringBuilder("SELECT ");
            for(TableSchema.Column column:table.columns){
                if(sql.length()>7)sql.append(',');
                String name=Sql.pg(column.name);
                sql.append(OracleDate.isColumn(column)?OracleDate.snapshotExpression(name):name);
            }
            sql.append(" FROM ").append(Sql.pg(table.database)).append('.').append(Sql.pg(table.name)).append(" AS OF SCN ?");
            PreparedStatement query=read.prepareStatement(sql.toString());
            query.setQueryTimeout(30);
            query.setFetchSize(256);
            SourcePosition anchor=snapshot;
            query.setBigDecimal(1,new BigDecimal(scn(anchor)));
            OracleVirtualNumbers.Plan generated=options.allowVirtualNumericColumns?OracleVirtualNumbers.plan(table):null;
            ResultSet rows=query.executeQuery();
            return new Snapshot(){
                public SourcePosition baseline(){return anchor;}
                public List<List<Cell>> next(int maximum)throws Exception{
                    if(maximum<1||maximum>10000)throw invalid("SNAPSHOT_READ_INVALID");
                    List<List<Cell>> result=new ArrayList<>();
                    while(result.size()<maximum&&rows.next()){
                        List<Cell> row=new ArrayList<>();
                        for(int i=0;i<table.columns.size();i++){
                            TableSchema.Column column=table.columns.get(i);
                            if(column.type==TableSchema.Type.STRING)
                                row.add(Cell.value(OracleCharacters.national(column)?rows.getNString(i+1):rows.getString(i+1)));
                            else if(column.type==TableSchema.Type.JSON)row.add(Cell.value(OracleTextJson.value(rows.getString(i+1))));
                            else if(OracleDate.isColumn(column))row.add(Cell.value(OracleDate.snapshotValue(rows.getString(i+1))));
                            else{
                                BigDecimal number=rows.getBigDecimal(i+1);
                                row.add(Cell.value(number==null?null:number.toPlainString()));
                            }
                        }
                        if(generated!=null)generated.verifySnapshot(row);
                        result.add(row);
                    }
                    return result;
                }
                public void close()throws Exception{
                    try{rows.close();}finally{try{query.close();}finally{read.close();}}
                }
            };
        }catch(Exception failure){
            try{read.close();}catch(Exception close){failure.addSuppressed(close);}
            throw failure;
        }
    }

    @Override public boolean includes(SourcePosition baseline,String transactionId){
        if(transactionId==null||!transactionId.matches("oracle:[1-9][0-9]{0,19}:(?:[A-F0-9]{16}|archive)"))
            throw invalid("ORACLE_TRANSACTION_ID_INVALID");
        return scn(baseline).compareTo(OracleArchiveWindow.scn(transactionId.split(":")[1]))>=0;
    }

    @Override public boolean covers(SourcePosition captured,SourcePosition required){
        return captured!=null&&required!=null&&scn(captured).compareTo(scn(required))>=0;
    }

    private OracleArchiveWindow.Coverage coverage(Connection root,BigInteger start,BigInteger end,
                                                   BigInteger now,boolean files)throws Exception{
        OracleArchiveWindow window=new OracleArchiveWindow();
        window.startScn=start.toString();
        window.endScnExclusive=end.toString();
        window.decodeCommitted=files;
        return window.inspect(root,authority.resetlogsScn,now.toString());
    }

    @Override public Stream stream(SourcePosition captured,Receiver receiver){
        return new Capture(captured,receiver);
    }

    private final class Capture implements Stream {
        final Receiver receiver;
        final Thread reader;
        final AtomicReference<Throwable> failure=new AtomicReference<>();
        SourcePosition durable;
        Floor pending;
        volatile boolean closing;

        Capture(SourcePosition captured,Receiver receiver){
            scn(captured);
            durable=captured;
            this.receiver=receiver;
            reader=new Thread(this::read,"rivomesh-oracle-capture");
            reader.setDaemon(true);
            reader.start();
        }

        void read(){
            try{
                while(!closing){
                    poll();
                    Thread.sleep(options.archivePollMillis);
                }
            }catch(Throwable e){
                if(!closing)failure.compareAndSet(null,e);
            }
        }

        void poll()throws Exception{
            try(Connection root=options.root.connect()){
                OracleSourceProbe.Authority observed=current(root);
                BigInteger now=OracleArchiveWindow.scn(observed.currentScn);
                BigInteger previous=scn(durable),start=floor(durable);
                if(now.compareTo(previous)<0)throw invalid("ORACLE_SOURCE_SCN_REWOUND");
                if(pending==null)pending=safeFloor(root,observed);
                BigInteger end=OracleArchiveWindow.latestEnd(root,authority.resetlogsScn).min(now);
                BigInteger closed=end.subtract(BigInteger.ONE);
                if(closed.compareTo(previous)<=0)return;
                OracleArchiveWindow.Coverage range=coverage(root,start,end,now,true);
                OracleArchiveRead.visit(root,config.database,range,tables,durable,previous,generation,maxTransactionRows,this::persist);
                BigInteger nextStart=start;
                if(closed.compareTo(pending.observed)>=0){
                    nextStart=start.max(pending.floor);
                    pending=null;
                }
                // Persist the closed archive boundary after its transactions. An empty record advances
                // the journal even when that interval contains only unrelated or rolled-back changes.
                SourceTransaction watermark=new SourceTransaction();
                watermark.id="oracle:"+closed+":archive";
                watermark.streamGeneration=generation;
                watermark.position=positionAt(closed,nextStart);
                persist(watermark);
            }catch(PipelineFailure|JdbcJournal.Fenced|InterruptedException e){throw e;}
            catch(IllegalStateException e){
                PipelineFailure.Phase phase="ORACLE_REDO_DDL_REQUIRES_PLAN".equals(e.getMessage())
                        ?PipelineFailure.Phase.DDL:PipelineFailure.Phase.SOURCE_DECODE;
                throw new PipelineFailure(phase,e);
            }catch(SQLException e){throw new PipelineFailure(PipelineFailure.Phase.SOURCE_READ,e);}
        }

        void persist(SourceTransaction transaction)throws Exception{
            if(closing)throw new InterruptedException("CAPTURE_DELIVERY_CANCELLED");
            receiver.committed(transaction);
            durable=transaction.position;
        }

        public void check()throws Exception{
            Throwable observed=failure.get();
            if(observed instanceof Exception)throw (Exception)observed;
            if(observed!=null)throw invalid("ORACLE_CAPTURE_FAILED");
        }

        public void close()throws Exception{
            closing=true;
            receiver.cancelled();
            reader.interrupt();
            reader.join(5000);
        }
    }

    @Override public void requireSeparateDatabase(String driver,String url,String username,String password){
        // The admitted PostgreSQL target and both permitted journal engines use other source protocols.
    }

    @Override public void close(){}

    private static IllegalStateException invalid(String code){return new IllegalStateException(code);}
}
