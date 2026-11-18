package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.api.SourcePosition;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class JournalTest {
    @TempDir Path directory;
    JdbcJournal.Settings settings(){JdbcJournal.Settings s=new JdbcJournal.Settings();s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0;LOCK_TIMEOUT=3000";s.pipelineId="pipeline";return s;}
    SourceTransaction transaction(int ordinal){
        SourceTransaction tx=new SourceTransaction();tx.id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:"+ordinal;tx.streamGeneration="generation";
        Map<String,String> position=new TreeMap<>();position.put("gtidSet","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-"+ordinal);
        tx.position=new SourcePosition(1,"MYSQL","source",position);
        Change change=new Change();change.eventId=tx.id+":0";change.tableId="table";change.operation=Change.Operation.INSERT;
        change.after=Arrays.asList(Cell.value("18446744073709551615"),Cell.value("12345678901234567890.123456789"),Cell.value(null),Cell.missing(),Cell.value("{\"large\":18446744073709551615}"));tx.changes.add(change);return tx;
    }
    void initialize(JdbcJournal journal)throws Exception{journal.initialize(Codec.sha256("config"),"generation",Arrays.asList("fast","slow"));}
    TableSchema schema(){TableSchema schema=new TableSchema();schema.id="table";schema.database="db";schema.name="items";TableSchema.Column column=new TableSchema.Column();column.name="value";column.type=TableSchema.Type.STRING;schema.columns.add(column);return schema;}

    @Test void durableFanoutNeverCollectsAnOfflineSubscribersHistory()throws Exception{
        JdbcJournal.Settings settings=settings();
        try(JdbcJournal journal=new JdbcJournal(settings)){
            initialize(journal);Journal.Lease lease=journal.claim("first-owner",30000);
            assertEquals(1,journal.append(lease,transaction(1)));assertEquals(2,journal.append(lease,transaction(2)));
            assertThrows(IllegalStateException.class,()->journal.acknowledge(lease,"fast",2));
            journal.acknowledge(lease,"fast",1);journal.acknowledge(lease,"fast",2);journal.pause(lease,"slow","SQL_ERROR");
            assertTrue(journal.read("slow",10).isEmpty());assertEquals(0,journal.collect(lease,100));
            assertThrows(IllegalStateException.class,()->journal.resume("slow",0));journal.resume("slow",1);journal.resume("slow",1);
            List<Journal.Record> pending=journal.read("slow",10);assertEquals(2,pending.size());
            assertTrue(pending.get(0).transaction.changes.get(0).after.get(2).present);assertNull(pending.get(0).transaction.changes.get(0).after.get(2).value);
            assertFalse(pending.get(0).transaction.changes.get(0).after.get(3).present);
            assertEquals("12345678901234567890.123456789",pending.get(0).transaction.changes.get(0).after.get(1).value);
            journal.acknowledge(lease,"slow",1);assertEquals(1,journal.collect(lease,100));
            assertEquals(1,journal.append(lease,transaction(1)),"GC must not permit transaction replay to create a second log entry");
            SourceTransaction changed=transaction(1);changed.changes.get(0).after.set(0,Cell.value("different"));assertThrows(IllegalStateException.class,()->journal.append(lease,changed));
            journal.release(lease);
        }
        try(JdbcJournal restored=new JdbcJournal(settings)){
            initialize(restored);Journal.Lease lease=restored.claim("second-owner",30000);
            assertEquals(transaction(2).position,restored.captured());assertEquals(2,restored.read("slow",1).get(0).sequence);
            restored.acknowledge(lease,"slow",2);assertEquals(1,restored.collect(lease,100));assertEquals(0,((Number)restored.status().get("pendingBytes")).longValue());
        }
    }
    @Test void ownerEpochsFenceOldAppendAndAcknowledgmentAfterTakeover()throws Exception{
        JdbcJournal.Settings settings=settings();
        try(JdbcJournal a=new JdbcJournal(settings);JdbcJournal b=new JdbcJournal(settings)){
            initialize(a);initialize(b);Journal.Lease old=a.claim("old",30000);
            assertThrows(JdbcJournal.Busy.class,()->b.claim("new",30000));a.append(old,transaction(1));
            try(Connection c=JdbcConnections.open(settings.driver,settings.url,"","");Statement s=c.createStatement()){s.executeUpdate("UPDATE \"rivomesh_v3\".\"rm3_job\" SET lease_until=0");}
            Journal.Lease next=b.claim("new",30000);assertTrue(next.epoch>old.epoch);
            assertThrows(JdbcJournal.Fenced.class,()->a.append(old,transaction(2)));
            assertThrows(JdbcJournal.Fenced.class,()->a.acknowledge(old,"fast",1));
            a.release(old);b.heartbeat(next,30000);assertEquals(1,b.read("fast",10).size());
        }
    }
    @Test void capacityFailureNeverAdvancesTheCapturedPosition()throws Exception{
        JdbcJournal.Settings settings=settings();settings.maxPendingBytes=Codec.encode(transaction(1)).getBytes(java.nio.charset.StandardCharsets.UTF_8).length+20;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            initialize(journal);Journal.Lease lease=journal.claim("owner",30000);journal.append(lease,transaction(1));
            assertThrows(JdbcJournal.Capacity.class,()->journal.append(lease,transaction(2)));assertEquals(transaction(1).position,journal.captured());
            journal.acknowledge(lease,"fast",1);journal.acknowledge(lease,"slow",1);journal.collect(lease,1);
            assertEquals(2,journal.append(lease,transaction(2)));
        }
    }
    @Test void retryDiagnosticsCannotResumeAPauseAdvanceCheckpointsOrBypassFencing()throws Exception{
        try(JdbcJournal journal=new JdbcJournal(settings())){
            initialize(journal);Journal.Lease lease=journal.claim("owner",30000);journal.retrying(lease,"fast","TARGET_CONNECTION_RETRY");Journal.Subscription sub=journal.subscriptions().get(0);assertEquals("RUNNING",sub.state);assertEquals(0,sub.acknowledged);assertEquals(0,sub.errorGeneration);assertEquals("TARGET_CONNECTION_RETRY",sub.errorCode);
            journal.retrying(lease,"fast",null);assertNull(journal.subscriptions().get(0).errorCode);journal.pause(lease,"fast","SQL_23514");journal.retrying(lease,"fast",null);sub=journal.subscriptions().get(0);assertEquals("PAUSED",sub.state);assertEquals("SQL_23514",sub.errorCode);assertEquals(1,sub.errorGeneration);
            journal.resume("fast",1);journal.release(lease);Journal.Lease next=journal.claim("new",30000);assertThrows(JdbcJournal.Fenced.class,()->journal.retrying(lease,"fast","TARGET_CONNECTION_RETRY"));journal.retrying(next,"fast",null);assertEquals(1,journal.subscriptions().get(0).errorGeneration);
        }
    }
    @Test void schemasAndSubscriptionsCannotSilentlyChangeIdentity()throws Exception{
        try(JdbcJournal journal=new JdbcJournal(settings())){
            initialize(journal);Journal.Lease lease=journal.claim("owner",30000);
            TableSchema schema=new TableSchema();schema.id="table";schema.database="db";schema.name="items";
            TableSchema.Column column=new TableSchema.Column();column.name="id";column.type=TableSchema.Type.UINT64;column.nullable=false;schema.columns.add(column);schema.primaryKey.add("id");
            journal.saveSchema(lease,schema);journal.saveSchema(lease,schema);assertEquals(1,journal.schemas().size());
            column.type=TableSchema.Type.STRING;assertThrows(IllegalStateException.class,()->journal.saveSchema(lease,schema));
            assertThrows(IllegalStateException.class,()->journal.initialize(Codec.sha256("other"),"generation",Arrays.asList("fast","slow")));
            assertThrows(IllegalStateException.class,()->journal.verifyIdentity(Codec.sha256("other"),Arrays.asList("fast","slow")));journal.verifyIdentity(Codec.sha256("config"),Arrays.asList("fast","slow"));
            assertThrows(IllegalStateException.class,()->journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("fast")));
        }
    }
    @Test void concurrentProducersUseTheLockedStreamCounterRatherThanDatabaseSequences()throws Exception{
        try(JdbcJournal journal=new JdbcJournal(settings())){
            initialize(journal);Journal.Lease lease=journal.claim("owner",30000);ExecutorService pool=Executors.newFixedThreadPool(4);
            try{
                List<Future<Long>> results=new ArrayList<>();
                for(int i=1;i<=16;i++){final int ordinal=i;results.add(pool.submit(()->{SourceTransaction tx=transaction(ordinal);tx.kind=SourceTransaction.Kind.SNAPSHOT;return journal.append(lease,tx);}));}
                Set<Long> sequences=new TreeSet<>();for(Future<Long> future:results)sequences.add(future.get(10,TimeUnit.SECONDS));
                assertEquals(16,sequences.size());assertEquals(1L,Collections.min(sequences));assertEquals(16L,Collections.max(sequences));assertNull(journal.captured(),"Snapshot records must never masquerade as captured binlog commits");
            }finally{pool.shutdownNow();}
        }
    }
    @Test void completedSnapshotArtifactsResumeWithDuplicateRowsAndIncompleteSessionsRestartSafely()throws Exception{
        JdbcJournal.Settings settings=settings();Journal.SnapshotState unfinished,complete;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            initialize(journal);Journal.Lease lease=journal.claim("first",30000);journal.baseline(lease,transaction(1).position);
            complete=journal.beginSnapshot(lease,"source",schema(),transaction(1).position);
            List<List<Cell>> duplicateRows=Arrays.asList(Collections.singletonList(Cell.value("same")),Collections.singletonList(Cell.value("same")));
            journal.appendSnapshot(lease,complete,0,duplicateRows);journal.appendSnapshot(lease,complete,0,duplicateRows);assertEquals(2,complete.rows);
            assertThrows(IllegalStateException.class,()->journal.snapshotParts("source","table",complete.snapshotId,0,1));
            assertThrows(IllegalStateException.class,()->journal.appendSnapshot(lease,complete,2,duplicateRows));journal.finishSnapshot(lease,complete);
            assertEquals(2,journal.snapshotParts("source","table",complete.snapshotId,0,1).get(0).rows.size());
            TableSchema second=schema();second.id="second";second.name="other";unfinished=journal.beginSnapshot(lease,"source",second,transaction(1).position);
            journal.appendSnapshot(lease,unfinished,0,Collections.singletonList(Collections.singletonList(Cell.value(null))));journal.release(lease);
        }
        try(JdbcJournal journal=new JdbcJournal(settings)){
            initialize(journal);Journal.Lease lease=journal.claim("second",30000);
            assertEquals(complete.snapshotId,journal.beginSnapshot(lease,"source",schema(),transaction(2).position).snapshotId);
            TableSchema second=schema();second.id="second";second.name="other";Journal.SnapshotState restarted=journal.beginSnapshot(lease,"source",second,transaction(2).position);
            assertNotEquals(unfinished.snapshotId,restarted.snapshotId);assertEquals(0,restarted.rows);
            assertThrows(IllegalStateException.class,()->journal.appendSnapshot(lease,unfinished,1,Collections.singletonList(Collections.singletonList(Cell.value("stale")))));
            journal.finishSnapshot(lease,restarted);assertTrue(journal.snapshotParts("source","second",restarted.snapshotId,0,1).isEmpty());
            assertEquals(transaction(1).position,journal.captured(),"A later snapshot baseline is a dependency, not captured CDC progress");
            assertTrue(journal.dependencies().contains(transaction(2).position));
        }
    }
    @Test void snapshotCapacityAndManifestsAreCheckedBeforeCommit()throws Exception{
        JdbcJournal.Settings settings=settings();settings.maxPendingBytes=100;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            initialize(journal);Journal.Lease lease=journal.claim("owner",30000);Journal.SnapshotState state=journal.beginSnapshot(lease,"source",schema(),transaction(1).position);
            assertThrows(JdbcJournal.Capacity.class,()->journal.appendSnapshot(lease,state,0,Collections.singletonList(Collections.singletonList(Cell.value(String.join("",Collections.nCopies(200,"x")))))));
            assertEquals(0,journal.snapshot("source","table").rows);assertEquals(0,((Number)journal.status().get("snapshotBytes")).longValue());
            journal.appendSnapshot(lease,state,0,Collections.singletonList(Collections.singletonList(Cell.value("ok"))));
            Journal.SnapshotState stale=Codec.decode(Codec.encode(state),Journal.SnapshotState.class);stale.rows=0;
            assertThrows(IllegalStateException.class,()->journal.finishSnapshot(lease,stale));journal.finishSnapshot(lease,state);
        }
    }
    @Test void missingLogOrSnapshotPartsAreDetectedRatherThanAcknowledgedPast()throws Exception{
        JdbcJournal.Settings settings=settings();
        try(JdbcJournal journal=new JdbcJournal(settings)){
            initialize(journal);Journal.Lease lease=journal.claim("owner",30000);
            journal.append(lease,transaction(1));journal.append(lease,transaction(2));journal.append(lease,transaction(3));journal.acknowledge(lease,"fast",1);
            Journal.SnapshotState state=journal.beginSnapshot(lease,"source",schema(),transaction(1).position);
            journal.appendSnapshot(lease,state,0,Collections.singletonList(Collections.singletonList(Cell.value("first"))));
            journal.appendSnapshot(lease,state,1,Collections.singletonList(Collections.singletonList(Cell.value("second"))));journal.finishSnapshot(lease,state);
            try(Connection c=JdbcConnections.open(settings.driver,settings.url,"","");Statement s=c.createStatement()){
                s.executeUpdate("DELETE FROM \"rivomesh_v3\".\"rm3_log\" WHERE stream_seq=2");
                s.executeUpdate("DELETE FROM \"rivomesh_v3\".\"rm3_snapshot_part\" WHERE part_no=0");
            }
            assertThrows(IllegalStateException.class,()->journal.read("fast",10));
            assertThrows(IllegalStateException.class,()->journal.acknowledge(lease,"fast",3));
            assertThrows(IllegalStateException.class,()->journal.snapshotParts("source","table",state.snapshotId,0,10));
        }
    }
    @Test void sharedPipelinePauseAndSourceErrorsCannotBeBypassedByAnotherOwner()throws Exception{
        try(JdbcJournal journal=new JdbcJournal(settings())){
            initialize(journal);Journal.Lease old=journal.claim("old",30000);journal.append(old,transaction(1));Journal.PipelineState paused=journal.pausePipeline();assertTrue(paused.epoch>old.epoch);assertEquals("PAUSED",paused.state);
            assertThrows(JdbcJournal.Fenced.class,()->journal.append(old,transaction(2)));assertThrows(JdbcJournal.PipelinePaused.class,()->journal.claim("standby",30000));
            assertThrows(IllegalStateException.class,()->journal.resumePipeline(paused.errorGeneration-1));assertEquals(paused.errorGeneration,journal.pausePipeline().errorGeneration);
            journal.resumePipeline(paused.errorGeneration);journal.resumePipeline(paused.errorGeneration);Journal.Lease next=journal.claim("standby",30000);journal.failPipeline(next,"DDL_PLAN_REQUIRED",PipelineFailure.Phase.DDL);
            assertEquals("ERROR",journal.status().get("run_state"));assertEquals("DDL_PLAN_REQUIRED",journal.status().get("errorCode"));assertEquals("DDL",journal.status().get("errorStage"));assertThrows(JdbcJournal.PipelinePaused.class,()->journal.claim("another",30000));assertEquals(transaction(1).position,journal.captured());
        }
    }
    @Test void snapshotPreviewSeparatesSourceArtifactsFromEveryTargetsDurableAcknowledgments()throws Exception{
        try(JdbcJournal journal=new JdbcJournal(settings())){
            initialize(journal);Journal.Lease lease=journal.claim("owner",30000);TableSchema table=schema();journal.saveSchema(lease,table);Journal.SnapshotState snapshot=journal.beginSnapshot(lease,"source",table,transaction(1).position);journal.appendSnapshot(lease,snapshot,0,Collections.singletonList(Collections.singletonList(Cell.value("prepared"))));journal.finishSnapshot(lease,snapshot);
            Map<String,Object> preview=journal.snapshotSummary(1);assertEquals(1L,preview.get("completed_tables"));assertTrue(((Map<?,?>)preview.get("snapshotAcknowledgments")).isEmpty());
            journal.acknowledgeSnapshot(lease,"fast",snapshot);assertEquals(Collections.singletonMap("fast",1L),journal.snapshotSummary(1).get("snapshotAcknowledgments"));assertEquals(0,journal.collectSnapshots(lease,100));journal.acknowledgeSnapshot(lease,"slow",snapshot);assertEquals(1,journal.collectSnapshots(lease,100));Map<?,?> confirmations=(Map<?,?>)journal.snapshotSummary(1).get("snapshotAcknowledgments");assertEquals(1L,confirmations.get("fast"));assertEquals(1L,confirmations.get("slow"));
        }
    }
}
