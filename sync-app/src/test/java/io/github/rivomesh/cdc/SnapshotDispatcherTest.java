package io.github.rivomesh.cdc;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotDispatcherTest {
    @TempDir Path directory;
    JdbcJournal journal()throws Exception{JournalTest fixture=new JournalTest();fixture.directory=directory;return new JdbcJournal(fixture.settings());}
    JdbcSink.Settings target(){JdbcSinkTest fixture=new JdbcSinkTest();fixture.directory=directory;return fixture.settings();}
    static final class FaultSink implements SinkAdapter {
        final JdbcSink sink;boolean prepareFailure,lostPartReply;List<Integer> parts=new ArrayList<>();
        FaultSink(JdbcSink sink){this.sink=sink;}
        public String id(){return sink.id();}public boolean includes(TableSchema table){return sink.includes(table);}public Consistency consistency(){return sink.consistency();}
        public Checkpoint activate(long epoch)throws Exception{return sink.activate(epoch);}public Checkpoint checkpoint()throws Exception{return sink.checkpoint();}
        public void prepare(long epoch,TableSchema table)throws Exception{if(prepareFailure)throw new SQLException("permission denied","42501");sink.prepare(epoch,table);}
        public void apply(long epoch,long sequence,SourceTransaction transaction)throws Exception{sink.apply(epoch,sequence,transaction);}
        public SnapshotCheckpoint beginSnapshot(long epoch,TableSchema table,Journal.SnapshotState state)throws Exception{return sink.beginSnapshot(epoch,table,state);}
        public void snapshotPart(long epoch,TableSchema table,String id,Journal.SnapshotPart part)throws Exception{parts.add(part.part);sink.snapshotPart(epoch,table,id,part);if(lostPartReply){lostPartReply=false;throw new SQLException("reply lost","08006");}}
        public void finishSnapshot(long epoch,TableSchema table,Journal.SnapshotState state)throws Exception{sink.finishSnapshot(epoch,table,state);}
        public Map<String,Object> diagnostics()throws Exception{return sink.diagnostics();}public void close()throws Exception{sink.close();}
    }
    static Journal.SnapshotState snapshot(JdbcJournal j,Journal.Lease lease,TableSchema table)throws Exception{
        Journal.SnapshotState state=j.beginSnapshot(lease,"source",table,JdbcSinkTest.transaction(1).position);
        j.appendSnapshot(lease,state,0,Collections.singletonList(JdbcSinkTest.row(1,"first")));j.appendSnapshot(lease,state,1,Collections.singletonList(JdbcSinkTest.row(2,"second")));j.finishSnapshot(lease,state);return state;
    }
    static void prepare(JdbcJournal j,Journal.Lease lease,SinkAdapter sink,TableSchema table,Journal.SnapshotState snapshot,SubscriberFailure phase)throws Exception{
        SnapshotDispatcher.prepare(j,lease,sink,Collections.singletonList(table),Collections.singletonMap(table.id,snapshot),()->{},phase);
    }
    @Test void prepareFailurePersistsAsPreparationWithoutInventingAnIncrementalSequence()throws Exception{
        TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal j=journal();FaultSink sink=new FaultSink(new JdbcSink(target()))){
            j.initialize(Codec.sha256("config"),"generation",Collections.singletonList("target"));Journal.Lease lease=j.claim("owner",30000);Journal.SnapshotState state=snapshot(j,lease,table);
            SubscriberFailure phase=new SubscriberFailure();phase.sequence=7;phase.transactionId="old";phase.projectedHash=Codec.sha256("old");sink.prepareFailure=true;
            assertThrows(SQLException.class,()->prepare(j,lease,sink,table,state,phase));assertEquals(SubscriberFailure.Phase.TARGET_PREPARE,phase.phase);assertEquals(0,phase.sequence);assertNull(phase.transactionId);assertNull(phase.projectedHash);
            j.pauseFailure(lease,"target","SQL_42501",phase);Map<String,Object> summary=j.subscriptionDiagnostics().get(0);assertEquals("TARGET_PREPARE",summary.get("failurePhase"));assertFalse(summary.containsKey("failedSequence"));assertEquals(0,sink.checkpoint().sequence);
        }
    }
    @Test void lostPartReplyResumesAtNativeSnapshotCheckpointWithoutDuplicateRows()throws Exception{
        TableSchema table=JdbcSinkTest.schema(false);JdbcSink.Settings settings=target();
        try(JdbcJournal j=journal();FaultSink sink=new FaultSink(new JdbcSink(settings))){
            j.initialize(Codec.sha256("config"),"generation",Collections.singletonList("target"));Journal.Lease lease=j.claim("owner",30000);Journal.SnapshotState state=snapshot(j,lease,table);SubscriberFailure phase=new SubscriberFailure();sink.lostPartReply=true;
            assertThrows(SQLException.class,()->prepare(j,lease,sink,table,state,phase));assertEquals(SubscriberFailure.Phase.TARGET_SNAPSHOT,phase.phase);assertEquals(1,JdbcSinkTest.count(settings,"items"));
            prepare(j,lease,sink,table,state,new SubscriberFailure());assertEquals(Arrays.asList(0,1),sink.parts);assertEquals(2,JdbcSinkTest.count(settings,"items"));assertEquals(0,sink.checkpoint().sequence);
            assertEquals(2,j.collectSnapshots(lease,100));
        }
    }
    @Test void corruptedSnapshotIsAJournalFailureNotATargetSqlFailure()throws Exception{
        TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal j=journal();FaultSink sink=new FaultSink(new JdbcSink(target()))){
            j.initialize(Codec.sha256("config"),"generation",Collections.singletonList("target"));Journal.Lease lease=j.claim("owner",30000);Journal.SnapshotState state=snapshot(j,lease,table);SubscriberFailure phase=new SubscriberFailure();
            try(Connection c=j.connect()){JdbcJournal.execute(c,"UPDATE "+j.table("snapshot_part")+" SET payload_hash=?",Codec.sha256("corrupt"));}
            assertThrows(FanoutDispatcher.JournalFailure.class,()->prepare(j,lease,sink,table,state,phase));assertEquals(SubscriberFailure.Phase.JOURNAL_READ,phase.phase);assertEquals("RUNNING",j.subscriptions().get(0).state);assertTrue(sink.parts.isEmpty());
        }
    }
    @Test void journalAckFailureAfterNativeSnapshotCommitRemainsRecoverable()throws Exception{
        TableSchema table=JdbcSinkTest.schema(false);JdbcSink.Settings settings=target();
        try(JdbcJournal j=journal();FaultSink sink=new FaultSink(new JdbcSink(settings))){
            j.initialize(Codec.sha256("config"),"generation",Collections.singletonList("target"));Journal.Lease lease=j.claim("owner",30000);Journal.SnapshotState state=snapshot(j,lease,table);SubscriberFailure phase=new SubscriberFailure();
            try(Connection c=j.connect()){JdbcJournal.execute(c,"ALTER TABLE "+j.table("snapshot_ack")+" ADD CONSTRAINT reject_snapshot_ack CHECK(subscription_id='other')");}
            assertThrows(FanoutDispatcher.JournalFailure.class,()->prepare(j,lease,sink,table,state,phase));assertEquals(SubscriberFailure.Phase.JOURNAL_ACK,phase.phase);assertEquals(2,JdbcSinkTest.count(settings,"items"));assertEquals("RUNNING",j.subscriptions().get(0).state);
            try(Connection c=j.connect()){JdbcJournal.execute(c,"ALTER TABLE "+j.table("snapshot_ack")+" DROP CONSTRAINT reject_snapshot_ack");}
            prepare(j,lease,sink,table,state,new SubscriberFailure());assertEquals(Arrays.asList(0,1),sink.parts);assertEquals(2,JdbcSinkTest.count(settings,"items"));assertEquals(2,j.collectSnapshots(lease,100));
        }
    }
    @Test void healthOrLeaseLossCannotBeConvertedIntoDestinationPause()throws Exception{
        TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal j=journal();FaultSink sink=new FaultSink(new JdbcSink(target()))){
            j.initialize(Codec.sha256("config"),"generation",Collections.singletonList("target"));Journal.Lease lease=j.claim("owner",30000);Journal.SnapshotState state=snapshot(j,lease,table);
            assertThrows(JdbcJournal.Fenced.class,()->SnapshotDispatcher.prepare(j,lease,sink,Collections.singletonList(table),Collections.singletonMap(table.id,state),()->{throw new JdbcJournal.Fenced();},new SubscriberFailure()));
            assertEquals("RUNNING",j.subscriptions().get(0).state);assertTrue(sink.parts.isEmpty());
        }
    }
}
