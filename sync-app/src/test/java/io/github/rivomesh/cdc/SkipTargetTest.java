package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.recovery.*;
import java.nio.file.Path;
import java.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SkipTargetTest {
    @TempDir Path directory;
    SkipJournalTest setup;
    FailureCheckpointInspectionTest fixture;
    SkipPlan plan;
    @BeforeEach void prepare()throws Exception{setup=new SkipJournalTest();setup.directory=directory;setup.prepare();fixture=setup.fixture;plan=setup.plan;setup.prepareIntent();try(JdbcSink sink=JdbcSink.forSkipRecovery(fixture.target)){sink.prepareSkipMetadata(plan);}}
    @AfterEach void close()throws Exception{if(setup!=null)setup.close();}
    static void code(String expected,org.junit.jupiter.api.function.Executable action){FailureCheckpointInspectionTest.code(expected,action);}
    String metadata(String suffix){return JdbcSinkTest.q(fixture.target,fixture.target.metadataSchema)+"."+JdbcSinkTest.q(fixture.target,"rm3_sink_"+suffix);}
    void sql(String query)throws Exception{try(Connection c=JdbcConnections.open(fixture.target.driver,fixture.target.url,fixture.target.username,fixture.target.password);Statement s=c.createStatement()){s.execute(query);}}
    long receipts()throws Exception{try(Connection c=JdbcConnections.open(fixture.target.driver,fixture.target.url,fixture.target.username,fixture.target.password);Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+metadata("skip"))){assertTrue(r.next());return r.getLong(1);}}
    SkipReceipt skip()throws Exception{try(JdbcSink sink=JdbcSink.forSkipRecovery(fixture.target)){return sink.skip(plan,fixture.retained.value);}}
    @Test void nativeSkipCommitsReceiptAndMarkedProgressWithoutWritingBusinessRows()throws Exception{
        SkipReceipt receipt=skip();receipt.validate(plan);assertEquals("SKIPPED",receipt.outcome);assertEquals(0,JdbcSinkTest.count(fixture.target,"items"));assertEquals(1,receipts());assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);
        try(JdbcSink sink=JdbcSink.existing(fixture.target)){SinkAdapter.Checkpoint cp=sink.checkpoint();assertEquals(1,cp.sequence);assertEquals(plan.id,cp.skipOperation);assertEquals(receipt.hash(),cp.skipHash);}
        assertEquals(receipt.hash(),skip().hash());assertEquals(1,receipts());assertEquals(0,JdbcSinkTest.count(fixture.target,"items"));
    }
    @Test void nativeFailureAfterReceiptInsertRollsBackBothReceiptAndCheckpoint()throws Exception{
        sql("ALTER TABLE "+metadata("job")+" ADD CONSTRAINT reject_skip_progress CHECK (stream_seq=0)");assertThrows(SQLException.class,()->skip());assertEquals(0,receipts());
        try(JdbcSink sink=JdbcSink.existing(fixture.target)){assertEquals(0,sink.checkpoint().sequence);assertNull(sink.checkpoint().skipOperation);}
        sql("ALTER TABLE "+metadata("job")+(fixture.target.product==TargetProduct.MYSQL?" DROP CHECK ":" DROP CONSTRAINT ")+"reject_skip_progress");assertEquals(1,skip().sequence);
    }
    @Test void nativeCommitThenFailedJournalAckReplaysTheSameReceiptAfterReopen()throws Exception{
        SkipReceipt receipt=skip();JdbcJournal j=fixture.journal;FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+" ADD CONSTRAINT reject_skipped_ack CHECK (acknowledged=0)");assertThrows(SQLException.class,()->setup.complete(receipt));assertEquals(0,j.subscriptions().get(0).acknowledged);assertEquals(0,setup.gaps());
        FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+(j.settings.driver.startsWith("com.mysql")?" DROP CHECK ":" DROP CONSTRAINT ")+"reject_skipped_ack");JdbcJournal.Settings settings=j.settings;j.close();fixture.journal=new JdbcJournal(settings);
        assertEquals(receipt.hash(),skip().hash());assertEquals(receipt.hash(),setup.complete(skip()).receipt.hash());assertEquals(1,setup.gaps());assertEquals(0,JdbcSinkTest.count(fixture.target,"items"));
    }
    @Test void normalApplyAndCommitConfirmationCannotRelabelTheSkippedHeadAsApplied()throws Exception{
        skip();try(JdbcSink sink=JdbcSink.existing(fixture.target)){code("SINK_TRANSACTION_WAS_SKIPPED",()->sink.apply(plan.epoch,plan.sequence,fixture.normalized));}
        code("FAILURE_CHECKPOINT_SKIP_RECOVERY_REQUIRED",()->fixture.inspect());code("FAILURE_ACK_COMMIT_NOT_PROVEN",()->setup.setup.confirm());assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);assertEquals(0,setup.gaps());
    }
    @Test void committedProofHoldsNativeLockWhileJournalGapAndAckAreCommitted()throws Exception{
        SkipReceipt original=skip();try(Connection c=fixture.journal.connect();JdbcSink sink=JdbcSink.forSkipRecovery(fixture.target)){
            c.setAutoCommit(false);fixture.journal.locked(c);JdbcSkipJournal.Stored complete=sink.resolveSkip(plan,true,receipt->{JdbcSkipJournal.Stored result=JdbcSkipJournal.complete(fixture.journal,c,plan,receipt,fixture.retained.value,SkipResolution.create(plan.id,"recover_request_1234","RECOVER","another-admin","Recovered the already committed native SKIP receipt"));c.commit();return result;});assertEquals(original.hash(),complete.receipt.hash());assertEquals("another-admin",complete.resolution.actor);
        }assertEquals(1,fixture.journal.subscriptions().get(0).acknowledged);assertEquals(1,setup.gaps());
    }
    @Test void cancellationRequiresNativeAbsenceAndCannotCancelAnAlreadyCommittedSkip()throws Exception{
        try(Connection c=fixture.journal.connect();JdbcSink sink=JdbcSink.forSkipRecovery(fixture.target)){
            c.setAutoCommit(false);fixture.journal.locked(c);sink.resolveSkip(plan,false,receipt->{assertNull(receipt);JdbcSkipJournal.cancel(fixture.journal,c,plan,fixture.retained.value,SkipResolution.create(plan.id,"cancel_request_1234","CANCEL","admin","Verified absence and cancelled the uncommitted operation"));c.commit();return null;});
        }assertEquals(0,receipts());assertEquals(0,setup.gaps());assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);
    }
    @Test void cancellationOfCommittedSkipRefusesAndPreservesPendingRecovery()throws Exception{
        skip();try(JdbcSink sink=JdbcSink.forSkipRecovery(fixture.target)){code("SKIP_ALREADY_COMMITTED",()->sink.resolveSkip(plan,false,r->null));}assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);code("SKIP_RECOVERY_PENDING",()->fixture.journal.resume("target",1));
    }
    @Test void missingNativeReceiptTableIsNotRecreatedOnceRecoveryColumnsExist()throws Exception{
        sql("DROP TABLE "+metadata("skip"));try(JdbcSink sink=JdbcSink.forSkipRecovery(fixture.target)){code("SINK_SKIP_METADATA_LOST",()->sink.prepareSkipMetadata(plan));}
        try(Connection c=JdbcConnections.open(fixture.target.driver,fixture.target.url,fixture.target.username,fixture.target.password);Statement s=c.createStatement()){assertThrows(SQLException.class,()->s.executeQuery("SELECT COUNT(*) FROM "+metadata("skip")));}
    }
    @Test void interruptedColumnMigrationCanFinishButOrdinaryOpensRefusePartialMetadata()throws Exception{
        sql("ALTER TABLE "+metadata("job")+" DROP COLUMN "+JdbcSinkTest.q(fixture.target,"skip_hash"));code("SINK_RECOVERY_METADATA_INCOMPLETE",()->JdbcSink.existing(fixture.target));
        try(JdbcSink sink=JdbcSink.forSkipRecovery(fixture.target)){sink.prepareSkipMetadata(plan);assertNull(sink.checkpoint().skipOperation);}assertEquals(1,skip().sequence);
    }
    @Test void confirmedGapIsCheckedBeforeNormalDeliveryContinuesAndMarkersClearOnNextApply()throws Exception{
        SkipReceipt receipt=skip();setup.complete(receipt);JdbcJournal j=fixture.journal;j.resumePipeline(j.pausePipeline().errorGeneration);Journal.Lease lease=j.claim("new-owner",30000);j.resume("target",2);
        try(JdbcSink sink=JdbcSink.existing(fixture.target)){
            sink.prepare(lease.epoch,fixture.table); // Fence first, as a new target runner does.
        }catch(IllegalStateException fenced){assertEquals("SINK_FENCED",fenced.getMessage());}
        try(JdbcSink sink=JdbcSink.existing(fixture.target)){
            sink.activate(lease.epoch);sink.prepare(lease.epoch,fixture.table);j.verifySkippedCheckpoint("target",sink.checkpoint());SourceTransaction next=JdbcSinkTest.transaction(2,JdbcSinkTest.insert(fixture.table,2,"after-gap"));j.append(lease,next);
            FanoutDispatcher dispatcher=new FanoutDispatcher(j,lease,java.util.Collections.singletonList(sink),java.util.Collections.singletonMap(fixture.table.id,fixture.table),r->r.transaction);assertEquals(1,dispatcher.drain(10));assertEquals(2,j.subscriptions().get(0).acknowledged);assertNull(sink.checkpoint().skipOperation);assertEquals(1,JdbcSinkTest.count(fixture.target,"items"));assertEquals(1,setup.gaps());
        }
    }
}
