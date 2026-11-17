package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.*;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Journal contracts use a synthetic native receipt; native SKIP execution is tested separately. */
class SkipJournalTest {
    @TempDir Path directory;
    CommitAcknowledgementTest setup;
    FailureCheckpointInspectionTest fixture;
    SkipPlan plan;
    @BeforeEach void prepare()throws Exception{
        setup=new CommitAcknowledgementTest();setup.directory=directory;setup.prepare();fixture=setup.fixture;setup.pauseAndRetain(true);
        plan=SkipPlan.create(fixture.retained,"skip_request_12345678","admin","Reviewed complete transaction;accept the explicit gap",fixture.normalized);
    }
    @AfterEach void close()throws Exception{if(setup!=null)setup.close();}
    void prepareIntent()throws Exception{
        JdbcSkipJournal.ensureMetadata(fixture.journal,fixture.retained.value);
        try(Connection c=fixture.journal.connect()){c.setAutoCommit(false);JdbcSkipJournal.prepare(fixture.journal,c,plan,fixture.retained.value);c.commit();}
    }
    SkipReceipt receipt(){SkipReceipt r=new SkipReceipt();r.operationId=plan.id;r.planHash=plan.hash();r.pipelineId=plan.pipelineId;r.subscriptionId=plan.subscriptionId;r.generation=plan.generation;r.fingerprint=plan.fingerprint;r.transactionId=plan.transactionId;r.projectedHash=plan.projectedHash;r.epoch=plan.epoch;r.sequence=plan.sequence;r.committedAt=Instant.now().toString();return r;}
    JdbcSkipJournal.Stored complete(SkipReceipt receipt)throws Exception{
        try(Connection c=fixture.journal.connect()){c.setAutoCommit(false);JdbcSkipJournal.Stored result=JdbcSkipJournal.complete(fixture.journal,c,plan,receipt,fixture.retained.value,SkipResolution.create(plan.id,"resolution_12345678","SKIP_COMMITTED","admin",plan.reason));c.commit();return result;}
    }
    static void code(String expected,org.junit.jupiter.api.function.Executable action){FailureCheckpointInspectionTest.code(expected,action);}
    long gaps()throws Exception{try(Connection c=fixture.journal.connect();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+fixture.journal.table("gap"))){assertTrue(r.next());return r.getLong(1);}}
    @Test void intentAndFormatUpgradeCommitTogetherAndBlockResumeWithoutAcknowledging()throws Exception{
        prepareIntent();JdbcJournal j=fixture.journal;assertEquals(4,j.status().get("dataFormat"));assertEquals(false,j.status().get("hasGaps"));assertEquals(0,j.subscriptions().get(0).acknowledged);
        code("SKIP_RECOVERY_PENDING",()->j.resumePipeline(j.pausePipeline().errorGeneration));code("SKIP_RECOVERY_PENDING",()->j.resume("target",1));
        try(Connection c=j.connect()){JdbcSkipJournal.Stored saved=JdbcSkipJournal.load(j,c,plan.id);assertEquals(plan.hash(),saved.hash);assertEquals("PREPARED",saved.state);}
    }
    @Test void rolledBackPrepareDoesNotUpgradeFormatOrLeaveAnIntent()throws Exception{
        JdbcSkipJournal.ensureMetadata(fixture.journal,fixture.retained.value);
        try(Connection c=fixture.journal.connect()){c.setAutoCommit(false);JdbcSkipJournal.prepare(fixture.journal,c,plan,fixture.retained.value);c.rollback();}
        assertEquals(3,fixture.journal.status().get("dataFormat"));try(Connection c=fixture.journal.connect()){assertNull(JdbcSkipJournal.load(fixture.journal,c,plan.id));}
    }
    @Test void gapAckResolutionAndNewErrorGenerationCommitTogetherAndRemainPaused()throws Exception{
        prepareIntent();SkipReceipt receipt=receipt();JdbcSkipJournal.Stored stored=complete(receipt);assertEquals("COMPLETED",stored.state);assertEquals("SKIPPED",stored.receipt.outcome);assertEquals("admin",stored.resolution.actor);assertEquals(1,gaps());assertEquals(true,fixture.journal.status().get("hasGaps"));
        Journal.Subscription sub=fixture.journal.subscriptions().get(0);assertEquals(1,sub.acknowledged);assertEquals(2,sub.errorGeneration);assertEquals("PAUSED",sub.state);assertEquals("TRANSACTION_SKIPPED_DATA_GAP",sub.errorCode);assertEquals("PAUSED",fixture.journal.status().get("run_state"));
        assertEquals(receipt.hash(),complete(receipt).receipt.hash());assertEquals(1,gaps());
    }
    @Test void failedAckRollsBackGapAndResolutionAndKeepsThePendingPlan()throws Exception{
        prepareIntent();JdbcJournal j=fixture.journal;FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+" ADD CONSTRAINT reject_gap_ack CHECK (acknowledged=0)");
        assertThrows(SQLException.class,()->complete(receipt()));assertEquals(0,gaps());assertEquals(0,j.subscriptions().get(0).acknowledged);assertEquals(1,j.subscriptions().get(0).errorGeneration);
        try(Connection c=j.connect()){assertEquals("PREPARED",JdbcSkipJournal.load(j,c,plan.id).state);}code("SKIP_RECOVERY_PENDING",()->j.resumePipeline(j.pausePipeline().errorGeneration));
    }
    @Test void committedGapReceiptStillResolvesAfterResumeAndPayloadCollection()throws Exception{
        prepareIntent();SkipReceipt receipt=receipt();complete(receipt);JdbcJournal j=fixture.journal;j.resumePipeline(j.pausePipeline().errorGeneration);Journal.Lease owner=j.claim("new-owner",30000);j.resume("target",2);assertEquals(1,j.collect(owner,100));assertEquals(receipt.hash(),complete(receipt).receipt.hash());assertEquals(1,gaps());
    }
    @Test void cancelledIntentKeepsOriginalFailureButInvalidatesOldErrorGeneration()throws Exception{
        prepareIntent();JdbcJournal j=fixture.journal;try(Connection c=j.connect()){c.setAutoCommit(false);JdbcSkipJournal.Stored cancelled=JdbcSkipJournal.cancel(j,c,plan,fixture.retained.value,SkipResolution.create(plan.id,"cancel_request_1234","CANCEL","another-admin","Native absence was verified before cancellation"));c.commit();assertEquals("CANCELLED",cancelled.state);}
        assertEquals(0,gaps());assertEquals(0,j.subscriptions().get(0).acknowledged);assertEquals(2,j.subscriptions().get(0).errorGeneration);assertEquals(SubscriberFailure.Phase.TARGET_APPLY,j.inspectFailure("target",2,100000).failure.phase);
        code("ERROR_GENERATION_CHANGED",()->j.resume("target",1));j.resumePipeline(j.pausePipeline().errorGeneration);j.resume("target",2);assertNotNull(j.claim("new-owner",30000));
    }
    @Test void missingFormatFourHistoryIsNeverRecreatedByInitialization()throws Exception{
        prepareIntent();JdbcJournal j=fixture.journal;FailureReviewTest.sql(j,"DROP TABLE "+j.table("gap"));code("SKIP_METADATA_LOST",()->j.verifyIdentity(plan.fingerprint,java.util.Collections.singletonList("target")));code("SKIP_METADATA_LOST",()->j.initialize(plan.fingerprint,plan.generation,java.util.Collections.singletonList("target")));
        try(Connection c=j.connect();Statement s=c.createStatement()){assertThrows(SQLException.class,()->s.executeQuery("SELECT COUNT(*) FROM "+j.table("gap")));}
    }
    @Test void ordinaryCheckpointJsonDoesNotAcquireNewNullFields()throws Exception{
        SinkAdapter.Checkpoint cp=new SinkAdapter.Checkpoint();cp.epoch=2;cp.sequence=1;cp.transactionId="tx";cp.transactionHash=Codec.sha256("tx");String encoded=Codec.encode(cp);assertFalse(encoded.contains("skipOperation"));assertFalse(encoded.contains("skipHash"));cp.skipOperation=plan.id;cp.skipHash=receipt().hash();assertTrue(Codec.encode(cp).contains("skipOperation"));
    }
}
