package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.recovery.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class TransactionSkipRecoveryTest {
    @TempDir Path directory;
    CommitAcknowledgementTest setup;
    FailureCheckpointInspectionTest fixture;
    String operation="skip_request_12345678",reason="Reviewed original complete transaction;accept this explicit gap";
    @BeforeEach void prepare()throws Exception{setup=new CommitAcknowledgementTest();setup.directory=directory;setup.prepare();setup.pauseAndRetain(true);fixture=setup.fixture;}
    @AfterEach void close()throws Exception{if(setup!=null)setup.close();}
    TransactionSkipRecovery.Result execute()throws Exception{return TransactionSkipRecovery.execute(fixture.journal,fixture.retained,fixture.target,operation,"admin",reason,true);}
    static void code(String expected,org.junit.jupiter.api.function.Executable action){FailureCheckpointInspectionTest.code(expected,action);}
    void preparedOnly()throws Exception{
        JdbcSkipJournal.ensureMetadata(fixture.journal,fixture.retained.value);SinkAdapter.Checkpoint cp;try(JdbcSink sink=JdbcSink.existing(fixture.target)){cp=sink.checkpoint();}
        SkipPlan plan=SkipPlan.create(fixture.retained,operation,"admin",reason,fixture.normalized,cp);try(Connection c=fixture.journal.connect()){c.setAutoCommit(false);JdbcSkipJournal.prepare(fixture.journal,c,plan,fixture.retained.value);c.commit();}
    }
    @Test void completeCoordinatorCreatesExplicitGapWithoutBusinessWritesOrAutomaticResume()throws Exception{
        TransactionSkipRecovery.Result result=execute();assertEquals("COMPLETED",result.state);assertEquals("SKIPPED",result.receipt.outcome);assertEquals("admin",result.receipt.executedBy);assertEquals(reason,result.receipt.executionReason);assertEquals(0,JdbcSinkTest.count(fixture.target,"items"));assertEquals(1,fixture.journal.subscriptions().get(0).acknowledged);assertEquals(2,fixture.journal.subscriptions().get(0).errorGeneration);assertEquals("PAUSED",fixture.journal.subscriptions().get(0).state);assertEquals("PAUSED",fixture.journal.status().get("run_state"));assertEquals(true,fixture.journal.status().get("hasGaps"));assertEquals(result.receipt.hash(),execute().receipt.hash());
    }
    @Test void explicitLossApprovalAndWholeTaskPauseAreRequired()throws Exception{
        assertThrows(IllegalArgumentException.class,()->TransactionSkipRecovery.execute(fixture.journal,fixture.retained,fixture.target,operation,"admin",reason,false));fixture.journal.resumePipeline(fixture.journal.pausePipeline().errorGeneration);code("SKIP_REQUIRES_PIPELINE_PAUSED",()->execute());assertEquals(3,fixture.journal.status().get("dataFormat"));assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);
    }
    @Test void alreadyAppliedCommitRequiresReconciliationAndNeverCreatesASkipIntent()throws Exception{
        try(JdbcSink sink=JdbcSink.existing(fixture.target)){sink.prepare(fixture.retained.value.review.epoch,fixture.table);sink.apply(fixture.retained.value.review.epoch,1,fixture.normalized);}
        code("SKIP_CHECKPOINT_RECONCILIATION_REQUIRED",()->execute());assertEquals(3,fixture.journal.status().get("dataFormat"));assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);assertEquals(1,JdbcSinkTest.count(fixture.target,"items"));
    }
    @Test void aNewAdministratorCanExplicitlyRetryTheOriginalUncommittedPlan()throws Exception{
        preparedOnly();code("SKIP_NATIVE_COMMIT_NOT_PROVEN",()->TransactionSkipRecovery.resolve(fixture.journal,fixture.retained,fixture.target,operation,"recover_before_commit","another-admin","Inspect the uncommitted original plan",false));
        TransactionSkipRecovery.Result result=TransactionSkipRecovery.retry(fixture.journal,fixture.retained,fixture.target,operation,"retry_by_new_admin","another-admin","Reapproved the same immutable original transaction scope",true);assertEquals("COMPLETED",result.state);assertEquals("another-admin",result.receipt.executedBy);assertEquals("retry_by_new_admin",result.receipt.executionRequest);assertEquals("another-admin",result.resolution.actor);
        try(Connection c=fixture.journal.connect()){assertEquals("admin",JdbcSkipJournal.load(fixture.journal,c,operation).plan.createdBy);}assertEquals(0,JdbcSinkTest.count(fixture.target,"items"));
    }
    @Test void cancelBeforeNativeCommitInvalidatesOldErrorButDoesNotAcknowledgeTheTransaction()throws Exception{
        preparedOnly();TransactionSkipRecovery.Result cancelled=TransactionSkipRecovery.resolve(fixture.journal,fixture.retained,fixture.target,operation,"cancel_by_new_admin","another-admin","Verified native absence and cancelled this attempt",true);assertEquals("CANCELLED",cancelled.state);assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);assertEquals(2,fixture.journal.subscriptions().get(0).errorGeneration);assertEquals(false,fixture.journal.status().get("hasGaps"));code("SKIP_PLAN_CANCELLED",()->execute());
    }
    @Test void uncertainNativeCommitRecoversItsOriginalExecutionActorAndOneGap()throws Exception{
        JdbcJournal j=fixture.journal;FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+" ADD CONSTRAINT reject_skip_ack CHECK (acknowledged=0)");assertThrows(SQLException.class,()->execute());assertEquals(0,j.subscriptions().get(0).acknowledged);code("SKIP_RECOVERY_PENDING",()->j.resume("target",1));
        FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+(j.settings.driver.startsWith("com.mysql")?" DROP CHECK ":" DROP CONSTRAINT ")+"reject_skip_ack");TransactionSkipRecovery.Result recovered=TransactionSkipRecovery.resolve(j,fixture.retained,fixture.target,operation,"recover_by_new_admin","recovery-admin","Recovered the already committed native receipt",false);assertEquals("COMPLETED",recovered.state);assertEquals("admin",recovered.receipt.executedBy);assertEquals("recovery-admin",recovered.resolution.actor);assertEquals(1,j.status().get("gapCount"));assertEquals(0,JdbcSinkTest.count(fixture.target,"items"));code("SKIP_ALREADY_COMMITTED",()->TransactionSkipRecovery.resolve(j,fixture.retained,fixture.target,operation,"cancel_after_commit","admin","Cannot cancel committed native progress",true));
    }
    @Test void terminalReceiptReplaysWithoutOpeningLostNativeMetadataAndConflictingReasonsRefuse()throws Exception{
        String hash=execute().receipt.hash();try(Connection c=JdbcConnections.open(fixture.target.driver,fixture.target.url,fixture.target.username,fixture.target.password);Statement s=c.createStatement()){s.execute("DROP TABLE "+JdbcSinkTest.q(fixture.target,fixture.target.metadataSchema)+"."+JdbcSinkTest.q(fixture.target,"rm3_sink_job"));}
        assertEquals(hash,execute().receipt.hash());code("SKIP_REQUEST_CONFLICT",()->TransactionSkipRecovery.execute(fixture.journal,fixture.retained,fixture.target,operation,"admin","A different data-loss approval reason",true));
    }
    @Test void competingRequestsCannotSkipTheSameHeadTwice()throws Exception{
        ExecutorService workers=Executors.newFixedThreadPool(2);CountDownLatch start=new CountDownLatch(1);List<String> outcomes=Collections.synchronizedList(new ArrayList<>());try{
            List<Future<?>> pending=new ArrayList<>();for(int i=0;i<2;i++){String id=operation+"_"+i;pending.add(workers.submit(()->{try{start.await();TransactionSkipRecovery.execute(fixture.journal,fixture.retained,fixture.target,id,"admin",reason,true);outcomes.add("COMPLETED");}catch(IllegalStateException refused){outcomes.add(refused.getMessage());}catch(Exception e){throw new RuntimeException(e);}}));}start.countDown();for(Future<?> future:pending)future.get(30,TimeUnit.SECONDS);assertEquals(1,Collections.frequency(outcomes,"COMPLETED"));assertEquals(2,outcomes.size());assertTrue(outcomes.contains("ERROR_GENERATION_CHANGED")||outcomes.contains("SKIP_RECOVERY_PENDING"));assertEquals(1,fixture.journal.status().get("gapCount"));assertEquals(0,JdbcSinkTest.count(fixture.target,"items"));
        }finally{workers.shutdownNow();}
    }
}
