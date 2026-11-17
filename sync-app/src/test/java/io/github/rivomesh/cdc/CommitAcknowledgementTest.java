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

class CommitAcknowledgementTest {
    @TempDir Path directory;
    FailureCheckpointInspectionTest fixture;
    final String operation="confirm_12345678",reason="Verified original committed transaction";
    @BeforeEach void prepare()throws Exception{fixture=new FailureCheckpointInspectionTest();fixture.directory=directory;fixture.prepare();}
    @AfterEach void close()throws Exception{if(fixture!=null)fixture.cleanup();}
    void pauseAndRetain(boolean fence)throws Exception{
        Journal.PipelineState state=fixture.journal.pausePipeline();
        if(fence)try(JdbcSink sink=JdbcSink.existing(fixture.target)){sink.activate(state.epoch);}
        fixture.retained=FailureEvidenceCapture.capture(fixture.journal,fixture.store,fixture.retained.value.scope,UUID.randomUUID().toString(),"fresh_request_12345678","paused-revision","admin","target",1,100000);
    }
    JdbcCommitAcknowledgement.Receipt confirm()throws Exception{return JdbcCommitAcknowledgement.confirm(fixture.journal,fixture.store.load(fixture.retained.value.id),fixture.target,fixture.retained.hash,operation,"admin",reason);}
    static void code(String expected,org.junit.jupiter.api.function.Executable action){FailureCheckpointInspectionTest.code(expected,action);}
    long receipts()throws Exception{try(Connection c=fixture.journal.connect();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+fixture.journal.table("recovery_ack"))){assertTrue(r.next());return r.getLong(1);}}
    @Test void targetPauseAloneCannotAuthorizeJournalAcknowledgment()throws Exception{
        fixture.commit();code("FAILURE_ACK_REQUIRES_PIPELINE_PAUSED",()->confirm());assertEquals(0,receipts());assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);
    }
    @Test void provenCommitAcknowledgesExactlyOneTransactionAndKeepsBothPauses()throws Exception{
        fixture.commit();pauseAndRetain(true);JdbcCommitAcknowledgement.Receipt result=confirm();Journal.Subscription sub=fixture.journal.subscriptions().get(0);
        assertEquals(1,result.sequence);assertEquals(0,result.previousAcknowledged);assertEquals(1,result.previousErrorGeneration);assertEquals(2,result.errorGeneration);assertEquals(fixture.retained.hash,result.evidenceHash);
        assertEquals(1,sub.acknowledged);assertEquals(2,sub.errorGeneration);assertEquals("PAUSED",sub.state);assertEquals("PAUSED",fixture.journal.status().get("run_state"));assertEquals("COMMIT_RECONCILED_REVIEW_NEXT",sub.errorCode);
        assertEquals(1,receipts());assertEquals(1,JdbcSinkTest.count(fixture.target,"items"));assertEquals(Codec.encode(result),Codec.encode(confirm()));
        code("ERROR_GENERATION_CHANGED",()->fixture.journal.resume("target",1));
    }
    @Test void uncommittedHeadCannotBeRelabeledAsACommittedApply()throws Exception{
        pauseAndRetain(true);code("FAILURE_ACK_COMMIT_NOT_PROVEN",()->confirm());assertEquals(0,receipts());assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);
    }
    @Test void evidenceCapturedBeforePauseAndFenceIsStale()throws Exception{
        fixture.commit();fixture.journal.pausePipeline();code("FAILURE_CHECKPOINT_JOURNAL_CHANGED",()->confirm());assertEquals(0,receipts());
    }
    @Test void missingNativeFenceRefusesWithoutActivatingOrAcknowledging()throws Exception{
        fixture.commit();pauseAndRetain(false);code("FAILURE_ACK_TARGET_FENCE_REQUIRED",()->confirm());try(JdbcSink sink=JdbcSink.existing(fixture.target)){assertEquals(fixture.lease.epoch,sink.checkpoint().epoch);}assertEquals(0,receipts());
    }
    @Test void changedHashReasonOrActorCannotReuseTheOriginalOperationId()throws Exception{
        fixture.commit();pauseAndRetain(true);confirm();
        code("FAILURE_EVIDENCE_CONTENT_CHANGED",()->JdbcCommitAcknowledgement.confirm(fixture.journal,fixture.retained,fixture.target,Codec.sha256("other-evidence"),operation,"admin",reason));
        code("FAILURE_ACK_REQUEST_CONFLICT",()->JdbcCommitAcknowledgement.confirm(fixture.journal,fixture.retained,fixture.target,fixture.retained.hash,operation,"admin","A different reason"));
        code("FAILURE_ACK_REQUEST_CONFLICT",()->JdbcCommitAcknowledgement.confirm(fixture.journal,fixture.retained,fixture.target,fixture.retained.hash,operation,"another-admin",reason));assertEquals(1,receipts());
    }
    @Test void failureAfterReceiptInsertRollsBackReceiptAckAndErrorGenerationTogether()throws Exception{
        fixture.commit();pauseAndRetain(true);JdbcJournal j=fixture.journal;
        FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+" ADD CONSTRAINT reject_recovery_ack CHECK (acknowledged=0)");
        assertThrows(SQLException.class,()->confirm());assertEquals(0,receipts());Journal.Subscription sub=j.subscriptions().get(0);assertEquals(0,sub.acknowledged);assertEquals(1,sub.errorGeneration);
        FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+(j.settings.driver.startsWith("com.mysql")?" DROP CHECK ":" DROP CONSTRAINT ")+"reject_recovery_ack");assertEquals(1,confirm().sequence);
    }
    @Test void receiptReplaysAfterReopenResumeCollectionAndMissingNativeMetadata()throws Exception{
        fixture.commit();pauseAndRetain(true);String first=Codec.encode(confirm());JdbcJournal j=fixture.journal;Journal.PipelineState state=j.pausePipeline();j.resumePipeline(state.errorGeneration);Journal.Lease owner=j.claim("restarted-owner",30000);j.resume("target",2);assertEquals(1,j.collect(owner,100));
        try(Connection c=JdbcConnections.open(fixture.target.driver,fixture.target.url,fixture.target.username,fixture.target.password);Statement sql=c.createStatement()){sql.execute("DROP TABLE "+JdbcSinkTest.q(fixture.target,fixture.target.metadataSchema)+"."+JdbcSinkTest.q(fixture.target,"rm3_sink_job"));}
        JdbcJournal.Settings settings=j.settings;j.close();fixture.journal=new JdbcJournal(settings);assertEquals(first,Codec.encode(confirm()));assertEquals(1,receipts());
    }
    @Test void changedReceiptChecksumRefusesRatherThanReexecuting()throws Exception{
        fixture.commit();pauseAndRetain(true);confirm();FailureReviewTest.sql(fixture.journal,"UPDATE "+fixture.journal.table("recovery_ack")+" SET payload_hash=?",Codec.sha256("tampered"));code("FAILURE_ACK_RECEIPT_CHANGED",()->confirm());assertEquals(1,fixture.journal.subscriptions().get(0).acknowledged);
    }
    @Test void missingRecoveryTableRequiresMigrationAndIsNeverCreatedByTheOperation()throws Exception{
        fixture.commit();pauseAndRetain(true);FailureReviewTest.sql(fixture.journal,"DROP TABLE "+fixture.journal.table("recovery_ack"));
        code("FAILURE_ACK_METADATA_UPGRADE_REQUIRED",()->confirm());assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);
        JdbcCommitAcknowledgement.prepare(fixture.journal,fixture.retained.value);assertEquals(0,receipts());assertEquals(0,fixture.journal.subscriptions().get(0).acknowledged);assertEquals(1,confirm().sequence);
    }
    @Test void missingRecoveryMetadataCannotBeMigratedWhileThePipelineRuns()throws Exception{
        JdbcJournal j=fixture.journal;FailureReviewTest.sql(j,"DROP TABLE "+j.table("recovery_ack"));
        code("FAILURE_ACK_REQUIRES_PIPELINE_PAUSED",()->JdbcCommitAcknowledgement.prepare(j,fixture.retained.value));
        code("FAILURE_ACK_METADATA_UPGRADE_REQUIRED",()->confirm());assertEquals(0,j.subscriptions().get(0).acknowledged);
    }
    @Test void nontransactionalRecoveryReceiptMetadataCannotSplitTheAckCommit()throws Exception{
        fixture.commit();pauseAndRetain(true);JdbcJournal j=fixture.journal;
        if(!j.settings.driver.startsWith("com.mysql")){assertEquals(1,confirm().sequence);return;}
        FailureReviewTest.sql(j,"ALTER TABLE "+j.table("recovery_ack")+" CONVERT TO CHARACTER SET latin1, ENGINE=MyISAM");
        code("FAILURE_ACK_METADATA_NOT_TRANSACTIONAL",()->confirm());assertEquals(0,receipts());assertEquals(0,j.subscriptions().get(0).acknowledged);
    }
    @Test void twoDifferentConfirmationsCannotAdvanceTheSamePausedHeadTwice()throws Exception{
        fixture.commit();pauseAndRetain(true);ExecutorService workers=Executors.newFixedThreadPool(2);CountDownLatch start=new CountDownLatch(1);List<String> outcomes=Collections.synchronizedList(new ArrayList<>());
        try{List<Future<?>> pending=new ArrayList<>();for(int i=0;i<2;i++){String id=operation+"_"+i;pending.add(workers.submit(()->{try{start.await();JdbcCommitAcknowledgement.confirm(fixture.journal,fixture.retained,fixture.target,fixture.retained.hash,id,"admin",reason);outcomes.add("CONFIRMED");}catch(IllegalStateException refused){outcomes.add(refused.getMessage());}catch(Exception e){throw new RuntimeException(e);}}));}start.countDown();for(Future<?> item:pending)item.get(30,TimeUnit.SECONDS);
            assertEquals(1,Collections.frequency(outcomes,"CONFIRMED"));assertEquals(1,Collections.frequency(outcomes,"ERROR_GENERATION_CHANGED"));assertEquals(1,receipts());assertEquals(1,JdbcSinkTest.count(fixture.target,"items"));
        }finally{workers.shutdownNow();}
    }
}
