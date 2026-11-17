package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.*;
import java.nio.file.Path;
import java.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class GapVerificationLedgerTest {
    @TempDir Path directory;
    JdbcGapVerificationTest setup;
    @BeforeEach void prepare()throws Exception{setup=new JdbcGapVerificationTest();setup.directory=directory;setup.prepare();setup.restoreLiveRows();}
    @AfterEach void close()throws Exception{if(setup!=null)setup.close();}
    JdbcJournal journal(){return setup.fixture.journal;}
    GapVerificationReceipt execute()throws Exception{return JdbcGapVerification.verifyAndClose(journal(),setup.fixture.target,setup.report,setup.plan,directory.resolve("scratch"),10000);}
    String verification()throws Exception{try(Connection c=journal().connect();Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT verification_id FROM "+journal().table("gap"))){assertTrue(r.next());return r.getString(1);}}
    @Test void closureIsDurableIdempotentAndNeverRewritesTheSkipOrResumes()throws Exception{
        String before;try(JdbcSink sink=JdbcSink.existing(setup.fixture.target)){before=Codec.encode(sink.checkpoint());}
        GapVerificationReceipt receipt=execute();assertEquals("VERIFIED_AT_COMMON_CUT",receipt.outcome);assertEquals(setup.plan.id,verification());assertEquals(receipt.hash(),execute().hash());
        try(Connection c=journal().connect()){JdbcSkipJournal.Stored skip=JdbcSkipJournal.load(journal(),c,setup.original.id);assertEquals("COMPLETED",skip.state);assertEquals("SKIPPED",skip.receipt.outcome);assertEquals(setup.receipt.hash(),skip.receipt.hash());}
        try(JdbcSink sink=JdbcSink.existing(setup.fixture.target)){assertEquals(before,Codec.encode(sink.checkpoint()));}
        assertEquals("PAUSED",journal().status().get("run_state"));assertEquals(1,journal().status().get("gapCount"));assertEquals(1,JdbcSinkTest.count(setup.fixture.target,"items"));
        journal().resumePipeline(setup.plan.errorGeneration);
        try(JdbcJournal reopened=new JdbcJournal(journal().settings)){assertEquals(receipt.hash(),JdbcGapVerification.verifyAndClose(reopened,setup.fixture.target,null,setup.plan,directory.resolve("scratch"),10000).hash());}
        assertEquals("RUNNING",journal().status().get("run_state"));
    }
    @Test void recordAndGapLinksRollBackTogetherThenRetrySucceeds()throws Exception{
        assertEquals("INJECTED_AFTER_RECEIPT_WRITE",assertThrows(IllegalStateException.class,()->JdbcGapVerification.verifyAndRecord(journal(),setup.fixture.target,setup.report,setup.plan,directory.resolve("scratch"),10000,(c,p)->{JdbcGapVerificationLedger.record(journal(),c,setup.plan,p);throw new IllegalStateException("INJECTED_AFTER_RECEIPT_WRITE");})).getMessage());
        assertNull(verification());try(Connection c=journal().connect()){assertNull(JdbcGapVerificationLedger.load(journal(),c,setup.plan.id));}
        assertEquals(setup.plan.hash(),execute().plan.hash());
    }
    @Test void sameRequestWithChangedApprovalAndDifferentRequestForClosedGapAreRefused()throws Exception{
        execute();GapVerificationPlan changed=Codec.decode(Codec.encode(setup.plan),GapVerificationPlan.class);changed.reason="A different reason cannot reuse a committed verification request";
        assertEquals("GAP_VERIFICATION_REQUEST_CONFLICT",assertThrows(IllegalStateException.class,()->JdbcGapVerification.verifyAndClose(journal(),setup.fixture.target,setup.report,changed,directory.resolve("scratch"),10000)).getMessage());
        GapVerificationPlan another=Codec.decode(Codec.encode(setup.plan),GapVerificationPlan.class);another.id="another_verify_request";
        assertEquals("GAP_ALREADY_VERIFIED",assertThrows(IllegalStateException.class,()->JdbcGapVerification.verifyAndClose(journal(),setup.fixture.target,setup.report,another,directory.resolve("scratch"),10000)).getMessage());
    }
    @Test void changedReceiptOrMissingGapLinkNeverReturnsHistoricalSuccess()throws Exception{
        execute();FailureReviewTest.sql(journal(),"UPDATE "+journal().table("gap_verification")+" SET payload='{}'");assertEquals("GAP_VERIFICATION_RECORD_CHANGED",assertThrows(IllegalStateException.class,this::execute).getMessage());
    }
    @Test void changedGapLinkAndLostVerificationTableAreRefused()throws Exception{
        execute();FailureReviewTest.sql(journal(),"UPDATE "+journal().table("gap")+" SET verification_hash='"+String.join("",java.util.Collections.nCopies(64,"0"))+"'");assertEquals("GAP_VERIFICATION_RECORD_CHANGED",assertThrows(IllegalStateException.class,this::execute).getMessage());
        FailureReviewTest.sql(journal(),"DROP TABLE "+journal().table("gap_verification"));assertEquals("SKIP_METADATA_LOST",assertThrows(IllegalStateException.class,()->journal().verifyIdentity(setup.plan.fingerprint,java.util.Collections.singletonList("target"))).getMessage());
    }
    @Test void concurrentReplaysReturnOneImmutableReceipt()throws Exception{
        java.util.concurrent.ExecutorService pool=java.util.concurrent.Executors.newFixedThreadPool(2);java.util.concurrent.CountDownLatch start=new java.util.concurrent.CountDownLatch(1);
        try{
            java.util.concurrent.Callable<String> operation=()->{start.await();return execute().hash();};java.util.concurrent.Future<String> first=pool.submit(operation),second=pool.submit(operation);start.countDown();assertEquals(first.get(30,java.util.concurrent.TimeUnit.SECONDS),second.get(30,java.util.concurrent.TimeUnit.SECONDS));
            try(Connection c=journal().connect();Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT COUNT(*) FROM "+journal().table("gap_verification"))){assertTrue(r.next());assertEquals(1,r.getInt(1));}
        }finally{pool.shutdownNow();pool.awaitTermination(5,java.util.concurrent.TimeUnit.SECONDS);}
    }
}
