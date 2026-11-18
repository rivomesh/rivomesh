package io.github.rivomesh.cdc;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class DeliveryProgressTest {
    @TempDir Path directory;
    @SuppressWarnings("unchecked") Map<String,Object> point(JdbcJournal journal,String id)throws Exception{return (Map<String,Object>)((Map<?,?>)journal.deliveryProgress().get("subscriptions")).get(id);}
    @Test void ordinaryAcknowledgmentIsCoverageAndContiguousDeliveryWithoutMetadataCreation()throws Exception{
        FailureReviewTest helper=new FailureReviewTest();helper.directory=directory;
        try(JdbcJournal journal=new JdbcJournal(helper.settings())){
            Journal.Lease lease=FailureReviewTest.prepare(journal);journal.acknowledge(lease,"fast",1);
            assertEquals("1",point(journal,"fast").get("safeCoveredSequence"));assertEquals("1",point(journal,"fast").get("contiguousDeliveredSequence"));assertNull(point(journal,"fast").get("firstSkippedSequence"));assertEquals("0",point(journal,"slow").get("safeCoveredSequence"));assertEquals(3,journal.status().get("dataFormat"));
        }finally{helper.cleanupExternalNamespace();}
    }
    @Test void skipAdvancesOnlyCoverageAndVerificationNeverRewritesTheAppliedPrefix()throws Exception{
        JdbcGapVerificationTest setup=new JdbcGapVerificationTest();setup.directory=directory;setup.prepare();
        try{
            Map<String,Object> before=point(setup.fixture.journal,"target");assertEquals("1",before.get("safeCoveredSequence"));assertEquals("0",before.get("contiguousDeliveredSequence"));assertEquals("1",before.get("firstSkippedSequence"));setup.restoreLiveRows();JdbcGapVerification.verifyAndClose(setup.fixture.journal,setup.fixture.target,setup.report,setup.plan,directory.resolve("scratch"),10000,"approval_progress_1234","approver","Approved a fresh common-cut verification for these tables");assertEquals(before,point(setup.fixture.journal,"target"));
        }finally{setup.close();}
    }
    @Test void sequencesBeyondJavascriptPrecisionRemainExactAndImpossibleAckRefuses()throws Exception{
        FailureReviewTest helper=new FailureReviewTest();helper.directory=directory;
        try(JdbcJournal journal=new JdbcJournal(helper.settings())){
            new JournalTest().initialize(journal);long sequence=9007199254740993L;FailureReviewTest.sql(journal,"UPDATE "+journal.table("job")+" SET next_seq=?",sequence+1);FailureReviewTest.sql(journal,"UPDATE "+journal.table("subscription")+" SET acknowledged=?",sequence);
            assertEquals(Long.toString(sequence),journal.deliveryProgress().get("capturedSequence"));assertEquals(Long.toString(sequence),point(journal,"fast").get("safeCoveredSequence"));FailureReviewTest.sql(journal,"UPDATE "+journal.table("subscription")+" SET acknowledged=?",sequence+1);assertEquals("GAP_PROGRESS_CHECKPOINT_INVALID",assertThrows(IllegalStateException.class,journal::deliveryProgress).getMessage());
        }finally{helper.cleanupExternalNamespace();}
    }
    @Test void inconsistentGapHeadersCannotIncreaseTheContiguousAppliedPrefix()throws Exception{
        TransactionSkipRecoveryTest setup=new TransactionSkipRecoveryTest();setup.directory=directory;setup.prepare();
        try{setup.execute();FailureReviewTest.sql(setup.fixture.journal,"UPDATE "+setup.fixture.journal.table("gap")+" SET stream_seq=stream_seq+1");assertEquals("GAP_PROGRESS_RECORD_CHANGED",assertThrows(IllegalStateException.class,setup.fixture.journal::deliveryProgress).getMessage());}finally{setup.close();}
    }
}
