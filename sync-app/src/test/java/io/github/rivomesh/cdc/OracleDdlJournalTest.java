package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class OracleDdlJournalTest {
    private static final String DDL = "ORACLE_REDO_DDL_REQUIRES_PLAN";

    @TempDir Path directory;

    @Test void recordsTheObservedDdlForItsExactGenerationAndCheckpointAfterReopening() throws Exception {
        JournalTest fixture = fixture();
        JdbcJournal.Settings settings = fixture.settings();
        settings.oracleDdlEvidence = true;
        SourcePosition checkpoint = position("100");
        OracleDdlEvidence original = event(checkpoint,"110","ADD EXTRA");
        OracleDdlEvidence later = event(checkpoint,"112","ADD LATER");
        Journal.Lease originalLease;
        try (JdbcJournal journal = new JdbcJournal(settings)) {
            fixture.initialize(journal);
            originalLease = journal.claim("original",30000);
            journal.baseline(originalLease,checkpoint);
            journal.failPipeline(originalLease,DDL,PipelineFailure.Phase.DDL,original);
            assertEquals(original,journal.oracleDdlEvidence(1,checkpoint).get("event"));
            assertThrows(JdbcJournal.Fenced.class,
                    () -> journal.failPipeline(originalLease,DDL,PipelineFailure.Phase.DDL,later));
        }
        try (JdbcJournal journal = new JdbcJournal(settings)) {
            assertEquals(checkpoint,journal.captured());
            assertEquals(original,journal.oracleDdlEvidence(1,checkpoint).get("event"));
            assertEquals("ORACLE_DDL_EVIDENCE_INVALID",journal.oracleDdlEvidence(1,position("101")).get("code"));
            assertEquals("ORACLE_DDL_EVIDENCE_UNRECORDED",journal.oracleDdlEvidence(2,checkpoint).get("code"));
            journal.resumePipeline(1);
            Journal.Lease next = journal.claim("next",30000);
            journal.failPipeline(next,DDL,PipelineFailure.Phase.DDL,later);
            assertEquals(later,journal.oracleDdlEvidence(2,checkpoint).get("event"));
            assertEquals("ORACLE_DDL_EVIDENCE_UNRECORDED",journal.oracleDdlEvidence(1,checkpoint).get("code"));
        }
    }

    @Test void aDifferentCutCannotBePublishedAndOldJournalsStayReadable() throws Exception {
        JournalTest fixture = fixture();
        JdbcJournal.Settings settings = fixture.settings();
        SourcePosition checkpoint = position("101");
        try (JdbcJournal old = new JdbcJournal(settings)) {
            fixture.initialize(old);
            assertEquals("ORACLE_DDL_EVIDENCE_UNRECORDED",old.oracleDdlEvidence(1,checkpoint).get("code"));
        }
        settings.oracleDdlEvidence = true;
        try (JdbcJournal journal = new JdbcJournal(settings)) {
            fixture.initialize(journal);
            Journal.Lease lease = journal.claim("owner",30000);
            journal.baseline(lease,checkpoint);
            journal.failPipeline(lease,DDL,PipelineFailure.Phase.DDL,event(position("100"),"110","ADD EXTRA"));
            assertEquals(DDL,journal.status().get("errorCode"));
            assertEquals(checkpoint,journal.captured());
            assertEquals("ORACLE_DDL_EVIDENCE_UNRECORDED",journal.oracleDdlEvidence(1,checkpoint).get("code"));
        }
    }

    @Test void optionalEvidenceWriteFailureKeepsTheDurableDdlStop() throws Exception {
        JournalTest fixture = fixture();
        JdbcJournal.Settings settings = fixture.settings();
        settings.oracleDdlEvidence = true;
        SourcePosition checkpoint = position("100");
        try (JdbcJournal journal = new JdbcJournal(settings)) {
            fixture.initialize(journal);
            try (Connection connection = journal.connect()) {
                JdbcJournal.execute(connection,"ALTER TABLE "+journal.table("oracle_ddl_failure")
                        +" ADD CONSTRAINT reject_evidence CHECK (error_generation<>1)");
            }
            Journal.Lease lease = journal.claim("owner",30000);
            journal.baseline(lease,checkpoint);
            journal.failPipeline(lease,DDL,PipelineFailure.Phase.DDL,event(checkpoint,"110","ADD EXTRA"));
            assertEquals("ERROR",journal.status().get("run_state"));
            assertEquals(DDL,journal.status().get("errorCode"));
            assertEquals("DDL",journal.status().get("errorStage"));
            assertEquals(checkpoint,journal.captured());
            assertEquals("ORACLE_DDL_EVIDENCE_UNRECORDED",journal.oracleDdlEvidence(1,checkpoint).get("code"));
        }
    }

    @Test void competingSourceAppendCannotBindAnObservationToTheWrongCut() throws Exception {
        JournalTest fixture = fixture();
        JdbcJournal.Settings settings = fixture.settings();
        settings.oracleDdlEvidence = true;
        SourcePosition checkpoint = position("100");
        OracleDdlEvidence original = event(checkpoint,"110","ADD EXTRA");
        SourceTransaction next = new SourceTransaction();
        next.id = "oracle:105:archive";
        next.streamGeneration = "generation";
        next.position = position("105");
        try (JdbcJournal journal = new JdbcJournal(settings);
             JdbcJournal peer = new JdbcJournal(settings);
             var workers = Executors.newFixedThreadPool(2)) {
            fixture.initialize(journal);
            Journal.Lease lease = journal.claim("owner",30000);
            journal.baseline(lease,checkpoint);
            CountDownLatch start = new CountDownLatch(1);
            var fault = workers.submit(() -> {
                start.await();
                journal.failPipeline(lease,DDL,PipelineFailure.Phase.DDL,original);
                return null;
            });
            var append = workers.submit(() -> {
                start.await();
                try {
                    peer.append(lease,next);
                    return true;
                } catch (JdbcJournal.Fenced fenced) {
                    return false;
                }
            });
            start.countDown();
            fault.get(10,TimeUnit.SECONDS);
            if (append.get(10,TimeUnit.SECONDS)) {
                assertEquals(next.position,journal.captured());
                assertEquals("ORACLE_DDL_EVIDENCE_UNRECORDED",journal.oracleDdlEvidence(1,next.position).get("code"));
            } else {
                assertEquals(checkpoint,journal.captured());
                assertEquals(original,journal.oracleDdlEvidence(1,checkpoint).get("event"));
            }
            assertEquals(DDL,journal.status().get("errorCode"));
        }
    }

    private JournalTest fixture() {
        JournalTest fixture = new JournalTest();
        fixture.directory = directory;
        return fixture;
    }

    private static SourcePosition position(String scn) {
        return new SourcePosition(1,"ORACLE","fixture",Map.of("scn",scn,"archiveStartScn","80"));
    }

    private static OracleDdlEvidence event(SourcePosition checkpoint,String scn,String sql) {
        return new OracleDdlEvidence(Codec.sha256(Codec.encode(checkpoint)),"80","120",scn,
                "0x000005.00000010.0010","0",OracleDdlEvidence.Kind.USER_DDL,Codec.sha256(sql));
    }
}
