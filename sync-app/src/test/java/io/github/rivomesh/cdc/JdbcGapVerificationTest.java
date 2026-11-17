package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.PipelineValidationBarrier;
import io.github.rivomesh.recovery.*;
import io.github.rivomesh.validation.ValidationStore;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class JdbcGapVerificationTest {
    @TempDir Path directory;
    TransactionSkipRecoveryTest setup;FailureCheckpointInspectionTest fixture;SkipPlan original;SkipReceipt receipt;ValidationStore report;GapVerificationPlan plan;
    @BeforeEach void prepare()throws Exception{
        setup=new TransactionSkipRecoveryTest();setup.directory=directory;setup.prepare();fixture=setup.fixture;receipt=setup.execute().receipt;
        try(Connection c=fixture.journal.connect()){original=JdbcSkipJournal.load(fixture.journal,c,setup.operation).plan;}
        createReport();
    }
    void createReport()throws Exception{
        PipelineValidationBarrier.Certificate cut=new PipelineValidationBarrier.Certificate();Map<String,Object> state=fixture.journal.status();cut.id=UUID.randomUUID().toString();cut.createdAt=Instant.now().toString();cut.fingerprint=original.fingerprint;cut.generation=original.generation;cut.epoch=((Number)state.get("epoch")).longValue();cut.sequence=((Number)state.get("lastSequence")).longValue();cut.errorGeneration=((Number)state.get("errorGeneration")).longValue();cut.sourcePosition=fixture.journal.captured();cut.capturedPosition=cut.sourcePosition;cut.selectedTargets.add("target");cut.schemas.put(fixture.table.id,Codec.sha256(Codec.encode(fixture.table)));try(JdbcSink sink=JdbcSink.existing(fixture.target)){cut.targetCheckpoints.put("target",sink.checkpoint());}
        report=ValidationStore.create(directory.resolve("reports"),cut.id,cut,Collections.singletonList(fixture.table),Collections.singletonMap("target",Collections.singleton(fixture.table.id)),new ValidationStore.Limits());
        for(Change change:fixture.normalized.changes)report.source(fixture.table.id,change.after);report.finishSourceTable(fixture.table.id);report.completeSource();for(Change change:fixture.normalized.changes)report.target("target",fixture.table.id,null,change.after);report.finishTarget("target",fixture.table.id);report.complete();
        plan=GapVerificationPlan.create(report,Collections.singletonList(original),Collections.singletonMap(original.id,receipt),"target","verify_live_gap_1234","verifier","Review sealed common-cut rows and recheck the live destination");
        FailureReviewTest.sql(fixture.journal,"CREATE TABLE "+fixture.journal.table("verification_witness")+" (operation_id VARCHAR(128) PRIMARY KEY,proof_hash VARCHAR(64) NOT NULL)");
    }
    @AfterEach void close()throws Exception{try{if(report!=null)report.close();}finally{if(setup!=null)setup.close();}}
    void restoreLiveRows()throws Exception{
        JdbcSink.Settings s=fixture.target;String table=JdbcSinkTest.q(s,s.databases.get("sales"))+"."+JdbcSinkTest.q(s,"items");
        try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);PreparedStatement p=c.prepareStatement("INSERT INTO "+table+" ("+JdbcSinkTest.q(s,"id")+","+JdbcSinkTest.q(s,"email")+","+JdbcSinkTest.q(s,"document")+","+JdbcSinkTest.q(s,"amount")+") VALUES (?,?,?,?)")){
            for(Change change:fixture.normalized.changes){List<Cell> row=change.after;p.setLong(1,Long.parseLong(row.get(0).value));p.setString(2,row.get(1).value);if(s.product==TargetProduct.POSTGRESQL)p.setObject(3,JsonValues.text(row.get(2).value),Types.OTHER);else p.setString(3,JsonValues.text(row.get(2).value));p.setBigDecimal(4,new BigDecimal(row.get(3).value));p.executeUpdate();}
        }
    }
    void witness(Connection c,JdbcGapVerification.Observation observation)throws Exception{try(PreparedStatement p=c.prepareStatement("INSERT INTO "+fixture.journal.table("verification_witness")+" VALUES (?,?)")){p.setString(1,plan.id);p.setString(2,Codec.sha256(Codec.encode(observation)));p.executeUpdate();}}
    long witnesses()throws Exception{try(Connection c=fixture.journal.connect();Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT COUNT(*) FROM "+fixture.journal.table("verification_witness"))){assertTrue(r.next());return r.getLong(1);}}
    @Test void historicalMatchCannotPassWhenTheLiveDestinationStillHasMissingRows()throws Exception{
        AtomicBoolean called=new AtomicBoolean();assertEquals("GAP_LIVE_DATA_DIFFERENCES",assertThrows(IllegalStateException.class,()->JdbcGapVerification.verifyAndRecord(fixture.journal,fixture.target,report,plan,directory.resolve("scratch"),10000,(c,p)->{called.set(true);return p;})).getMessage());assertFalse(called.get());assertEquals(0,witnesses());assertEquals(1,fixture.journal.status().get("gapCount"));assertEquals("VALIDATION_MATCH",report.manifest().code);
    }
    @Test void freshCompleteScanRecordsProofWithoutChangingBusinessRowsOrOriginalGapHistory()throws Exception{
        restoreLiveRows();JdbcGapVerification.Observation proof=JdbcGapVerification.verifyAndRecord(fixture.journal,fixture.target,report,plan,directory.resolve("scratch"),10000,(c,p)->{witness(c,p);return p;});assertEquals("MATCHED_AT_COMMON_CUT",proof.outcome);assertEquals(plan.hash(),proof.planHash);assertEquals(plan.tables.get(fixture.table.id).sourceRoot,proof.tables.get(fixture.table.id).root);assertEquals(1,witnesses());assertEquals(1,JdbcSinkTest.count(fixture.target,"items"));assertEquals(1,fixture.journal.status().get("gapCount"));assertEquals("PAUSED",fixture.journal.status().get("run_state"));assertFalse(Codec.encode(proof).contains("private-cp-value"));
    }
    @Test void failedSourceRecordRollsBackAndDoesNotModifyTheComparisonReport()throws Exception{
        restoreLiveRows();String before=Codec.encode(report.manifest());assertEquals("INJECTED_RECORD_FAILURE",assertThrows(IllegalStateException.class,()->JdbcGapVerification.verifyAndRecord(fixture.journal,fixture.target,report,plan,directory.resolve("scratch"),10000,(c,p)->{witness(c,p);throw new IllegalStateException("INJECTED_RECORD_FAILURE");})).getMessage());assertEquals(0,witnesses());assertEquals(before,Codec.encode(report.manifest()));assertEquals(1,fixture.journal.status().get("gapCount"));
    }
    @Test void changedNativeCheckpointOrUnpausedSourceNeverCommitsARecord()throws Exception{
        restoreLiveRows();JdbcSink.Settings s=fixture.target;try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);Statement q=c.createStatement()){q.executeUpdate("UPDATE "+JdbcSinkTest.q(s,s.metadataSchema)+"."+JdbcSinkTest.q(s,"rm3_sink_job")+" SET stream_seq=stream_seq+1");}
        assertEquals("VALIDATION_TARGET_CHECKPOINT_MISMATCH",assertThrows(IllegalStateException.class,()->JdbcGapVerification.verifyAndRecord(fixture.journal,s,report,plan,directory.resolve("scratch"),10000,(c,p)->{witness(c,p);return p;})).getMessage());assertEquals(0,witnesses());
        fixture.journal.resumePipeline(plan.errorGeneration);assertEquals("VALIDATION_BARRIER_MOVED",assertThrows(IllegalStateException.class,()->JdbcGapVerification.verifyAndRecord(fixture.journal,s,report,plan,directory.resolve("scratch"),10000,(c,p)->p)).getMessage());
    }
    @Test void scopeAndApprovalTamperingCannotInvokeTheRecordCallback()throws Exception{
        restoreLiveRows();plan.tables.get(fixture.table.id).rows++;assertEquals("GAP_VERIFICATION_PLAN_CHANGED",assertThrows(IllegalStateException.class,()->JdbcGapVerification.verifyAndRecord(fixture.journal,fixture.target,report,plan,directory.resolve("scratch"),10000,(c,p)->p)).getMessage());assertEquals(0,witnesses());
    }
    @Test void resumeWaitsUntilTheVerifiedSourceRecordHasCommitted()throws Exception{
        restoreLiveRows();ExecutorService thread=Executors.newSingleThreadExecutor();CountDownLatch entered=new CountDownLatch(1);List<Future<?>> pending=new ArrayList<>();
        try{JdbcGapVerification.verifyAndRecord(fixture.journal,fixture.target,report,plan,directory.resolve("scratch"),10000,(c,p)->{pending.add(thread.submit(()->{entered.countDown();try{fixture.journal.resumePipeline(plan.errorGeneration);}catch(Exception e){throw new RuntimeException(e);}}));assertTrue(entered.await(2,TimeUnit.SECONDS));assertThrows(TimeoutException.class,()->pending.get(0).get(150,TimeUnit.MILLISECONDS));witness(c,p);return p;});pending.get(0).get(3,TimeUnit.SECONDS);assertEquals(1,witnesses());assertEquals("RUNNING",fixture.journal.status().get("run_state"));}
        finally{thread.shutdownNow();thread.awaitTermination(5,TimeUnit.SECONDS);}
    }
}
