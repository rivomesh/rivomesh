package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineValidationBarrier;
import io.github.rivomesh.recovery.*;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class GapVerificationPlanTest {
    @TempDir Path directory;
    SkipPlan original;SkipReceipt receipt;TableSchema table;FailureEvidence evidence;PipelineValidationBarrier.Certificate cut;
    @BeforeEach void prepare()throws Exception{
        FailureEvidenceStoreTest fixture=new FailureEvidenceStoreTest();fixture.directory=directory;evidence=fixture.sample();evidence.review.failure.projectedHash=evidence.review.normalizedHash;
        original=SkipPlan.create(fixture.store().retain(evidence),"original_skip_1234","admin","Reviewed and accepted complete transaction gap",evidence.review.normalized);table=evidence.schemas.values().iterator().next();
        receipt=new SkipReceipt();receipt.operationId=original.id;receipt.planHash=original.hash();receipt.pipelineId=original.pipelineId;receipt.subscriptionId=original.subscriptionId;receipt.generation=original.generation;receipt.fingerprint=original.fingerprint;receipt.transactionId=original.transactionId;receipt.projectedHash=original.projectedHash;receipt.epoch=original.epoch;receipt.sequence=original.sequence;receipt.committedAt=Instant.now().toString();receipt.executionRequest=original.id;receipt.executedBy="admin";receipt.executionReason=original.reason;receipt.executionReasonHash=Codec.sha256(original.reason);
        cut=new PipelineValidationBarrier.Certificate();cut.id=UUID.randomUUID().toString();cut.createdAt=Instant.now().toString();cut.fingerprint=original.fingerprint;cut.generation=original.generation;cut.epoch=original.epoch+1;cut.sequence=original.sequence;cut.sourcePosition=original.sourcePosition;cut.capturedPosition=original.sourcePosition;cut.selectedTargets.add("target");cut.schemas.put(table.id,Codec.sha256(Codec.encode(table)));SinkAdapter.Checkpoint cp=new SinkAdapter.Checkpoint();cp.epoch=cut.epoch;cp.sequence=cut.sequence;cp.transactionId=original.transactionId;cp.transactionHash=original.projectedHash;cut.targetCheckpoints.put("target",cp);
    }
    ValidationStore report(boolean matching,boolean complete)throws Exception{
        ValidationStore store=ValidationStore.create(directory.resolve("reports"),cut.id,cut,Collections.singletonList(table),Collections.singletonMap("target",Collections.singleton(table.id)),new ValidationStore.Limits());
        for(Change change:evidence.review.normalized.changes)store.source(table.id,change.after);store.finishSourceTable(table.id);store.completeSource();
        if(complete){if(matching)for(Change change:evidence.review.normalized.changes)store.target("target",table.id,null,change.after);store.finishTarget("target",table.id);store.complete();}return store;
    }
    GapVerificationPlan plan(ValidationStore store)throws Exception{return GapVerificationPlan.create(store,Collections.singletonList(original),Collections.singletonMap(original.id,receipt),"target","verify_gap_12345678","verifier","Reviewed common-cut report;require fresh fenced verification");}
    @Test void bindsReceiptsCoverageAndSealedRootsWithoutPublishingBusinessRows()throws Exception{
        try(ValidationStore store=report(true,true)){GapVerificationPlan plan=plan(store);assertEquals(original.pipelineId,plan.pipelineId);assertEquals(receipt.hash(),plan.gaps.get(original.id).receiptHash);assertEquals(2,plan.tables.get(table.id).rows);assertFalse(Codec.encode(plan).contains("private-business-value"));assertEquals(plan.hash(),Codec.decode(Codec.encode(plan),GapVerificationPlan.class).hash());assertEquals("COMPLETE",store.manifest().state);}
    }
    @Test void partialOrDifferentReportsCannotCreateAVerificationScope()throws Exception{
        try(ValidationStore store=report(true,false)){assertEquals("GAP_COMPLETE_MATCH_REQUIRED",assertThrows(IllegalStateException.class,()->plan(store)).getMessage());}
        cut.id=UUID.randomUUID().toString();try(ValidationStore store=report(false,true)){assertEquals("GAP_COMPLETE_MATCH_REQUIRED",assertThrows(IllegalStateException.class,()->plan(store)).getMessage());}
    }
    @Test void aReportBeforeTheGapOrWithDifferentGenerationCannotApproveClosure()throws Exception{
        cut.sequence=0;cut.targetCheckpoints.get("target").sequence=0;try(ValidationStore store=report(true,true)){assertEquals("GAP_REPORT_SCOPE_CHANGED",assertThrows(IllegalStateException.class,()->plan(store)).getMessage());}
        cut.id=UUID.randomUUID().toString();cut.sequence=original.sequence;cut.targetCheckpoints.get("target").sequence=cut.sequence;cut.generation="different";try(ValidationStore store=report(true,true)){assertEquals("GAP_REPORT_SCOPE_CHANGED",assertThrows(IllegalStateException.class,()->plan(store)).getMessage());}
    }
    @Test void changedSchemaAndMissingNativeExecutionProofAreRejected()throws Exception{
        table.version++;cut.schemas.put(table.id,Codec.sha256(Codec.encode(table)));try(ValidationStore store=report(true,true)){assertEquals("GAP_REPORT_SCHEMA_CHANGED",assertThrows(IllegalStateException.class,()->plan(store)).getMessage());}
        table.version--;cut.id=UUID.randomUUID().toString();cut.schemas.put(table.id,Codec.sha256(Codec.encode(table)));receipt.executionRequest=null;receipt.executedBy=null;receipt.executionReason=null;receipt.executionReasonHash=null;try(ValidationStore store=report(true,true)){assertEquals("GAP_NATIVE_RECEIPT_REQUIRED",assertThrows(IllegalStateException.class,()->plan(store)).getMessage());}
    }
    @Test void mismatchedCheckpointAndInvalidApprovalAreRejected()throws Exception{
        cut.targetCheckpoints.get("target").epoch--;try(ValidationStore store=report(true,true)){assertEquals("GAP_REPORT_CHECKPOINT_INVALID",assertThrows(IllegalStateException.class,()->plan(store)).getMessage());}
        cut.targetCheckpoints.get("target").epoch=cut.epoch;cut.id=UUID.randomUUID().toString();try(ValidationStore store=report(true,true)){assertThrows(IllegalArgumentException.class,()->GapVerificationPlan.create(store,Collections.singletonList(original),Collections.singletonMap(original.id,receipt),"target","verify_gap_12345678","verifier","short"));}
    }
}
