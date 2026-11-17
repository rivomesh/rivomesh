package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.*;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SkipPlanTest {
    @TempDir Path directory;
    FailureEvidenceStoreTest fixture;
    FailureEvidence value;
    @BeforeEach void prepare(){fixture=new FailureEvidenceStoreTest();fixture.directory=directory;value=fixture.sample();value.review.failure.projectedHash=value.review.normalizedHash;}
    FailureEvidenceStore.Envelope retained()throws Exception{return fixture.store().retain(value);}
    SkipPlan plan()throws Exception{return SkipPlan.create(retained(),"skip_request_12345678","admin","Reviewed complete transaction;accept this explicit data gap",value.review.normalized);}
    @Test void bindsOneWholeTransactionWithoutCopyingBusinessImagesIntoThePlan()throws Exception{
        SkipPlan plan=plan();plan.validate();assertEquals(value.review.sequence,plan.sequence);assertEquals(value.review.raw.id,plan.transactionId);assertEquals(value.review.rawHash,plan.rawHash);assertEquals(value.review.normalizedHash,plan.projectedHash);assertEquals(value.scope.ownerDirectoryId,plan.scope.ownerDirectoryId);assertFalse(Codec.encode(plan).contains("private-business-value"));assertEquals(1,plan.schemaVersions.size());
    }
    @Test void roundTripKeepsCanonicalHashWithManyHistoricalReferences()throws Exception{
        SkipPlan plan=plan();for(int i=0;i<100;i++)plan.schemaVersions.add(Codec.sha256("table-"+i)+"/1");String hash=plan.hash();SkipPlan restored=Codec.decode(Codec.encode(plan),SkipPlan.class);assertEquals(hash,restored.hash());assertEquals(plan.schemaVersions,restored.schemaVersions);
    }
    @Test void unsupportedFailurePhaseOrMissingNormalizationNeverCreatesASkipApproval()throws Exception{
        value.review.failure.phase=SubscriberFailure.Phase.TARGET_PREPARE;
        assertEquals("SKIP_REQUIRES_COMPLETE_APPLY_FAILURE",assertThrows(IllegalStateException.class,()->plan()).getMessage());
        value=fixture.sample();value.review.normalized=null;value.review.normalizedHash=null;
        FailureEvidenceStore.Envelope original=retained();assertEquals("SKIP_REQUIRES_COMPLETE_APPLY_FAILURE",assertThrows(IllegalStateException.class,()->SkipPlan.create(original,"skip_request_12345678","admin","Explicit approval reason",original.value.review.raw)).getMessage());
    }
    @Test void projectionMismatchAndEmptyProjectionAreRefused()throws Exception{
        SourceTransaction empty=Codec.decode(Codec.encode(value.review.normalized),SourceTransaction.class);empty.changes.clear();
        FailureEvidenceStore.Envelope original=retained();assertEquals("SKIP_PROJECTION_CHANGED",assertThrows(IllegalStateException.class,()->SkipPlan.create(original,"skip_request_12345678","admin","Explicit approval reason",empty)).getMessage());
        value=fixture.sample();value.review.failure.projectedHash=Codec.sha256(Codec.encode(empty));final FailureEvidenceStore.Envelope noRows=retained();
        assertEquals("SKIP_EMPTY_PROJECTION",assertThrows(IllegalStateException.class,()->SkipPlan.create(noRows,"skip_request_12345678","admin","Explicit approval reason",empty)).getMessage());
    }
    @Test void changingApprovalIdentityOrReasonInvalidatesTheRequestBinding()throws Exception{
        SkipPlan plan=plan();plan.reason="Another approval reason";assertThrows(IllegalArgumentException.class,plan::validate);plan=plan();plan.createdBy="another-admin";assertThrows(IllegalArgumentException.class,plan::validate);
    }
    @Test void nativeReceiptSaysSkippedAndRejectsAnotherScopeOrHash()throws Exception{
        SkipPlan plan=plan();SkipReceipt receipt=new SkipReceipt();receipt.operationId=plan.id;receipt.planHash=plan.hash();receipt.pipelineId=plan.pipelineId;receipt.subscriptionId=plan.subscriptionId;receipt.generation=plan.generation;receipt.fingerprint=plan.fingerprint;receipt.transactionId=plan.transactionId;receipt.projectedHash=plan.projectedHash;receipt.epoch=plan.epoch;receipt.sequence=plan.sequence;receipt.committedAt=Instant.now().toString();receipt.validate(plan);assertEquals("SKIPPED",receipt.outcome);receipt.outcome="APPLIED";assertThrows(IllegalStateException.class,()->receipt.validate(plan));
    }
}
