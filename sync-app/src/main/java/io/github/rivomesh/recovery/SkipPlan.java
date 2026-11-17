package io.github.rivomesh.recovery;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/** Immutable approval scope for one complete subscriber transaction; contains no row images. */
public final class SkipPlan {
    public int version=1;
    public String id,requestHash,evidenceId,evidenceHash,createdAt,createdBy,reason;
    public String pipelineId,subscriptionId,generation,fingerprint,transactionId,rawHash,normalizedHash,projectedHash;
    public FailureEvidence.Scope scope;
    public SourcePosition sourcePosition;
    public SinkAdapter.Checkpoint expectedCheckpoint;
    public long epoch,sequence,acknowledged,errorGeneration;
    public SortedSet<String> schemaVersions=new TreeSet<>();
    public void validate(){
        if(version!=1||id==null||!id.matches("[A-Za-z0-9_-]{8,128}")||!FailureEvidence.hash(requestHash)||!FailureEvidence.hash(evidenceHash)||!FailureEvidence.hash(fingerprint)||!FailureEvidence.hash(rawHash)||!FailureEvidence.hash(normalizedHash)||!FailureEvidence.hash(projectedHash))throw invalid();
        FailureEvidence.uuid(evidenceId);if(scope==null||!Objects.equals(pipelineId,scope.taskId)||subscriptionId==null||!subscriptionId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw invalid();scope.validate();
        if(createdBy==null||createdBy.isEmpty()||createdBy.length()>128||reason==null||reason.trim().length()<8||reason.length()>1000||generation==null||generation.isEmpty()||generation.length()>128||transactionId==null||transactionId.isEmpty()||transactionId.length()>512||sourcePosition==null||epoch<1||acknowledged<0||sequence!=Math.addExact(acknowledged,1)||errorGeneration<1||schemaVersions==null||schemaVersions.isEmpty())throw invalid();
        try{Instant.parse(createdAt);}catch(Exception malformed){throw invalid();}
        if(expectedCheckpoint==null||expectedCheckpoint.epoch!=epoch||expectedCheckpoint.sequence!=acknowledged)throw invalid();
        if(acknowledged==0&&(expectedCheckpoint.transactionId!=null||expectedCheckpoint.transactionHash!=null||expectedCheckpoint.skipOperation!=null||expectedCheckpoint.skipHash!=null))throw invalid();
        if(acknowledged>0&&(expectedCheckpoint.transactionId==null||expectedCheckpoint.transactionId.isEmpty()||!FailureEvidence.hash(expectedCheckpoint.transactionHash)))throw invalid();
        if((expectedCheckpoint.skipOperation==null)!=(expectedCheckpoint.skipHash==null)||expectedCheckpoint.skipOperation!=null&&(!expectedCheckpoint.skipOperation.matches("[A-Za-z0-9_-]{8,128}")||!FailureEvidence.hash(expectedCheckpoint.skipHash)))throw invalid();
        for(String reference:schemaVersions)if(reference==null||!reference.matches("[0-9a-f]{64}/[1-9][0-9]{0,9}"))throw invalid();
        if(!requestHash.equals(requestHash(id,evidenceId,evidenceHash,createdBy,reason)))throw invalid();
        if(Codec.encode(this).getBytes(StandardCharsets.UTF_8).length>4L*1024*1024)throw new IllegalArgumentException("SKIP_PLAN_TOO_LARGE");
    }
    public String hash(){validate();return Codec.sha256(Codec.encode(this));}
    public static String requestHash(String operation,String evidence,String evidenceHash,String actor,String reason){
        Map<String,Object> fields=new TreeMap<>();fields.put("operation",operation);fields.put("evidence",evidence);fields.put("evidenceHash",evidenceHash);fields.put("actor",actor);fields.put("reason",reason);fields.put("acceptDataLoss",true);return Codec.sha256(Codec.encode(fields));
    }
    public static SkipPlan create(FailureEvidenceStore.Envelope retained,String operation,String actor,String reason,SourceTransaction projected){
        if(retained.value.review.subscription.acknowledged!=0)throw new IllegalStateException("SKIP_NATIVE_CHECKPOINT_REQUIRED");
        SinkAdapter.Checkpoint initial=new SinkAdapter.Checkpoint();initial.epoch=retained.value.review.epoch;
        return create(retained,operation,actor,reason,projected,initial);
    }
    public static SkipPlan create(FailureEvidenceStore.Envelope retained,String operation,String actor,String reason,SourceTransaction projected,SinkAdapter.Checkpoint checkpoint){
        FailureEvidence evidence=retained.value;evidence.validate();Journal.FailureReview review=evidence.review;
        if(review.failure.phase!=SubscriberFailure.Phase.TARGET_APPLY||review.failure.sequence!=review.sequence||review.normalized==null||review.raw.kind!=SourceTransaction.Kind.DATA||review.normalized.kind!=SourceTransaction.Kind.DATA||projected.kind!=SourceTransaction.Kind.DATA)throw new IllegalStateException("SKIP_REQUIRES_COMPLETE_APPLY_FAILURE");
        projected.validate();String projectedHash=Codec.sha256(Codec.encode(projected));
        if(!review.raw.id.equals(projected.id)||!review.generation.equals(projected.streamGeneration)||!review.raw.position.equals(projected.position)||!projectedHash.equals(review.failure.projectedHash))throw new IllegalStateException("SKIP_PROJECTION_CHANGED");
        SkipPlan plan=new SkipPlan();plan.id=operation;plan.evidenceId=evidence.id;plan.evidenceHash=retained.hash;plan.createdBy=actor;plan.reason=reason;plan.requestHash=requestHash(operation,evidence.id,retained.hash,actor,reason);plan.createdAt=Instant.now().toString();
        plan.pipelineId=review.pipelineId;plan.subscriptionId=review.subscription.id;plan.generation=review.generation;plan.fingerprint=review.fingerprint;plan.transactionId=review.raw.id;plan.rawHash=review.rawHash;plan.normalizedHash=review.normalizedHash;plan.projectedHash=projectedHash;
        plan.scope=Codec.decode(Codec.encode(evidence.scope),FailureEvidence.Scope.class);plan.sourcePosition=Codec.decode(Codec.encode(review.raw.position),SourcePosition.class);plan.epoch=review.epoch;plan.sequence=review.sequence;plan.acknowledged=review.subscription.acknowledged;plan.errorGeneration=review.subscription.errorGeneration;
        plan.expectedCheckpoint=checkpoint==null?null:Codec.decode(Codec.encode(checkpoint),SinkAdapter.Checkpoint.class);
        for(Change change:projected.changes){
            if(change.operation==Change.Operation.BARRIER)continue;
            if(!Arrays.asList(Change.Operation.INSERT,Change.Operation.UPDATE,Change.Operation.DELETE).contains(change.operation))throw new IllegalStateException("SKIP_REQUIRES_COMPLETE_APPLY_FAILURE");
            if(!evidence.schemas.containsKey(change.tableId+"/"+change.schemaVersion))throw new IllegalStateException("HISTORICAL_SCHEMA_MISSING");plan.schemaVersions.add(change.tableId+"/"+change.schemaVersion);
        }
        if(plan.schemaVersions.isEmpty())throw new IllegalStateException("SKIP_EMPTY_PROJECTION");plan.validate();return plan;
    }
    private static IllegalArgumentException invalid(){return new IllegalArgumentException("SKIP_PLAN_INVALID");}
}
