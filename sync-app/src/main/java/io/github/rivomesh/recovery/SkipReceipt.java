package io.github.rivomesh.recovery;

import io.github.rivomesh.cdc.Codec;
import java.time.Instant;
import java.util.*;

/** Native receipt describes SKIPPED explicitly; it never claims business rows were applied. */
public final class SkipReceipt {
    public int version=1;
    public String outcome="SKIPPED",operationId,planHash,pipelineId,subscriptionId,generation,fingerprint,transactionId,projectedHash,committedAt;
    public String executionRequest,executedBy,executionReason,executionReasonHash;
    public long epoch,sequence;
    public void validate(SkipPlan plan){
        plan.validate();
        if(version!=1||!"SKIPPED".equals(outcome)||!Objects.equals(operationId,plan.id)||!Objects.equals(planHash,plan.hash())||!Objects.equals(pipelineId,plan.pipelineId)||!Objects.equals(subscriptionId,plan.subscriptionId)||!Objects.equals(generation,plan.generation)||!Objects.equals(fingerprint,plan.fingerprint)||!Objects.equals(transactionId,plan.transactionId)||!Objects.equals(projectedHash,plan.projectedHash)||epoch!=plan.epoch||sequence!=plan.sequence)throw new IllegalStateException("SKIP_RECEIPT_SCOPE_CHANGED");
        try{Instant.parse(committedAt);}catch(Exception malformed){throw new IllegalStateException("SKIP_RECEIPT_INVALID");}
        if(executionRequest!=null||executedBy!=null||executionReason!=null||executionReasonHash!=null){if(executionRequest==null||!executionRequest.matches("[A-Za-z0-9_-]{8,128}")||executedBy==null||executedBy.isEmpty()||executedBy.length()>128||executionReason==null||executionReason.trim().length()<8||executionReason.length()>1000||!FailureEvidence.hash(executionReasonHash)||!Codec.sha256(executionReason).equals(executionReasonHash))throw new IllegalStateException("SKIP_RECEIPT_INVALID");}
    }
    public String hash(){return Codec.sha256(Codec.encode(this));}
}
