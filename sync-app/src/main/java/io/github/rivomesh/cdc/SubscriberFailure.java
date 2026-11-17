package io.github.rivomesh.cdc;

/** Bounded, non-value diagnostic. A phase is an observation, never permission to skip. */
public final class SubscriberFailure {
    public enum Phase { UNKNOWN,TARGET_ACTIVATE,TARGET_PREPARE,TARGET_SNAPSHOT,JOURNAL_READ,NORMALIZE,PROJECT,RECONCILE,TARGET_APPLY,JOURNAL_ACK }
    public Phase phase=Phase.UNKNOWN;
    /** Zero means that no complete transaction was identified at the failure site. */
    public long sequence;
    public String transactionId,projectedHash;
    public void validate(){
        if(phase==null||sequence<0)throw new IllegalArgumentException("FAILURE_DIAGNOSTIC_INVALID");
        if(sequence==0){if(transactionId!=null||projectedHash!=null)throw new IllegalArgumentException("FAILURE_DIAGNOSTIC_INVALID");}
        else if(transactionId==null||transactionId.isEmpty()||transactionId.length()>512)throw new IllegalArgumentException("FAILURE_DIAGNOSTIC_INVALID");
        if(projectedHash!=null&&!projectedHash.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("FAILURE_DIAGNOSTIC_INVALID");
    }
}
