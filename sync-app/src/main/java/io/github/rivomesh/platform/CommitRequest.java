package io.github.rivomesh.platform;

/** Explicit evidence-bound confirmation of an already committed destination transaction. */
public final class CommitRequest {
    public String evidenceId,evidenceHash,reason;
    public void validate(){
        io.github.rivomesh.recovery.FailureEvidence.uuid(evidenceId);
        if(!io.github.rivomesh.recovery.FailureEvidence.hash(evidenceHash)||reason==null||reason.trim().length()<8||reason.length()>1000)throw new IllegalArgumentException("FAILURE_ACK_REQUEST_INVALID");
    }
}
