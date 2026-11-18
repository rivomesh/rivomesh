package io.github.rivomesh.platform;

import io.github.rivomesh.recovery.FailureEvidence;

public final class CompensationRequest {
    public String evidenceId, evidenceHash, approvedHash, reason;

    static boolean handles(String action) {
        return "COMPENSATION_PLAN".equals(action) || "COMPENSATION_EXECUTE".equals(action);
    }

    void validate(String action) {
        FailureEvidence.uuid(evidenceId);
        boolean execute = "COMPENSATION_EXECUTE".equals(action);
        if (!FailureEvidence.hash(evidenceHash) || (execute
                ? !FailureEvidence.hash(approvedHash) || reason == null || reason.trim().length() < 8 || reason.length() > 1000
                : approvedHash != null || reason != null))
            throw new IllegalArgumentException("COMPENSATION_APPROVAL_INVALID");
    }
}
