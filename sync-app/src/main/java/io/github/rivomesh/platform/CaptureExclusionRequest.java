package io.github.rivomesh.platform;

import io.github.rivomesh.recovery.FailureEvidence;

/** Explicit administrator approval of the exact retained source transaction and impact. */
public final class CaptureExclusionRequest {
    public String boundaryHash, archiveSha256, planHash, reason;
    public boolean acceptDataLoss;

    void validate() {
        if (!acceptDataLoss || !FailureEvidence.hash(boundaryHash) || !FailureEvidence.hash(archiveSha256)
                || !FailureEvidence.hash(planHash) || reason == null
                || reason.trim().length() < 8 || reason.length() > 1000)
            throw new IllegalArgumentException("CAPTURE_EXCLUSION_APPROVAL_REQUIRED");
    }
}
