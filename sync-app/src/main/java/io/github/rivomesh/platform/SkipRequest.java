package io.github.rivomesh.platform;

import io.github.rivomesh.recovery.FailureEvidence;

/** Explicit approval or resolution of one immutable complete-transaction skip plan. */
public final class SkipRequest {
    public String evidenceId,evidenceHash,planId,planHash,reason;
    public boolean acceptDataLoss;
    public void validate(String action){
        if(!java.util.Arrays.asList("SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL").contains(action))throw new IllegalArgumentException("SKIP_ACTION_REQUIRED");
        if(reason==null||reason.trim().length()<8||reason.length()>1000)throw new IllegalArgumentException("SKIP_REASON_REQUIRED");
        if("SKIP_TRANSACTION".equals(action)){
            FailureEvidence.uuid(evidenceId);if(!FailureEvidence.hash(evidenceHash)||planId!=null||planHash!=null||!acceptDataLoss)throw new IllegalArgumentException("SKIP_DATA_LOSS_APPROVAL_REQUIRED");
        }else{
            if(planId==null||!planId.matches("[A-Za-z0-9_-]{8,128}")||!FailureEvidence.hash(planHash)||evidenceId!=null||evidenceHash!=null)throw new IllegalArgumentException("SKIP_PLAN_REFERENCE_INVALID");
            if("SKIP_RETRY".equals(action)&&!acceptDataLoss)throw new IllegalArgumentException("SKIP_DATA_LOSS_APPROVAL_REQUIRED");
            if(!"SKIP_RETRY".equals(action)&&acceptDataLoss)throw new IllegalArgumentException("SKIP_RESOLUTION_INVALID");
        }
    }
}
