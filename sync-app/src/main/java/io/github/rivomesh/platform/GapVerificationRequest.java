package io.github.rivomesh.platform;

import io.github.rivomesh.recovery.FailureEvidence;
import java.util.*;

/** A bounded review scope, or explicit approval of a previously persisted plan. */
public final class GapVerificationRequest {
    public String reportId,planId,approvedHash,reason;
    public Set<String> gaps=new TreeSet<>();
    public long maximumMillis=300000;
    public void validate(boolean execute){
        if(reason==null||reason.trim().length()<8||reason.length()>1000||gaps==null||maximumMillis<1000||maximumMillis>86400000)throw new IllegalArgumentException("GAP_VERIFICATION_REQUEST_INVALID");
        if(execute){
            if(planId==null||!planId.matches("[A-Za-z0-9_-]{8,128}")||!FailureEvidence.hash(approvedHash)||reportId!=null||!gaps.isEmpty()||maximumMillis!=300000)throw new IllegalArgumentException("GAP_VERIFICATION_APPROVAL_INVALID");
        }else{
            FailureEvidence.uuid(reportId);if(planId!=null||approvedHash!=null||gaps.isEmpty()||gaps.size()>100||gaps.stream().anyMatch(id->id==null||!id.matches("[A-Za-z0-9_-]{8,128}")))throw new IllegalArgumentException("GAP_VERIFICATION_SCOPE_INVALID");
        }
        gaps=new TreeSet<>(gaps);
    }
}
