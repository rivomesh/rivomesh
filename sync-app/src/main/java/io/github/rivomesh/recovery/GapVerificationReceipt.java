package io.github.rivomesh.recovery;

import io.github.rivomesh.cdc.Codec;
import java.time.Instant;

/** Equality of the reviewed tables at one sealed cut, never a claim of current live equality. */
public final class GapVerificationReceipt {
    public int version=1;
    public String outcome="VERIFIED_AT_COMMON_CUT",verifiedAt;
    public GapVerificationPlan plan;
    public String executionRequest,executedBy,executionReason;
    public void validate(){
        if(version!=1||!"VERIFIED_AT_COMMON_CUT".equals(outcome)||plan==null)throw new IllegalArgumentException("GAP_VERIFICATION_RECEIPT_INVALID");
        plan.validate();validateApproval(executionRequest,executedBy,executionReason);try{Instant.parse(verifiedAt);}catch(Exception invalid){throw new IllegalArgumentException("GAP_VERIFICATION_RECEIPT_INVALID");}
    }
    public static void validateApproval(String request,String actor,String reason){
        if(request==null||!request.matches("[A-Za-z0-9_-]{8,128}")||actor==null||actor.isEmpty()||actor.length()>128||reason==null||reason.trim().length()<8||reason.length()>1000)throw new IllegalArgumentException("GAP_VERIFICATION_APPROVAL_INVALID");
    }
    public void verifyApproval(String request,String actor,String reason){
        validate();validateApproval(request,actor,reason);if(!executionRequest.equals(request)||!executedBy.equals(actor)||!executionReason.equals(reason))throw new IllegalStateException("GAP_VERIFICATION_APPROVAL_CONFLICT");
    }
    public String hash(){validate();return Codec.sha256(Codec.encode(this));}
}
