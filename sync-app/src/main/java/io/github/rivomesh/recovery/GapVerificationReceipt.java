package io.github.rivomesh.recovery;

import io.github.rivomesh.cdc.Codec;
import java.time.Instant;

/** Equality of the reviewed tables at one sealed cut, never a claim of current live equality. */
public final class GapVerificationReceipt {
    public int version=1;
    public String outcome="VERIFIED_AT_COMMON_CUT",verifiedAt;
    public GapVerificationPlan plan;
    public void validate(){
        if(version!=1||!"VERIFIED_AT_COMMON_CUT".equals(outcome)||plan==null)throw new IllegalArgumentException("GAP_VERIFICATION_RECEIPT_INVALID");
        plan.validate();try{Instant.parse(verifiedAt);}catch(Exception invalid){throw new IllegalArgumentException("GAP_VERIFICATION_RECEIPT_INVALID");}
    }
    public String hash(){validate();return Codec.sha256(Codec.encode(this));}
}
