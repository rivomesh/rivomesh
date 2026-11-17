package io.github.rivomesh.recovery;

import io.github.rivomesh.cdc.Codec;
import java.time.Instant;
import java.util.*;

/** Journal-side completion/cancellation actor; immutable separately from native approval. */
public final class SkipResolution {
    public int version=1;
    public String planId,requestId,action,actor,reason,at;
    public void validate(){
        if(version!=1||planId==null||!planId.matches("[A-Za-z0-9_-]{8,128}")||requestId==null||!requestId.matches("[A-Za-z0-9_-]{8,128}")||!Arrays.asList("SKIP_COMMITTED","RECOVER","CANCEL").contains(action)||actor==null||actor.isEmpty()||actor.length()>128||reason==null||reason.trim().length()<8||reason.length()>1000)throw new IllegalArgumentException("SKIP_RESOLUTION_INVALID");
        try{Instant.parse(at);}catch(Exception invalid){throw new IllegalArgumentException("SKIP_RESOLUTION_INVALID");}
    }
    public String hash(){validate();return Codec.sha256(Codec.encode(this));}
    public static SkipResolution create(String plan,String request,String action,String actor,String reason){SkipResolution value=new SkipResolution();value.planId=plan;value.requestId=request;value.action=action;value.actor=actor;value.reason=reason;value.at=Instant.now().toString();value.validate();return value;}
}
