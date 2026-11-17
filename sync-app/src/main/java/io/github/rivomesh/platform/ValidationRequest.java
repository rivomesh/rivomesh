package io.github.rivomesh.platform;

import io.github.rivomesh.validation.ValidationStore;
import java.util.*;

/** Persisted job inputs contain scope/limits, never database credentials or business values. */
public final class ValidationRequest {
    public Set<String> targets=new TreeSet<>();
    public String reportId;
    public long gateMillis=30000,sourceMillis=3600000,targetMillis=600000;
    public ValidationStore.Limits limits=new ValidationStore.Limits();
    void validate(boolean retry){
        if(limits==null)throw new IllegalArgumentException("VALIDATION_LIMIT_INVALID");limits.validate();
        if(gateMillis<1000||gateMillis>120000||sourceMillis<gateMillis||sourceMillis>86400000||targetMillis<1000||targetMillis>86400000)throw new IllegalArgumentException("VALIDATION_LIMIT_INVALID");
        if(retry){if(reportId==null||!UUID.fromString(reportId).toString().equals(reportId))throw new IllegalArgumentException("VALIDATION_ID_INVALID");}
        else if(reportId!=null||targets==null||targets.isEmpty()||targets.stream().anyMatch(id->id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")))throw new IllegalArgumentException("VALIDATION_SCOPE_INVALID");
        targets=targets==null?new TreeSet<>():new TreeSet<>(targets);
    }
}
