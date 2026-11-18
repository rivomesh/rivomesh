package io.github.rivomesh.platform;

import io.github.rivomesh.cdc.CorrectionBatch;
import io.github.rivomesh.recovery.FailureEvidence;

public final class CorrectionRequest {
    public String evidenceId, evidenceHash, approvedHash, reason;
    public CorrectionBatch batch;
    public String importJson;
    private static final com.fasterxml.jackson.databind.ObjectMapper IMPORT=io.github.rivomesh.cdc.Codec.JSON.copy()
            .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    CorrectionBatch resolvedBatch(){
        if(importJson==null)return batch;
        try{
            CorrectionBatch parsed=IMPORT.readValue(importJson,CorrectionBatch.class);
            if(parsed==null)throw new IllegalArgumentException("CORRECTION_IMPORT_INVALID");
            parsed.validate();
            return parsed;
        }catch(java.io.IOException invalid){
            throw new IllegalArgumentException("CORRECTION_IMPORT_INVALID");
        }
    }

    void validate(boolean schema,boolean execute) {
        FailureEvidence.uuid(evidenceId);
        if(!FailureEvidence.hash(evidenceHash))throw new IllegalArgumentException("CORRECTION_EVIDENCE_REQUIRED");
        if(execute){
            if(!FailureEvidence.hash(approvedHash)||reason==null||reason.trim().length()<8||reason.length()>1000)
                throw new IllegalArgumentException("CORRECTION_APPROVAL_REQUIRED");
        } else if(approvedHash!=null||reason!=null)throw new IllegalArgumentException("CORRECTION_PREVIEW_INVALID");
        if(schema){
            if(batch!=null||importJson!=null)throw new IllegalArgumentException("CORRECTION_SCHEMA_INVALID");
        } else {
            if((batch==null)==(importJson==null))throw new IllegalArgumentException("CORRECTION_BATCH_REQUIRED");
            if(batch!=null)batch.validate();
            else if(importJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>CorrectionBatch.MAX_BYTES)
                throw new IllegalArgumentException("CORRECTION_BATCH_LIMIT");
        }
    }
}
