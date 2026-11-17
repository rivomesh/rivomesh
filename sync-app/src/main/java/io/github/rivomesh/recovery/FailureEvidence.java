package io.github.rivomesh.recovery;

import io.github.rivomesh.cdc.*;
import java.time.Instant;
import java.util.*;

/** Retained historical observation. Never a skip/compensation approval or proof of non-commit. */
public final class FailureEvidence {
    public static final class Scope {
        public String taskId,ownerNodeId,ownerDirectoryId;
        public void validate(){
            if(taskId==null||!taskId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||ownerNodeId==null||!ownerNodeId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw invalid();
            uuid(ownerDirectoryId);
        }
    }
    public int version=1;
    public String id,requestId,createdAt,createdBy,configurationRevision;
    public Scope scope;
    public Journal.FailureReview review;
    /** tableId/schemaVersion; only historical schemas referenced by this transaction. */
    public Map<String,TableSchema> schemas=new TreeMap<>();
    public void validate(){
        uuid(id);
        if(version!=1||scope==null||review==null||schemas==null||requestId==null||!requestId.matches("[A-Za-z0-9_-]{8,128}")||createdBy==null||createdBy.isEmpty()||createdBy.length()>128||configurationRevision==null||configurationRevision.isEmpty()||configurationRevision.length()>128)throw invalid();
        scope.validate();try{Instant.parse(createdAt);}catch(Exception bad){throw invalid();}
        Journal.Subscription sub=review.subscription;
        if(!scope.taskId.equals(review.pipelineId)||sub==null||sub.id==null||!sub.id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||!"PAUSED".equals(sub.state)||sub.acknowledged<0||sub.errorGeneration<1||review.epoch<1||review.sequence!=Math.addExact(sub.acknowledged,1)||review.generation==null||review.generation.isEmpty()||!hash(review.fingerprint)||review.failure==null||review.raw==null)throw invalid();
        review.failure.validate();transaction(review.raw,review.rawHash);
        if(!review.generation.equals(review.raw.streamGeneration))throw invalid();
        if(review.failure.sequence!=0&&(review.failure.sequence!=review.sequence||!review.raw.id.equals(review.failure.transactionId)))throw invalid();
        if(review.normalized==null){if(review.normalizedHash!=null)throw invalid();}
        else{
            transaction(review.normalized,review.normalizedHash);
            if(!review.raw.id.equals(review.normalized.id)||!review.raw.streamGeneration.equals(review.normalized.streamGeneration)||!review.raw.position.equals(review.normalized.position))throw invalid();
        }
        Set<String> required=new TreeSet<>();references(review.raw,required);if(review.normalized!=null)references(review.normalized,required);
        if(!schemas.keySet().equals(required))throw new IllegalArgumentException("FAILURE_EVIDENCE_SCHEMA_SCOPE_INVALID");
        for(Map.Entry<String,TableSchema> entry:schemas.entrySet()){
            TableSchema table=entry.getValue();if(table==null)throw invalid();table.validate();
            if(!entry.getKey().equals(table.id+"/"+table.version))throw new IllegalArgumentException("FAILURE_EVIDENCE_SCHEMA_SCOPE_INVALID");
        }
    }
    private static void references(SourceTransaction tx,Set<String> result){for(Change change:tx.changes)if(change.operation!=Change.Operation.BARRIER)result.add(change.tableId+"/"+change.schemaVersion);}
    private static void transaction(SourceTransaction tx,String expected){tx.validate();if(!hash(expected)||!Codec.sha256(Codec.encode(tx)).equals(expected))throw new IllegalArgumentException("FAILURE_EVIDENCE_TRANSACTION_CHANGED");}
    public static boolean hash(String value){return value!=null&&value.matches("[0-9a-f]{64}");}
    public static void uuid(String value){try{if(value==null||!UUID.fromString(value).toString().equals(value))throw invalid();}catch(RuntimeException bad){throw invalid();}}
    private static IllegalArgumentException invalid(){return new IllegalArgumentException("FAILURE_EVIDENCE_INVALID");}
}
