package io.github.rivomesh.recovery;

import io.github.rivomesh.cdc.*;
import java.time.Instant;
import java.util.*;

/** Retains a historical observation without activating a target or changing a journal checkpoint. */
public final class FailureEvidenceCapture {
    private FailureEvidenceCapture(){}
    public static FailureEvidenceStore.Envelope capture(JdbcJournal journal,FailureEvidenceStore store,FailureEvidence.Scope scope,
            String id,String requestId,String revision,String actor,String subscription,long errorGeneration,long maximumTransactionBytes)throws Exception{
        try{return original(store.load(id),scope,requestId,revision,actor,subscription,errorGeneration);}
        catch(IllegalStateException missing){if(!"FAILURE_EVIDENCE_NOT_FOUND".equals(missing.getMessage()))throw missing;}
        FailureEvidence value=new FailureEvidence();value.id=id;value.requestId=requestId;value.configurationRevision=revision;value.createdBy=actor;value.createdAt=Instant.now().toString();value.scope=scope;
        value.review=journal.inspectFailure(subscription,errorGeneration,maximumTransactionBytes);Set<String> versions=new TreeSet<>();
        references(value.review.raw,versions);if(value.review.normalized!=null)references(value.review.normalized,versions);
        value.schemas=journal.historicalSchemas(versions,2L*1024*1024);
        try{return store.retain(value);}
        catch(IllegalStateException concurrent){if(!"FAILURE_EVIDENCE_ID_CONFLICT".equals(concurrent.getMessage()))throw concurrent;return original(store.load(id),scope,requestId,revision,actor,subscription,errorGeneration);}
    }
    public static FailureEvidenceStore.Envelope existing(FailureEvidenceStore store,FailureEvidence.Scope scope,String id,String requestId,String revision,String actor,String subscription,long generation)throws Exception{return original(store.load(id),scope,requestId,revision,actor,subscription,generation);}
    private static FailureEvidenceStore.Envelope original(FailureEvidenceStore.Envelope existing,FailureEvidence.Scope scope,String request,String revision,String actor,String subscription,long generation){
        FailureEvidence value=existing.value;
        if(!Codec.encode(scope).equals(Codec.encode(value.scope))||!Objects.equals(request,value.requestId)||!Objects.equals(revision,value.configurationRevision)||!Objects.equals(actor,value.createdBy)||!Objects.equals(subscription,value.review.subscription.id)||generation!=value.review.subscription.errorGeneration)throw new IllegalStateException("FAILURE_EVIDENCE_REQUEST_CONFLICT");
        return existing;
    }
    private static void references(SourceTransaction tx,Set<String> versions){for(Change change:tx.changes)if(change.operation!=Change.Operation.BARRIER)versions.add(change.tableId+"/"+change.schemaVersion);}
}
