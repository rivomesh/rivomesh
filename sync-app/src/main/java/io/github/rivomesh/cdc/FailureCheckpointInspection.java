package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.recovery.*;
import java.time.Instant;
import java.util.*;

/** Read-only observation, never a recovery approval. No activation, normalization or ACK. */
public final class FailureCheckpointInspection {
    private static final long MAXIMUM_TRANSACTION_BYTES=4L*1024*1024;
    private FailureCheckpointInspection(){}
    public enum Verdict { CHECKPOINT_NOT_ADVANCED, COMMITTED_UNACKNOWLEDGED }
    public static final class Observation {
        public String evidenceId,evidenceHash,observedAt,subscriptionId,pipelineId,generation,fingerprint,projectedHash;
        public long journalEpoch,sequence,acknowledged,errorGeneration;
        public SinkAdapter.Checkpoint targetCheckpoint;
        public Verdict verdict;
        /** A later mutation must fence this older target epoch before changing the journal. */
        public boolean targetFenceRequired;
    }
    /** Public entry derives identity, target and mappings from the current validated task config. */
    public static Observation inspect(PipelineConfig config,FailureEvidenceStore store,String evidenceId)throws Exception{
        FailureEvidenceStore.Envelope retained=store.load(evidenceId);
        FailureEvidence value=retained.value;
        if(!config.pipelineId.equals(value.review.pipelineId)||!config.fingerprint().equals(value.review.fingerprint))throw changed("CONFIGURATION");
        PipelineConfig.Target target=config.targets.stream().filter(t->t.id.equals(value.review.subscription.id)).findFirst().orElseThrow(()->changed("SUBSCRIPTION"));
        if(target.product!=TargetProduct.POSTGRESQL&&target.product!=TargetProduct.MYSQL)throw new IllegalStateException("FAILURE_CHECKPOINT_TARGET_UNSUPPORTED");
        JdbcSink.Settings settings=config.targetSettings(target,value.review.generation);
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            return inspect(journal,retained,settings,()->JdbcSink.existing(settings));
        }
    }
    interface TargetOpen { SinkAdapter open()throws Exception; }
    /** Package-private seam for fault injection; production always opens existing native metadata. */
    static Observation inspect(JdbcJournal journal,FailureEvidenceStore.Envelope retained,JdbcSink.Settings settings,TargetOpen targetOpen)throws Exception{
        FailureEvidence value=retained.value;value.validate();
        if(!FailureEvidence.hash(retained.hash))throw new IllegalArgumentException("FAILURE_EVIDENCE_INVALID");
        Journal.FailureReview expected=value.review;
        if(!Objects.equals(settings.id,expected.subscription.id)||!Objects.equals(settings.pipelineId,expected.pipelineId)||!Objects.equals(settings.fingerprint,expected.fingerprint)||!Objects.equals(settings.generation,expected.generation))throw changed("CONFIGURATION");
        Journal.FailureReview before=journal.inspectFailure(expected.subscription.id,expected.subscription.errorGeneration,MAXIMUM_TRANSACTION_BYTES);
        verify(expected,before);
        SourceTransaction projected=projected(value,settings);
        String hash=Codec.sha256(Codec.encode(projected));
        SinkAdapter.Checkpoint checkpoint;
        try(SinkAdapter sink=targetOpen.open()){
            if(!settings.id.equals(sink.id())||sink.consistency()!=SinkAdapter.Consistency.TRANSACTIONAL)throw new IllegalStateException("FAILURE_CHECKPOINT_TARGET_UNSUPPORTED");
            checkpoint=sink.checkpoint();
        }
        if(checkpoint==null||checkpoint.epoch<0||checkpoint.epoch>before.epoch)throw new IllegalStateException("FAILURE_CHECKPOINT_EPOCH_DIVERGED");
        if(checkpoint.sequence==before.sequence&&checkpoint.skipOperation!=null)throw new IllegalStateException("FAILURE_CHECKPOINT_SKIP_RECOVERY_REQUIRED");
        Verdict verdict;
        if(checkpoint.sequence==before.subscription.acknowledged){
            // A zero sequence must be the native empty checkpoint, not a contradictory receipt.
            if(checkpoint.sequence==0&&(checkpoint.transactionId!=null||checkpoint.transactionHash!=null)||checkpoint.sequence>0&&(checkpoint.transactionId==null||checkpoint.transactionId.isEmpty()||!FailureEvidence.hash(checkpoint.transactionHash)))throw new IllegalStateException("FAILURE_CHECKPOINT_CONTENT_MISMATCH");
            verdict=Verdict.CHECKPOINT_NOT_ADVANCED;
        }else if(checkpoint.sequence==before.sequence){
            if(!projected.id.equals(checkpoint.transactionId)||!hash.equals(checkpoint.transactionHash))throw new IllegalStateException("FAILURE_CHECKPOINT_CONTENT_MISMATCH");
            verdict=Verdict.COMMITTED_UNACKNOWLEDGED;
        }else throw new IllegalStateException("FAILURE_CHECKPOINT_SEQUENCE_DIVERGED");
        // Detect resume, ownership change, normalization or changed failure during native I/O.
        verify(expected,journal.inspectFailure(expected.subscription.id,expected.subscription.errorGeneration,MAXIMUM_TRANSACTION_BYTES));
        Observation result=new Observation();result.evidenceId=value.id;result.evidenceHash=retained.hash;result.observedAt=Instant.now().toString();result.subscriptionId=expected.subscription.id;result.pipelineId=expected.pipelineId;
        result.generation=expected.generation;result.fingerprint=expected.fingerprint;result.projectedHash=hash;result.journalEpoch=before.epoch;result.sequence=before.sequence;result.acknowledged=before.subscription.acknowledged;result.errorGeneration=before.subscription.errorGeneration;
        result.targetCheckpoint=Codec.decode(Codec.encode(checkpoint),SinkAdapter.Checkpoint.class);result.targetFenceRequired=checkpoint.epoch<before.epoch;result.verdict=verdict;return result;
    }
    static void verify(Journal.FailureReview expected,Journal.FailureReview actual){
        if(!Codec.encode(expected).equals(Codec.encode(actual)))throw changed("JOURNAL");
    }
    static SourceTransaction projected(FailureEvidence value,JdbcSink.Settings settings){
        Journal.FailureReview expected=value.review;
        if(expected.normalized==null)throw new IllegalStateException("FAILURE_CHECKPOINT_NORMALIZATION_REQUIRED");
        SourceTransaction projected=TransactionProjection.project(expected.normalized,change->value.schemas.get(change.tableId+"/"+change.schemaVersion),table->settings.databases.containsKey(table.database));
        if(expected.failure.projectedHash!=null&&!Codec.sha256(Codec.encode(projected)).equals(expected.failure.projectedHash))throw changed("PROJECTION");
        return projected;
    }
    private static IllegalStateException changed(String part){return new IllegalStateException("FAILURE_CHECKPOINT_"+part+"_CHANGED");}
}
