package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.recovery.*;
import java.sql.*;
import java.util.*;

/** Forward recovery across two local commits. Requires authorized whole-task-paused admission. */
public final class TransactionSkipRecovery {
    private TransactionSkipRecovery(){}
    public static final class Result {
        public String planId,planHash,state;
        public SkipReceipt receipt;
        public SkipResolution resolution;
    }
    public static Result execute(PipelineConfig config,FailureEvidenceStore store,String evidence,String hash,String operation,String actor,String reason,boolean acceptDataLoss)throws Exception{
        FailureEvidenceStore.Envelope retained=store.load(evidence);if(!Objects.equals(hash,retained.hash))throw new IllegalStateException("FAILURE_EVIDENCE_CONTENT_CHANGED");
        JdbcSink.Settings target=target(config,retained.value);
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){journal.verifyIdentity(config.fingerprint(),config.subscribers());return execute(journal,retained,target,operation,actor,reason,acceptDataLoss);}
    }
    public static Result resolve(PipelineConfig config,FailureEvidenceStore store,String planId,String planHash,String request,String actor,String reason,boolean cancel)throws Exception{
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());JdbcSkipJournal.Stored stored=lookup(journal,planId);if(stored==null)throw new IllegalStateException("SKIP_PLAN_NOT_FOUND");if(!stored.hash.equals(planHash))throw new IllegalStateException("SKIP_PLAN_CHANGED");
            FailureEvidenceStore.Envelope retained=store.load(stored.plan.evidenceId);JdbcSink.Settings settings=target(config,retained.value);
            return resolve(journal,retained,settings,planId,request,actor,reason,cancel);
        }
    }
    public static Result retry(PipelineConfig config,FailureEvidenceStore store,String planId,String planHash,String request,String actor,String reason,boolean acceptDataLoss)throws Exception{
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());JdbcSkipJournal.Stored stored=lookup(journal,planId);if(stored==null)throw new IllegalStateException("SKIP_PLAN_NOT_FOUND");if(!stored.hash.equals(planHash))throw new IllegalStateException("SKIP_PLAN_CHANGED");
            FailureEvidenceStore.Envelope retained=store.load(stored.plan.evidenceId);return retry(journal,retained,target(config,retained.value),planId,request,actor,reason,acceptDataLoss);
        }
    }
    private static JdbcSink.Settings target(PipelineConfig config,FailureEvidence evidence){
        evidence.validate();if(!config.pipelineId.equals(evidence.review.pipelineId)||!config.fingerprint().equals(evidence.review.fingerprint))throw new IllegalStateException("SKIP_CONFIGURATION_CHANGED");
        PipelineConfig.Target target=config.targets.stream().filter(t->t.id.equals(evidence.review.subscription.id)).findFirst().orElseThrow(()->new IllegalStateException("SKIP_TARGET_SCOPE_CHANGED"));
        if(target.product!=TargetProduct.POSTGRESQL&&target.product!=TargetProduct.MYSQL)throw new IllegalStateException("SKIP_TARGET_UNSUPPORTED");return config.targetSettings(target,evidence.review.generation);
    }
    private static void scope(FailureEvidenceStore.Envelope retained,JdbcSink.Settings settings){
        retained.value.validate();Journal.FailureReview review=retained.value.review;
        if(!Objects.equals(settings.pipelineId,review.pipelineId)||!Objects.equals(settings.id,review.subscription.id)||!Objects.equals(settings.generation,review.generation)||!Objects.equals(settings.fingerprint,review.fingerprint))throw new IllegalStateException("SKIP_TARGET_SCOPE_CHANGED");
    }
    private static JdbcSkipJournal.Stored lookup(JdbcJournal journal,String operation)throws Exception{
        if(operation==null||!operation.matches("[A-Za-z0-9_-]{8,128}"))throw new IllegalArgumentException("SKIP_REQUEST_INVALID");
        try(Connection c=journal.connect()){c.setAutoCommit(false);Map<String,Object> job=journal.locked(c);JdbcSkipJournal.Stored result=((Number)job.get("format")).intValue()==4?JdbcSkipJournal.load(journal,c,operation):null;c.rollback();return result;}
    }
    private static void original(JdbcSkipJournal.Stored stored,FailureEvidenceStore.Envelope retained,JdbcSink.Settings settings){
        SourceTransaction projected=FailureCheckpointInspection.projected(retained.value,settings);
        SkipPlan expected=SkipPlan.create(retained,stored.plan.id,stored.plan.createdBy,stored.plan.reason,projected,stored.plan.expectedCheckpoint);expected.createdAt=stored.plan.createdAt;
        if(!stored.hash.equals(expected.hash()))throw new IllegalStateException("SKIP_PLAN_SCOPE_CHANGED");
    }
    static Result execute(JdbcJournal journal,FailureEvidenceStore.Envelope retained,JdbcSink.Settings settings,String operation,String actor,String reason,boolean acceptDataLoss)throws Exception{
        scope(retained,settings);if(!acceptDataLoss)throw new IllegalArgumentException("SKIP_DATA_LOSS_APPROVAL_REQUIRED");
        String requestHash=SkipPlan.requestHash(operation,retained.value.id,retained.hash,actor,reason);JdbcSkipJournal.Stored existing=lookup(journal,operation);
        if(existing!=null){
            if(!requestHash.equals(existing.plan.requestHash))throw new IllegalStateException("SKIP_REQUEST_CONFLICT");original(existing,retained,settings);
            if("COMPLETED".equals(existing.state))return result(existing);if("CANCELLED".equals(existing.state))throw new IllegalStateException("SKIP_PLAN_CANCELLED");
        }else{
            JdbcSkipJournal.ensureMetadata(journal,retained.value);
            try(Connection c=journal.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
                c.setAutoCommit(false);Map<String,Object> job=journal.locked(c);
                JdbcSkipJournal.Stored raced=((Number)job.get("format")).intValue()==4?JdbcSkipJournal.load(journal,c,operation):null;
                if(raced!=null){if(!requestHash.equals(raced.plan.requestHash))throw new IllegalStateException("SKIP_REQUEST_CONFLICT");original(raced,retained,settings);existing=raced;c.rollback();}
                else if(!"PAUSED".equals(job.get("state")))throw new IllegalStateException("SKIP_REQUIRES_PIPELINE_PAUSED");
                else try(JdbcSink sink=JdbcSink.existing(settings)){
                FailureCheckpointInspection.verify(retained.value.review,new JdbcFailureReview(journal).inspect(c,settings.id,retained.value.review.subscription.errorGeneration,4L*1024*1024));
                SinkAdapter.Checkpoint checkpoint=sink.checkpoint();if(checkpoint.epoch!=retained.value.review.epoch)throw new IllegalStateException("SKIP_TARGET_FENCE_REQUIRED");
                if(checkpoint.sequence!=retained.value.review.subscription.acknowledged)throw new IllegalStateException(checkpoint.skipOperation!=null?"SKIP_RECOVERY_REQUIRED":"SKIP_CHECKPOINT_RECONCILIATION_REQUIRED");
                if(checkpoint.skipOperation!=null)JdbcSkipJournal.verifyCheckpoint(journal,c,job,settings.id,checkpoint);
                SourceTransaction projected=FailureCheckpointInspection.projected(retained.value,settings);SkipPlan plan=SkipPlan.create(retained,operation,actor,reason,projected,checkpoint);
                existing=sink.withSkipPreflight(plan,retained.value,cp->{JdbcSkipJournal.Stored saved=JdbcSkipJournal.prepare(journal,c,plan,retained.value);deadline.verify();c.commit();return saved;});
                }
            }
        }
        return finish(journal,retained,settings,existing.plan,SkipResolution.create(existing.plan.id,operation,"SKIP_COMMITTED",actor,reason),false,false);
    }
    static Result resolve(JdbcJournal journal,FailureEvidenceStore.Envelope retained,JdbcSink.Settings settings,String planId,String request,String actor,String reason,boolean cancel)throws Exception{
        scope(retained,settings);JdbcSkipJournal.Stored stored=lookup(journal,planId);if(stored==null)throw new IllegalStateException("SKIP_PLAN_NOT_FOUND");original(stored,retained,settings);
        SkipResolution resolution=SkipResolution.create(planId,request,cancel?"CANCEL":"RECOVER",actor,reason);
        if("COMPLETED".equals(stored.state)){if(cancel)throw new IllegalStateException("SKIP_ALREADY_COMMITTED");return result(stored);}
        if("CANCELLED".equals(stored.state)){if(!cancel)throw new IllegalStateException("SKIP_PLAN_CANCELLED");return result(stored);}
        return finish(journal,retained,settings,stored.plan,resolution,true,cancel);
    }
    static Result retry(JdbcJournal journal,FailureEvidenceStore.Envelope retained,JdbcSink.Settings settings,String planId,String request,String actor,String reason,boolean acceptDataLoss)throws Exception{
        if(!acceptDataLoss)throw new IllegalArgumentException("SKIP_DATA_LOSS_APPROVAL_REQUIRED");scope(retained,settings);JdbcSkipJournal.Stored stored=lookup(journal,planId);if(stored==null)throw new IllegalStateException("SKIP_PLAN_NOT_FOUND");original(stored,retained,settings);
        SkipResolution resolution=SkipResolution.create(planId,request,"SKIP_COMMITTED",actor,reason);
        if("COMPLETED".equals(stored.state))return result(stored);if("CANCELLED".equals(stored.state))throw new IllegalStateException("SKIP_PLAN_CANCELLED");
        return finish(journal,retained,settings,stored.plan,resolution,false,false);
    }
    private static Result finish(JdbcJournal journal,FailureEvidenceStore.Envelope retained,JdbcSink.Settings settings,SkipPlan plan,SkipResolution resolution,boolean resolving,boolean cancel)throws Exception{
        try(Connection c=journal.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);Map<String,Object> job=journal.locked(c);JdbcSkipJournal.Stored stored=JdbcSkipJournal.load(journal,c,plan.id);if(stored==null)throw new IllegalStateException("SKIP_PLAN_NOT_FOUND");original(stored,retained,settings);
            if("COMPLETED".equals(stored.state)){if(cancel)throw new IllegalStateException("SKIP_ALREADY_COMMITTED");c.rollback();return result(stored);}if("CANCELLED".equals(stored.state))throw new IllegalStateException("SKIP_PLAN_CANCELLED");
            if(!"PAUSED".equals(job.get("state")))throw new IllegalStateException("SKIP_REQUIRES_PIPELINE_PAUSED");
            FailureCheckpointInspection.verify(retained.value.review,new JdbcFailureReview(journal).inspect(c,settings.id,plan.errorGeneration,4L*1024*1024));
            try(JdbcSink sink=JdbcSink.forSkipRecovery(settings)){
                sink.prepareSkipMetadata(plan);
                if(resolving){
                    return sink.resolveSkip(plan,!cancel,receipt->{JdbcSkipJournal.Stored completed=cancel?JdbcSkipJournal.cancel(journal,c,plan,retained.value,resolution):JdbcSkipJournal.complete(journal,c,plan,receipt,retained.value,resolution);deadline.verify();c.commit();return result(completed);});
                }
                SkipReceipt receipt=sink.skip(plan,retained.value,resolution);JdbcSkipJournal.Stored completed=JdbcSkipJournal.complete(journal,c,plan,receipt,retained.value,resolution);deadline.verify();c.commit();return result(completed);
            }
        }
    }
    private static Result result(JdbcSkipJournal.Stored stored){Result result=new Result();result.planId=stored.plan.id;result.planHash=stored.hash;result.state=stored.state;result.receipt=stored.receipt;result.resolution=stored.resolution;return result;}
}
