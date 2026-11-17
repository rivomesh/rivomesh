package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.*;
import io.github.rivomesh.repair.RepairProbe;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.*;

/** Internal fresh verification. A source record is committed before releasing the mutation fence. */
final class JdbcGapVerification {
    private JdbcGapVerification(){}
    static final class Observation {
        public String outcome="MATCHED_AT_COMMON_CUT",planHash,reportId,sourceSeal,observedAt;
        public long epoch,sequence;
        public Map<String,RepairProbe.Result> tables=new TreeMap<>();
    }
    interface Record<T>{T record(Connection transaction,Observation observation)throws Exception;}

    static GapVerificationReceipt verifyAndClose(JdbcJournal journal,JdbcSink.Settings settings,ValidationStore report,GapVerificationPlan plan,Path scratch,long maximumMillis,String request,String actor,String reason)throws Exception{
        verifyScope(journal,settings,plan);GapVerificationReceipt.validateApproval(request,actor,reason);
        if(maximumMillis<1000||maximumMillis>86400000)throw new IllegalArgumentException("GAP_VERIFICATION_LIMIT_INVALID");
        List<String> subscriptions=new ArrayList<>();for(Journal.Subscription sub:journal.subscriptions())subscriptions.add(sub.id);journal.verifyIdentity(plan.fingerprint,subscriptions);
        // A lost commit response can be resolved even after an operator has resumed the pipeline.
        // Replaying a receipt only reports historical success; it never repeats or refreshes a scan.
        GapVerificationReceipt previous=replay(journal,plan);if(previous!=null){previous.verifyApproval(request,actor,reason);return previous;}
        try{return verifyAndRecord(journal,settings,report,plan,scratch,maximumMillis,(c,observation)->JdbcGapVerificationLedger.record(journal,c,plan,observation,request,actor,reason));}
        catch(IllegalStateException concurrent){
            if(!"GAP_ALREADY_VERIFIED".equals(concurrent.getMessage()))throw concurrent;
            previous=replay(journal,plan);if(previous!=null){previous.verifyApproval(request,actor,reason);return previous;}throw concurrent;
        }
    }

    private static GapVerificationReceipt replay(JdbcJournal journal,GapVerificationPlan plan)throws Exception{
        try(Connection c=journal.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);journal.locked(c);GapVerificationReceipt receipt=JdbcGapVerificationLedger.replay(journal,c,plan);deadline.verify();c.rollback();return receipt;
        }
    }

    private static void verifyScope(JdbcJournal journal,JdbcSink.Settings settings,GapVerificationPlan plan){
        plan.validate();
        if(!plan.pipelineId.equals(journal.settings.pipelineId)||!plan.pipelineId.equals(settings.pipelineId)||!plan.subscriptionId.equals(settings.id)||!plan.fingerprint.equals(settings.fingerprint)||!plan.generation.equals(settings.generation)||!Arrays.asList(TargetProduct.POSTGRESQL,TargetProduct.MYSQL,TargetProduct.H2).contains(settings.product))throw new IllegalStateException("GAP_VERIFICATION_SCOPE_CHANGED");
    }

    static <T>T verifyAndRecord(JdbcJournal journal,JdbcSink.Settings settings,ValidationStore report,GapVerificationPlan plan,Path scratch,long maximumMillis,Record<T> record)throws Exception{
        plan.validate();if(maximumMillis<1000||maximumMillis>86400000||record==null)throw new IllegalArgumentException("GAP_VERIFICATION_LIMIT_INVALID");
        verifyScope(journal,settings,plan);
        ValidationStore.Manifest manifest=report.manifest();
        if(!plan.certificateHash.equals(Codec.sha256(Codec.encode(manifest.certificate)))||!plan.sourceSeal.equals(manifest.sourceSeal))throw new IllegalStateException("GAP_VERIFICATION_REPORT_CHANGED");
        List<String> subscriptions=new ArrayList<>();for(Journal.Subscription sub:journal.subscriptions())subscriptions.add(sub.id);journal.verifyIdentity(plan.fingerprint,subscriptions);
        try(PausedRepairFence fence=new PausedRepairFence(journal,manifest.certificate,maximumMillis)){
            Connection transaction=fence.transaction();List<SkipPlan> originals=new ArrayList<>();Map<String,SkipReceipt> receipts=new TreeMap<>();
            for(Map.Entry<String,GapVerificationPlan.Gap> entry:plan.gaps.entrySet()){
                JdbcSkipJournal.Stored stored=JdbcSkipJournal.load(journal,transaction,entry.getKey());
                if(stored==null||!"COMPLETED".equals(stored.state)||!entry.getValue().planHash.equals(stored.hash)||stored.receipt==null||!entry.getValue().receiptHash.equals(stored.receipt.hash()))throw new IllegalStateException("GAP_VERIFICATION_GAP_CHANGED");
                JdbcGapVerificationLedger.requireOpen(journal,transaction,entry.getKey());
                originals.add(stored.plan);receipts.put(stored.plan.id,stored.receipt);
            }
            GapVerificationPlan reconstructed=GapVerificationPlan.create(report,originals,receipts,plan.subscriptionId,plan.id,plan.createdBy,plan.reason);reconstructed.createdAt=plan.createdAt;reconstructed.maximumMillis=plan.maximumMillis;
            if(!plan.hash().equals(reconstructed.hash()))throw new IllegalStateException("GAP_VERIFICATION_PLAN_CHANGED");
            Observation observation=new Observation();observation.planHash=plan.hash();observation.reportId=plan.reportId;observation.sourceSeal=plan.sourceSeal;observation.epoch=plan.epoch;observation.sequence=plan.sequence;
            try(JdbcSink sink=JdbcSink.existing(settings)){
                for(Map.Entry<String,GapVerificationPlan.Table> entry:plan.tables.entrySet()){
                    fence.verify();TableSchema table=manifest.schemas.get(entry.getKey());SinkAdapter.Checkpoint checkpoint=manifest.certificate.targetCheckpoints.get(plan.subscriptionId);
                    sink.checkRepairDependencies(table,checkpoint);
                    try(RepairProbe probe=new RepairProbe(scratch,table,manifest.limits.maximumBytes,manifest.limits.maximumRows,manifest.limits.maximumRowBytes);TargetRead reader=sink.validationRead(table,checkpoint,maximumMillis,manifest.limits.maximumRowBytes)){
                        for(List<TargetRead.Row> rows;!(rows=reader.next(128)).isEmpty();){fence.verify();for(TargetRead.Row row:rows)probe.append(row);}
                        reader.verify();RepairProbe.Result current=probe.finish();
                        if(current.rows!=entry.getValue().rows||!current.root.equals(entry.getValue().sourceRoot))throw new IllegalStateException("GAP_LIVE_DATA_DIFFERENCES");
                        observation.tables.put(entry.getKey(),current);
                    }
                }
                // Every service mutation and resume takes the same source job row fence.
                // Target data is exclusively written by the service; external business writes are excluded.
                fence.verify();observation.observedAt=Instant.now().toString();T result=record.record(transaction,observation);fence.commitRecord();return result;
            }
        }
    }
}
