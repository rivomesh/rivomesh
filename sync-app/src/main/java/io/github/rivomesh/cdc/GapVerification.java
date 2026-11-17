package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.PipelineValidationBarrier;
import io.github.rivomesh.recovery.*;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

/** Public service boundary: immutable source-side plans and explicit, fenced confirmation. */
public final class GapVerification {
    private GapVerification(){}
    private static PipelineConfig.Target target(PipelineConfig config,String id){
        for(PipelineConfig.Target t:config.targets)if(id.equals(t.id)){
            if(t.product!=TargetProduct.POSTGRESQL&&t.product!=TargetProduct.MYSQL)throw new IllegalArgumentException("GAP_VERIFICATION_TARGET_UNVERIFIED");return t;
        }
        throw new IllegalArgumentException("GAP_VERIFICATION_TARGET_UNKNOWN");
    }
    static GapVerificationPlan load(JdbcJournal j,Connection c,String id)throws Exception{
        if(id==null||!id.matches("[A-Za-z0-9_-]{8,128}"))throw new IllegalArgumentException("GAP_VERIFICATION_PLAN_INVALID");
        try(PreparedStatement p=c.prepareStatement("SELECT plan_hash,payload FROM "+j.table("gap_verification_plan")+" WHERE pipeline_id=? AND operation_id=?")){
            p.setString(1,j.settings.pipelineId);p.setString(2,id);try(ResultSet r=p.executeQuery()){
                if(!r.next())return null;String text=JdbcGapVerificationLedger.bounded(r.getCharacterStream(2),512*1024);
                if(!Codec.sha256(text).equals(r.getString(1)))throw new IllegalStateException("GAP_VERIFICATION_PLAN_CHANGED");GapVerificationPlan plan=Codec.decode(text,GapVerificationPlan.class);plan.validate();
                if(!id.equals(plan.id)||!j.settings.pipelineId.equals(plan.pipelineId)||!plan.hash().equals(r.getString(1)))throw new IllegalStateException("GAP_VERIFICATION_PLAN_CHANGED");return plan;
            }
        }
    }
    private static GapVerificationPlan required(JdbcJournal j,Connection c,String id)throws Exception{GapVerificationPlan plan=load(j,c,id);if(plan==null)throw new IllegalStateException("GAP_VERIFICATION_PLAN_NOT_FOUND");return plan;}
    private static void sameRequest(GapVerificationPlan plan,String report,String subscription,Set<String> gaps,String actor,String reason,String createdAt,long maximumMillis){
        if(!plan.reportId.equals(report)||!plan.subscriptionId.equals(subscription)||!plan.gaps.keySet().equals(gaps)||!plan.createdBy.equals(actor)||!plan.reason.equals(reason)||!plan.createdAt.equals(createdAt)||plan.maximumMillis!=maximumMillis)throw new IllegalStateException("GAP_VERIFICATION_REQUEST_CONFLICT");
    }
    public static GapVerificationPlan prepare(PipelineConfig config,Path reports,String reportId,String subscription,Set<String> gaps,String id,String actor,String reason,String createdAt,long maximumMillis,long errorGeneration)throws Exception{
        target(config,subscription);
        if(gaps==null||gaps.isEmpty()||gaps.size()>100)throw new IllegalArgumentException("GAP_VERIFICATION_SCOPE_INVALID");
        try(JdbcJournal j=new JdbcJournal(config.existingJournalSettings())){
            j.verifyIdentity(config.fingerprint(),config.subscribers());
            try(Connection c=j.connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,30000)){
                c.setAutoCommit(false);j.locked(c);GapVerificationPlan previous=load(j,c,id);guard.verify();c.rollback();
                if(previous!=null){sameRequest(previous,reportId,subscription,gaps,actor,reason,createdAt,maximumMillis);if(errorGeneration!=previous.errorGeneration)throw new IllegalStateException("ERROR_GENERATION_CHANGED");return previous;}
            }
            try(ValidationStore report=ValidationStore.open(reports,reportId)){
                ValidationStore.Manifest manifest=report.manifest();PipelineValidationBarrier.verifyCertificate(config,manifest.certificate);
                try(PausedRepairFence fence=new PausedRepairFence(j,manifest.certificate,maximumMillis)){
                    Connection c=fence.transaction();List<SkipPlan> originals=new ArrayList<>();Map<String,SkipReceipt> receipts=new TreeMap<>();
                    GapVerificationPlan previous=load(j,c,id);
                    if(previous!=null){sameRequest(previous,reportId,subscription,gaps,actor,reason,createdAt,maximumMillis);if(errorGeneration!=previous.errorGeneration)throw new IllegalStateException("ERROR_GENERATION_CHANGED");return previous;}
                    for(String gap:new TreeSet<>(gaps)){
                        JdbcSkipJournal.Stored skip=JdbcSkipJournal.load(j,c,gap);if(skip==null||!"COMPLETED".equals(skip.state))throw new IllegalStateException("GAP_VERIFICATION_GAP_CHANGED");JdbcGapVerificationLedger.requireOpen(j,c,gap);originals.add(skip.plan);receipts.put(gap,skip.receipt);
                    }
                    GapVerificationPlan plan=GapVerificationPlan.create(report,originals,receipts,subscription,id,actor,reason);plan.createdAt=createdAt;plan.maximumMillis=maximumMillis;plan.validate();
                    if(errorGeneration!=plan.errorGeneration)throw new IllegalStateException("ERROR_GENERATION_CHANGED");
                    JdbcJournal.execute(c,"INSERT INTO "+j.table("gap_verification_plan")+" (pipeline_id,operation_id,plan_hash,payload,created_at) VALUES (?,?,?,?,?)",config.pipelineId,id,plan.hash(),Codec.encode(plan),createdAt);fence.commitRecord();return plan;
                }
            }
        }
    }
    public static Map<String,Object> read(PipelineConfig config,String id)throws Exception{
        try(JdbcJournal j=new JdbcJournal(config.existingJournalSettings());Connection c=j.connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,30000)){
            j.verifyIdentity(config.fingerprint(),config.subscribers());c.setAutoCommit(false);j.locked(c);GapVerificationPlan plan=required(j,c,id);if(!plan.fingerprint.equals(config.fingerprint()))throw new IllegalStateException("GAP_VERIFICATION_SCOPE_CHANGED");GapVerificationReceipt receipt=JdbcGapVerificationLedger.replay(j,c,plan);
            Map<String,Object> result=new LinkedHashMap<>();result.put("id",id);result.put("hash",plan.hash());result.put("plan",plan);result.put("sequence",Long.toString(plan.sequence));result.put("receipt",receipt==null?null:receiptView(receipt));result.put("state",receipt==null?"PREPARED":"VERIFIED_AT_COMMON_CUT");result.put("code",receipt==null?"GAP_VERIFICATION_PLAN_READY":"GAP_VERIFICATION_COMPLETED");guard.verify();c.rollback();return result;
        }
    }
    public static GapVerificationReceipt execute(PipelineConfig config,Path reports,String id,String approvedHash,String subscription,long errorGeneration,String request,String actor,String reason)throws Exception{
        GapVerificationReceipt.validateApproval(request,actor,reason);PipelineConfig.Target destination=target(config,subscription);
        try(JdbcJournal j=new JdbcJournal(config.existingJournalSettings())){
            j.verifyIdentity(config.fingerprint(),config.subscribers());GapVerificationPlan plan;
            try(Connection c=j.connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,30000)){
                c.setAutoCommit(false);j.locked(c);plan=required(j,c,id);
                if(!plan.hash().equals(approvedHash)||!plan.fingerprint.equals(config.fingerprint())||!plan.subscriptionId.equals(subscription)||plan.errorGeneration!=errorGeneration)throw new IllegalStateException("GAP_VERIFICATION_APPROVAL_SCOPE_CHANGED");
                GapVerificationReceipt receipt=JdbcGapVerificationLedger.replay(j,c,plan);guard.verify();c.rollback();if(receipt!=null){receipt.verifyApproval(request,actor,reason);return receipt;}
            }
            try(ValidationStore report=ValidationStore.open(reports,plan.reportId)){
                return JdbcGapVerification.verifyAndClose(j,config.targetSettings(destination,plan.generation),report,plan,reports.resolve(plan.reportId).resolve("gap-scratch"),plan.maximumMillis,request,actor,reason);
            }
        }
    }
    public static Map<String,Object> receiptView(GapVerificationReceipt receipt){
        receipt.validate();Map<String,Object> result=new LinkedHashMap<>();result.put("hash",receipt.hash());result.put("outcome",receipt.outcome);result.put("verifiedAt",receipt.verifiedAt);result.put("executionRequest",receipt.executionRequest);result.put("executedBy",receipt.executedBy);result.put("executionReason",receipt.executionReason);result.put("reportId",receipt.plan.reportId);result.put("epoch",receipt.plan.epoch);result.put("sequence",Long.toString(receipt.plan.sequence));return result;
    }
}
