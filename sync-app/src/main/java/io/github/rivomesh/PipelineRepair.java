package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import io.github.rivomesh.repair.*;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.Path;
import java.util.*;

/** Historical COMMON_CUT reports, explicit plans and SQL per-table receipts. */
public final class PipelineRepair {
    private PipelineRepair(){}
    public static RepairPlan plan(PipelineConfig config,Path reports,String reportId,Set<String> scope,String actor)throws Exception{
        return plan(config,reports,reportId,scope,actor,UUID.randomUUID().toString(),java.time.Instant.now().toString());
    }
    public static RepairPlan plan(PipelineConfig config,Path reports,String reportId,Set<String> scope,String actor,String id,String createdAt)throws Exception{
        try(ValidationStore store=ValidationStore.open(reports,reportId)){
            ValidationStore.Manifest manifest=store.manifest();if(!Objects.equals(config.fingerprint(),manifest.certificate.fingerprint))throw new IllegalArgumentException("VALIDATION_CONFIG_MISMATCH");
            if(java.nio.file.Files.exists(reports.resolve(reportId).resolve("repair-plans").resolve(id+".json"))){RepairPlan old=RepairPlans.load(reports,reportId,id);RepairPlans.verifyReport(old,store);if(!old.tables.keySet().equals(scope)||!Objects.equals(old.createdBy,actor)||!Objects.equals(old.createdAt,createdAt))throw new IllegalStateException("REPAIR_PLAN_CONFLICT");return old;}
            PipelineValidationBarrier.verifyCertificate(config,manifest.certificate);RepairPlan plan=RepairPlans.create(store,scope,actor,id,createdAt);
            for(RepairPlan.Table table:plan.tables.values())try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target(config,table.target),plan.certificate.generation))){sink.checkRepairDependencies(manifest.schemas.get(table.tableId),plan.certificate.targetCheckpoints.get(table.target));}
            RepairPlans.save(reports,plan);return plan;
        }
    }
    public static RepairPlan readPlan(PipelineConfig config,Path reports,String reportId,String planId)throws Exception{
        try(ValidationStore store=ValidationStore.open(reports,reportId)){RepairPlan plan=RepairPlans.load(reports,reportId,planId);if(!Objects.equals(config.fingerprint(),plan.certificate.fingerprint))throw new IllegalArgumentException("VALIDATION_CONFIG_MISMATCH");if(!Objects.equals(plan.sourceSeal,store.manifest().sourceSeal))throw new IllegalStateException("REPAIR_REPORT_CHANGED");return plan;}
    }
    private static PipelineConfig.Target target(PipelineConfig config,String id){
        for(PipelineConfig.Target target:config.targets)if(id.equals(target.id)){
            if(target.product!=TargetProduct.MYSQL&&target.product!=TargetProduct.POSTGRESQL)throw new IllegalArgumentException("REPAIR_TARGET_PROFILE_UNVERIFIED");return target;
        }throw new IllegalArgumentException("REPAIR_TARGET_UNKNOWN");
    }
    public static Map<String,JdbcSink.RepairReceipt> execute(PipelineConfig config,Path reports,String reportId,String planId,String approvedHash,String actor,String requestId)throws Exception{
        if(actor==null||actor.isEmpty()||actor.length()>128||requestId==null||!UUID.fromString(requestId).toString().equals(requestId))throw new IllegalArgumentException("REPAIR_APPROVAL_INVALID");
        try(ValidationStore store=ValidationStore.open(reports,reportId)){
            RepairPlan plan=RepairPlans.load(reports,reportId,planId);RepairPlans.verifyReport(plan,store);
            if(!Objects.equals(config.fingerprint(),plan.certificate.fingerprint)||!Objects.equals(plan.hash,approvedHash))throw new IllegalArgumentException("REPAIR_APPROVAL_HASH_MISMATCH");
            Map<String,JdbcSink.RepairReceipt> result=new TreeMap<>();List<RepairPlan.Table> pending=new ArrayList<>();
            for(RepairPlan.Table table:plan.tables.values()){
                PipelineConfig.Target target=target(config,table.target);try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target,plan.certificate.generation))){
                    JdbcSink.RepairReceipt receipt=sink.repairReceipt(plan,table);if(receipt==null)pending.add(table);else result.put(table.target+"/"+table.tableId,receipt);
                }
            }
            if(pending.isEmpty())return result;
            PipelineValidationBarrier.verifyCertificate(config,plan.certificate);
            for(RepairPlan.Table table:pending)try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target(config,table.target),plan.certificate.generation))){sink.checkRepairDependencies(store.manifest().schemas.get(table.tableId),plan.certificate.targetCheckpoints.get(table.target));}
            String requestHash=Codec.sha256(Codec.encode(Arrays.asList(plan.hash,actor,requestId)));
            try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings());PausedRepairFence fence=new PausedRepairFence(journal,plan.certificate,plan.maximumMillis)){
                journal.verifyIdentity(config.fingerprint(),config.subscribers());
                for(RepairPlan.Table table:pending)try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target(config,table.target),plan.certificate.generation))){
                    fence.verify();sink.prepareRepair();
                    result.put(table.target+"/"+table.tableId,sink.repair(plan,table,store,reports.resolve(reportId).resolve("repair-scratch"),actor,requestHash,fence));
                }
            }
            return result;
        }
    }
}
