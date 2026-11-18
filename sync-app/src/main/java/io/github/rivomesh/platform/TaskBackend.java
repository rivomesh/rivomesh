package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import io.github.rivomesh.cdc.KafkaRecoveryReview;
import java.sql.*;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Database controls share the existing transactional lease/fencing protocol. */
public interface TaskBackend {
    Map<String,Object> status(Config config) throws Exception;
    void pause(Config config) throws Exception;
    void resume(Config config, long errorGeneration) throws Exception;
    Map<String,Object> check(Config config) throws Exception;
    default Map<String,Object> status(TaskDefinition task)throws Exception{return task.pipeline==null?status(task.legacy):PipelineBackend.status(task.pipeline);}
    default Map<String,Object> pause(TaskDefinition task)throws Exception{if(task.pipeline==null){pause(task.legacy);return Collections.emptyMap();}return PipelineBackend.pause(task.pipeline);}
    default void resume(TaskDefinition task,long generation)throws Exception{if(task.pipeline==null)resume(task.legacy,generation);else PipelineBackend.resume(task.pipeline,generation);}
    default Map<String,Object> check(TaskDefinition task)throws Exception{return task.pipeline==null?check(task.legacy):PipelineEngine.check(task.pipeline);}
    default void subscription(TaskDefinition task,String id,String action,long generation)throws Exception{if(task.pipeline==null)throw new IllegalArgumentException("SUBSCRIPTION_ACTION_INVALID");PipelineBackend.subscription(task.pipeline,id,action,generation);}
    default KafkaRecoveryReview.Observation reviewKafkaReceipts(TaskDefinition task,String subscription,long generation)throws Exception{
        return KafkaRecoveryReview.inspect(task.pipeline,subscription,generation);
    }
    default Map<String,Object> reviewFailure(TaskDefinition task,java.nio.file.Path directory,io.github.rivomesh.recovery.FailureEvidence.Scope scope,String evidence,String request,String revision,String actor,String subscription,long generation)throws Exception{
        if(task.pipeline==null)throw new IllegalArgumentException("FAILURE_REVIEW_V3_REQUIRED");
        io.github.rivomesh.recovery.FailureEvidenceStore store=new io.github.rivomesh.recovery.FailureEvidenceStore(directory,scope,new io.github.rivomesh.recovery.FailureEvidenceStore.Limits());
        try{io.github.rivomesh.recovery.FailureEvidenceCapture.existing(store,scope,evidence,request,revision,actor,subscription,generation);}
        catch(IllegalStateException missing){
            if(!"FAILURE_EVIDENCE_NOT_FOUND".equals(missing.getMessage()))throw missing;
            try(io.github.rivomesh.cdc.JdbcJournal journal=new io.github.rivomesh.cdc.JdbcJournal(task.pipeline.existingJournalSettings())){
                journal.verifyIdentity(task.pipeline.fingerprint(),task.pipeline.subscribers());
                io.github.rivomesh.recovery.FailureEvidenceCapture.capture(journal,store,scope,evidence,request,revision,actor,subscription,generation,4L*1024*1024);
            }
        }
        Map<String,Object> result=store.summary(evidence);result.put("code","FAILURE_EVIDENCE_READY");
        // Additive protocol5 result; older members return no observation. Retained evidence
        // remains available when a retry can no longer inspect a collected or changed journal.
        try{result.put("checkpointObservation",io.github.rivomesh.cdc.FailureCheckpointInspection.inspect(task.pipeline,store,evidence));}
        catch(InterruptedException stopped){Thread.currentThread().interrupt();throw stopped;}
        catch(Exception unavailable){String problem=unavailable.getMessage();result.put("checkpointProblem",problem!=null&&problem.matches("[A-Z][A-Z0-9_]{1,127}")?problem:"FAILURE_CHECKPOINT_UNAVAILABLE");}
        return result;
    }
    default Map<String,Object> confirmCommit(TaskDefinition task,java.nio.file.Path directory,io.github.rivomesh.recovery.FailureEvidence.Scope scope,CommitRequest request,String operation,String actor,String subscription,long generation)throws Exception{
        request.validate();if(task.pipeline==null)throw new IllegalArgumentException("FAILURE_REVIEW_V3_REQUIRED");
        if(!java.nio.file.Files.isDirectory(directory,java.nio.file.LinkOption.NOFOLLOW_LINKS)||!java.nio.file.Files.exists(directory.resolve("authority.json"),java.nio.file.LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("FAILURE_EVIDENCE_NOT_FOUND");
        io.github.rivomesh.recovery.FailureEvidenceStore store=new io.github.rivomesh.recovery.FailureEvidenceStore(directory,scope,new io.github.rivomesh.recovery.FailureEvidenceStore.Limits());
        io.github.rivomesh.recovery.FailureEvidenceStore.Envelope retained=store.load(request.evidenceId);io.github.rivomesh.recovery.FailureEvidence value=retained.value;
        if(!java.util.Objects.equals(retained.hash,request.evidenceHash))throw new IllegalStateException("FAILURE_EVIDENCE_CONTENT_CHANGED");
        if(!java.util.Objects.equals(subscription,value.review.subscription.id)||generation!=value.review.subscription.errorGeneration)throw new IllegalStateException("FAILURE_ACK_EVIDENCE_SCOPE_CHANGED");
        io.github.rivomesh.cdc.JdbcCommitAcknowledgement.prepare(task.pipeline,value);
        io.github.rivomesh.cdc.JdbcCommitAcknowledgement.Receipt receipt=io.github.rivomesh.cdc.JdbcCommitAcknowledgement.confirm(task.pipeline,store,request.evidenceId,request.evidenceHash,operation,actor,request.reason);
        Map<String,Object> result=new LinkedHashMap<>();result.put("code","FAILURE_ACK_CONFIRMED");result.put("receipt",receipt);result.put("hash",io.github.rivomesh.cdc.Codec.sha256(io.github.rivomesh.cdc.Codec.encode(receipt)));return result;
    }
    default Map<String,Object> correction(TaskDefinition task,java.nio.file.Path directory,
            io.github.rivomesh.recovery.FailureEvidence.Scope scope,CorrectionRequest request,boolean schema,
            String operation,String actor,String subscription,long generation,BooleanSupplier cancelled)throws Exception {
        request.validate(schema,operation!=null);
        if(task.pipeline==null)throw new IllegalArgumentException("CORRECTION_V3_REQUIRED");
        io.github.rivomesh.recovery.FailureEvidenceStore.Envelope retained=reviewedRecoveryEvidence(
                directory,scope,request.evidenceId,request.evidenceHash,subscription,generation,"CORRECTION_EVIDENCE_SCOPE_CHANGED");
        if(schema)return io.github.rivomesh.cdc.TransactionCompensation.correctionTables(task.pipeline,retained);
        return io.github.rivomesh.cdc.TransactionCompensation.run(task.pipeline,retained,request.resolvedBatch(),
                request.approvedHash,operation,actor,request.reason,cancelled);
    }
    default Map<String,Object> compensate(TaskDefinition task,java.nio.file.Path directory,
            io.github.rivomesh.recovery.FailureEvidence.Scope scope,String action,CompensationRequest request,
            String operation,String actor,String subscription,long generation,BooleanSupplier cancelled)throws Exception {
        request.validate(action);
        if (task.pipeline==null) throw new IllegalArgumentException("COMPENSATION_V3_REQUIRED");
        io.github.rivomesh.recovery.FailureEvidenceStore.Envelope retained=reviewedRecoveryEvidence(
                directory,scope,request.evidenceId,request.evidenceHash,subscription,generation,"COMPENSATION_EVIDENCE_SCOPE_CHANGED");
        return io.github.rivomesh.cdc.TransactionCompensation.run(task.pipeline,retained,null,request.approvedHash,operation,actor,request.reason,cancelled);
    }
    default Map<String,Object> recoveryReceipt(TaskDefinition task,java.nio.file.Path directory,
            io.github.rivomesh.recovery.FailureEvidence.Scope scope,TaskService.Operation operation)throws Exception {
        CorrectionRequest correction=operation.correction;
        CompensationRequest compensation=operation.compensation;
        String evidence=correction!=null?correction.evidenceId:compensation.evidenceId;
        String hash=correction!=null?correction.evidenceHash:compensation.evidenceHash;
        io.github.rivomesh.recovery.FailureEvidenceStore.Envelope retained=reviewedRecoveryEvidence(directory,scope,
                evidence,hash,operation.subscriptionId,operation.errorCount,"RECOVERY_EVIDENCE_SCOPE_CHANGED");
        return io.github.rivomesh.cdc.TransactionCompensation.receipt(task.pipeline,retained,
                correction==null?null:correction.resolvedBatch(),correction!=null?correction.approvedHash:compensation.approvedHash,
                operation.id,operation.actorId,correction!=null?correction.reason:compensation.reason);
    }
    static io.github.rivomesh.recovery.FailureEvidenceStore.Envelope reviewedRecoveryEvidence(
            java.nio.file.Path directory,io.github.rivomesh.recovery.FailureEvidence.Scope scope,
            String evidence,String hash,String subscription,long generation,String scopeCode)throws Exception {
        if(!java.nio.file.Files.exists(directory.resolve("authority.json"),java.nio.file.LinkOption.NOFOLLOW_LINKS))
            throw new IllegalStateException("FAILURE_EVIDENCE_NOT_FOUND");
        io.github.rivomesh.recovery.FailureEvidenceStore store=new io.github.rivomesh.recovery.FailureEvidenceStore(
                directory,scope,new io.github.rivomesh.recovery.FailureEvidenceStore.Limits());
        io.github.rivomesh.recovery.FailureEvidenceStore.Envelope retained=store.load(evidence);
        if(!retained.hash.equals(hash))throw new IllegalStateException("FAILURE_EVIDENCE_CONTENT_CHANGED");
        if(!Objects.equals(subscription,retained.value.review.subscription.id)
                ||generation!=retained.value.review.subscription.errorGeneration)throw new IllegalStateException(scopeCode);
        return retained;
    }
    default Map<String,Object> skip(TaskDefinition task,java.nio.file.Path directory,io.github.rivomesh.recovery.FailureEvidence.Scope scope,String action,SkipRequest request,String operation,String actor,String subscription,long generation)throws Exception{
        request.validate(action);if(task.pipeline==null)throw new IllegalArgumentException("SKIP_V3_REQUIRED");
        if(!java.nio.file.Files.isDirectory(directory,java.nio.file.LinkOption.NOFOLLOW_LINKS)||!java.nio.file.Files.exists(directory.resolve("authority.json"),java.nio.file.LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("FAILURE_EVIDENCE_NOT_FOUND");
        io.github.rivomesh.recovery.FailureEvidenceStore store=new io.github.rivomesh.recovery.FailureEvidenceStore(directory,scope,new io.github.rivomesh.recovery.FailureEvidenceStore.Limits());
        io.github.rivomesh.cdc.TransactionSkipRecovery.Result completed;
        if("SKIP_TRANSACTION".equals(action)){
            io.github.rivomesh.recovery.FailureEvidence value=store.load(request.evidenceId).value;if(!Objects.equals(subscription,value.review.subscription.id)||generation!=value.review.subscription.errorGeneration)throw new IllegalStateException("SKIP_PLAN_SCOPE_CHANGED");
            completed=io.github.rivomesh.cdc.TransactionSkipRecovery.execute(task.pipeline,store,request.evidenceId,request.evidenceHash,operation,actor,request.reason,request.acceptDataLoss);
        }else{
            Map<String,Object> plan=io.github.rivomesh.cdc.RecoveryLedger.plan(task.pipeline,request.planId,false);
            if(!Objects.equals(subscription,plan.get("target"))||!Objects.equals(request.planHash,plan.get("planHash"))||!(plan.get("errorGeneration") instanceof Number)||generation!=((Number)plan.get("errorGeneration")).longValue())throw new IllegalStateException("SKIP_PLAN_SCOPE_CHANGED");
            completed="SKIP_RETRY".equals(action)?io.github.rivomesh.cdc.TransactionSkipRecovery.retry(task.pipeline,store,request.planId,request.planHash,operation,actor,request.reason,request.acceptDataLoss):io.github.rivomesh.cdc.TransactionSkipRecovery.resolve(task.pipeline,store,request.planId,request.planHash,operation,actor,request.reason,"SKIP_CANCEL".equals(action));
        }
        Map<String,Object> result=new LinkedHashMap<>();result.put("planId",completed.planId);result.put("planHash",completed.planHash);result.put("state",completed.state);result.put("receipt",completed.receipt);result.put("resolution",completed.resolution);result.put("hash",io.github.rivomesh.cdc.Codec.sha256(io.github.rivomesh.cdc.Codec.encode(completed)));result.put("code","SKIP_CANCEL".equals(action)?"SKIP_CANCELLED":"SKIP_RECOVER".equals(action)?"SKIP_RECOVERED":"SKIP_COMPLETED");return result;
    }
    default Map<String,Object> skipPlan(TaskDefinition task,String plan,boolean nativeProof)throws Exception{if(task.pipeline==null)throw new IllegalArgumentException("SKIP_V3_REQUIRED");return io.github.rivomesh.cdc.RecoveryLedger.plan(task.pipeline,plan,nativeProof);}
    default Map<String,Object> recoveryPage(TaskDefinition task,String subscription,String cursor,int limit,boolean gaps)throws Exception{if(task.pipeline==null)throw new IllegalArgumentException("SKIP_V3_REQUIRED");return gaps?io.github.rivomesh.cdc.RecoveryLedger.gaps(task.pipeline,subscription,cursor,limit):io.github.rivomesh.cdc.RecoveryLedger.plans(task.pipeline,subscription,cursor,limit);}
    default Map<String,Object> gapVerification(TaskDefinition task,java.nio.file.Path reports,GapVerificationRequest request,boolean execute,String operation,String actor,String createdAt,String subscription,long generation)throws Exception{
        request.validate(execute);if(task.pipeline==null)throw new IllegalArgumentException("GAP_VERIFICATION_V3_REQUIRED");
        if(!execute){
            io.github.rivomesh.recovery.GapVerificationPlan plan=io.github.rivomesh.cdc.GapVerification.prepare(task.pipeline,reports,request.reportId,subscription,request.gaps,operation,actor,request.reason,createdAt,request.maximumMillis,generation);
            Map<String,Object> result=new LinkedHashMap<>();result.put("code","GAP_VERIFICATION_PLAN_READY");result.put("id",plan.id);result.put("hash",plan.hash());return result;
        }
        io.github.rivomesh.recovery.GapVerificationReceipt receipt=io.github.rivomesh.cdc.GapVerification.execute(task.pipeline,reports,request.planId,request.approvedHash,subscription,generation,operation,actor,request.reason);
        Map<String,Object> result=new LinkedHashMap<>();result.put("code","GAP_VERIFICATION_COMPLETED");result.put("id",request.planId);result.put("hash",receipt.hash());result.put("receipt",io.github.rivomesh.cdc.GapVerification.receiptView(receipt));return result;
    }
    default Map<String,Object> gapVerificationPlan(TaskDefinition task,String id)throws Exception{
        if(task.pipeline==null)throw new IllegalArgumentException("GAP_VERIFICATION_V3_REQUIRED");return io.github.rivomesh.cdc.GapVerification.read(task.pipeline,id);
    }
    default Map<String,Object> failureEvidence(java.nio.file.Path directory,io.github.rivomesh.recovery.FailureEvidence.Scope scope,String id,String view,int from,int limit)throws Exception{
        if(!java.nio.file.Files.isDirectory(directory,java.nio.file.LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("FAILURE_EVIDENCE_NOT_FOUND");
        if(!java.nio.file.Files.exists(directory.resolve("authority.json"),java.nio.file.LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("FAILURE_EVIDENCE_AUTHORITY_MISSING");
        io.github.rivomesh.recovery.FailureEvidenceStore store=new io.github.rivomesh.recovery.FailureEvidenceStore(directory,scope,new io.github.rivomesh.recovery.FailureEvidenceStore.Limits());
        return view==null?store.summary(id):store.events(id,"NORMALIZED".equals(view),from,limit,256*1024);
    }
    default Map<String,Object> validateRows(TaskDefinition task,java.nio.file.Path reports,ValidationRequest request,boolean retry)throws Exception{
        if(task.pipeline==null)throw new IllegalArgumentException("VALIDATION_V3_REQUIRED");
        if(!retry&&!java.nio.file.Files.exists(reports.resolve(request.reportId)))PipelineReconciliation.stage(task.pipeline,request.targets,reports,request.limits,request.gateMillis,request.sourceMillis,request.reportId);
        return PipelineReconciliation.compare(task.pipeline,reports,request.reportId,request.targetMillis);
    }
    default Map<String,Object> validationReport(TaskDefinition task,java.nio.file.Path reports,String reportId)throws Exception{
        if(!java.nio.file.Files.isDirectory(reports.resolve(reportId)))throw new TaskService.Conflict("VALIDATION_REPORT_NOT_CREATED");
        try(io.github.rivomesh.validation.ValidationStore store=io.github.rivomesh.validation.ValidationStore.open(reports,reportId)){
            Map<String,Object> result=store.summary();java.util.List<Map<String,String>> tables=new java.util.ArrayList<>();
            for(io.github.rivomesh.cdc.TableSchema table:store.manifest().schemas.values()){Map<String,String> row=new LinkedHashMap<>();row.put("id",table.id);row.put("database",table.database);row.put("name",table.name);tables.add(row);}result.put("tables",tables);return result;
        }catch(SyncFailure failure){if("VALIDATION_REPORT_BUSY".equals(failure.code)||"VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED".equals(failure.code))throw new TaskService.Conflict(failure.code);throw failure;}
    }
    default Map<String,Object> validationDifferences(TaskDefinition task,java.nio.file.Path reports,String reportId,String target,String table,String after,int limit,String identity)throws Exception{
        if(limit<1||limit>100||after==null||!after.isEmpty()&&!after.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("VALIDATION_DIFF_PAGE_INVALID");
        if(!java.nio.file.Files.isDirectory(reports.resolve(reportId)))throw new TaskService.Conflict("VALIDATION_REPORT_NOT_CREATED");
        try(io.github.rivomesh.validation.ValidationStore store=io.github.rivomesh.validation.ValidationStore.open(reports,reportId)){
            io.github.rivomesh.validation.ValidationStore.Manifest manifest=store.manifest();
            if(!manifest.targetTables.containsKey(target)||!manifest.targetTables.get(target).contains(table))throw new IllegalArgumentException("VALIDATION_DIFF_SCOPE_INVALID");
            Map<String,Object> result=new LinkedHashMap<>();result.put("reportId",reportId);result.put("target",target);result.put("tableId",table);
            List<Map<String,String>> columns=new ArrayList<>();for(io.github.rivomesh.cdc.TableSchema.Column column:manifest.schemas.get(table).columns){Map<String,String> field=new LinkedHashMap<>();field.put("name",column.name);field.put("type",column.type.name());columns.add(field);}result.put("columns",columns);
            if(identity!=null){result.put("identityHash",identity);result.put("values",store.differenceValues(target,table,identity,1024*1024));}
            else{List<io.github.rivomesh.validation.ValidationStore.Difference> page=store.differences(target,table,after,limit+1);boolean more=page.size()>limit;if(more)page.remove(page.size()-1);result.put("items",page);result.put("next",more?page.get(page.size()-1).identityHash:null);}
            return result;
        }catch(SyncFailure failure){if("VALIDATION_REPORT_BUSY".equals(failure.code)||"VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED".equals(failure.code))throw new TaskService.Conflict(failure.code);throw failure;}
    }

    default Map<String,Object> repair(TaskDefinition task,java.nio.file.Path reports,RepairRequest request,boolean execute,String actor,String operation,String createdAt)throws Exception{
        if(task.pipeline==null)throw new IllegalArgumentException("REPAIR_V3_REQUIRED");
        if(!execute)return repairProjection(PipelineRepair.plan(task.pipeline,reports,request.reportId,request.scope,actor,request.planId,createdAt));
        Map<String,Object> result=new LinkedHashMap<>();result.put("reportId",request.reportId);result.put("planId",request.planId);result.put("approvedHash",request.approvedHash);
        result.put("receipts",PipelineRepair.execute(task.pipeline,reports,request.reportId,request.planId,request.approvedHash,actor,UUID.nameUUIDFromBytes((task.jobId+"/"+operation).getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString()));result.put("code","REPAIR_COMPLETED");return result;
    }
    default Map<String,Object> repairPlan(TaskDefinition task,java.nio.file.Path reports,String report,String id)throws Exception{
        if(task.pipeline==null)throw new IllegalArgumentException("REPAIR_V3_REQUIRED");Map<String,Object> result=repairProjection(PipelineRepair.readPlan(task.pipeline,reports,report,id));try(io.github.rivomesh.validation.ValidationStore store=io.github.rivomesh.validation.ValidationStore.open(reports,report)){Map<String,String> names=new TreeMap<>();for(io.github.rivomesh.cdc.TableSchema table:store.manifest().schemas.values())names.put(table.id,table.database+"."+table.name);result.put("tableNames",names);}return result;
    }
    static Map<String,Object> repairProjection(io.github.rivomesh.repair.RepairPlan plan){
        Map<String,Object> result=new LinkedHashMap<>();result.put("code","REPAIR_PLAN_READY");result.put("id",plan.id);result.put("reportId",plan.reportId);result.put("hash",plan.hash);result.put("createdBy",plan.createdBy);result.put("createdAt",plan.createdAt);result.put("tables",plan.tables);result.put("maximumMillis",plan.maximumMillis);result.put("maximumAffectedRows",plan.maximumAffectedRows);result.put("maximumProbeBytes",plan.maximumProbeBytes);result.put("maximumRowBytes",plan.maximumRowBytes);return result;
    }

    final class Database implements TaskBackend {
        public Map<String,Object> status(Config c) throws Exception {
            try {
                StateStore store = new StateStore(c);
                // openGauss uses product-specific missing-schema SQLStates. Discover without DDL.
                try(Connection connection=store.connect();PreparedStatement p=connection.prepareStatement("SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name='sync_job_v2'")){
                    p.setString(1,c.target.metadataSchema);
                    try(ResultSet rs=p.executeQuery()){if(!rs.next())return Collections.emptyMap();}
                }
                if (!store.exists()) return Collections.emptyMap();
                store.validateConfig(); return store.status(100);
            } catch (SQLException e) {
                if (Arrays.asList("42P01","3F000","3F001").contains(e.getSQLState())) return Collections.emptyMap();
                throw e;
            }
        }
        public void pause(Config c) throws Exception { new StateStore(c).pause(); }
        public void resume(Config c, long generation) throws Exception { new StateStore(c).resume(generation); }
        public Map<String,Object> check(Config c) throws Exception {
            MysqlSource source = new SourceSelector(c).select(null, "");
            source.validateAndIdentity(); int tables = source.discover().size();
            Map<String,Object> result = new LinkedHashMap<>();
            try (Connection target = new StateStore(c).connect()) {
                result.put("tables", tables); result.put("targetProduct", target.getMetaData().getDatabaseProductName());
            }
            return result;
        }
    }
}
