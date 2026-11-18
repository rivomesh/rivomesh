package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.Journal;
import io.github.rivomesh.cdc.SourceAdapter;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.recovery.CaptureExclusionPlan;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/** Checks source admission at a failed capture checkpoint without changing the journal or source. */
public final class CaptureRecoveryReview {
    private CaptureRecoveryReview() {}

    public static Map<String,Object> inspect(PipelineConfig config)throws Exception{return inspect(config,false,null);}

    static Map<String,Object> inspect(PipelineConfig config,boolean verifyContents,Path archiveDirectory)throws Exception{
        return inspect(config,verifyContents,archiveDirectory,null);
    }

    public static Map<String,Object> retain(PipelineConfig config,Path archiveDirectory,long errorGeneration,
                                             String boundaryHash,Callable<Void> authorize)throws Exception{
        return retain(config,archiveDirectory,errorGeneration,boundaryHash,null,authorize);
    }

    public static Map<String,Object> retain(PipelineConfig config,Path archiveDirectory,long errorGeneration,
                                             String boundaryHash,CaptureArchiveReceipt.Owner original,
                                             Callable<Void> authorize)throws Exception{
        if(original!=null&&!config.pipelineId.equals(original.task()))
            throw new IllegalArgumentException("CAPTURE_EVIDENCE_OWNER_INVALID");
        if(errorGeneration<0||boundaryHash==null||!boundaryHash.matches("[0-9a-f]{64}")||authorize==null)
            throw new IllegalArgumentException("CAPTURE_EVIDENCE_REQUEST_INVALID");
        return inspect(config,true,archiveDirectory,new Retention(errorGeneration,boundaryHash,original,authorize));
    }

    public static Map<String,Object> exclude(PipelineConfig config, Path archiveDirectory,
                long errorGeneration, String boundaryHash, String archiveSha256, String planHash,
                String operation, String actor, String reason, Callable<Void> authorize) throws Exception {
        String approvalHash = Codec.sha256(Codec.encode(Arrays.asList(
                errorGeneration, boundaryHash, archiveSha256, planHash, actor, reason)));
        authorize.call();
        try (JdbcJournal journal = new JdbcJournal(config.existingJournalSettings())) {
            journal.verifyIdentity(config.fingerprint(), config.subscribers());
            Map<String,Object> receipt = journal.excludedCapture(operation, approvalHash);
            if (receipt != null) {
                authorize.call();
                return receipt;
            }
            Map<String,Object> before = journal.status();
            SourcePosition checkpoint = journal.captured();
            if (checkpoint == null || !"SOURCE_DECODE".equals(before.get("errorStage"))
                    || "CLAIMED".equals(before.get("captureLeaseState"))
                    || !Objects.equals(errorGeneration, before.get("errorGeneration")))
                throw new IllegalStateException("SOURCE_REVIEW_STALE");
            String checkpointHash = Codec.sha256(Codec.encode(checkpoint));
            Path directory = MysqlContentArchive.directory(archiveDirectory, false);
            String reference = archiveReference(config,before,checkpointHash,errorGeneration,boundaryHash);
            Map<String,Object> retained = MysqlContentArchive.existing(directory,reference,config.pipelineId,
                    checkpointHash,boundaryHash,errorGeneration);
            if (retained == null) throw new IllegalStateException("CAPTURE_EXCLUSION_ARCHIVE_REQUIRED");
            CaptureExclusionPlan plan = plan(config,journal,before,retained);
            if (!planHash.equals(plan.hash()) || !archiveSha256.equals(plan.archiveSha256))
                throw new IllegalStateException("CAPTURE_EXCLUSION_APPROVAL_STALE");

            Map<?,?> boundary = (Map<?,?>) retained.get("nextTransactionHeaders");
            List<SourcePosition> dependencies = journal.dependencies();
            SourcePosition after;
            try (SourceAdapter opened = PipelineSources.open(config,plan.generation)) {
                if (!(opened instanceof MysqlCdcSource))
                    throw new IllegalStateException("CAPTURE_EXCLUSION_PROFILE_UNSUPPORTED");
                MysqlCdcSource source = (MysqlCdcSource) opened;
                source.validateFailover(checkpoint, dependencies);
                if (!Codec.encode(boundary).equals(Codec.encode(source.inspectDecodeBoundary(checkpoint))))
                    throw new IllegalStateException("CAPTURE_EXCLUSION_BOUNDARY_UNAVAILABLE");
                Map<String,String> payload = new LinkedHashMap<>(checkpoint.getPayload());
                payload.put("offset", (String) boundary.get("endOffset"));
                payload.put("gtidSet", Gtids.union(payload.get("gtidSet"), plan.transactionId));
                after = new SourcePosition(checkpoint.getFormatVersion(),checkpoint.getSourceType(),
                        checkpoint.getSourceId(),payload);
                source.validateFailover(after,dependencies);
            }
            requireUnchanged(journal,before,checkpoint);
            return journal.excludeCapture(plan,after,operation,approvalHash,actor,reason,authorize);
        }
    }

    private static final class Retention {
        final long errorGeneration;
        final String boundaryHash;
        final CaptureArchiveReceipt.Owner original;
        final Callable<Void> authorize;
        Retention(long errorGeneration,String boundaryHash,CaptureArchiveReceipt.Owner original,Callable<Void> authorize){
            this.errorGeneration=errorGeneration;this.boundaryHash=boundaryHash;this.original=original;this.authorize=authorize;
        }
    }

    private static Map<String,Object> inspect(PipelineConfig config,boolean verifyContents,Path archiveDirectory,
                                              Retention request)throws Exception{
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            Map<String,Object> before=journal.status();
            String phase=(String)before.get("errorStage");
            if(!"ERROR".equals(before.get("run_state"))||phase==null||
                    !Arrays.asList("SOURCE_CONNECT","SOURCE_READ","SOURCE_DECODE","SOURCE_SNAPSHOT","SOURCE_DISCOVERY","DDL","NORMALIZE").contains(phase))
                throw new IllegalStateException("SOURCE_REVIEW_REQUIRES_CAPTURE_ERROR");
            if(verifyContents&&(!"MYSQL".equals(config.effectiveSourceType())||!"SOURCE_DECODE".equals(phase)
                    ||"CLAIMED".equals(before.get("captureLeaseState"))))
                throw new IllegalStateException("MYSQL_BINLOG_CONTENT_REVIEW_UNAVAILABLE");
            SourcePosition checkpoint=journal.captured();
            String checkpointHash=checkpoint==null?null:Codec.sha256(Codec.encode(checkpoint));
            String reference=null;
            CaptureArchiveReceipt.Capture capture=null;
            if(request!=null){
                request.authorize.call();
                if(checkpointHash==null||!(before.get("errorGeneration") instanceof Number)
                        ||((Number)before.get("errorGeneration")).longValue()!=request.errorGeneration)
                    throw new IllegalStateException("SOURCE_REVIEW_STALE");
                archiveDirectory=MysqlContentArchive.directory(archiveDirectory,true);
                reference=archiveReference(config,before,checkpointHash,request.errorGeneration,request.boundaryHash);
                if(request.original!=null)capture=new CaptureArchiveReceipt.Capture(request.original,config.fingerprint(),
                        (String)before.get("generation"),checkpointHash,request.errorGeneration,request.boundaryHash);
                Map<String,Object> existing=MysqlContentArchive.existing(archiveDirectory,reference,config.pipelineId,
                        checkpointHash,request.boundaryHash,request.errorGeneration,capture);
                if(existing!=null){
                    requireUnchanged(journal,before,checkpoint);
                    request.authorize.call();
                    addExclusionPlan(config,journal,before,existing);
                    return existing;
                }
            }
            Map<String,Object> nativeEvidence=new LinkedHashMap<>();
            ByteArrayOutputStream retained=archiveDirectory==null?null:new ByteArrayOutputStream();
            String admission=checkpoint==null?"SOURCE_POSITION_UNAVAILABLE":
                    probe(config,(String)before.get("generation"),checkpoint,journal,phase,nativeEvidence,verifyContents,retained,request==null?null:request.boundaryHash);

            if(verifyContents&&!nativeEvidence.containsKey("nextTransactionContents"))
                throw new IllegalStateException(request==null?"MYSQL_BINLOG_CONTENT_REVIEW_UNAVAILABLE: "+admission:
                        "SOURCE_REVIEW_STALE".equals(admission)?admission:"MYSQL_BINLOG_CONTENT_REVIEW_UNAVAILABLE");
            requireUnchanged(journal,before,checkpoint);

            Map<String,Object> report=new LinkedHashMap<>();
            report.put("taskId",config.pipelineId);
            report.put("sourceType",config.effectiveSourceType());
            report.put("errorGeneration",before.get("errorGeneration"));
            report.put("phase",phase);
            report.put("failureCode",before.get("errorCode"));
            report.put("lastCapturedSequence",before.get("lastSequence").toString());
            if(checkpointHash!=null)report.put("checkpointHash",checkpointHash);
            report.put("sourceAdmission",admission);
            report.put("affectedSubscriptions",config.subscribers());
            report.put("scope","UNPROVEN_SOURCE_RANGE");
            report.put("actions",actions(admission,phase));
            report.putAll(nativeEvidence);
            if(archiveDirectory!=null){
                if(request!=null)request.authorize.call();
                report.put("retainedEvidence",MysqlContentArchive.retain(archiveDirectory,report,retained.toByteArray(),reference,capture));
                if(request!=null)addExclusionPlan(config,journal,before,report);
            }
            return report;
        }
    }

    private static String archiveReference(PipelineConfig config, Map<String,Object> state, String checkpoint,
                                           long errorGeneration, String boundary) {
        return CaptureArchiveReceipt.locator(config.fingerprint(),(String)state.get("generation"),checkpoint,errorGeneration,boundary);
    }

    private static void addExclusionPlan(PipelineConfig config, JdbcJournal journal, Map<String,Object> state,
                                         Map<String,Object> report) throws Exception {
        try {
            report.put("exclusionPlan",plan(config,journal,state,report).summary());
        } catch (IllegalStateException ineligible) {
            if (!"CAPTURE_EXCLUSION_PROFILE_UNSUPPORTED".equals(ineligible.getMessage())) throw ineligible;
            report.put("exclusionBlock",ineligible.getMessage());
        }
    }

    private static CaptureExclusionPlan plan(PipelineConfig config, JdbcJournal journal, Map<String,Object> state,
                                             Map<String,Object> report) throws Exception {
        Map<?,?> boundary = (Map<?,?>) report.get("nextTransactionHeaders");
        Map<?,?> archive = (Map<?,?>) report.get("retainedEvidence");
        CaptureExclusionPlan plan = new CaptureExclusionPlan();
        plan.pipelineId = config.pipelineId;
        plan.generation = (String) state.get("generation");
        plan.epoch = ((Number) state.get("epoch")).longValue();
        plan.errorGeneration = ((Number) state.get("errorGeneration")).longValue();
        plan.afterSequence = ((Number) state.get("lastSequence")).longValue();
        plan.checkpointHash = (String) report.get("checkpointHash");
        plan.boundaryHash = (String) boundary.get("boundaryHash");
        plan.transactionId = (String) boundary.get("transactionId");
        plan.archiveSha256 = (String) archive.get("archiveSha256");
        for (Object rawName : (Iterable<?>) boundary.get("tables")) {
            String tableName = (String) rawName;
            int separator = tableName.indexOf('.');
            String database = tableName.substring(0,separator);
            if (!config.databases.contains(database)) continue;
            String tableId = TableSchema.identity(database,"",tableName.substring(separator+1));
            TableSchema table = journal.latestSchema(tableId);
            Journal.SnapshotState snapshot = journal.snapshot("source",tableId);
            if (table == null || table.keyless() || !"InnoDB".equals(table.sourceEngine)
                    || snapshot == null || !"COMPLETE".equals(snapshot.state)
                    || snapshot.schemaVersion != table.version)
                throw new IllegalStateException("CAPTURE_EXCLUSION_PROFILE_UNSUPPORTED");
            for (PipelineConfig.Target target : config.targets) {
                if (!target.mappings.containsKey(database)) continue;
                if (target.product.messaging() || target.product.materialized())
                    throw new IllegalStateException("CAPTURE_EXCLUSION_PROFILE_UNSUPPORTED");
                plan.affectedSubscriptions.computeIfAbsent(target.id,ignored -> new ArrayList<>()).add(tableName);
            }
        }
        if (plan.affectedSubscriptions.isEmpty())
            throw new IllegalStateException("CAPTURE_EXCLUSION_PROFILE_UNSUPPORTED");
        return plan;
    }

    private static void requireUnchanged(JdbcJournal journal,Map<String,Object> before,SourcePosition checkpoint)throws Exception{
        Map<String,Object> after=journal.status();
        for(String key:Arrays.asList("generation","epoch","run_state","errorGeneration","errorCode","lastSequence"))
            if(!Objects.equals(before.get(key),after.get(key)))throw new IllegalStateException("SOURCE_REVIEW_STALE");
        if(!Objects.equals(checkpoint,journal.captured()))throw new IllegalStateException("SOURCE_REVIEW_STALE");
    }

    private static String probe(PipelineConfig config,String generation,SourcePosition checkpoint,JdbcJournal journal,
                                String phase,Map<String,Object> nativeEvidence,boolean verifyContents,
                                ByteArrayOutputStream retained,String expectedBoundary)throws Exception{
        // Read journal dependencies before the source probe so storage errors cannot look like source failures.
        List<SourcePosition> dependencies=journal.dependencies();
        try(SourceAdapter source=PipelineSources.open(config,generation)){
            source.validateFailover(checkpoint,dependencies);
            if(source instanceof MysqlCdcSource&&"SOURCE_DECODE".equals(phase)){
                try{
                    Map<String,Object> boundary=((MysqlCdcSource)source).inspectDecodeBoundary(checkpoint);
                    if(boundary!=null){
                        if(expectedBoundary!=null&&!expectedBoundary.equals(boundary.get("boundaryHash")))
                            return "SOURCE_REVIEW_STALE";
                        nativeEvidence.put("nextTransactionHeaders",boundary);
                        if(verifyContents)nativeEvidence.put("nextTransactionContents",((MysqlCdcSource)source).verifyBinlogContents(checkpoint,boundary,retained));
                    }
                }catch(java.sql.SQLException unavailable){
                    // A missing, rotated or unreadable source file leaves only the admission result.
                }
            }
            return "SOURCE_CHECKPOINT_AVAILABLE";
        }catch(InterruptedException interrupted){
            Thread.currentThread().interrupt();
            throw interrupted;
        }catch(Exception failure){
            return PipelineEngine.sourceErrorCode(failure);
        }
    }

    private static List<String> actions(String admission,String phase){
        if("SOURCE_POSITION_UNAVAILABLE".equals(admission))
            return Arrays.asList("PRESERVE_SOURCE_LOGS","REPAIR_CAPTURE_CAUSE","REVIEW_NEW_BASELINE");
        if("SOURCE_CHECKPOINT_AVAILABLE".equals(admission)){
            if("SOURCE_CONNECT".equals(phase)||"SOURCE_READ".equals(phase))
                return Arrays.asList("PRESERVE_SOURCE_LOGS","RETRY_ORIGINAL_CHECKPOINT");
            return Arrays.asList("PRESERVE_SOURCE_LOGS","REPAIR_CAPTURE_CAUSE","RETRY_ORIGINAL_CHECKPOINT");
        }
        return Arrays.asList("PRESERVE_SOURCE_LOGS","RESTORE_LOGS_OR_SELECT_ELIGIBLE_SOURCE","REVIEW_NEW_BASELINE");
    }
}
