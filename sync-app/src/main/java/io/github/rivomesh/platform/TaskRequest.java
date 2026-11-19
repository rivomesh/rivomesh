package io.github.rivomesh.platform;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.JournalWriterAssignment;
import io.github.rivomesh.validation.ValidationArchive;
import io.github.rivomesh.validation.ValidationRetention;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Fixed business operations shared by HTTP and durable owner dispatch. Never reflect method names. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic=true)
final class TaskRequest {
    enum Kind { LIST,VIEW,CREATE,CONFIGURATION,CONFIG_HISTORY,CONFIG_PREVIEW,CONFIG_UPDATE,RESTORE_PREVIEW,RESTORE,VALIDATION_REPORTS,VALIDATION_REPORT,VALIDATION_DIFFERENCES,VALIDATION_STORAGE,REPAIR_PLAN,CAPTURE_RECOVERY,CAPTURE_EXCLUSIONS,DIAGNOSTICS,FAILURE_EVIDENCE_SUMMARY,FAILURE_EVIDENCE_EVENTS,SKIP_PLAN,SKIP_PLANS,GAPS,GAP_VERIFICATION,CORRECTION_SCHEMA,CORRECTION_PREVIEW,HANDOFF_ASSESS,HANDOFF_LATEST,HANDOFF_PROBE,HANDOFF_PREPARE,HANDOFF_PREPARATION,HANDOFF_RELEASE,HANDOFF_TARGET_FENCE,HANDOFF_START_WORKER,HANDOFF_STAGE,HANDOFF_COMMIT_OWNER,HANDOFF_CANDIDATE_PROBE,HANDOFF_EVIDENCE_PROBE,HANDOFF_CUSTODY_PROBE,HANDOFF_STAGING_RPC,HANDOFF_READINESS_RPC,RECOVERY_RECEIPT,KAFKA_NOTICE_PLAN,ACTION }
    public int version=1;
    public Kind kind;
    public String taskId,requestId,revision,yaml,reference,cursor,reportId,planId,target,tableId,after,identityHash,action,subscriptionId,ownerNodeId;
    public String placement;
    public JsonNode document;
    public long errorCount=-1;
    public int limit=50;
    public ValidationRequest validation;
    public RepairRequest repair;
    public CommitRequest commit;
    public SkipRequest skip;
    public GapVerificationRequest gap;
    public CompensationRequest compensation;
    public CorrectionRequest correction;
    public CaptureExclusionRequest captureExclusion;
    public ValidationRetention.Request validationRetention;
    public ValidationArchive.Request validationArchive;
    public JournalWriterAssignment.ExpectedCut handoffCut;
    public SharedHandoffEvidence.Scope handoffEvidenceScope;
    public SharedHandoffEvidence.Archive handoffArchive;
    public SharedHandoffEvidence.ReceiptPlan handoffClosedReceipts;
    public HandoffStaging.Binding handoffStaging;
    public DurableCommands.KafkaNoticePredecessor kafkaNoticePredecessor;
    public HandoffPreparations.Plan handoffPreparation;
    public HandoffEvidenceAdoption.Receipt adoptedEvidence;
    TaskRequest(){}
    static TaskRequest of(Kind kind,String task){TaskRequest request=new TaskRequest();request.kind=kind;request.taskId=task;return request;}
    boolean mutation(){return kind==Kind.CREATE||kind==Kind.CONFIG_UPDATE||kind==Kind.RESTORE||kind==Kind.HANDOFF_ASSESS||kind==Kind.HANDOFF_PREPARE||kind==Kind.HANDOFF_RELEASE||kind==Kind.HANDOFF_TARGET_FENCE||kind==Kind.HANDOFF_START_WORKER||kind==Kind.HANDOFF_STAGE||kind==Kind.HANDOFF_COMMIT_OWNER||kind==Kind.ACTION;}
    boolean admin(){return kind!=Kind.LIST&&kind!=Kind.VIEW&&kind!=Kind.VALIDATION_REPORT&&kind!=Kind.VALIDATION_REPORTS&&(kind!=Kind.ACTION||!Arrays.asList("CHECK","START","STOP","PAUSE","RESUME","RESUME_TARGET").contains(action));}
    void validate(){
        if(version!=1)throw new IllegalArgumentException("TASK_REQUEST_VERSION_INVALID");
        if(kind==null)throw new IllegalArgumentException("TASK_REQUEST_KIND_REQUIRED");
        if(ownerNodeId!=null&&!ownerNodeId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}"))throw new IllegalArgumentException("TASK_OWNER_INVALID");
        if(placement!=null&&(kind!=Kind.CREATE||!"AUTO".equals(placement)||ownerNodeId!=null))
            throw new IllegalArgumentException("TASK_PLACEMENT_INVALID");
        if(taskId==null&&kind!=Kind.LIST&&kind!=Kind.CREATE
                ||taskId!=null&&!taskId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))
            throw new IllegalArgumentException("TASK_REQUEST_ID_INVALID");
        if(mutation()&&(requestId==null||!requestId.matches("[A-Za-z0-9_-]{8,128}")))throw new IllegalArgumentException("REQUEST_ID_REQUIRED");
        if(kind==Kind.HANDOFF_ASSESS&&(ownerNodeId==null||revision==null||!revision.matches("[0-9a-f-]{36}")
                ||reference!=null&&(!reference.matches("[A-Za-z0-9_-]{8,128}")||reference.equals(requestId))))
            throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
        if((kind==Kind.HANDOFF_PREPARE||kind==Kind.HANDOFF_RELEASE)&&(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")
                ||reference.equals(requestId)||kind==Kind.HANDOFF_PREPARE&&(revision==null||!revision.matches("[0-9a-f-]{36}")
                ||identityHash==null||!identityHash.matches("[0-9a-f]{64}"))))
            throw new IllegalArgumentException("HANDOFF_PREPARATION_REQUEST_INVALID");
        if((kind==Kind.HANDOFF_TARGET_FENCE||kind==Kind.HANDOFF_START_WORKER)&&(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")
                ||reference.equals(requestId)||revision==null||!revision.matches("[0-9a-f-]{36}")))
            throw new IllegalArgumentException(kind==Kind.HANDOFF_TARGET_FENCE?"HANDOFF_TARGET_FENCE_REQUEST_INVALID":"HANDOFF_WORKER_START_REQUEST_INVALID");
        if((handoffRpc()||kind==Kind.HANDOFF_PROBE)&&(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")
                ||handoffRpc()&&(revision==null||!revision.matches("[0-9a-f-]{36}"))))
            throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
        if((kind==Kind.HANDOFF_STAGE||kind==Kind.HANDOFF_COMMIT_OWNER||preparedHandoffRpc())
                &&(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")||revision==null||!revision.matches("[0-9a-f-]{36}")
                ||identityHash==null||!identityHash.matches("[a-f0-9]{64}")
                ||requestId==null||!requestId.matches("[A-Za-z0-9_-]{8,128}")||requestId.equals(reference))
                ||handoffStaging!=null&&!preparedHandoffRpc()
                ||handoffPreparation!=null&&kind!=Kind.HANDOFF_READINESS_RPC)
            throw new IllegalArgumentException("HANDOFF_STAGING_REQUEST_INVALID");
        if(handoffCut!=null&&kind!=Kind.HANDOFF_CANDIDATE_PROBE)
            throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
        if(handoffEvidenceScope!=null&&kind!=Kind.HANDOFF_EVIDENCE_PROBE&&kind!=Kind.HANDOFF_CUSTODY_PROBE
                ||(handoffArchive!=null)!=(kind==Kind.HANDOFF_CUSTODY_PROBE))
            throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
        if(handoffClosedReceipts!=null&&kind!=Kind.HANDOFF_EVIDENCE_PROBE)
            throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
        if(kind==Kind.CREATE&&(yaml==null||yaml.getBytes(StandardCharsets.UTF_8).length>1024*1024))throw new IllegalArgumentException("CONFIG_REQUIRED");
        if(kind==Kind.CONFIG_PREVIEW||kind==Kind.CONFIG_UPDATE){if(document==null||!document.isObject())throw new IllegalArgumentException("CONFIG_REQUIRED");ConfigurationDocuments.validate(document);}
        if(kind==Kind.VALIDATION_REPORTS&&(cursor==null||!cursor.isEmpty()&&!cursor.matches("[0-9]{1,18}_[a-f0-9-]{36}")
                ||limit<1||limit>25))throw new IllegalArgumentException("VALIDATION_CATALOG_PAGE_INVALID");
        if(kind==Kind.VALIDATION_STORAGE&&(reportId==null||!reportId.matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")))
            throw new IllegalArgumentException("VALIDATION_ID_INVALID");
        if(reviewValidationRetention()){
            if(validationRetention==null||revision==null||revision.isEmpty()||revision.length()>128)
                throw new IllegalArgumentException("VALIDATION_RETENTION_REVIEW_INVALID");
            validationRetention.validate();
        }else if(validationRetention!=null)throw new IllegalArgumentException("VALIDATION_RETENTION_REVIEW_INVALID");
        if(archivesValidation()){
            if(validationArchive==null||revision==null||revision.isEmpty()||revision.length()>128)
                throw new IllegalArgumentException("VALIDATION_ARCHIVE_REQUEST_INVALID");
            validationArchive.validate(action);
        }else if(validationArchive!=null)throw new IllegalArgumentException("VALIDATION_ARCHIVE_REQUEST_INVALID");
        if(adoptedEvidence!=null&&(!readsAdoptedEvidence()||!taskId.equals(adoptedEvidence.task())))
            throw new IllegalArgumentException("HANDOFF_ADOPTED_SCOPE_CHANGED");
        if(kind==Kind.RECOVERY_RECEIPT&&(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")))
            throw new IllegalArgumentException("RECOVERY_RECEIPT_ID_INVALID");
        if(kind==Kind.KAFKA_NOTICE_PLAN){
            if(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")
                    ||revision==null||!revision.matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"))
                throw new IllegalArgumentException("KAFKA_NOTICE_REQUEST_INVALID");
            if(kafkaNoticePredecessor!=null&&(kafkaNoticePredecessor.task()==null||kafkaNoticePredecessor.administrator()==null
                    ||kafkaNoticePredecessor.receipt()==null||!taskId.equals(kafkaNoticePredecessor.task().taskId())
                    ||!revision.equals(kafkaNoticePredecessor.task().revision())
                    ||!reference.equals(kafkaNoticePredecessor.receipt().requestId())))
                throw new IllegalArgumentException("KAFKA_NOTICE_REQUEST_INVALID");
        }else if(kafkaNoticePredecessor!=null)throw new IllegalArgumentException("KAFKA_NOTICE_REQUEST_INVALID");
        if(kind==Kind.DIAGNOSTICS)WorkerDiagnostics.validatePage(cursor,limit,subscriptionId);
        if(kind==Kind.CAPTURE_EXCLUSIONS&&(cursor==null||!cursor.isEmpty()&&!cursor.matches("[A-Za-z0-9_-]{8,128}")
                ||limit<1||limit>25))throw new IllegalArgumentException("CAPTURE_EXCLUSION_PAGE_INVALID");
        if(captureRetention()&&(errorCount<0||subscriptionId!=null||reference==null||!reference.matches("[0-9a-f]{64}")))
            throw new IllegalArgumentException("CAPTURE_EVIDENCE_REQUEST_INVALID");
        if(excludesCapture()){
            if(captureExclusion==null||errorCount<0||subscriptionId!=null)
                throw new IllegalArgumentException("CAPTURE_EXCLUSION_APPROVAL_REQUIRED");
            captureExclusion.validate();
        }else if(captureExclusion!=null)throw new IllegalArgumentException("CAPTURE_EXCLUSION_ACTION_REQUIRED");
        if(kind==Kind.FAILURE_EVIDENCE_SUMMARY||kind==Kind.FAILURE_EVIDENCE_EVENTS){
            io.github.rivomesh.recovery.FailureEvidence.uuid(reference);
            if(kind==Kind.FAILURE_EVIDENCE_EVENTS&&(!Arrays.asList("RAW","NORMALIZED").contains(action)||limit<1||limit>100||cursor==null||!cursor.matches("0|[1-9][0-9]{0,6}")))throw new IllegalArgumentException("FAILURE_EVIDENCE_PAGE_INVALID");
        }
        if(correctionAction()||correctionRead()){
            if(correction==null||targetForCorrectionInvalid())throw new IllegalArgumentException("CORRECTION_REQUEST_INVALID");
            correction.validate(kind==Kind.CORRECTION_SCHEMA,correctionAction());
        }else if(correction!=null)throw new IllegalArgumentException("CORRECTION_ACTION_REQUIRED");
        if (compensationAction()) {
            if (compensation == null) throw new IllegalArgumentException("COMPENSATION_APPROVAL_INVALID");
            compensation.validate(action);
        } else if (compensation != null) throw new IllegalArgumentException("COMPENSATION_ACTION_REQUIRED");
        if(gapAction()){if(gap==null)throw new IllegalArgumentException("GAP_VERIFICATION_REQUEST_INVALID");gap.validate("GAP_VERIFY_EXECUTE".equals(action));}else if(gap!=null)throw new IllegalArgumentException("GAP_VERIFICATION_ACTION_REQUIRED");
        if(kind==Kind.GAP_VERIFICATION&&(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")))throw new IllegalArgumentException("GAP_VERIFICATION_PLAN_INVALID");
        if(skipAction()){if(skip==null)throw new IllegalArgumentException("SKIP_REQUEST_INVALID");skip.validate(action);}else if(skip!=null)throw new IllegalArgumentException("SKIP_ACTION_REQUIRED");
        if(kind==Kind.SKIP_PLAN&&(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")||action!=null&&!"NATIVE".equals(action)))throw new IllegalArgumentException("SKIP_PLAN_REFERENCE_INVALID");
        if(kind==Kind.SKIP_PLANS||kind==Kind.GAPS){
            if(target==null||!target.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||limit<1||limit>100||cursor==null)throw new IllegalArgumentException("SKIP_PAGE_INVALID");
            if(kind==Kind.SKIP_PLANS&&!cursor.isEmpty()&&!cursor.matches("[A-Za-z0-9_-]{8,128}"))throw new IllegalArgumentException("SKIP_PAGE_INVALID");
            if(kind==Kind.GAPS)try{if(!cursor.matches("0|[1-9][0-9]{0,18}")||Long.parseLong(cursor)<0)throw new IllegalArgumentException();}catch(RuntimeException invalid){throw new IllegalArgumentException("GAP_CURSOR_INVALID");}
        }
        if(commitRecovery()){if(commit==null)throw new IllegalArgumentException("FAILURE_ACK_REQUEST_INVALID");commit.validate();}else if(commit!=null)throw new IllegalArgumentException("FAILURE_ACK_ACTION_REQUIRED");
        if(recoveryCancel()&&(reference==null||!reference.matches("[A-Za-z0-9_-]{8,128}")||reference.equals(requestId)))
            throw new IllegalArgumentException("RECOVERY_CANCEL_REQUEST_INVALID");
        if(kafkaReview())validateKafkaScope(revision,subscriptionId,errorCount);
        if(kind==Kind.ACTION&&!validationMaintenance()&&!captureRetention()&&!excludesCapture()&&!recoveryCancel()&&!correctionAction()&&!compensationAction()&&!kafkaReview()&&!Arrays.asList("START","STOP","PAUSE","RESUME","CHECK","RESUME_TARGET","VALIDATE","VALIDATE_RETRY","VALIDATE_CANCEL","REPAIR_PLAN","REPAIR_EXECUTE","REPAIR_CANCEL","FAILURE_REVIEW","CONFIRM_COMMIT","SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL","GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE","GAP_VERIFY_CANCEL").contains(action))throw new IllegalArgumentException("ACTION_INVALID");
    }
    static final String KAFKA_REVIEW_ACTION="REVIEW_KAFKA_RECEIPTS";
    static void validateKafkaScope(String revision,String subscription,long generation){
        if(revision==null||!revision.matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")
                ||subscription==null||!subscription.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||generation<0)
            throw new IllegalArgumentException("KAFKA_RECOVERY_REQUEST_INVALID");
    }
    boolean kafkaReview(){return kind==Kind.ACTION&&KAFKA_REVIEW_ACTION.equals(action);}
    boolean reviewValidationRetention(){return kind==Kind.ACTION&&ValidationRetention.ACTION.equals(action);}
    boolean archivesValidation(){return kind==Kind.ACTION&&(ValidationArchive.COPY.equals(action)
            ||ValidationArchive.RESTORE.equals(action)||ValidationArchive.RECLAIM.equals(action));}
    boolean validationMaintenance(){return reviewValidationRetention()||archivesValidation();}
    boolean skipAction(){return kind==Kind.ACTION&&Arrays.asList("SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL").contains(action);}
    private boolean targetForCorrectionInvalid(){return subscriptionId==null||!subscriptionId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||errorCount<0||revision==null||revision.isEmpty()||revision.length()>128;}
    boolean correctionAction(){return kind==Kind.ACTION&&"CORRECTION_EXECUTE".equals(action);}
    boolean correctionRead(){return kind==Kind.CORRECTION_SCHEMA||kind==Kind.CORRECTION_PREVIEW;}
    boolean handoffRpc(){return kind==Kind.HANDOFF_CANDIDATE_PROBE||kind==Kind.HANDOFF_EVIDENCE_PROBE
            ||kind==Kind.HANDOFF_CUSTODY_PROBE||preparedHandoffRpc();}
    boolean preparedHandoffRpc(){return kind==Kind.HANDOFF_STAGING_RPC||kind==Kind.HANDOFF_READINESS_RPC;}
    boolean readsAdoptedEvidence(){return kind==Kind.VALIDATION_REPORTS||kind==Kind.VALIDATION_REPORT
            ||kind==Kind.VALIDATION_STORAGE||kind==Kind.VALIDATION_DIFFERENCES||kind==Kind.RECOVERY_RECEIPT;}
    boolean readAtRevision(){return kind==Kind.CONFIG_PREVIEW||kind==Kind.KAFKA_NOTICE_PLAN||correctionRead()||handoffRpc();}
    boolean compensationAction(){return kind==Kind.ACTION&&CompensationRequest.handles(action);}
    boolean gapAction(){return kind==Kind.ACTION&&Arrays.asList("GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE").contains(action);}
    boolean recoveryCancel(){return kind==Kind.ACTION&&"RECOVERY_CANCEL".equals(action);}
    static boolean cancellableRecovery(String action){return CompensationRequest.handles(action)||"CORRECTION_EXECUTE".equals(action);}
    boolean skipRecovery(){return recoveryCancel()||correctionRead()||correctionAction()||compensationAction()||kind==Kind.ACTION&&"GAP_VERIFY_CANCEL".equals(action)||gapAction()||kind==Kind.GAP_VERIFICATION||skipAction()||kind==Kind.SKIP_PLAN||kind==Kind.SKIP_PLANS||kind==Kind.GAPS;}
    boolean commitRecovery(){return kind==Kind.ACTION&&"CONFIRM_COMMIT".equals(action);}
    boolean excludesCapture(){return kind==Kind.ACTION&&"CAPTURE_EXCLUDE".equals(action);}
    boolean captureRetention(){return kind==Kind.ACTION&&"CAPTURE_RETAIN".equals(action);}
    boolean failureReview(){return kafkaReview()||kind==Kind.KAFKA_NOTICE_PLAN||kind==Kind.FAILURE_EVIDENCE_SUMMARY||kind==Kind.FAILURE_EVIDENCE_EVENTS||kind==Kind.ACTION&&"FAILURE_REVIEW".equals(action);}
    TaskRequest snapshot()throws Exception{validate();return Model.JSON.readValue(Model.JSON.writeValueAsBytes(this),TaskRequest.class);}
}
