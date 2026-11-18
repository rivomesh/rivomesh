package io.github.rivomesh.platform;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.Model;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Fixed business operations shared by HTTP and durable owner dispatch. Never reflect method names. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic=true)
final class TaskRequest {
    enum Kind { LIST,VIEW,CREATE,CONFIGURATION,CONFIG_HISTORY,CONFIG_PREVIEW,CONFIG_UPDATE,RESTORE_PREVIEW,RESTORE,VALIDATION_REPORTS,VALIDATION_REPORT,VALIDATION_DIFFERENCES,REPAIR_PLAN,CAPTURE_RECOVERY,CAPTURE_EXCLUSIONS,DIAGNOSTICS,FAILURE_EVIDENCE_SUMMARY,FAILURE_EVIDENCE_EVENTS,SKIP_PLAN,SKIP_PLANS,GAPS,GAP_VERIFICATION,CORRECTION_SCHEMA,CORRECTION_PREVIEW,ACTION }
    public int version=1;
    public Kind kind;
    public String taskId,requestId,revision,yaml,reference,cursor,reportId,planId,target,tableId,after,identityHash,action,subscriptionId,ownerNodeId;
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
    TaskRequest(){}
    static TaskRequest of(Kind kind,String task){TaskRequest request=new TaskRequest();request.kind=kind;request.taskId=task;return request;}
    boolean mutation(){return kind==Kind.CREATE||kind==Kind.CONFIG_UPDATE||kind==Kind.RESTORE||kind==Kind.ACTION;}
    boolean admin(){return kind!=Kind.LIST&&kind!=Kind.VIEW&&kind!=Kind.VALIDATION_REPORT&&kind!=Kind.VALIDATION_REPORTS&&(kind!=Kind.ACTION||!Arrays.asList("CHECK","START","STOP","PAUSE","RESUME","RESUME_TARGET").contains(action));}
    void validate(){
        if(version!=1)throw new IllegalArgumentException("TASK_REQUEST_VERSION_INVALID");
        if(kind==null)throw new IllegalArgumentException("TASK_REQUEST_KIND_REQUIRED");
        if(ownerNodeId!=null&&!ownerNodeId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}"))throw new IllegalArgumentException("TASK_OWNER_INVALID");
        if(taskId==null&&kind!=Kind.LIST&&kind!=Kind.CREATE
                ||taskId!=null&&!taskId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))
            throw new IllegalArgumentException("TASK_REQUEST_ID_INVALID");
        if(mutation()&&(requestId==null||!requestId.matches("[A-Za-z0-9_-]{8,128}")))throw new IllegalArgumentException("REQUEST_ID_REQUIRED");
        if(kind==Kind.CREATE&&(yaml==null||yaml.getBytes(StandardCharsets.UTF_8).length>1024*1024))throw new IllegalArgumentException("CONFIG_REQUIRED");
        if(kind==Kind.CONFIG_PREVIEW||kind==Kind.CONFIG_UPDATE){if(document==null||!document.isObject())throw new IllegalArgumentException("CONFIG_REQUIRED");ConfigurationDocuments.validate(document);}
        if(kind==Kind.VALIDATION_REPORTS&&(cursor==null||!cursor.isEmpty()&&!cursor.matches("[0-9]{1,18}_[a-f0-9-]{36}")
                ||limit<1||limit>25))throw new IllegalArgumentException("VALIDATION_CATALOG_PAGE_INVALID");
        if(kind==Kind.DIAGNOSTICS)WorkerDiagnostics.validatePage(cursor,limit);
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
        if(kind==Kind.ACTION&&!captureRetention()&&!excludesCapture()&&!recoveryCancel()&&!correctionAction()&&!compensationAction()&&!Arrays.asList("START","STOP","PAUSE","RESUME","CHECK","RESUME_TARGET","VALIDATE","VALIDATE_RETRY","VALIDATE_CANCEL","REPAIR_PLAN","REPAIR_EXECUTE","REPAIR_CANCEL","FAILURE_REVIEW","CONFIRM_COMMIT","SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL","GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE","GAP_VERIFY_CANCEL").contains(action))throw new IllegalArgumentException("ACTION_INVALID");
    }
    boolean skipAction(){return kind==Kind.ACTION&&Arrays.asList("SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL").contains(action);}
    private boolean targetForCorrectionInvalid(){return subscriptionId==null||!subscriptionId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||errorCount<0||revision==null||revision.isEmpty()||revision.length()>128;}
    boolean correctionAction(){return kind==Kind.ACTION&&"CORRECTION_EXECUTE".equals(action);}
    boolean correctionRead(){return kind==Kind.CORRECTION_SCHEMA||kind==Kind.CORRECTION_PREVIEW;}
    boolean readAtRevision(){return kind==Kind.CONFIG_PREVIEW||correctionRead();}
    boolean compensationAction(){return kind==Kind.ACTION&&CompensationRequest.handles(action);}
    boolean gapAction(){return kind==Kind.ACTION&&Arrays.asList("GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE").contains(action);}
    boolean recoveryCancel(){return kind==Kind.ACTION&&"RECOVERY_CANCEL".equals(action);}
    static boolean cancellableRecovery(String action){return CompensationRequest.handles(action)||"CORRECTION_EXECUTE".equals(action);}
    boolean skipRecovery(){return recoveryCancel()||correctionRead()||correctionAction()||compensationAction()||kind==Kind.ACTION&&"GAP_VERIFY_CANCEL".equals(action)||gapAction()||kind==Kind.GAP_VERIFICATION||skipAction()||kind==Kind.SKIP_PLAN||kind==Kind.SKIP_PLANS||kind==Kind.GAPS;}
    boolean commitRecovery(){return kind==Kind.ACTION&&"CONFIRM_COMMIT".equals(action);}
    boolean excludesCapture(){return kind==Kind.ACTION&&"CAPTURE_EXCLUDE".equals(action);}
    boolean captureRetention(){return kind==Kind.ACTION&&"CAPTURE_RETAIN".equals(action);}
    boolean failureReview(){return kind==Kind.FAILURE_EVIDENCE_SUMMARY||kind==Kind.FAILURE_EVIDENCE_EVENTS||kind==Kind.ACTION&&"FAILURE_REVIEW".equals(action);}
    TaskRequest snapshot()throws Exception{validate();return Model.JSON.readValue(Model.JSON.writeValueAsBytes(this),TaskRequest.class);}
}
