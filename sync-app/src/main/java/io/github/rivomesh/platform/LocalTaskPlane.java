package io.github.rivomesh.platform;

import java.util.*;

/** The same authorization and business implementation backs standalone and owner execution. */
final class LocalTaskPlane implements TaskControlPlane {
    private final TaskService tasks;
    LocalTaskPlane(TaskService tasks){this.tasks=tasks;}
    public Map<String,Object> ownerQuery(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();if(request.mutation()||request.kind==TaskRequest.Kind.LIST)throw new IllegalArgumentException("OWNER_QUERY_READ_REQUIRED");
        if(request.kind==TaskRequest.Kind.KAFKA_NOTICE_PLAN)return tasks.planKafkaNotice(request,actor);
        if(request.kind==TaskRequest.Kind.HANDOFF_STAGING_RPC)return tasks.stageHandoffEvidence(request,actor);
        if(request.kind==TaskRequest.Kind.HANDOFF_READINESS_RPC)return tasks.prepareHandoffCandidate(request,actor);
        if(request.kind==TaskRequest.Kind.RECOVERY_RECEIPT)return tasks.adoptedHandoffReceipt(request.taskId,request.reference,actor,request.adoptedEvidence);
        if(request.handoffRpc())return tasks.handoffProbe(request,actor);
        if(request.readAtRevision())request.revision=(String)tasks.view(request.taskId).get("revision");return invoke(request,actor);
    }
    public Map<String,Object> invoke(TaskRequest supplied,AccessControl.Principal proposed)throws Exception{
        TaskRequest r=supplied.snapshot();AccessControl.Principal actor=tasks.currentPrincipal(proposed);
        if(r.kind==TaskRequest.Kind.ACTION&&!actor.operates())throw new SecurityException("FORBIDDEN");
        if(r.admin()&&!actor.administers())throw new SecurityException("FORBIDDEN");
        if(r.kind!=TaskRequest.Kind.LIST&&r.kind!=TaskRequest.Kind.CREATE&&(!tasks.nodeAllowed(actor)||!actor.task(r.taskId)))throw new NoSuchElementException("TASK_NOT_FOUND");
        if(r.skipRecovery()&&!tasks.skipRecoveryEnabled())throw new TaskService.Conflict("SKIP_DISABLED");
        if(r.commitRecovery()&&!tasks.commitRecoveryEnabled())throw new TaskService.Conflict("FAILURE_ACK_DISABLED");
        if(r.failureReview()&&!tasks.failureReviewEnabled())throw new TaskService.Conflict("FAILURE_REVIEW_DISABLED");
        if(r.mutation()&&r.taskId!=null)tasks.requireLocalHandoffSettled(r.taskId);
        switch(r.kind){
            case LIST:return tasks.list(actor);
            case VIEW:return tasks.view(r.taskId);
            case RECOVERY_RECEIPT:case KAFKA_NOTICE_PLAN:throw new TaskService.Conflict("GLOBAL_RECOVERY_RECEIPTS_REQUIRED");
            case HANDOFF_ASSESS:case HANDOFF_LATEST:case HANDOFF_PROBE:
            case HANDOFF_PREPARE:case HANDOFF_PREPARATION:case HANDOFF_RELEASE:case HANDOFF_STAGE:case HANDOFF_TARGET_FENCE:throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
            case CREATE:if(r.placement!=null)throw new TaskService.Conflict("GLOBAL_PLACEMENT_REQUIRED");return tasks.create(r.yaml,r.requestId,actor);
            case CONFIGURATION:return tasks.configuration(r.taskId);
            case CONFIG_HISTORY:return tasks.configurationHistory(r.taskId,r.cursor);
            case CONFIG_PREVIEW:return tasks.previewConfiguration(r.taskId,r.document,r.revision);
            case CONFIG_UPDATE:return tasks.updateConfiguration(r.taskId,r.document,r.revision,r.requestId,actor);
            case RESTORE_PREVIEW:return tasks.previewRestoreConfiguration(r.taskId,r.reference,r.revision);
            case RESTORE:return tasks.restoreConfiguration(r.taskId,r.reference,r.revision,r.requestId,actor);
            case VALIDATION_REPORTS:return tasks.validationReports(r.taskId,r.cursor,r.limit,actor,r.adoptedEvidence);
            case VALIDATION_REPORT:return tasks.validationReport(r.taskId,r.reportId,actor,r.adoptedEvidence);
            case VALIDATION_STORAGE:return tasks.validationStorage(r.taskId,r.reportId,actor,r.adoptedEvidence);
            case VALIDATION_DIFFERENCES:return tasks.validationDifferences(r.taskId,r.reportId,r.target,r.tableId,r.after,r.limit,r.identityHash,actor,r.adoptedEvidence);
            case REPAIR_PLAN:return tasks.repairPlan(r.taskId,r.reportId,r.planId,actor);
            case CAPTURE_RECOVERY:return tasks.captureRecovery(r.taskId,actor);
            case CAPTURE_EXCLUSIONS:return tasks.captureExclusions(r.taskId,r.cursor,r.limit,actor);
            case DIAGNOSTICS:return tasks.workerDiagnostics(r.taskId,r.cursor,r.limit,r.subscriptionId,actor);
            case FAILURE_EVIDENCE_SUMMARY:return tasks.failureEvidence(r.taskId,r.reference,null,0,1,actor);
            case FAILURE_EVIDENCE_EVENTS:return tasks.failureEvidence(r.taskId,r.reference,r.action,Integer.parseInt(r.cursor),r.limit,actor);
            case SKIP_PLAN:return tasks.skipPlan(r.taskId,r.reference,"NATIVE".equals(r.action),actor);
            case SKIP_PLANS:return tasks.recoveryPage(r.taskId,r.target,r.cursor,r.limit,false,actor);
            case GAP_VERIFICATION:return tasks.gapVerificationPlan(r.taskId,r.reference,actor);
            case GAPS:return tasks.recoveryPage(r.taskId,r.target,r.cursor,r.limit,true,actor);
            case CORRECTION_SCHEMA:case CORRECTION_PREVIEW:return tasks.correctionPreview(r,actor);
            case ACTION:
                if(r.recoveryCancel())return tasks.cancelRecovery(r,actor);
                if(r.validationMaintenance())return tasks.validationMaintenance(r,actor);
                return tasks.command(r.taskId,r.action,r.revision,r.requestId,r.errorCount,r.subscriptionId,actor,r.validation,r.repair,r.commit,r.skip,r.gap,r.compensation,r.correction,r.captureRetention()?r.reference:null,r.captureExclusion);
            default:throw new IllegalArgumentException("TASK_REQUEST_KIND_REQUIRED");
        }
    }
}
