package io.github.rivomesh.platform;

import io.github.rivomesh.validation.ValidationArchive;
import io.github.rivomesh.validation.ValidationRetention;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Public references from an authenticated command. Approval payloads stay in the private command envelope. */
final class RecoveryReceipts {
    private static final Set<String> ACTIONS=Set.of(
            "VALIDATE","VALIDATE_RETRY","VALIDATE_CANCEL","REPAIR_PLAN","REPAIR_EXECUTE","REPAIR_CANCEL",
            "FAILURE_REVIEW","CONFIRM_COMMIT","SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL",
            "GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE","GAP_VERIFY_CANCEL","COMPENSATION_PLAN","COMPENSATION_EXECUTE",
            "CORRECTION_EXECUTE","RECOVERY_CANCEL","CAPTURE_RETAIN","CAPTURE_EXCLUDE",ValidationRetention.ACTION,
            ValidationArchive.COPY,ValidationArchive.RESTORE,ValidationArchive.RECLAIM);

    private RecoveryReceipts(){}

    static boolean supports(TaskRequest request){
        return request.kind==TaskRequest.Kind.ACTION&&request.action!=null&&ACTIONS.contains(request.action);
    }

    static Map<String,String> references(TaskRequest request){
        Map<String,String> references=new LinkedHashMap<>();
        if(request.validation!=null)add(references,"reportId",request.validation.reportId);
        if(request.validationRetention!=null)add(references,"reportId",request.validationRetention.reportId());
        if(request.validationArchive!=null){
            add(references,"reportId",request.validationArchive.reportId());
            add(references,"archiveId",ValidationArchive.COPY.equals(request.action)?request.requestId:request.validationArchive.expectedArchiveId());
        }
        if(request.repair!=null){
            add(references,"reportId",request.repair.reportId);
            add(references,"repairPlanId",request.repair.planId);
        }
        if(request.commit!=null)add(references,"failureEvidenceId",request.commit.evidenceId);
        if(request.skip!=null){
            add(references,"failureEvidenceId",request.skip.evidenceId);
            add(references,"skipPlanId","SKIP_TRANSACTION".equals(request.action)?request.requestId:request.skip.planId);
        }
        if(request.gap!=null){
            add(references,"reportId",request.gap.reportId);
            add(references,"gapPlanId","GAP_VERIFY_PLAN".equals(request.action)?request.requestId:request.gap.planId);
        }
        if(request.compensation!=null)add(references,"failureEvidenceId",request.compensation.evidenceId);
        if(request.correction!=null)add(references,"failureEvidenceId",request.correction.evidenceId);
        return references;
    }

    private static void add(Map<String,String> references,String kind,String id){
        if(id!=null&&id.matches("[A-Za-z0-9_-]{8,128}"))references.put(kind,id);
    }
}
