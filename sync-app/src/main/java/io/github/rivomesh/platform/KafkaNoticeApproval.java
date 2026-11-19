package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.KafkaNoticePlanning.Prepared;
import io.github.rivomesh.cdc.KafkaNoticeWriterReview.Result;
import io.github.rivomesh.recovery.FailureEvidence;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Immutable approval of a reviewed scope. This record cannot authorize Kafka dispatch. */
final class KafkaNoticeApproval {
    static final String RECORDED="KAFKA_NOTICE_APPROVAL_RECORDED_BLOCKED";
    static final String READY="KAFKA_NOTICE_APPROVAL_RECORDED";
    static final String DISPATCHED="KAFKA_NOTICE_NATIVE_RECEIPT_VERIFIED";
    static final String HISTORY="KAFKA_NOTICE_HISTORICAL_OWNERSHIP_UNVERIFIED";
    static final String TOPIC_ID_PRODUCE="KAFKA_NOTICE_TOPIC_ID_PRODUCE_REQUIRED";
    private static final Set<String> VERIFIED_CATALOG=Set.of("VERIFIED_SINGLETON","VERIFIED_AT_REVIEW");

    record Scope(Prepared plan,String configurationCutSha256,String physicalRoutesSha256,Result writer,
                 String historicalOwnership) {
        Scope {
            if(plan==null||plan.task()==null||plan.administrator()==null||plan.controlTopic()==null||plan.catalogIdentity()==null
                    ||plan.failure()==null||!FailureEvidence.hash(plan.failure().contentSha256())||!FailureEvidence.hash(plan.predecessorReceiptSha256())
                    ||!FailureEvidence.hash(plan.noticeId())||!FailureEvidence.hash(plan.payloadSha256())
                    ||!FailureEvidence.hash(configurationCutSha256)||!FailureEvidence.hash(physicalRoutesSha256)
                    ||!(HISTORY.equals(historicalOwnership)||FailureEvidence.hash(historicalOwnership))||writer==null||!validWriter(writer))
                throw new TaskService.Conflict("KAFKA_NOTICE_APPROVAL_EVIDENCE_INVALID");
        }

        String digest(){return Codec.sha256("rivomesh-kafka-notice-approval-v1\n"+Codec.encode(this));}

        String fixedHistory(){return HISTORY.equals(historicalOwnership)?null:historicalOwnership;}

        List<String> blockers(){
            List<String> result=new ArrayList<>();
            if(fixedHistory()==null)result.add(HISTORY);
            if(!"VERIFIED_AT_REVIEW".equals(writer.status()))result.add(writer.code());
            return List.copyOf(result);
        }
    }

    private KafkaNoticeApproval(){}

    static Scope reviewed(Prepared plan,Map<String,Object> coverage,Object entitlement){
        Object status=coverage.get("status");
        if(!(status instanceof String value)||!VERIFIED_CATALOG.contains(value))return null;
        Result writer=entitlement==null?Result.notConfigured():Model.JSON.convertValue(entitlement,Result.class);
        if("VERIFIED_AT_REVIEW".equals(writer.status())&&writer.evidenceSha256()==null)
            writer=new Result("BLOCKED","KAFKA_NOTICE_WRITER_EVIDENCE_REFRESH_REQUIRED");
        String fixed=hash(coverage,"fixedHistorySha256");
        return new Scope(plan,hash(coverage,"configurationCutSha256"),hash(coverage,"physicalRoutesSha256"),writer,fixed==null?HISTORY:fixed);
    }

    static Map<String,Object> senderPreflight(String approvalId,Scope scope,Map<String,Object> receipt){
        var plan=scope.plan();var failure=plan.failure();var task=plan.task();
        Object status=receipt.get("status"),code=receipt.get("code");
        boolean exact=task.configurationVersion().equals(receipt.get("configurationVersion"))
                &&failure.targetId().equals(receipt.get("targetId"))
                &&failure.acknowledgedSequence().equals(receipt.get("acknowledgedSequence"));
        var blockers=new ArrayList<>(scope.blockers());
        if(!Boolean.TRUE.equals(scope.writer().physicalTransactionsReady()))blockers.add(TOPIC_ID_PRODUCE);
        var predecessor=new LinkedHashMap<String,Object>();
        if(exact&&"VERIFIED".equals(status)&&"KAFKA_PHYSICAL_RECEIPT_VERIFIED".equals(code)
                &&receipt.get("receiptSha256") instanceof String digest&&FailureEvidence.hash(digest)){
            predecessor.put("receiptSha256",digest);
        }else if(exact&&"MISSING".equals(status)&&("KAFKA_PHYSICAL_RECEIPT_UNVERIFIED".equals(code)
                ||"KAFKA_PHYSICAL_RECEIPT_TOPIC_CHANGED".equals(code))){
            blockers.add("KAFKA_NOTICE_PREDECESSOR_PHYSICAL_RECEIPT_REQUIRED");
        }else if("UNREACHABLE".equals(status)&&code instanceof String problem
                &&(exact&&"KAFKA_PHYSICAL_RECEIPT_BROKER_UNAVAILABLE".equals(problem)
                ||Set.of("OWNER_QUERY_UNAVAILABLE","OWNER_QUERY_FAILED","OWNER_QUERY_TIMEOUT","OWNER_QUERY_EXPIRED").contains(problem))){
            blockers.add("KAFKA_NOTICE_PREDECESSOR_PHYSICAL_RECEIPT_UNAVAILABLE");
        }else throw new TaskService.Conflict("KAFKA_NOTICE_PREFLIGHT_RECEIPT_CHANGED");
        predecessor.put("status",status);

        var report=new LinkedHashMap<String,Object>();
        report.put("status",blockers.isEmpty()?"SENDER_READY":"SENDER_BLOCKED");
        report.put("executionAuthorized",false);
        report.put("approvalRequestId",approvalId);
        report.put("approvalScopeSha256",scope.digest());
        report.put("task",task);
        report.put("failure",failure);
        report.put("controlTopicId",plan.controlTopic().topicId());
        report.put("noticeId",plan.noticeId());
        report.put("payloadSha256",plan.payloadSha256());
        report.put("precedingPhysicalReceipt",predecessor);
        report.put("blockers",List.copyOf(blockers));
        return report;
    }

    static String recordedCode(Scope scope){
        return scope.blockers().isEmpty()&&Boolean.TRUE.equals(scope.writer().physicalTransactionsReady())?READY:RECORDED;
    }

    private static String hash(Map<String,Object> coverage,String field){
        Object value=coverage.get(field);
        return value instanceof String digest?digest:null;
    }

    private static boolean validWriter(Result writer){
        if("VERIFIED_AT_REVIEW".equals(writer.status()))
            return "KAFKA_NOTICE_WRITER_EFFECTIVE_PERMISSIONS_VERIFIED".equals(writer.code())
                    &&FailureEvidence.hash(writer.evidenceSha256());
        if("NOT_CONFIGURED".equals(writer.status()))
            return "KAFKA_NOTICE_WRITER_NOT_CONFIGURED".equals(writer.code())&&writer.evidenceSha256()==null
                    &&writer.physicalTransactionsReady()==null;
        return "BLOCKED".equals(writer.status())&&writer.evidenceSha256()==null&&writer.code()!=null
                &&writer.code().matches("KAFKA_NOTICE_WRITER_[A-Z_]{1,64}");
    }
}
