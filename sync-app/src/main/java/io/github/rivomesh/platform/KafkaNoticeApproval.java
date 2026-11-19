package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.KafkaNoticePlanning.Prepared;
import io.github.rivomesh.cdc.KafkaNoticeWriterReview.Result;
import io.github.rivomesh.recovery.FailureEvidence;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Immutable approval of a reviewed scope. This record cannot authorize Kafka dispatch. */
final class KafkaNoticeApproval {
    static final String RECORDED="KAFKA_NOTICE_APPROVAL_RECORDED_BLOCKED";
    static final String HISTORY="KAFKA_NOTICE_HISTORICAL_OWNERSHIP_UNVERIFIED";
    private static final Set<String> VERIFIED_CATALOG=Set.of("VERIFIED_SINGLETON","VERIFIED_AT_REVIEW");

    record Scope(Prepared plan,String configurationCutSha256,String physicalRoutesSha256,Result writer,
                 String historicalOwnership) {
        Scope {
            if(plan==null||plan.task()==null||plan.administrator()==null||plan.controlTopic()==null||plan.catalogIdentity()==null
                    ||plan.failure()==null||!FailureEvidence.hash(plan.failure().contentSha256())||!FailureEvidence.hash(plan.predecessorReceiptSha256())
                    ||!FailureEvidence.hash(plan.noticeId())||!FailureEvidence.hash(plan.payloadSha256())
                    ||!FailureEvidence.hash(configurationCutSha256)||!FailureEvidence.hash(physicalRoutesSha256)
                    ||!HISTORY.equals(historicalOwnership)||writer==null||!validWriter(writer))
                throw new TaskService.Conflict("KAFKA_NOTICE_APPROVAL_EVIDENCE_INVALID");
        }

        String digest(){return Codec.sha256("rivomesh-kafka-notice-approval-v1\n"+Codec.encode(this));}

        List<String> blockers(){
            List<String> result=new ArrayList<>(List.of(HISTORY));
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
        return new Scope(plan,hash(coverage,"configurationCutSha256"),hash(coverage,"physicalRoutesSha256"),writer,HISTORY);
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
            return "KAFKA_NOTICE_WRITER_NOT_CONFIGURED".equals(writer.code())&&writer.evidenceSha256()==null;
        return "BLOCKED".equals(writer.status())&&writer.evidenceSha256()==null&&writer.code()!=null
                &&writer.code().matches("KAFKA_NOTICE_WRITER_[A-Z_]{1,64}");
    }
}
