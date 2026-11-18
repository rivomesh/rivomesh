package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.recovery.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Bounded administrator metadata views. Listing never loads plan payloads or event images. */
public final class RecoveryLedger {
    private RecoveryLedger(){}
    public static Map<String,Object> plans(PipelineConfig config,String target,String after,int limit)throws Exception{return page(config,target,after,limit,false);}
    public static Map<String,Object> gaps(PipelineConfig config,String target,String after,int limit)throws Exception{return page(config,target,after,limit,true);}
    private static Map<String,Object> page(PipelineConfig config,String target,String after,int limit,boolean gaps)throws Exception{
        if(target==null||config.targets.stream().noneMatch(t->target.equals(t.id))||after==null||limit<1||limit>100)throw new IllegalArgumentException("SKIP_PAGE_INVALID");
        long sequence=0;if(gaps)try{if(!after.matches("0|[1-9][0-9]{0,18}"))throw new IllegalArgumentException();sequence=Long.parseLong(after);}catch(RuntimeException invalid){throw new IllegalArgumentException("GAP_CURSOR_INVALID");}
        else if(!after.isEmpty()&&!after.matches("[A-Za-z0-9_-]{8,128}"))throw new IllegalArgumentException("SKIP_PAGE_INVALID");
        List<Map<String,Object>> items=new ArrayList<>();Map<String,Object> result=new LinkedHashMap<>();result.put("target",target);result.put("items",items);result.put("next",null);
        try(JdbcJournal j=new JdbcJournal(config.existingJournalSettings());Connection c=j.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            j.verifyIdentity(config.fingerprint(),config.subscribers());
            try(PreparedStatement p=c.prepareStatement("SELECT format_version FROM "+j.table("job")+" WHERE pipeline_id=?")){p.setString(1,config.pipelineId);try(ResultSet r=p.executeQuery()){if(!r.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");if(r.getInt(1)==3)return result;}}
            String sql=gaps?"SELECT operation_id,plan_hash,stream_seq,receipt_hash,created_at,verification_id,verification_hash FROM "+j.table("gap")+" WHERE pipeline_id=? AND subscription_id=? AND stream_seq>? ORDER BY stream_seq LIMIT ?":"SELECT operation_id,plan_hash,stream_seq,run_state FROM "+j.table("skip_plan")+" WHERE pipeline_id=? AND subscription_id=? AND operation_id>? ORDER BY operation_id LIMIT ?";
            try(PreparedStatement p=c.prepareStatement(sql)){
                p.setString(1,config.pipelineId);p.setString(2,target);if(gaps)p.setLong(3,sequence);else p.setString(3,after);p.setInt(4,limit+1);
                try(ResultSet r=p.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>();row.put("planId",r.getString(1));row.put("planHash",r.getString(2));row.put("sequence",Long.toString(r.getLong(3)));if(gaps){row.put("receiptHash",r.getString(4));row.put("createdAt",r.getString(5));row.put("outcome","SKIPPED");String verified=r.getString(6),hash=r.getString(7);if(verified==null&&hash==null)row.put("state","OPEN");else{if(verified==null||!FailureEvidence.hash(hash))throw new IllegalStateException("GAP_VERIFICATION_RECORD_CHANGED");row.put("state","VERIFIED_AT_COMMON_CUT");row.put("verificationId",verified);row.put("verificationHash",hash);}}else row.put("state",r.getString(4));items.add(row);}}
            }
            if(items.size()>limit){items.remove(items.size()-1);result.put("next",items.get(items.size()-1).get(gaps?"sequence":"planId"));}deadline.verify();return result;
        }
    }
    public static Map<String,Object> plan(PipelineConfig config,String operation,boolean nativeProof)throws Exception{
        if(operation==null||!operation.matches("[A-Za-z0-9_-]{8,128}"))throw new IllegalArgumentException("SKIP_PLAN_REFERENCE_INVALID");
        JdbcSkipJournal.Stored stored;
        try(JdbcJournal j=new JdbcJournal(config.existingJournalSettings());Connection c=j.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            j.verifyIdentity(config.fingerprint(),config.subscribers());c.setAutoCommit(false);Map<String,Object> job=j.locked(c);if(!JdbcJournal.hasTargetGaps((int)job.get("format")))throw new IllegalStateException("SKIP_PLAN_NOT_FOUND");stored=JdbcSkipJournal.load(j,c,operation);if(stored==null)throw new IllegalStateException("SKIP_PLAN_NOT_FOUND");deadline.verify();c.rollback();
        }
        SkipPlan plan=stored.plan;Map<String,Object> result=new LinkedHashMap<>();result.put("planId",plan.id);result.put("planHash",stored.hash);result.put("state",stored.state);result.put("target",plan.subscriptionId);result.put("evidenceId",plan.evidenceId);result.put("evidenceHash",plan.evidenceHash);result.put("createdAt",plan.createdAt);result.put("createdBy",plan.createdBy);result.put("reason",plan.reason);result.put("epoch",plan.epoch);result.put("errorGeneration",plan.errorGeneration);result.put("sequence",Long.toString(plan.sequence));result.put("acknowledged",Long.toString(plan.acknowledged));result.put("transactionId",plan.transactionId);result.put("rawHash",plan.rawHash);result.put("normalizedHash",plan.normalizedHash);result.put("projectedHash",plan.projectedHash);result.put("fingerprint",plan.fingerprint);result.put("generation",plan.generation);result.put("owner",plan.scope);result.put("expectedCheckpoint",plan.expectedCheckpoint);result.put("receipt",stored.receipt);result.put("resolution",stored.resolution);
        List<String> references=new ArrayList<>(plan.schemaVersions);result.put("schemaVersions",references.subList(0,Math.min(100,references.size())));result.put("schemaVersionsLimited",references.size()>100);
        result.put("sourceType",plan.sourcePosition.getSourceType());result.put("sourceId",plan.sourcePosition.getSourceId());result.put("sourcePositionHash",Codec.sha256(Codec.encode(plan.sourcePosition)));
        Map<String,String> position=new TreeMap<>();for(String key:Arrays.asList("gtidSet","file","offset","serverUuid","lsn","scn","dbid","incarnation"))if(plan.sourcePosition.getPayload().containsKey(key))position.put(key,plan.sourcePosition.getPayload().get(key));
        boolean omitted=Codec.encode(position).getBytes(StandardCharsets.UTF_8).length>16384;result.put("sourcePositionOmitted",omitted);if(!omitted)result.put("sourcePosition",position);
        if(nativeProof&&"PREPARED".equals(stored.state)){
            Map<String,Object> proof=new LinkedHashMap<>();proof.put("observedAt",Instant.now().toString());result.put("nativeProof",proof);
            try{
                PipelineConfig.Target target=config.targets.stream().filter(t->plan.subscriptionId.equals(t.id)).findFirst().orElseThrow(()->new IllegalStateException("SKIP_TARGET_SCOPE_CHANGED"));if(!config.fingerprint().equals(plan.fingerprint))throw new IllegalStateException("SKIP_CONFIGURATION_CHANGED");
                try(JdbcSink sink=JdbcSink.forSkipRecovery(config.targetSettings(target,plan.generation))){proof.putAll(sink.skipProof(plan));}
            }catch(InterruptedException stopped){Thread.currentThread().interrupt();throw stopped;}
            catch(Exception unavailable){String problem=unavailable.getMessage();proof.put("state","UNKNOWN");proof.put("problem",problem!=null&&problem.matches("(?:SINK|SKIP|TARGET|REPAIR|JOURNAL)_[A-Z0-9_]{1,110}")?problem:"SKIP_NATIVE_PROOF_UNAVAILABLE");}
        }
        return result;
    }
}
