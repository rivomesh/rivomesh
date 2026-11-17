package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.*;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Format4 forward intent and durable gap ledger. Never fabricates an ordinary APPLY receipt. */
final class JdbcSkipJournal {
    static final class Stored {SkipPlan plan;String hash,state;SkipReceipt receipt;SkipResolution resolution;}
    private JdbcSkipJournal(){}
    static void verifyTables(JdbcJournal j,Connection c)throws Exception{
        try(Statement s=c.createStatement()){
            try(ResultSet ignored=s.executeQuery("SELECT operation_id,request_hash,plan_hash,payload,run_state,resolution,resolution_hash FROM "+j.table("skip_plan")+" WHERE 1=0")){}
            try(ResultSet ignored=s.executeQuery("SELECT subscription_id,stream_seq,operation_id,plan_hash,receipt_hash,payload,verification_id,verification_hash FROM "+j.table("gap")+" WHERE 1=0")){}
            try(ResultSet ignored=s.executeQuery("SELECT operation_id,plan_hash,receipt_hash,payload,created_at FROM "+j.table("gap_verification")+" WHERE 1=0")){}
            try(ResultSet ignored=s.executeQuery("SELECT operation_id,plan_hash,payload,created_at FROM "+j.table("gap_verification_plan")+" WHERE 1=0")){}
        }catch(SQLException missing){if(missing(missing))throw new IllegalStateException("SKIP_METADATA_LOST");throw missing;}
        if(j.settings.driver.startsWith("com.mysql"))try(PreparedStatement p=c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rm3_job','rm3_subscription','rm3_failure','rm3_skip_plan','rm3_gap','rm3_gap_verification','rm3_gap_verification_plan')")){
            p.setString(1,j.settings.namespace);try(ResultSet r=p.executeQuery()){int count=0;while(r.next()){count++;if(!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new IllegalStateException("SKIP_METADATA_NOT_TRANSACTIONAL");}if(count!=7)throw new IllegalStateException("SKIP_METADATA_LOST");}
        }
    }
    /** Additive DDL only while preparing a first format3 recovery; format4 never recreates history. */
    static void ensureMetadata(JdbcJournal j,FailureEvidence evidence)throws Exception{
        try(Connection c=j.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);Map<String,Object> job=j.locked(c);
            if(((Number)job.get("format")).intValue()==4){verifyTables(j,c);c.rollback();return;}
            if(!"PAUSED".equals(job.get("state")))throw new IllegalStateException("SKIP_REQUIRES_PIPELINE_PAUSED");
            FailureCheckpointInspection.verify(evidence.review,new JdbcFailureReview(j).inspect(c,evidence.review.subscription.id,evidence.review.subscription.errorGeneration,4L*1024*1024));
            c.rollback();c.setAutoCommit(true);
            j.ddl(c,"CREATE TABLE IF NOT EXISTS "+j.table("skip_plan")+" (pipeline_id VARCHAR(128) NOT NULL,operation_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,stream_seq BIGINT NOT NULL,request_hash VARCHAR(64) NOT NULL,plan_hash VARCHAR(64) NOT NULL,payload "+j.text()+" NOT NULL,run_state VARCHAR(16) NOT NULL,resolution "+j.text()+",resolution_hash VARCHAR(64),PRIMARY KEY(pipeline_id,operation_id))");
            j.ddl(c,"CREATE TABLE IF NOT EXISTS "+j.table("gap")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,stream_seq BIGINT NOT NULL,operation_id VARCHAR(128) NOT NULL,plan_hash VARCHAR(64) NOT NULL,receipt_hash VARCHAR(64) NOT NULL,payload "+j.text()+" NOT NULL,created_at VARCHAR(64) NOT NULL,verification_id VARCHAR(128),verification_hash VARCHAR(64),PRIMARY KEY(pipeline_id,subscription_id,stream_seq),UNIQUE(pipeline_id,operation_id))");
            j.ddl(c,"CREATE TABLE IF NOT EXISTS "+j.table("gap_verification")+" (pipeline_id VARCHAR(128) NOT NULL,operation_id VARCHAR(128) NOT NULL,plan_hash VARCHAR(64) NOT NULL,receipt_hash VARCHAR(64) NOT NULL,payload "+j.text()+" NOT NULL,created_at VARCHAR(64) NOT NULL,PRIMARY KEY(pipeline_id,operation_id))");
            j.ddl(c,"CREATE TABLE IF NOT EXISTS "+j.table("gap_verification_plan")+" (pipeline_id VARCHAR(128) NOT NULL,operation_id VARCHAR(128) NOT NULL,plan_hash VARCHAR(64) NOT NULL,payload "+j.text()+" NOT NULL,created_at VARCHAR(64) NOT NULL,PRIMARY KEY(pipeline_id,operation_id))");
            c.setAutoCommit(false);deadline.verify();verifyTables(j,c);c.commit();
        }
    }
    static Stored load(JdbcJournal j,Connection c,String operation)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT request_hash,plan_hash,payload,run_state,resolution,resolution_hash FROM "+j.table("skip_plan")+" WHERE pipeline_id=? AND operation_id=?")){
            p.setString(1,j.settings.pipelineId);p.setString(2,operation);try(ResultSet r=p.executeQuery()){
                if(!r.next())return null;String payload=bounded(r.getCharacterStream(3),4*1024*1024);Stored result=new Stored();result.hash=r.getString(2);result.state=r.getString(4);
                if(!Codec.sha256(payload).equals(result.hash))throw new IllegalStateException("SKIP_PLAN_CHANGED");result.plan=Codec.decode(payload,SkipPlan.class);result.plan.validate();
                if(!j.settings.pipelineId.equals(result.plan.pipelineId)||!operation.equals(result.plan.id)||!Objects.equals(r.getString(1),result.plan.requestHash)||!result.plan.hash().equals(result.hash)||!Arrays.asList("PREPARED","COMPLETED","CANCELLED").contains(result.state))throw new IllegalStateException("SKIP_PLAN_CHANGED");
                if(!"PREPARED".equals(result.state)){
                    String resolution=bounded(r.getCharacterStream(5),32768);if(!Codec.sha256(resolution).equals(r.getString(6)))throw new IllegalStateException("SKIP_RESOLUTION_CHANGED");result.resolution=Codec.decode(resolution,SkipResolution.class);result.resolution.validate();if(!result.plan.id.equals(result.resolution.planId))throw new IllegalStateException("SKIP_RESOLUTION_CHANGED");
                }else if(r.getString(5)!=null||r.getString(6)!=null)throw new IllegalStateException("SKIP_RESOLUTION_CHANGED");
                if("COMPLETED".equals(result.state))result.receipt=gap(j,c,result.plan);return result;
            }
        }catch(SQLException missing){if(missing(missing))throw new IllegalStateException("SKIP_METADATA_LOST");throw missing;}
    }
    /** Caller holds the job/native preflight locks. Commit this intent before native SKIP. */
    static Stored prepare(JdbcJournal j,Connection c,SkipPlan plan,FailureEvidence evidence)throws Exception{
        plan.validate();Map<String,Object> job=j.locked(c);if(!"PAUSED".equals(job.get("state")))throw new IllegalStateException("SKIP_REQUIRES_PIPELINE_PAUSED");
        Stored existing=load(j,c,plan.id);
        if(existing!=null){if(((Number)job.get("format")).intValue()!=4)throw new IllegalStateException("SKIP_JOURNAL_FORMAT_MISMATCH");if(!existing.plan.requestHash.equals(plan.requestHash))throw new IllegalStateException("SKIP_REQUEST_CONFLICT");return existing;}
        FailureCheckpointInspection.verify(evidence.review,new JdbcFailureReview(j).inspect(c,plan.subscriptionId,plan.errorGeneration,4L*1024*1024));
        requireNoPending(j,c,plan.subscriptionId);verifyTables(j,c);
        JdbcJournal.execute(c,"INSERT INTO "+j.table("skip_plan")+" (pipeline_id,operation_id,subscription_id,stream_seq,request_hash,plan_hash,payload,run_state) VALUES (?,?,?,?,?,?,?,'PREPARED')",plan.pipelineId,plan.id,plan.subscriptionId,plan.sequence,plan.requestHash,plan.hash(),Codec.encode(plan));
        JdbcJournal.execute(c,"UPDATE "+j.table("job")+" SET format_version=4 WHERE pipeline_id=?",plan.pipelineId);
        Stored stored=new Stored();stored.plan=plan;stored.hash=plan.hash();stored.state="PREPARED";return stored;
    }
    static Stored complete(JdbcJournal j,Connection c,SkipPlan plan,SkipReceipt receipt,FailureEvidence evidence,SkipResolution resolution)throws Exception{
        receipt.validate(plan);resolution.validate();if(!plan.id.equals(resolution.planId)||"CANCEL".equals(resolution.action))throw new IllegalArgumentException("SKIP_RESOLUTION_INVALID");Map<String,Object> job=j.locked(c);Stored stored=load(j,c,plan.id);
        if(stored==null||!stored.hash.equals(plan.hash()))throw new IllegalStateException("SKIP_PLAN_CHANGED");
        if("COMPLETED".equals(stored.state)){if(!stored.receipt.hash().equals(receipt.hash()))throw new IllegalStateException("SKIP_RECEIPT_SCOPE_CHANGED");return stored;}
        if(!"PREPARED".equals(stored.state))throw new IllegalStateException("SKIP_PLAN_CANCELLED");
        if(!"PAUSED".equals(job.get("state"))||((Number)job.get("format")).intValue()!=4)throw new IllegalStateException("SKIP_REQUIRES_PIPELINE_PAUSED");
        FailureCheckpointInspection.verify(evidence.review,new JdbcFailureReview(j).inspect(c,plan.subscriptionId,plan.errorGeneration,4L*1024*1024));
        verifyTables(j,c);
        JdbcJournal.execute(c,"INSERT INTO "+j.table("gap")+" (pipeline_id,subscription_id,stream_seq,operation_id,plan_hash,receipt_hash,payload,created_at) VALUES (?,?,?,?,?,?,?,?)",plan.pipelineId,plan.subscriptionId,plan.sequence,plan.id,plan.hash(),receipt.hash(),Codec.encode(receipt),resolution.at);
        JdbcJournal.execute(c,"UPDATE "+j.table("subscription")+" SET acknowledged=?,error_generation=?,error_code='TRANSACTION_SKIPPED_DATA_GAP' WHERE pipeline_id=? AND subscription_id=?",plan.sequence,Math.addExact(plan.errorGeneration,1),plan.pipelineId,plan.subscriptionId);
        JdbcFailureReview.save(j,c,plan.subscriptionId,Math.addExact(plan.errorGeneration,1),plan.epoch,null);
        JdbcJournal.execute(c,"UPDATE "+j.table("skip_plan")+" SET run_state='COMPLETED',resolution=?,resolution_hash=? WHERE pipeline_id=? AND operation_id=?",Codec.encode(resolution),resolution.hash(),plan.pipelineId,plan.id);
        stored.state="COMPLETED";stored.receipt=receipt;stored.resolution=resolution;return stored;
    }
    /** Native absence must already be proven under its row lock; caller commits atomically. */
    static Stored cancel(JdbcJournal j,Connection c,SkipPlan plan,FailureEvidence evidence,SkipResolution resolution)throws Exception{
        resolution.validate();if(!"CANCEL".equals(resolution.action)||!plan.id.equals(resolution.planId))throw new IllegalArgumentException("SKIP_RESOLUTION_INVALID");
        Map<String,Object> job=j.locked(c);Stored stored=load(j,c,plan.id);if(stored==null||!stored.hash.equals(plan.hash()))throw new IllegalStateException("SKIP_PLAN_CHANGED");
        if("CANCELLED".equals(stored.state))return stored;if(!"PREPARED".equals(stored.state))throw new IllegalStateException("SKIP_ALREADY_COMMITTED");
        if(!"PAUSED".equals(job.get("state")))throw new IllegalStateException("SKIP_REQUIRES_PIPELINE_PAUSED");
        FailureCheckpointInspection.verify(evidence.review,new JdbcFailureReview(j).inspect(c,plan.subscriptionId,plan.errorGeneration,4L*1024*1024));
        long generation=Math.addExact(plan.errorGeneration,1);
        JdbcJournal.execute(c,"UPDATE "+j.table("subscription")+" SET error_generation=? WHERE pipeline_id=? AND subscription_id=?",generation,plan.pipelineId,plan.subscriptionId);
        JdbcFailureReview.save(j,c,plan.subscriptionId,generation,evidence.review.failureEpoch,evidence.review.failure);
        JdbcJournal.execute(c,"UPDATE "+j.table("skip_plan")+" SET run_state='CANCELLED',resolution=?,resolution_hash=? WHERE pipeline_id=? AND operation_id=?",Codec.encode(resolution),resolution.hash(),plan.pipelineId,plan.id);
        stored.state="CANCELLED";stored.resolution=resolution;return stored;
    }
    static SkipReceipt gap(JdbcJournal j,Connection c,SkipPlan plan)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT operation_id,plan_hash,receipt_hash,payload FROM "+j.table("gap")+" WHERE pipeline_id=? AND subscription_id=? AND stream_seq=?")){
            p.setString(1,plan.pipelineId);p.setString(2,plan.subscriptionId);p.setLong(3,plan.sequence);try(ResultSet r=p.executeQuery()){
                if(!r.next())throw new IllegalStateException("SKIP_GAP_MISSING");String payload=bounded(r.getCharacterStream(4),32768);
                if(!plan.id.equals(r.getString(1))||!plan.hash().equals(r.getString(2))||!Codec.sha256(payload).equals(r.getString(3)))throw new IllegalStateException("SKIP_GAP_CHANGED");
                SkipReceipt receipt=Codec.decode(payload,SkipReceipt.class);receipt.validate(plan);return receipt;
            }
        }
    }
    static void requireNoPending(JdbcJournal j,Connection c,String subscription)throws Exception{
        String condition=subscription==null?"":" AND subscription_id=?";
        try(PreparedStatement p=c.prepareStatement("SELECT operation_id FROM "+j.table("skip_plan")+" WHERE pipeline_id=? AND run_state='PREPARED'"+condition+" LIMIT 1")){
            p.setString(1,j.settings.pipelineId);if(subscription!=null)p.setString(2,subscription);try(ResultSet r=p.executeQuery()){if(r.next())throw new IllegalStateException("SKIP_RECOVERY_PENDING");}
        }catch(SQLException missing){if(missing(missing))throw new IllegalStateException("SKIP_METADATA_LOST");throw missing;}
    }
    static void verifyCheckpoint(JdbcJournal j,Connection c,Map<String,Object> job,String subscription,SinkAdapter.Checkpoint checkpoint)throws Exception{
        Journal.Subscription sub=j.subscription(c,subscription,true);
        if(((Number)job.get("format")).intValue()!=4||checkpoint.sequence!=sub.acknowledged)throw new IllegalStateException("SKIP_CHECKPOINT_GAP_MISMATCH");
        Stored stored=load(j,c,checkpoint.skipOperation);
        if(stored==null||!"COMPLETED".equals(stored.state)||!stored.plan.subscriptionId.equals(subscription)||stored.plan.sequence!=checkpoint.sequence||!stored.plan.generation.equals(job.get("generation"))||!stored.plan.transactionId.equals(checkpoint.transactionId)||!stored.plan.projectedHash.equals(checkpoint.transactionHash)||!stored.receipt.hash().equals(checkpoint.skipHash))throw new IllegalStateException("SKIP_CHECKPOINT_GAP_MISMATCH");
    }
    static Map<String,Object> summary(JdbcJournal j,Connection c)throws Exception{
        Map<String,Object> result=new LinkedHashMap<>();Map<String,Integer> counts=new TreeMap<>();int total=0,open=0,verified=0;
        try(PreparedStatement p=c.prepareStatement("SELECT g.subscription_id,g.verification_id,g.verification_hash,v.receipt_hash FROM "+j.table("gap")+" g LEFT JOIN "+j.table("gap_verification")+" v ON v.pipeline_id=g.pipeline_id AND v.operation_id=g.verification_id WHERE g.pipeline_id=? ORDER BY g.subscription_id,g.stream_seq LIMIT 1001")){
            p.setString(1,j.settings.pipelineId);try(ResultSet r=p.executeQuery()){while(r.next()){total++;counts.merge(r.getString(1),1,Integer::sum);String id=r.getString(2),hash=r.getString(3);if(id==null&&hash==null)open++;else{if(id==null||hash==null||!hash.equals(r.getString(4)))throw new IllegalStateException("GAP_VERIFICATION_RECORD_CHANGED");verified++;}}}
        }catch(SQLException missing){if(missing(missing))throw new IllegalStateException("SKIP_METADATA_LOST");throw missing;}
        result.put("openGapCount",open);result.put("verifiedGapCount",verified);result.put("hasOpenGaps",open>0?Boolean.TRUE:total==1001?null:Boolean.FALSE);result.put("hasGaps",total>0);result.put("gapCount",total);result.put("gapCountLimited",total==1001);result.put("gapsBySubscription",counts);return result;
    }
    private static boolean missing(SQLException e){return "42P01".equals(e.getSQLState())||"42S02".equals(e.getSQLState())||e.getErrorCode()==1146;}
    private static String bounded(Reader value,int maximum)throws Exception{
        if(value==null)throw new IllegalStateException("SKIP_PAYLOAD_MISSING");try(Reader reader=value){StringBuilder text=new StringBuilder();char[] buffer=new char[4096];int n;while((n=reader.read(buffer))!=-1){if(text.length()+n>maximum)throw new IllegalStateException("SKIP_PAYLOAD_TOO_LARGE");text.append(buffer,0,n);}String result=text.toString();if(result.getBytes(StandardCharsets.UTF_8).length>maximum)throw new IllegalStateException("SKIP_PAYLOAD_TOO_LARGE");return result;}
    }
}
