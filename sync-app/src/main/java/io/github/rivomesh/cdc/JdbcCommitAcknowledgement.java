package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.recovery.*;
import java.io.Reader;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Confirms an already committed APPLY. Used only after durable administrator admission. */
public final class JdbcCommitAcknowledgement {
    private JdbcCommitAcknowledgement(){}
    public static final class Receipt {
        public int version=1;
        public String operationId,requestHash,evidenceId,evidenceHash,actor,reason,confirmedAt,pipelineId,subscriptionId,generation,fingerprint;
        public long journalEpoch,sequence,previousAcknowledged,previousErrorGeneration,errorGeneration;
        public SinkAdapter.Checkpoint checkpoint;
    }
    static void initialize(JdbcJournal journal,Connection c)throws Exception{
        journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+journal.table("recovery_ack")+" (pipeline_id VARCHAR(128) NOT NULL,operation_id VARCHAR(128) NOT NULL,request_hash VARCHAR(64) NOT NULL,payload_hash VARCHAR(64) NOT NULL,payload "+journal.text()+" NOT NULL,PRIMARY KEY(pipeline_id,operation_id))");
    }
    /** Explicit admin mutation may add its receipt table to an existing paused pre-upgrade journal. */
    public static void prepare(PipelineConfig config,FailureEvidence value)throws Exception{
        value.validate();if(!config.pipelineId.equals(value.review.pipelineId)||!config.fingerprint().equals(value.review.fingerprint))throw new IllegalStateException("FAILURE_CHECKPOINT_CONFIGURATION_CHANGED");
        PipelineConfig.Target target=config.targets.stream().filter(t->t.id.equals(value.review.subscription.id)).findFirst().orElseThrow(()->new IllegalStateException("FAILURE_CHECKPOINT_SUBSCRIPTION_CHANGED"));
        if(target.product!=TargetProduct.POSTGRESQL&&target.product!=TargetProduct.MYSQL)throw new IllegalStateException("FAILURE_CHECKPOINT_TARGET_UNSUPPORTED");
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){journal.verifyIdentity(config.fingerprint(),config.subscribers());prepare(journal,value);}
    }
    static void prepare(JdbcJournal journal,FailureEvidence value)throws Exception{
        try(Connection c=journal.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            try(PreparedStatement p=c.prepareStatement("SELECT operation_id FROM "+journal.table("recovery_ack")+" WHERE pipeline_id=? AND operation_id=?")){
                p.setString(1,journal.settings.pipelineId);p.setString(2,"");try(ResultSet ignored=p.executeQuery()){return;}
            }catch(SQLException missing){if(!"42P01".equals(missing.getSQLState())&&!"42S02".equals(missing.getSQLState())&&missing.getErrorCode()!=1146)throw missing;}
            c.setAutoCommit(false);if(!"PAUSED".equals(journal.locked(c).get("state")))throw new IllegalStateException("FAILURE_ACK_REQUIRES_PIPELINE_PAUSED");
            FailureCheckpointInspection.verify(value.review,new JdbcFailureReview(journal).inspect(c,value.review.subscription.id,value.review.subscription.errorGeneration,4L*1024*1024));
            c.rollback(); // MySQL DDL commits implicitly. No recovery fence is assumed across DDL.
            initialize(journal,c);deadline.verify();c.commit();
            // The confirmation transaction reacquires every fence and revalidates the live head.
        }
    }
    /** Caller supplies a durably admitted administrator identity; the journal and native guards revalidate the commit. */
    public static Receipt confirm(PipelineConfig config,FailureEvidenceStore store,String evidenceId,String evidenceHash,String operation,String actor,String reason)throws Exception{
        FailureEvidenceStore.Envelope retained=store.load(evidenceId);FailureEvidence value=retained.value;
        if(!config.pipelineId.equals(value.review.pipelineId)||!config.fingerprint().equals(value.review.fingerprint))throw new IllegalStateException("FAILURE_CHECKPOINT_CONFIGURATION_CHANGED");
        PipelineConfig.Target target=config.targets.stream().filter(t->t.id.equals(value.review.subscription.id)).findFirst().orElseThrow(()->new IllegalStateException("FAILURE_CHECKPOINT_SUBSCRIPTION_CHANGED"));
        if(target.product!=TargetProduct.POSTGRESQL&&target.product!=TargetProduct.MYSQL)throw new IllegalStateException("FAILURE_CHECKPOINT_TARGET_UNSUPPORTED");
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            return confirm(journal,retained,config.targetSettings(target,value.review.generation),evidenceHash,operation,actor,reason);
        }
    }
    static Receipt confirm(JdbcJournal journal,FailureEvidenceStore.Envelope retained,JdbcSink.Settings settings,String evidenceHash,String operation,String actor,String reason)throws Exception{
        FailureEvidence value=retained.value;value.validate();Journal.FailureReview expected=value.review;
        if(!Objects.equals(retained.hash,evidenceHash))throw new IllegalStateException("FAILURE_EVIDENCE_CONTENT_CHANGED");
        if(operation==null||!operation.matches("[A-Za-z0-9_-]{8,128}")||actor==null||actor.isEmpty()||actor.length()>128||reason==null||reason.trim().length()<8||reason.length()>1000)throw new IllegalArgumentException("FAILURE_ACK_REQUEST_INVALID");
        if(!Objects.equals(settings.pipelineId,expected.pipelineId)||!Objects.equals(settings.id,expected.subscription.id)||!Objects.equals(settings.fingerprint,expected.fingerprint)||!Objects.equals(settings.generation,expected.generation))throw new IllegalStateException("FAILURE_CHECKPOINT_CONFIGURATION_CHANGED");
        Map<String,Object> request=new TreeMap<>();request.put("operation",operation);request.put("evidenceId",value.id);request.put("evidenceHash",evidenceHash);request.put("actor",actor);request.put("reason",reason);
        String requestHash=Codec.sha256(Codec.encode(request));
        try(Connection c=journal.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);Map<String,Object> job=journal.locked(c);
            Receipt prior=receipt(journal,c,operation,requestHash);
            if(prior!=null){c.rollback();return prior;}
            transactionalMetadata(journal,c);
            // Whole-pipeline pause has advanced the epoch and revoked all old Workers. A target
            // pause alone cannot fence a stale in-flight writer and is not sufficient here.
            if(!"PAUSED".equals(job.get("state")))throw new IllegalStateException("FAILURE_ACK_REQUIRES_PIPELINE_PAUSED");
            Journal.FailureReview current=new JdbcFailureReview(journal).inspect(c,expected.subscription.id,expected.subscription.errorGeneration,4L*1024*1024);
            FailureCheckpointInspection.verify(expected,current);
            SourceTransaction projected=FailureCheckpointInspection.projected(value,settings);
            String projectedHash=Codec.sha256(Codec.encode(projected));
            try(JdbcSink sink=JdbcSink.existing(settings)){
                SinkAdapter.Checkpoint checkpoint=sink.checkpoint();
                if(checkpoint.epoch!=current.epoch)throw new IllegalStateException("FAILURE_ACK_TARGET_FENCE_REQUIRED");
                if(checkpoint.skipOperation!=null||checkpoint.sequence!=current.sequence||!projected.id.equals(checkpoint.transactionId)||!projectedHash.equals(checkpoint.transactionHash))throw new IllegalStateException("FAILURE_ACK_COMMIT_NOT_PROVEN");
                return sink.withRecoveryCheckpoint(current.epoch,checkpoint,locked->{
                    deadline.verify();
                    Receipt result=new Receipt();result.operationId=operation;result.requestHash=requestHash;result.evidenceId=value.id;result.evidenceHash=evidenceHash;result.actor=actor;result.reason=reason;result.confirmedAt=Instant.now().toString();
                    result.pipelineId=current.pipelineId;result.subscriptionId=current.subscription.id;result.generation=current.generation;result.fingerprint=current.fingerprint;result.journalEpoch=current.epoch;result.sequence=current.sequence;result.previousAcknowledged=current.subscription.acknowledged;result.previousErrorGeneration=current.subscription.errorGeneration;result.errorGeneration=Math.addExact(current.subscription.errorGeneration,1);result.checkpoint=locked;
                    String payload=Codec.encode(result);
                    JdbcJournal.execute(c,"INSERT INTO "+journal.table("recovery_ack")+" (pipeline_id,operation_id,request_hash,payload_hash,payload) VALUES (?,?,?,?,?)",current.pipelineId,operation,requestHash,Codec.sha256(payload),payload);
                    JdbcJournal.execute(c,"UPDATE "+journal.table("subscription")+" SET acknowledged=?,error_generation=?,error_code='COMMIT_RECONCILED_REVIEW_NEXT' WHERE pipeline_id=? AND subscription_id=?",current.sequence,result.errorGeneration,current.pipelineId,current.subscription.id);
                    JdbcFailureReview.save(journal,c,current.subscription.id,result.errorGeneration,current.epoch,null);
                    deadline.verify();c.commit();return result;
                });
            }
        }
    }
    private static void transactionalMetadata(JdbcJournal journal,Connection c)throws Exception{
        if(!c.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT).contains("mysql"))return;
        try(PreparedStatement p=c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rm3_job','rm3_subscription','rm3_failure','rm3_recovery_ack')")){
            p.setString(1,journal.settings.namespace);try(ResultSet rs=p.executeQuery()){
                int count=0;while(rs.next()){count++;if(!"InnoDB".equalsIgnoreCase(rs.getString(1)))throw new IllegalStateException("FAILURE_ACK_METADATA_NOT_TRANSACTIONAL");}
                if(count!=4)throw new IllegalStateException("FAILURE_ACK_METADATA_UPGRADE_REQUIRED");
            }
        }
    }
    private static Receipt receipt(JdbcJournal journal,Connection c,String operation,String requestHash)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT request_hash,payload_hash,payload FROM "+journal.table("recovery_ack")+" WHERE pipeline_id=? AND operation_id=?")){
            p.setString(1,journal.settings.pipelineId);p.setString(2,operation);try(ResultSet rs=p.executeQuery()){
                if(!rs.next())return null;
                if(!requestHash.equals(rs.getString(1)))throw new IllegalStateException("FAILURE_ACK_REQUEST_CONFLICT");
                StringBuilder text=new StringBuilder();try(Reader reader=rs.getCharacterStream(3)){char[] buffer=new char[1024];int n;while((n=reader.read(buffer))!=-1){if(text.length()+n>32768)throw new IllegalStateException("FAILURE_ACK_RECEIPT_INVALID");text.append(buffer,0,n);}}
                String payload=text.toString();if(!Codec.sha256(payload).equals(rs.getString(2)))throw new IllegalStateException("FAILURE_ACK_RECEIPT_CHANGED");
                Receipt result=Codec.decode(payload,Receipt.class);
                if(result.version!=1||!operation.equals(result.operationId)||!requestHash.equals(result.requestHash)||!journal.settings.pipelineId.equals(result.pipelineId))throw new IllegalStateException("FAILURE_ACK_RECEIPT_INVALID");return result;
            }
        }catch(SQLException missing){if("42P01".equals(missing.getSQLState())||"42S02".equals(missing.getSQLState())||missing.getErrorCode()==1146)throw new IllegalStateException("FAILURE_ACK_METADATA_UPGRADE_REQUIRED");throw missing;}
    }
}
