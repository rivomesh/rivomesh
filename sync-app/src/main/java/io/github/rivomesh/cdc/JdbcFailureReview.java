package io.github.rivomesh.cdc;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

import static io.github.rivomesh.cdc.JdbcJournal.execute;

/** Read-only paused-head inspection. Never normalizes or changes either checkpoint. */
final class JdbcFailureReview {
    static final long MAXIMUM_BYTES=4L*1024*1024;
    private final JdbcJournal journal;
    JdbcFailureReview(JdbcJournal journal){this.journal=journal;}
    static void initialize(JdbcJournal journal,Connection c)throws Exception{
        journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+journal.table("failure")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,error_generation BIGINT NOT NULL,owner_epoch BIGINT NOT NULL,payload "+journal.text()+" NOT NULL,payload_hash VARCHAR(64) NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
    }
    static void save(JdbcJournal journal,Connection c,String subscription,long generation,long epoch,SubscriberFailure failure)throws Exception{
        execute(c,"DELETE FROM "+journal.table("failure")+" WHERE pipeline_id=? AND subscription_id=?",journal.settings.pipelineId,subscription);
        if(failure==null)return;
        String payload=Codec.encode(failure);
        execute(c,"INSERT INTO "+journal.table("failure")+" (pipeline_id,subscription_id,error_generation,owner_epoch,payload,payload_hash) VALUES (?,?,?,?,?,?)",journal.settings.pipelineId,subscription,generation,epoch,payload,Codec.sha256(payload));
    }
    List<Map<String,Object>> summaries()throws Exception{
        try(Connection c=journal.connect()){
            c.setAutoCommit(false);journal.locked(c);List<Map<String,Object>> result=new ArrayList<>();
            for(Journal.Subscription sub:journal.subscriptions(c)){
                Map<String,Object> row=new LinkedHashMap<>();row.put("id",sub.id);row.put("state",sub.state);row.put("acknowledged",sub.acknowledged);row.put("errorGeneration",sub.errorGeneration);row.put("errorCode",sub.errorCode);
                if("PAUSED".equals(sub.state)){
                    Journal.FailureReview review=new Journal.FailureReview();review.subscription=sub;
                    try{
                        diagnostic(c,review);row.put("failurePhase",review.failure.phase.name());
                        if(review.failure.phase!=SubscriberFailure.Phase.UNKNOWN){row.put("failureEpoch",review.failureEpoch);if(review.failure.sequence>0)row.put("failedSequence",review.failure.sequence);}
                    }catch(IllegalStateException|IllegalArgumentException corrupt){row.put("failurePhase","UNKNOWN");row.put("failureDiagnosticProblem","FAILURE_DIAGNOSTIC_UNAVAILABLE");}
                }result.add(row);
            }c.rollback();return result;
        }
    }
    Map<String,TableSchema> schemas(Set<String> versions,long maximumBytes)throws Exception{
        if(versions==null||maximumBytes<1||maximumBytes>2L*1024*1024)throw new IllegalArgumentException("FAILURE_SCHEMA_LIMIT_INVALID");
        Map<String,TableSchema> result=new TreeMap<>();long remaining=maximumBytes;
        try(Connection c=journal.connect()){
            for(String reference:new TreeSet<>(versions)){
                int split=reference==null?-1:reference.lastIndexOf('/');if(split<1||split>64)throw new IllegalArgumentException("FAILURE_SCHEMA_REFERENCE_INVALID");
                String id=reference.substring(0,split);int version;
                try{version=Integer.parseInt(reference.substring(split+1));if(version<1||!reference.equals(id+"/"+version))throw new NumberFormatException();}catch(RuntimeException bad){throw new IllegalArgumentException("FAILURE_SCHEMA_REFERENCE_INVALID");}
                try(PreparedStatement p=c.prepareStatement("SELECT payload,payload_hash FROM "+journal.table("schema")+" WHERE pipeline_id=? AND table_id=? AND schema_version=?")){
                    p.setString(1,journal.settings.pipelineId);p.setString(2,id);p.setInt(3,version);try(ResultSet rs=p.executeQuery()){
                        if(!rs.next())throw new IllegalStateException("HISTORICAL_SCHEMA_MISSING");String payload=bounded(rs.getCharacterStream(1),remaining);
                        if(!Codec.sha256(payload).equals(rs.getString(2)))throw new IllegalStateException("SCHEMA_CHECKSUM_MISMATCH");
                        TableSchema schema=Codec.decode(payload,TableSchema.class);schema.validate();if(!id.equals(schema.id)||version!=schema.version)throw new IllegalStateException("HISTORICAL_SCHEMA_IDENTITY_CHANGED");
                        remaining-=payload.getBytes(StandardCharsets.UTF_8).length;result.put(reference,schema);
                    }
                }
            }
        }return result;
    }
    Journal.FailureReview inspect(String subscription,long expectedGeneration,long maximumBytes)throws Exception{
        if(expectedGeneration<0||maximumBytes<1||maximumBytes>MAXIMUM_BYTES)throw new IllegalArgumentException("FAILURE_REVIEW_LIMIT_INVALID");
        try(Connection c=journal.connect()){
            c.setAutoCommit(false);Journal.FailureReview review=inspect(c,subscription,expectedGeneration,maximumBytes);c.rollback();return review;
        }
    }
    Journal.FailureReview inspect(Connection c,String subscription,long expectedGeneration,long maximumBytes)throws Exception{
        if(expectedGeneration<0||maximumBytes<1||maximumBytes>MAXIMUM_BYTES)throw new IllegalArgumentException("FAILURE_REVIEW_LIMIT_INVALID");
            // Same lock order as append/ACK/collection: resume or collection cannot split this view.
            Map<String,Object> job=journal.locked(c);
            Journal.Subscription sub=journal.subscription(c,subscription,true);
            if(sub.errorGeneration!=expectedGeneration)throw new IllegalStateException("ERROR_GENERATION_CHANGED");
            if(!"PAUSED".equals(sub.state))throw new IllegalStateException("FAILURE_REVIEW_REQUIRES_PAUSED");
            long sequence=Math.addExact(sub.acknowledged,1);
            if(sequence>=(long)job.get("next"))throw new IllegalStateException("FAILURE_TRANSACTION_UNAVAILABLE");
            Journal.FailureReview review=new Journal.FailureReview();review.pipelineId=journal.settings.pipelineId;
            review.epoch=(long)job.get("epoch");review.generation=(String)job.get("generation");review.sequence=sequence;review.subscription=sub;
            diagnostic(c,review);
            if(review.failure.phase==SubscriberFailure.Phase.TARGET_PREPARE||review.failure.phase==SubscriberFailure.Phase.TARGET_SNAPSHOT)throw new IllegalStateException("FAILURE_REVIEW_REQUIRES_INCREMENTAL_FAILURE");
            try(PreparedStatement p=c.prepareStatement("SELECT fingerprint FROM "+journal.table("job")+" WHERE pipeline_id=?")){
                p.setString(1,journal.settings.pipelineId);try(ResultSet rs=p.executeQuery()){if(!rs.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");review.fingerprint=rs.getString(1);}
            }
            long remaining=maximumBytes;
            try(PreparedStatement p=c.prepareStatement("SELECT transaction_id,payload_hash,payload_bytes,payload FROM "+journal.table("log")+" WHERE pipeline_id=? AND stream_seq=?")){
                p.setString(1,journal.settings.pipelineId);p.setLong(2,sequence);try(ResultSet rs=p.executeQuery()){
                    if(!rs.next())throw new IllegalStateException("JOURNAL_SEQUENCE_GAP");
                    checkLength(rs.getLong(3),remaining);String payload=bounded(rs.getCharacterStream(4),remaining);
                    review.rawHash=rs.getString(2);if(!Codec.sha256(payload).equals(review.rawHash))throw new IllegalStateException("JOURNAL_CHECKSUM_MISMATCH");
                    review.raw=Codec.decode(payload,SourceTransaction.class);review.raw.validate();
                    if(!rs.getString(1).equals(review.raw.id)||!review.generation.equals(review.raw.streamGeneration))throw new IllegalStateException("FAILURE_TRANSACTION_IDENTITY_MISMATCH");
                    remaining-=payload.getBytes(StandardCharsets.UTF_8).length;
                }
            }
            try(PreparedStatement p=c.prepareStatement("SELECT raw_hash,payload_hash,payload_bytes,payload FROM "+journal.table("normalized")+" WHERE pipeline_id=? AND stream_seq=?")){
                p.setString(1,journal.settings.pipelineId);p.setLong(2,sequence);try(ResultSet rs=p.executeQuery()){if(rs.next()){
                    checkLength(rs.getLong(3),remaining);String payload=bounded(rs.getCharacterStream(4),remaining);review.normalizedHash=rs.getString(2);
                    if(!review.rawHash.equals(rs.getString(1))||!Codec.sha256(payload).equals(review.normalizedHash))throw new IllegalStateException("NORMALIZED_TRANSACTION_CHECKSUM_MISMATCH");
                    review.normalized=Codec.decode(payload,SourceTransaction.class);review.normalized.validate();
                    if(!review.raw.id.equals(review.normalized.id)||!review.generation.equals(review.normalized.streamGeneration)||!review.raw.position.equals(review.normalized.position))throw new IllegalStateException("FAILURE_TRANSACTION_IDENTITY_MISMATCH");
                }}
            }
            if(review.failure.sequence>0&&(review.failure.sequence!=sequence||!review.raw.id.equals(review.failure.transactionId)))throw new IllegalStateException("FAILURE_LOCATION_CHANGED");
            return review;
    }
    private void diagnostic(Connection c,Journal.FailureReview review)throws Exception{
        review.failure=new SubscriberFailure();Savepoint point=c.setSavepoint();
        try(PreparedStatement p=c.prepareStatement("SELECT error_generation,owner_epoch,payload,payload_hash FROM "+journal.table("failure")+" WHERE pipeline_id=? AND subscription_id=?")){
            p.setString(1,journal.settings.pipelineId);p.setString(2,review.subscription.id);
            try(ResultSet rs=p.executeQuery()){if(rs.next()&&rs.getLong(1)==review.subscription.errorGeneration){
                String payload=bounded(rs.getCharacterStream(3),16384);
                if(!Codec.sha256(payload).equals(rs.getString(4)))throw new IllegalStateException("FAILURE_DIAGNOSTIC_CHECKSUM_MISMATCH");
                review.failure=Codec.decode(payload,SubscriberFailure.class);review.failure.validate();review.failureEpoch=rs.getLong(2);
            }}
        }catch(SQLException missing){
            // Read-only inspection of a pre-diagnostic journal does not run migration DDL.
            if(!"42S02".equals(missing.getSQLState())&&!"42P01".equals(missing.getSQLState())&&missing.getErrorCode()!=1146)throw missing;
            c.rollback(point);
        }finally{c.releaseSavepoint(point);}
    }
    private static void checkLength(long bytes,long limit){if(bytes<0)throw new IllegalStateException("FAILURE_PAYLOAD_SIZE_INVALID");if(bytes>limit)throw new IllegalStateException("FAILURE_REVIEW_TOO_LARGE");}
    private static String bounded(Reader reader,long maximumBytes)throws Exception{
        if(reader==null)throw new IllegalStateException("FAILURE_PAYLOAD_MISSING");
        try(Reader input=reader){
            StringBuilder payload=new StringBuilder();char[] buffer=new char[4096];int n;
            while((n=input.read(buffer))!=-1){if((long)payload.length()+n>maximumBytes)throw new IllegalStateException("FAILURE_REVIEW_TOO_LARGE");payload.append(buffer,0,n);}
            String value=payload.toString();if(value.getBytes(StandardCharsets.UTF_8).length>maximumBytes)throw new IllegalStateException("FAILURE_REVIEW_TOO_LARGE");return value;
        }
    }
}
