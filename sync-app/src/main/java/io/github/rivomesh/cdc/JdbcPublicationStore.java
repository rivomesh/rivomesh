package io.github.rivomesh.cdc;

import java.sql.*;
import java.util.*;
import static io.github.rivomesh.cdc.JdbcJournal.execute;

/** Local-journal receipts for at-least-once event destinations; not a broker transaction claim. */
public final class JdbcPublicationStore implements PublicationStore {
    private final JdbcJournal journal;
    private final Journal.Lease lease;
    private final String subscription,pipeline;
    public JdbcPublicationStore(JdbcJournal journal,String subscription,Journal.Lease lease){this.journal=journal;this.subscription=subscription;this.lease=lease;pipeline=journal.settings.pipelineId;}
    static void initialize(JdbcJournal j,Connection c)throws Exception{
        j.ddl(c,"CREATE TABLE IF NOT EXISTS "+j.table("publication")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,generation VARCHAR(128) NOT NULL,epoch BIGINT NOT NULL,stream_seq BIGINT NOT NULL,transaction_id VARCHAR(512),transaction_hash VARCHAR(64),PRIMARY KEY(pipeline_id,subscription_id))");
        j.ddl(c,"CREATE TABLE IF NOT EXISTS "+j.table("publication_schema")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,schema_version INTEGER NOT NULL,payload_hash VARCHAR(64) NOT NULL,PRIMARY KEY(pipeline_id,subscription_id,table_id,schema_version))");
        j.ddl(c,"CREATE TABLE IF NOT EXISTS "+j.table("publication_snapshot")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,snapshot_id VARCHAR(36) NOT NULL,run_state VARCHAR(16) NOT NULL,next_part INTEGER NOT NULL,row_count BIGINT NOT NULL,last_hash VARCHAR(64),PRIMARY KEY(pipeline_id,subscription_id,table_id))");
        j.ddl(c,"CREATE TABLE IF NOT EXISTS "+j.table("publication_transport")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,generation VARCHAR(128) NOT NULL,payload VARCHAR(2048) NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
    }
    private String t(String table){return journal.table(table);}
    @Override public String pipelineId(){return pipeline;}
    @Override public String generation()throws Exception{return (String)journal.status().get("generation");}
    private Map<String,Object> fence(Connection c,long epoch)throws Exception{
        Map<String,Object> job=lease==null?journal.locked(c):journal.fence(c,lease);
        if((long)job.get("epoch")!=epoch)throw new JdbcJournal.Fenced();
        if(lease==null&&"RUNNING".equals(job.get("state")))throw new IllegalStateException("CONTROL_FENCE_REQUIRES_PAUSE");
        boolean exists=false;for(Journal.Subscription sub:journal.subscriptions(c))if(subscription.equals(sub.id)){exists=true;if(lease!=null&&!"RUNNING".equals(sub.state))throw new JdbcJournal.Paused();}if(!exists)throw new IllegalArgumentException("SUBSCRIPTION_NOT_FOUND");return job;
    }
    @Override public void check(long epoch)throws Exception{try(Connection c=journal.connect()){c.setAutoCommit(false);fence(c,epoch);c.rollback();}}
    void beforeSend(long epoch,List<ConfirmedPublisher.Message> messages)throws Exception{
        if(journal.historyOrigin()==null||!journal.settings.kafkaPublicationTargets.contains(subscription))return;
        try(Connection c=journal.connect()){
            c.setAutoCommit(false);
            if(lease==null)throw new JdbcJournal.Fenced();
            Map<String,Object> job=fence(c,epoch);
            JdbcKafkaHistoryOrigin.beforeSend(journal,c,subscription,(String)job.get("generation"),epoch,messages);
            c.commit();
        }
    }
    private String progress(Connection c,String generation)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT generation,payload FROM "+t("publication_transport")+" WHERE pipeline_id=? AND subscription_id=?")){p.setString(1,pipeline);p.setString(2,subscription);try(ResultSet rs=p.executeQuery()){if(!rs.next())return null;if(!generation.equals(rs.getString(1)))throw new IllegalStateException("PUBLICATION_GENERATION_MISMATCH");return rs.getString(2);}}
    }
    @Override public String progress()throws Exception{String generation=generation();try(Connection c=journal.connect()){return progress(c,generation);}}
    @Override public void progress(long epoch,String expected,String confirmed)throws Exception{
        if(confirmed==null||confirmed.isEmpty()||confirmed.length()>2048)throw new IllegalArgumentException("PUBLICATION_TRANSPORT_PROGRESS_INVALID");
        try(Connection c=journal.connect()){c.setAutoCommit(false);Map<String,Object> job=fence(c,epoch);String generation=(String)job.get("generation"),previous=progress(c,generation);
            if(!Objects.equals(previous,expected))throw new IllegalStateException("PUBLICATION_TRANSPORT_PROGRESS_CHANGED");
            if(previous==null)execute(c,"INSERT INTO "+t("publication_transport")+" (pipeline_id,subscription_id,generation,payload) VALUES (?,?,?,?)",pipeline,subscription,generation,confirmed);
            else execute(c,"UPDATE "+t("publication_transport")+" SET payload=? WHERE pipeline_id=? AND subscription_id=?",confirmed,pipeline,subscription);c.commit();}
    }
    @Override public void identity(long epoch,String identity)throws Exception{
        if(identity==null||identity.isEmpty())throw new IllegalArgumentException("BROKER_IDENTITY_REQUIRED");String hash=Codec.sha256(identity);
        // Real table versions start at one. Version zero is a reserved transport binding.
        try(Connection c=journal.connect()){c.setAutoCommit(false);fence(c,epoch);String previous=null;
            try(PreparedStatement p=c.prepareStatement("SELECT payload_hash FROM "+t("publication_schema")+" WHERE pipeline_id=? AND subscription_id=? AND table_id='_transport' AND schema_version=0")){p.setString(1,pipeline);p.setString(2,subscription);try(ResultSet rs=p.executeQuery()){if(rs.next())previous=rs.getString(1);}}
            if(previous==null)execute(c,"INSERT INTO "+t("publication_schema")+" (pipeline_id,subscription_id,table_id,schema_version,payload_hash) VALUES (?,?,'_transport',0,?)",pipeline,subscription,hash);
            else if(!previous.equals(hash))throw new IllegalStateException("BROKER_IDENTITY_CHANGED");c.commit();
        }
    }
    private SinkAdapter.Checkpoint checkpoint(Connection c)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT epoch,stream_seq,transaction_id,transaction_hash FROM "+t("publication")+" WHERE pipeline_id=? AND subscription_id=?")){
            p.setString(1,pipeline);p.setString(2,subscription);try(ResultSet rs=p.executeQuery()){SinkAdapter.Checkpoint cp=new SinkAdapter.Checkpoint();if(rs.next()){cp.epoch=rs.getLong(1);cp.sequence=rs.getLong(2);cp.transactionId=rs.getString(3);cp.transactionHash=rs.getString(4);}return cp;}
        }
    }
    @Override public SinkAdapter.Checkpoint checkpoint()throws Exception{try(Connection c=journal.connect()){return checkpoint(c);}}
    @Override public SinkAdapter.Checkpoint activate(long epoch)throws Exception{
        try(Connection c=journal.connect()){c.setAutoCommit(false);Map<String,Object> job=fence(c,epoch);
            boolean exists;try(PreparedStatement p=c.prepareStatement("SELECT generation FROM "+t("publication")+" WHERE pipeline_id=? AND subscription_id=?")){p.setString(1,pipeline);p.setString(2,subscription);try(ResultSet rs=p.executeQuery()){exists=rs.next();if(exists&&!job.get("generation").equals(rs.getString(1)))throw new IllegalStateException("PUBLICATION_GENERATION_MISMATCH");}}
            if(!exists)execute(c,"INSERT INTO "+t("publication")+" (pipeline_id,subscription_id,generation,epoch,stream_seq) VALUES (?,?,?,?,0)",pipeline,subscription,job.get("generation"),epoch);
            else execute(c,"UPDATE "+t("publication")+" SET epoch=? WHERE pipeline_id=? AND subscription_id=?",epoch,pipeline,subscription);
            SinkAdapter.Checkpoint cp=checkpoint(c);c.commit();return cp;
        }
    }
    private String schemaHash(Connection c,TableSchema table)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT payload_hash FROM "+t("publication_schema")+" WHERE pipeline_id=? AND subscription_id=? AND table_id=? AND schema_version=?")){
            p.setString(1,pipeline);p.setString(2,subscription);p.setString(3,table.id);p.setInt(4,table.version);try(ResultSet rs=p.executeQuery()){return rs.next()?rs.getString(1):null;}
        }
    }
    @Override public boolean schemaPrepared(TableSchema table)throws Exception{try(Connection c=journal.connect()){String hash=schemaHash(c,table);if(hash!=null&&!hash.equals(Codec.sha256(Codec.encode(table))))throw new IllegalStateException("PUBLICATION_SCHEMA_CHANGED");return hash!=null;}}
    @Override public void schemaPrepared(long epoch,TableSchema table)throws Exception{
        try(Connection c=journal.connect()){c.setAutoCommit(false);fence(c,epoch);String hash=schemaHash(c,table),expected=Codec.sha256(Codec.encode(table));if(hash!=null&&!hash.equals(expected))throw new IllegalStateException("PUBLICATION_SCHEMA_CHANGED");
            if(hash==null)execute(c,"INSERT INTO "+t("publication_schema")+" (pipeline_id,subscription_id,table_id,schema_version,payload_hash) VALUES (?,?,?,?,?)",pipeline,subscription,table.id,table.version,expected);c.commit();}
    }
    @Override public void committed(long epoch,long sequence,String transactionId,String hash)throws Exception{
        committed(epoch,sequence,transactionId,hash,null);
    }
    void committed(long epoch,long sequence,String transactionId,String hash,KafkaPhysicalReceipt.Signed evidence)throws Exception{
        try(Connection c=journal.connect()){c.setAutoCommit(false);var job=fence(c,epoch);SinkAdapter.Checkpoint cp=checkpoint(c);
            if(cp.epoch!=epoch)throw new JdbcJournal.Fenced();if(cp.sequence==sequence&&transactionId.equals(cp.transactionId)&&hash.equals(cp.transactionHash)){c.commit();return;}
            if(sequence!=cp.sequence+1)throw new IllegalStateException("PUBLICATION_SEQUENCE_GAP");
            if(evidence!=null)JdbcKafkaPhysicalHistory.stage(journal,c,subscription,(String)job.get("generation"),epoch,sequence,hash,evidence);
            execute(c,"UPDATE "+t("publication")+" SET stream_seq=?,transaction_id=?,transaction_hash=? WHERE pipeline_id=? AND subscription_id=?",sequence,transactionId,hash,pipeline,subscription);c.commit();}
    }
    boolean fixedKafkaRoute(){
        return journal.settings.kafkaFixedTopics.containsKey(subscription);
    }
    KafkaPhysicalReceipt.Signed kafkaPhysicalReceipt(String generation,KafkaPhysicalReceipt.Grant grant)throws Exception{
        try(Connection c=journal.connect()){
            return JdbcKafkaPhysicalHistory.verified(journal,c,subscription,generation,grant);
        }
    }
    private SinkAdapter.SnapshotCheckpoint snapshot(Connection c,String tableId)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT snapshot_id,run_state,next_part,row_count FROM "+t("publication_snapshot")+" WHERE pipeline_id=? AND subscription_id=? AND table_id=?")){
            p.setString(1,pipeline);p.setString(2,subscription);p.setString(3,tableId);try(ResultSet rs=p.executeQuery()){if(!rs.next())return null;SinkAdapter.SnapshotCheckpoint cp=new SinkAdapter.SnapshotCheckpoint();cp.snapshotId=rs.getString(1);cp.state=rs.getString(2);cp.nextPart=rs.getInt(3);cp.rows=rs.getLong(4);return cp;}
        }
    }
    @Override public SinkAdapter.SnapshotCheckpoint snapshot(String tableId)throws Exception{try(Connection c=journal.connect()){return snapshot(c,tableId);}}
    @Override public void begin(long epoch,TableSchema table,Journal.SnapshotState state)throws Exception{
        try(Connection c=journal.connect()){c.setAutoCommit(false);fence(c,epoch);SinkAdapter.SnapshotCheckpoint cp=snapshot(c,table.id);
            if(cp==null)execute(c,"INSERT INTO "+t("publication_snapshot")+" (pipeline_id,subscription_id,table_id,snapshot_id,run_state,next_part,row_count) VALUES (?,?,?,?,'BUILDING',0,0)",pipeline,subscription,table.id,state.snapshotId);
            else if(!state.snapshotId.equals(cp.snapshotId))throw new IllegalStateException("PUBLICATION_SNAPSHOT_REBASE_REQUIRES_PLAN");c.commit();}
    }
    @Override public void part(long epoch,TableSchema table,String snapshotId,int part,int rows,String hash)throws Exception{
        try(Connection c=journal.connect()){c.setAutoCommit(false);fence(c,epoch);SinkAdapter.SnapshotCheckpoint cp=snapshot(c,table.id);
            if(cp==null||!snapshotId.equals(cp.snapshotId))throw new IllegalStateException("PUBLICATION_SNAPSHOT_CHANGED");
            if(part==cp.nextPart-1){try(PreparedStatement p=c.prepareStatement("SELECT last_hash FROM "+t("publication_snapshot")+" WHERE pipeline_id=? AND subscription_id=? AND table_id=?")){p.setString(1,pipeline);p.setString(2,subscription);p.setString(3,table.id);try(ResultSet rs=p.executeQuery()){rs.next();if(!hash.equals(rs.getString(1)))throw new IllegalStateException("PUBLICATION_PART_CHANGED");}}c.commit();return;}
            if(!"BUILDING".equals(cp.state)||part!=cp.nextPart)throw new IllegalStateException("PUBLICATION_PART_GAP");
            execute(c,"UPDATE "+t("publication_snapshot")+" SET next_part=?,row_count=?,last_hash=? WHERE pipeline_id=? AND subscription_id=? AND table_id=?",Math.addExact(part,1),Math.addExact(cp.rows,rows),hash,pipeline,subscription,table.id);c.commit();}
    }
    @Override public void finish(long epoch,TableSchema table,Journal.SnapshotState state)throws Exception{
        try(Connection c=journal.connect()){c.setAutoCommit(false);fence(c,epoch);SinkAdapter.SnapshotCheckpoint cp=snapshot(c,table.id);
            if(cp==null||!state.snapshotId.equals(cp.snapshotId)||cp.nextPart!=state.nextPart||cp.rows!=state.rows)throw new IllegalStateException("PUBLICATION_SNAPSHOT_MANIFEST_MISMATCH");
            execute(c,"UPDATE "+t("publication_snapshot")+" SET run_state='COMPLETE' WHERE pipeline_id=? AND subscription_id=? AND table_id=?",pipeline,subscription,table.id);c.commit();}
    }
}
