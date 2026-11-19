package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.recovery.CaptureExclusionPlan;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Version 3 durable journal: one fenced source order, independent subscriber acknowledgments. */
public final class JdbcJournal implements NormalizedJournal {
    public static final class Settings {
        public String driver,url,username="",password="",namespace="rivomesh_v3",pipelineId;
        public String writerAssignment,writerNode;
        public boolean managedWriter;
        public boolean deliveryTiming;
        public Set<String> kafkaPublicationTargets=Set.of();
        @com.fasterxml.jackson.annotation.JsonIgnore
        public KafkaPhysicalReceipt.Grant kafkaPhysicalGrant;
        public long maxPendingBytes=1024L*1024*1024;
        public long leaseMillis=30000;
    }
    public static final class Busy extends IllegalStateException { public Busy(){super("JOURNAL_BUSY");} }
    public static final class Fenced extends IllegalStateException { public Fenced(){super("JOURNAL_LEASE_LOST");} }
    public static final class Capacity extends IllegalStateException { public Capacity(){super("JOURNAL_CAPACITY");} }
    public static final class Paused extends IllegalStateException { public Paused(){super("SUBSCRIPTION_PAUSED");} }
    public static final class PipelinePaused extends IllegalStateException { public PipelinePaused(){super("PIPELINE_PAUSED");} }
    private enum Dialect { H2,POSTGRES,MYSQL }
    private static final long INCREMENTAL_BATCH_BYTES=8L*1024*1024;
    final Settings settings;
    private final Dialect dialect;
    private Connection anchor;

    public JdbcJournal(Settings settings)throws Exception{
        if(settings==null||settings.url==null||settings.driver==null||settings.pipelineId==null
                ||!settings.pipelineId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                ||settings.namespace==null||!settings.namespace.matches("[A-Za-z_][A-Za-z0-9_]{0,62}")||settings.maxPendingBytes<1
                ||settings.managedWriter&&(settings.writerAssignment==null||settings.leaseMillis<1)
                ||settings.writerAssignment!=null&&(!JournalWriterAssignment.validId(settings.writerAssignment)
                ||settings.writerNode==null||!settings.writerNode.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")))
            throw new IllegalArgumentException("JOURNAL_CONFIG_INVALID");
        this.settings=settings;
        anchor=JdbcConnections.open(settings.driver,settings.url,settings.username,settings.password);
        String product=anchor.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        if(product.contains("h2"))dialect=Dialect.H2;
        else if(product.contains("mysql")||product.contains("mariadb"))dialect=Dialect.MYSQL;
        else if(product.contains("postgres")||product.contains("gauss"))dialect=Dialect.POSTGRES;
        else{anchor.close();throw new IllegalArgumentException("JOURNAL_PRODUCT_UNSUPPORTED");}
    }
    Connection connect()throws SQLException{return JdbcConnections.open(settings.driver,settings.url,settings.username,settings.password);}
    private String q(String name){return dialect==Dialect.MYSQL?"`"+name+"`":"\""+name+"\"";}
    String table(String name){return q(settings.namespace)+"."+q("rm3_"+name);}
    String text(){return dialect==Dialect.MYSQL?"LONGTEXT":dialect==Dialect.H2?"CLOB":"TEXT";}
    boolean localH2(){return dialect==Dialect.H2;}
    void requirePhysicalEvidenceTable(Connection c,String name)throws SQLException{
        String sql=switch(dialect){
            case MYSQL -> "SELECT ENGINE='InnoDB' AND TABLE_COLLATION='utf8mb4_bin' FROM information_schema.TABLES "
                    +"WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";
            case POSTGRES -> "SELECT r.relkind='r' AND r.relpersistence='p' AND NOT r.relhasrules FROM pg_catalog.pg_class r "
                    +"JOIN pg_catalog.pg_namespace n ON r.relnamespace=n.oid WHERE n.nspname=? AND r.relname=?";
            case H2 -> "SELECT TABLE_TYPE='BASE TABLE' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";
        };
        try(PreparedStatement p=c.prepareStatement(sql)){
            p.setQueryTimeout(5);p.setString(1,settings.namespace);p.setString(2,"rm3_"+name);
            try(ResultSet r=p.executeQuery()){
                if(!r.next()||!r.getBoolean(1)||r.next())throw new SQLException("Receipt storage requires a durable ordinary table","42P16");
            }
        }
    }
    static int execute(Connection c,String sql,Object... values)throws SQLException{
        try(PreparedStatement p=c.prepareStatement(sql)){for(int i=0;i<values.length;i++)p.setObject(i+1,values[i]);return p.executeUpdate();}
    }
    private static boolean duplicate(SQLException e){return "23505".equals(e.getSQLState())||e.getErrorCode()==1062;}
    void ddl(Connection c,String sql)throws SQLException{
        if(dialect==Dialect.MYSQL&&sql.startsWith("CREATE TABLE "))sql+=" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
        try(Statement s=c.createStatement()){s.execute(sql);}
        catch(SQLException e){if(!Arrays.asList("42P06","42P07","42710","23505").contains(e.getSQLState()))throw e;}
    }
    static boolean assignedWriter(int format){return format>=7&&format<=22;}
    static boolean managedWriter(int format){return format>=11&&format<=22;}
    static boolean workerAdmitted(int format){return format>=19&&format<=22;}
    private static int assignmentFamily(int format){
        if(workerAdmitted(format))return 16;
        if(format>=15&&format<=18)return 12;
        return managedWriter(format)?8:assignedWriter(format)?4:0;
    }
    static int withParkedAdmission(int format){
        if(!managedWriter(format))throw new IllegalArgumentException("HANDOFF_MANAGED_CONTROL_CUT_INVALID");
        return baseFormat(format)+12;
    }
    static int withWorkerAdmission(int format){
        if(format<15||format>18)throw new IllegalStateException("HANDOFF_WORKER_JOURNAL_NOT_PARKED");
        return baseFormat(format)+16;
    }
    private static int baseFormat(int format){return format-assignmentFamily(format);}
    static boolean hasTargetGaps(int format){return baseFormat(format)==4||baseFormat(format)==6;}
    static boolean hasSourceExclusions(int format){return baseFormat(format)==5||baseFormat(format)==6;}
    static int withTargetGaps(int format){return (hasSourceExclusions(format)?6:4)+assignmentFamily(format);}
    static int withSourceExclusions(int format){return (hasTargetGaps(format)?6:5)+assignmentFamily(format);}
    private static boolean supported(int format){return format>=3&&format<=22;}
    private void verifyRecoveryTables(Connection c,int format)throws Exception{
        if(hasTargetGaps(format))JdbcSkipJournal.verifyTables(this,c);
        if(hasSourceExclusions(format))JdbcSourceExclusion.verify(this,c);
    }
    @Override public void initialize(String fingerprint,String generation,List<String> subscribers)throws Exception{
        if(settings.writerAssignment!=null){
            verifyIdentity(fingerprint,subscribers);
            try(Connection c=connect()){
                c.setAutoCommit(false);
                Map<String,Object> job=locked(c);
                if(generation!=null&&!generation.equals(job.get("generation")))
                    throw new IllegalStateException("JOURNAL_IDENTITY_MISMATCH");
                c.commit();
                if(settings.kafkaPhysicalGrant!=null)JdbcKafkaPhysicalHistory.prepare(this,c,false);
            }
        }else initializeStorage(fingerprint,generation,subscribers);
    }
    void initializeStorage(String fingerprint,String generation,List<String> subscribers)throws Exception{
        if(fingerprint==null||fingerprint.length()!=64||(generation!=null&&(generation.isEmpty()||generation.length()>128))||subscribers==null||subscribers.isEmpty())
            throw new IllegalArgumentException("JOURNAL_IDENTITY_INVALID");
        Set<String> ids=new TreeSet<>();
        for(String id:subscribers)if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||!ids.add(id))throw new IllegalArgumentException("SUBSCRIPTION_ID_INVALID");
        try(Connection c=connect()){
            createStorage(c);
            c.setAutoCommit(false);
            try{
                String stream=generation==null?UUID.randomUUID().toString():generation;
                execute(c,"INSERT INTO "+table("job")+" (pipeline_id,format_version,fingerprint,generation,epoch,owner,lease_until,next_seq,captured,pending_bytes,snapshot_bytes,normalized_seq,identity_bytes,run_state,error_generation) VALUES (?,?,?,?,0,NULL,0,1,NULL,0,0,0,0,'RUNNING',0)",settings.pipelineId,settings.managedWriter?11:settings.writerAssignment==null?3:7,fingerprint,stream);
                for(String id:ids)execute(c,"INSERT INTO "+table("subscription")+" (pipeline_id,subscription_id,acknowledged,run_state,error_generation) VALUES (?,?,0,'RUNNING',0)",settings.pipelineId,id);
                if(settings.writerAssignment!=null)JournalWriterAssignment.initial(this,c,stream);
                c.commit();
            }catch(SQLException e){c.rollback();if(!duplicate(e))throw e;}
            try(PreparedStatement p=c.prepareStatement("SELECT format_version,fingerprint,generation FROM "+table("job")+" WHERE pipeline_id=?")){
                p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){
                    if(!rs.next()||!supported(rs.getInt(1))||!fingerprint.equals(rs.getString(2))||(generation!=null&&!generation.equals(rs.getString(3))))throw new IllegalStateException("JOURNAL_IDENTITY_MISMATCH");
                    verifyRecoveryTables(c,rs.getInt(1));
                }
            }
            Set<String> existing=new TreeSet<>();for(Subscription sub:subscriptions(c))existing.add(sub.id);
            if(!existing.equals(ids))throw new IllegalStateException("SUBSCRIPTION_MIGRATION_REQUIRED");
            if(settings.writerAssignment!=null){locked(c);c.commit();}
        }
    }
    void createStorage(Connection c)throws Exception{
        try{ddl(c,(dialect==Dialect.MYSQL?"CREATE DATABASE IF NOT EXISTS ":"CREATE SCHEMA IF NOT EXISTS ")+q(settings.namespace));}
        catch(SQLException rejected){
            if(settings.kafkaPhysicalGrant!=null)throw JdbcKafkaPhysicalHistory.notReady(rejected);
            throw rejected;
        }
        if(settings.kafkaPhysicalGrant!=null)JdbcKafkaPhysicalHistory.prepare(this,c,true);
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("job")+" (pipeline_id VARCHAR(128) PRIMARY KEY,format_version INTEGER NOT NULL,fingerprint VARCHAR(64) NOT NULL,generation VARCHAR(128) NOT NULL,epoch BIGINT NOT NULL,owner VARCHAR(128),lease_until BIGINT NOT NULL,next_seq BIGINT NOT NULL,captured "+text()+",pending_bytes BIGINT NOT NULL,snapshot_bytes BIGINT NOT NULL,normalized_seq BIGINT NOT NULL,identity_bytes BIGINT NOT NULL,run_state VARCHAR(16) NOT NULL,error_generation BIGINT NOT NULL,error_code VARCHAR(128))");
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("pipeline_failure")+" (pipeline_id VARCHAR(128) PRIMARY KEY,error_generation BIGINT NOT NULL,phase VARCHAR(32) NOT NULL)");
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("subscription")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,acknowledged BIGINT NOT NULL,run_state VARCHAR(16) NOT NULL,error_generation BIGINT NOT NULL,error_code VARCHAR(128),PRIMARY KEY(pipeline_id,subscription_id))");
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("log")+" (pipeline_id VARCHAR(128) NOT NULL,stream_seq BIGINT NOT NULL,transaction_id VARCHAR(512) NOT NULL,payload "+text()+" NOT NULL,payload_hash VARCHAR(64) NOT NULL,payload_bytes BIGINT NOT NULL,PRIMARY KEY(pipeline_id,stream_seq))");
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("receipt")+" (pipeline_id VARCHAR(128) NOT NULL,transaction_id VARCHAR(512) NOT NULL,stream_seq BIGINT NOT NULL,payload_hash VARCHAR(64) NOT NULL,PRIMARY KEY(pipeline_id,transaction_id))");
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("schema")+" (pipeline_id VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,schema_version INTEGER NOT NULL,payload "+text()+" NOT NULL,payload_hash VARCHAR(64) NOT NULL,PRIMARY KEY(pipeline_id,table_id,schema_version))");
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("snapshot")+" (pipeline_id VARCHAR(128) NOT NULL,snapshot_scope VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,snapshot_id VARCHAR(36) NOT NULL,schema_version INTEGER NOT NULL,column_count INTEGER NOT NULL,baseline "+text()+" NOT NULL,run_state VARCHAR(16) NOT NULL,next_part INTEGER NOT NULL,row_count BIGINT NOT NULL,retained INTEGER NOT NULL,PRIMARY KEY(pipeline_id,snapshot_scope,table_id))");
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("snapshot_part")+" (pipeline_id VARCHAR(128) NOT NULL,snapshot_scope VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,snapshot_id VARCHAR(36) NOT NULL,part_no INTEGER NOT NULL,payload "+text()+" NOT NULL,payload_hash VARCHAR(64) NOT NULL,row_count BIGINT NOT NULL,payload_bytes BIGINT NOT NULL,PRIMARY KEY(pipeline_id,snapshot_scope,table_id,part_no))");
        ddl(c,"CREATE TABLE IF NOT EXISTS "+table("snapshot_ack")+" (pipeline_id VARCHAR(128) NOT NULL,snapshot_scope VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,subscription_id VARCHAR(128) NOT NULL,snapshot_id VARCHAR(36) NOT NULL,PRIMARY KEY(pipeline_id,snapshot_scope,table_id,subscription_id))");
        new JdbcNormalization(this).initialize(c);
        JdbcPublicationStore.initialize(this,c);
        JdbcFailureReview.initialize(this,c);
        JdbcCommitAcknowledgement.initialize(this,c);
        if(settings.deliveryTiming)JdbcDeliveryTiming.initialize(this,c,dialect==Dialect.MYSQL);
        if(settings.writerAssignment!=null)JournalWriterAssignment.initialize(this,c);
        if(dialect==Dialect.MYSQL)try(PreparedStatement p=c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rm3_job','rm3_subscription','rm3_log','rm3_receipt','rm3_schema','rm3_snapshot','rm3_snapshot_part','rm3_snapshot_ack','rm3_normalized','rm3_identity','rm3_identity_seed','rm3_publication','rm3_publication_schema','rm3_publication_snapshot','rm3_failure','rm3_recovery_ack','rm3_pipeline_failure')")){
            p.setString(1,settings.namespace);try(ResultSet rs=p.executeQuery()){int count=0;while(rs.next()){count++;if(!"InnoDB".equalsIgnoreCase(rs.getString(1)))throw new IllegalStateException("JOURNAL_ENGINE_NOT_TRANSACTIONAL");}if(count!=17)throw new IllegalStateException("JOURNAL_TABLES_MISSING");}
        }
    }
    /** Read-only identity guard for status and administrative controls, without initialization DDL. */
    public void verifyIdentity(String fingerprint,List<String> subscribers)throws Exception{
        try(Connection c=connect();PreparedStatement p=c.prepareStatement("SELECT format_version,fingerprint FROM "+table("job")+" WHERE pipeline_id=?")){
            p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){if(!rs.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");if(!supported(rs.getInt(1))||!fingerprint.equals(rs.getString(2)))throw new IllegalStateException("JOURNAL_IDENTITY_MISMATCH");verifyRecoveryTables(c,rs.getInt(1));}
            Set<String> actual=new TreeSet<>();for(Subscription sub:subscriptions(c))actual.add(sub.id);if(!actual.equals(new TreeSet<>(subscribers)))throw new IllegalStateException("SUBSCRIPTION_MIGRATION_REQUIRED");
        }
    }
    long now(Connection c)throws SQLException{
        String sql=dialect==Dialect.MYSQL?"SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)":dialect==Dialect.POSTGRES?"SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT)":"SELECT DATEDIFF('MILLISECOND',TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00:00',CURRENT_TIMESTAMP)";
        try(Statement s=c.createStatement();ResultSet rs=s.executeQuery(sql)){rs.next();return rs.getLong(1);}
    }
    Map<String,Object> locked(Connection c)throws SQLException{
        Map<String,Object> row=assignmentLock(c);
        JournalWriterAssignment.requireConfigured(this,c,row);
        return row;
    }
    Map<String,Object> assignmentLock(Connection c)throws SQLException{
        try(PreparedStatement p=c.prepareStatement("SELECT epoch,owner,lease_until,next_seq,captured,pending_bytes,generation,snapshot_bytes,normalized_seq,identity_bytes,run_state,error_generation,error_code,format_version,fingerprint FROM "+table("job")+" WHERE pipeline_id=? FOR UPDATE")){
            p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){
                if(!rs.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");Map<String,Object> row=new HashMap<>();
                row.put("epoch",rs.getLong(1));row.put("owner",rs.getString(2));row.put("lease",rs.getLong(3));row.put("next",rs.getLong(4));row.put("captured",rs.getString(5));row.put("bytes",rs.getLong(6));row.put("generation",rs.getString(7));row.put("snapshotBytes",rs.getLong(8));row.put("normalized",rs.getLong(9));row.put("identityBytes",rs.getLong(10));row.put("state",rs.getString(11));row.put("errorGeneration",rs.getLong(12));row.put("errorCode",rs.getString(13));row.put("format",rs.getInt(14));row.put("fingerprint",rs.getString(15));if(!supported(rs.getInt(14)))throw new IllegalStateException("JOURNAL_IDENTITY_MISMATCH");return row;
            }
        }
    }
    Map<String,Object> fence(Connection c,Lease lease)throws SQLException{
        Map<String,Object> row=locked(c);
        if(lease==null||lease.owner==null||!lease.owner.equals(row.get("owner"))||lease.epoch!=(long)row.get("epoch")||(long)row.get("lease")<=now(c))throw new Fenced();
        return row;
    }
    void capacity(Map<String,Object> job,long additional){
        long used=Math.addExact(Math.addExact((long)job.get("bytes"),(long)job.get("snapshotBytes")),(long)job.get("identityBytes"));
        if(additional>0&&(additional>settings.maxPendingBytes||used>settings.maxPendingBytes-additional))throw new Capacity();
    }
    @Override public Lease claim(String owner,long leaseMillis)throws Exception{
        if(owner==null||owner.isEmpty()||owner.length()>128||leaseMillis<100)throw new IllegalArgumentException("LEASE_INVALID");
        try(Connection c=connect()){
            c.setAutoCommit(false);Map<String,Object> row=locked(c);long clock=now(c),epoch=(long)row.get("epoch");
            if(!"RUNNING".equals(row.get("state")))throw new PipelinePaused();
            if(hasTargetGaps((int)row.get("format")))JdbcSkipJournal.requireNoPending(this,c,null);
            if(row.get("owner")!=null&&(long)row.get("lease")>clock){if(!owner.equals(row.get("owner")))throw new Busy();}
            else epoch=Math.addExact(epoch,1);
            execute(c,"UPDATE "+table("job")+" SET owner=?,epoch=?,lease_until=? WHERE pipeline_id=?",owner,epoch,Math.addExact(clock,leaseMillis),settings.pipelineId);c.commit();return new Lease(owner,epoch);
        }
    }
    @Override public void heartbeat(Lease lease,long leaseMillis)throws Exception{
        if(leaseMillis<100)throw new IllegalArgumentException("LEASE_INVALID");
        try(Connection c=connect()){c.setAutoCommit(false);fence(c,lease);execute(c,"UPDATE "+table("job")+" SET lease_until=? WHERE pipeline_id=?",Math.addExact(now(c),leaseMillis),settings.pipelineId);c.commit();}
    }
    @Override public void release(Lease lease)throws Exception{
        try(Connection c=connect()){execute(c,"UPDATE "+table("job")+" SET owner=NULL,lease_until=0 WHERE pipeline_id=? AND owner=? AND epoch=?",settings.pipelineId,lease.owner,lease.epoch);}
    }
    private PipelineState state(Map<String,Object> row){PipelineState state=new PipelineState();state.state=(String)row.get("state");state.epoch=(long)row.get("epoch");state.errorGeneration=(long)row.get("errorGeneration");state.errorCode=(String)row.get("errorCode");return state;}
    @Override public PipelineState pausePipeline()throws Exception{
        try(Connection c=connect()){c.setAutoCommit(false);Map<String,Object> row=locked(c);
            if("RUNNING".equals(row.get("state"))){execute(c,"UPDATE "+table("job")+" SET run_state='PAUSED',error_generation=error_generation+1,error_code='OPERATOR_PAUSED',epoch=epoch+1,owner=NULL,lease_until=0 WHERE pipeline_id=?",settings.pipelineId);row=locked(c);}c.commit();return state(row);}
    }
    /** Compare and pause one fully applied cut under the same row lock used by append/ACK. */
    public PipelineState pauseDrained(String generation,long expectedEpoch,long sequence,SourcePosition captured,Set<String> selected,int tableCount)throws Exception{
        if(generation==null||captured==null||selected==null||selected.isEmpty()||tableCount<1||sequence<0)throw new IllegalArgumentException("VALIDATION_BARRIER_INVALID");
        try(Connection c=connect()){
            c.setAutoCommit(false);Map<String,Object> row=locked(c);
            if(!generation.equals(row.get("generation"))||(long)row.get("epoch")!=expectedEpoch||!"RUNNING".equals(row.get("state"))
                    ||(long)row.get("next")-1!=sequence||(long)row.get("normalized")!=sequence||!Codec.encode(captured).equals(row.get("captured")))throw new IllegalStateException("VALIDATION_BARRIER_MOVED");
            try(PreparedStatement p=c.prepareStatement("SELECT COUNT(*),COALESCE(SUM(CASE WHEN run_state='COMPLETE' THEN 1 ELSE 0 END),0) FROM "+table("snapshot")+" WHERE pipeline_id=? AND snapshot_scope='source'")){
                p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){rs.next();if(rs.getLong(1)!=tableCount||rs.getLong(2)!=tableCount)throw new IllegalStateException("VALIDATION_SNAPSHOT_INCOMPLETE");}
            }
            Map<String,Subscription> subscriptions=new HashMap<>();for(Subscription sub:subscriptions(c))subscriptions.put(sub.id,sub);
            for(String id:selected){
                Subscription sub=subscriptions.get(id);if(sub==null||!"RUNNING".equals(sub.state)||sub.acknowledged!=sequence)throw new IllegalStateException("VALIDATION_TARGET_NOT_DRAINED");
                try(PreparedStatement p=c.prepareStatement("SELECT COUNT(*) FROM "+table("snapshot_ack")+" ack JOIN "+table("snapshot")+" sp ON sp.pipeline_id=ack.pipeline_id AND sp.snapshot_scope=ack.snapshot_scope AND sp.table_id=ack.table_id AND sp.snapshot_id=ack.snapshot_id WHERE ack.pipeline_id=? AND ack.subscription_id=? AND ack.snapshot_scope='source' AND sp.run_state='COMPLETE'")){
                    p.setString(1,settings.pipelineId);p.setString(2,id);try(ResultSet rs=p.executeQuery()){rs.next();if(rs.getLong(1)!=tableCount)throw new IllegalStateException("VALIDATION_TARGET_SNAPSHOT_INCOMPLETE");}
                }
            }
            execute(c,"UPDATE "+table("job")+" SET run_state='PAUSED',error_generation=error_generation+1,error_code='VALIDATION_BARRIER',epoch=epoch+1,owner=NULL,lease_until=0 WHERE pipeline_id=?",settings.pipelineId);
            PipelineState result=state(locked(c));c.commit();return result;
        }
    }
    @Override public void resumePipeline(long expectedErrorGeneration)throws Exception{
        try(Connection c=connect()){c.setAutoCommit(false);Map<String,Object> row=locked(c);if((long)row.get("errorGeneration")!=expectedErrorGeneration)throw new IllegalStateException("ERROR_GENERATION_CHANGED");
            if("TARGET_WRITER".equals(row.get("state")))throw new IllegalStateException("TARGET_WRITER_AUTHORITY_ACTIVE");
            if(hasTargetGaps((int)row.get("format")))JdbcSkipJournal.requireNoPending(this,c,null);
            execute(c,"UPDATE "+table("job")+" SET run_state='RUNNING',error_code=NULL WHERE pipeline_id=?",settings.pipelineId);c.commit();}
    }
    @Override public void failPipeline(Lease lease,String code)throws Exception{
        failPipeline(lease,code,null);
    }
    public void failPipeline(Lease lease,String code,PipelineFailure.Phase phase)throws Exception{
        if(code==null||!code.matches("[A-Z][A-Z0-9_]{1,127}"))throw new IllegalArgumentException("ERROR_CODE_INVALID");
        try(Connection c=connect()){
            c.setAutoCommit(false);
            Map<String,Object> job=fence(c,lease);
            long generation=Math.addExact((long)job.get("errorGeneration"),1);
            execute(c,"UPDATE "+table("job")+" SET run_state='ERROR',error_generation=?,error_code=?,epoch=epoch+1,owner=NULL,lease_until=0 WHERE pipeline_id=?",generation,code,settings.pipelineId);
            if(phase!=null){
                execute(c,"DELETE FROM "+table("pipeline_failure")+" WHERE pipeline_id=?",settings.pipelineId);
                execute(c,"INSERT INTO "+table("pipeline_failure")+" (pipeline_id,error_generation,phase) VALUES (?,?,?)",
                        settings.pipelineId,generation,phase.name());
            }
            c.commit();
        }
    }
    private String pipelineFailurePhase(Connection c,long generation)throws SQLException{
        try(PreparedStatement p=c.prepareStatement("SELECT phase FROM "+table("pipeline_failure")+" WHERE pipeline_id=? AND error_generation=?")){
            p.setString(1,settings.pipelineId);p.setLong(2,generation);
            try(ResultSet r=p.executeQuery()){
                if(!r.next())return "UNKNOWN";
                try{return PipelineFailure.Phase.valueOf(r.getString(1)).name();}
                catch(IllegalArgumentException invalid){return "UNKNOWN";}
            }
        }catch(SQLException missing){
            // Status may open a journal that has never been started with the diagnostic schema.
            if(Arrays.asList("42P01","42S02","42102","42104").contains(missing.getSQLState()))return "UNKNOWN";
            throw missing;
        }
    }
    @Override public SourcePosition captured()throws Exception{
        try(Connection c=connect();PreparedStatement p=c.prepareStatement("SELECT captured FROM "+table("job")+" WHERE pipeline_id=?")){
            p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){if(!rs.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");String payload=rs.getString(1);return payload==null?null:Codec.decode(payload,SourcePosition.class);}
        }
    }
    @Override public void baseline(Lease lease,SourcePosition position)throws Exception{
        if(position==null)throw new IllegalArgumentException("BASELINE_REQUIRED");
        try(Connection c=connect()){c.setAutoCommit(false);Map<String,Object> job=fence(c,lease);String existing=(String)job.get("captured");
            if(existing!=null&&!Codec.decode(existing,SourcePosition.class).equals(position))throw new IllegalStateException("CAPTURE_BASELINE_ALREADY_SET");
            execute(c,"UPDATE "+table("job")+" SET captured=? WHERE pipeline_id=?",Codec.encode(position),settings.pipelineId);c.commit();}
    }
    @Override public long append(Lease lease,SourceTransaction transaction)throws Exception{
        return append(lease,transaction,0);
    }
    /** Stored logical charge includes workspace until normalization atomically consumes it. */
    @Override public long append(Lease lease,SourceTransaction transaction,long normalizationReserve)throws Exception{
        if(normalizationReserve<0)throw new IllegalArgumentException("NORMALIZATION_RESERVE_INVALID");
        transaction.validate();String payload=Codec.encode(transaction),hash=Codec.sha256(payload);long bytes=payload.getBytes(StandardCharsets.UTF_8).length;
        long charge=Math.addExact(bytes,normalizationReserve);
        try(Connection c=connect()){
            c.setAutoCommit(false);Map<String,Object> row=fence(c,lease);
            if(!transaction.streamGeneration.equals(row.get("generation")))throw new IllegalStateException("STREAM_GENERATION_MISMATCH");
            if(hasSourceExclusions((int)row.get("format")))JdbcSourceExclusion.requireNotExcluded(this,c,transaction.id);
            try(PreparedStatement p=c.prepareStatement("SELECT stream_seq,payload_hash FROM "+table("receipt")+" WHERE pipeline_id=? AND transaction_id=?")){
                p.setString(1,settings.pipelineId);p.setString(2,transaction.id);try(ResultSet rs=p.executeQuery()){
                    if(rs.next()){if(!hash.equals(rs.getString(2)))throw new IllegalStateException("TRANSACTION_CONTENT_CHANGED");long existing=rs.getLong(1);c.commit();return existing;}
                }
            }
            long pending=(long)row.get("bytes"),sequence=(long)row.get("next");
            capacity(row,charge);
            String captured=(String)row.get("captured");
            if(transaction.kind==SourceTransaction.Kind.DATA||transaction.kind==SourceTransaction.Kind.DDL){
                if(captured!=null){SourcePosition old=Codec.decode(captured,SourcePosition.class);if(!old.getSourceId().equals(transaction.position.getSourceId())||!old.getSourceType().equals(transaction.position.getSourceType()))throw new IllegalStateException("SOURCE_IDENTITY_CHANGED");}
                captured=Codec.encode(transaction.position);
            }
            execute(c,"INSERT INTO "+table("log")+" (pipeline_id,stream_seq,transaction_id,payload,payload_hash,payload_bytes) VALUES (?,?,?,?,?,?)",settings.pipelineId,sequence,transaction.id,payload,hash,charge);
            if(settings.deliveryTiming&&transaction.kind==SourceTransaction.Kind.DATA&&transaction.sourceCommittedAt!=null
                    &&!transaction.changes.isEmpty()){
                String type=transaction.position.getSourceType();
                if("POSTGRESQL".equals(type)||"MYSQL".equals(type))
                    JdbcDeliveryTiming.capture(this,c,sequence,transaction.sourceCommittedAt);
            }
            execute(c,"INSERT INTO "+table("receipt")+" (pipeline_id,transaction_id,stream_seq,payload_hash) VALUES (?,?,?,?)",settings.pipelineId,transaction.id,sequence,hash);
            execute(c,"UPDATE "+table("job")+" SET next_seq=?,captured=?,pending_bytes=? WHERE pipeline_id=?",Math.addExact(sequence,1),captured,Math.addExact(pending,charge),settings.pipelineId);c.commit();return sequence;
        }
    }
    List<Subscription> subscriptions(Connection c)throws SQLException{
        List<Subscription> result=new ArrayList<>();try(PreparedStatement p=c.prepareStatement("SELECT subscription_id,acknowledged,run_state,error_generation,error_code FROM "+table("subscription")+" WHERE pipeline_id=? ORDER BY subscription_id")){
            p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){while(rs.next()){Subscription sub=new Subscription();sub.id=rs.getString(1);sub.acknowledged=rs.getLong(2);sub.state=rs.getString(3);sub.errorGeneration=rs.getLong(4);sub.errorCode=rs.getString(5);result.add(sub);}}
        }return result;
    }
    @Override public List<Subscription> subscriptions()throws Exception{try(Connection c=connect()){return subscriptions(c);}}
    Subscription subscription(Connection c,String id,boolean lock)throws SQLException{
        try(PreparedStatement p=c.prepareStatement("SELECT acknowledged,run_state,error_generation,error_code FROM "+table("subscription")+" WHERE pipeline_id=? AND subscription_id=?"+(lock?" FOR UPDATE":""))){
            p.setString(1,settings.pipelineId);p.setString(2,id);try(ResultSet rs=p.executeQuery()){
                if(!rs.next())throw new IllegalArgumentException("SUBSCRIPTION_NOT_FOUND");Subscription sub=new Subscription();sub.id=id;sub.acknowledged=rs.getLong(1);sub.state=rs.getString(2);sub.errorGeneration=rs.getLong(3);sub.errorCode=rs.getString(4);return sub;
            }
        }
    }
    @Override public List<Record> read(String subscription,int maximumTransactions)throws Exception{
        if(maximumTransactions<1||maximumTransactions>1000)throw new IllegalArgumentException("READ_LIMIT_INVALID");
        try(Connection c=connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);
            long last;
            try(PreparedStatement p=c.prepareStatement("SELECT next_seq FROM "+table("job")+" WHERE pipeline_id=?")){
                p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){if(!rs.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");last=rs.getLong(1)-1;}
            }
            Subscription sub=subscription(c,subscription,false);
            if(!"RUNNING".equals(sub.state)||sub.acknowledged>=last)return Collections.emptyList();
            int batch=maximumTransactions==1?1:incrementalBatch(c,sub.acknowledged,maximumTransactions);
            if(batch==0)throw new IllegalStateException("JOURNAL_SEQUENCE_GAP");
            try(PreparedStatement p=c.prepareStatement("SELECT stream_seq,payload,payload_hash FROM "+table("log")+" WHERE pipeline_id=? AND stream_seq>? ORDER BY stream_seq LIMIT ?")){
                p.setString(1,settings.pipelineId);p.setLong(2,sub.acknowledged);p.setInt(3,batch);
                try(ResultSet rs=p.executeQuery()){List<Record> result=new ArrayList<>();long expected=sub.acknowledged+1;while(rs.next()){
                    Record record=new Record();record.sequence=rs.getLong(1);if(record.sequence!=expected++)throw new IllegalStateException("JOURNAL_SEQUENCE_GAP");String payload=rs.getString(2);if(!Codec.sha256(payload).equals(rs.getString(3)))throw new IllegalStateException("JOURNAL_CHECKSUM_MISMATCH");
                    record.transaction=Codec.decode(payload,SourceTransaction.class);record.transaction.validate();result.add(record);
                }if(result.isEmpty()&&sub.acknowledged<last)throw new IllegalStateException("JOURNAL_SEQUENCE_GAP");return result;}
            }
        }
    }
    private int incrementalBatch(Connection c,long acknowledged,int maximum)throws SQLException{
        // Select a short prefix using sizes before fetching large payloads from the JDBC driver.
        try(PreparedStatement query=c.prepareStatement("SELECT stream_seq,payload_bytes FROM "+table("log")
                +" WHERE pipeline_id=? AND stream_seq>? ORDER BY stream_seq LIMIT ?")){
            query.setString(1,settings.pipelineId);query.setLong(2,acknowledged);query.setInt(3,maximum);
            try(ResultSet rows=query.executeQuery()){
                long bytes=0,sequence=acknowledged;
                int count=0;
                while(rows.next()){
                    long size=rows.getLong(2);
                    if(rows.getLong(1)!=Math.addExact(sequence,1))throw new IllegalStateException("JOURNAL_SEQUENCE_GAP");
                    if(size<1)throw new IllegalStateException("JOURNAL_PAYLOAD_SIZE_INVALID");
                    // Preserve the head transaction even when it alone exceeds the batch byte budget.
                    if(count>0&&size>INCREMENTAL_BATCH_BYTES-bytes)break;
                    bytes=Math.addExact(bytes,size);
                    sequence++;
                    count++;
                }
                return count;
            }
        }
    }
    @Override public void acknowledge(Lease lease,String subscription,long sequence)throws Exception{
        acknowledge(lease,subscription,sequence,null);
    }
    @Override public void acknowledgeApplied(Lease lease,String subscription,long sequence)throws Exception{
        acknowledge(lease,subscription,sequence,settings.deliveryTiming?Instant.now():null);
    }
    void boundAcknowledgementIdle(Connection c)throws SQLException{
        // Install the bound before taking journal row locks in case the client disappears.
        try(PreparedStatement p=c.prepareStatement(
                "SELECT pg_catalog.set_config('idle_in_transaction_session_timeout', "
                +"least(coalesce(nullif(setting::bigint,0),2147483647),?)::text || 'ms', true) "
                +"FROM pg_catalog.pg_settings WHERE name='idle_in_transaction_session_timeout'")){
            p.setLong(1,Math.min(settings.leaseMillis,Integer.MAX_VALUE));
            try(ResultSet applied=p.executeQuery()){
                if(!applied.next())throw new IllegalStateException("JOURNAL_ACK_IDLE_TIMEOUT_UNAVAILABLE");
            }
        }
    }

    private void acknowledge(Lease lease,String subscription,long sequence,Instant confirmedAt)throws Exception{
        boolean bounded=dialect==Dialect.POSTGRES&&settings.managedWriter;
        try(Connection c=connect()){
            c.setAutoCommit(false);
            if(bounded)boundAcknowledgementIdle(c);
            Map<String,Object> fenced=fence(c,lease);Subscription sub=subscription(c,subscription,true);
            if(sequence>=0&&sequence<=sub.acknowledged){c.commit();return;}if(!"RUNNING".equals(sub.state))throw new Paused();
            if(sequence!=Math.addExact(sub.acknowledged,1))throw new IllegalStateException("ACK_WOULD_SKIP_TRANSACTION");
            try(PreparedStatement p=c.prepareStatement("SELECT 1 FROM "+table("log")+" WHERE pipeline_id=? AND stream_seq=?")){
                p.setString(1,settings.pipelineId);p.setLong(2,sequence);try(ResultSet rs=p.executeQuery()){if(!rs.next())throw new IllegalStateException("JOURNAL_SEQUENCE_GAP");}
            }
            if(confirmedAt!=null)JdbcDeliveryTiming.confirm(this,c,subscription,sequence,confirmedAt);
            if(settings.kafkaPhysicalGrant!=null&&settings.kafkaPublicationTargets.contains(subscription))
                JdbcKafkaPhysicalHistory.acknowledge(this,c,subscription,(String)fenced.get("generation"),lease.epoch,sequence);
            execute(c,"UPDATE "+table("subscription")+" SET acknowledged=? WHERE pipeline_id=? AND subscription_id=?",sequence,settings.pipelineId,subscription);c.commit();
        }catch(SQLException failure){
            if(bounded&&"25P03".equals(failure.getSQLState()))
                throw new SQLTransientConnectionException("JOURNAL_ACK_CONNECTION_EXPIRED","08006",failure);
            throw failure;
        }
    }
    @Override public void verifySkippedCheckpoint(String subscription,SinkAdapter.Checkpoint checkpoint)throws Exception{
        if(checkpoint.skipOperation==null)return;
        try(Connection c=connect()){
            c.setAutoCommit(false);Map<String,Object> job=locked(c);JdbcSkipJournal.verifyCheckpoint(this,c,job,subscription,checkpoint);
            c.rollback();
        }
    }
    @Override public void retrying(Lease lease,String subscription,String code)throws Exception{
        if(code!=null&&!code.matches("[A-Z][A-Z0-9_]{1,127}"))throw new IllegalArgumentException("ERROR_CODE_INVALID");
        try(Connection c=connect()){c.setAutoCommit(false);fence(c,lease);Subscription sub=subscription(c,subscription,true);
            if("RUNNING".equals(sub.state)&&!Objects.equals(sub.errorCode,code))execute(c,"UPDATE "+table("subscription")+" SET error_code=? WHERE pipeline_id=? AND subscription_id=?",code,settings.pipelineId,subscription);c.commit();}
    }
    @Override public void pause(Lease lease,String subscription,String code)throws Exception{
        pauseFailure(lease,subscription,code,null);
    }
    @Override public void pauseFailure(Lease lease,String subscription,String code,SubscriberFailure failure)throws Exception{
        if(code==null||code.length()>128)throw new IllegalArgumentException("ERROR_CODE_INVALID");
        if(failure!=null)failure.validate();
        try(Connection c=connect()){c.setAutoCommit(false);fence(c,lease);Subscription sub=subscription(c,subscription,true);
            if(!"PAUSED".equals(sub.state)){
                long next=Math.addExact(sub.errorGeneration,1);
                execute(c,"UPDATE "+table("subscription")+" SET run_state='PAUSED',error_generation=?,error_code=? WHERE pipeline_id=? AND subscription_id=?",next,code,settings.pipelineId,subscription);
                JdbcFailureReview.save(this,c,subscription,next,lease.epoch,failure);
            }c.commit();}
    }
    @Override public FailureReview inspectFailure(String subscription,long expectedErrorGeneration,long maximumBytes)throws Exception{return new JdbcFailureReview(this).inspect(subscription,expectedErrorGeneration,maximumBytes);}
    /** Safe status fields only; never reads transaction payloads or activates a destination. */
    public List<Map<String,Object>> subscriptionDiagnostics()throws Exception{return new JdbcFailureReview(this).summaries();}
    /** Immutable historical versions only, fetched for the requested transaction rather than the catalog. */
    public Map<String,TableSchema> historicalSchemas(Set<String> versions,long maximumBytes)throws Exception{return new JdbcFailureReview(this).schemas(versions,maximumBytes);}
    @Override public void resume(String subscription,long expectedErrorGeneration)throws Exception{
        try(Connection c=connect()){c.setAutoCommit(false);Map<String,Object> job=locked(c);if(hasTargetGaps((int)job.get("format")))JdbcSkipJournal.requireNoPending(this,c,subscription);Subscription sub=subscription(c,subscription,true);
            if(sub.errorGeneration!=expectedErrorGeneration)throw new IllegalStateException("ERROR_GENERATION_CHANGED");
            if(!"RUNNING".equals(sub.state))execute(c,"UPDATE "+table("subscription")+" SET run_state='RUNNING',error_code=NULL WHERE pipeline_id=? AND subscription_id=?",settings.pipelineId,subscription);c.commit();}
    }
    @Override public long collect(Lease lease,int maximumTransactions)throws Exception{
        if(maximumTransactions<1||maximumTransactions>10000)throw new IllegalArgumentException("COLLECTION_LIMIT_INVALID");
        try(Connection c=connect()){c.setAutoCommit(false);Map<String,Object> job=fence(c,lease);long minimum=Long.MAX_VALUE;
            for(Subscription sub:subscriptions(c))minimum=Math.min(minimum,sub.acknowledged);
            boolean recovery=hasTargetGaps((int)job.get("format"));
            List<Long> sequences=new ArrayList<>(),verifiedGaps=new ArrayList<>();long bytes=0;
            String gapMatch="SELECT 1 FROM "+table("gap")+" g WHERE g.pipeline_id=l.pipeline_id AND g.stream_seq=l.stream_seq";
            String columns=recovery?",CASE WHEN EXISTS ("+gapMatch+") THEN 1 ELSE 0 END":"";
            String unpinned=recovery?" AND NOT EXISTS ("+gapMatch+" AND (g.verification_id IS NULL OR g.verification_hash IS NULL))"
                    +" AND NOT EXISTS (SELECT 1 FROM "+table("skip_plan")+" p WHERE p.pipeline_id=l.pipeline_id"
                    +" AND p.stream_seq=l.stream_seq AND p.run_state='PREPARED')":"";
            try(PreparedStatement p=c.prepareStatement("SELECT l.stream_seq,l.payload_bytes"+columns+" FROM "+table("log")
                    +" l WHERE l.pipeline_id=? AND l.stream_seq<=?"+unpinned+" ORDER BY l.stream_seq LIMIT ?")){
                p.setString(1,settings.pipelineId);p.setLong(2,minimum);p.setInt(3,maximumTransactions);
                try(ResultSet rs=p.executeQuery()){
                    while(rs.next()){
                        long sequence=rs.getLong(1);
                        sequences.add(sequence);bytes+=rs.getLong(2);
                        if(recovery&&rs.getInt(3)==1)verifiedGaps.add(sequence);
                    }
                }
            }
            for(long sequence:verifiedGaps)JdbcGapVerificationLedger.verifyRelease(this,c,sequence,
                    (String)job.get("generation"),(String)job.get("fingerprint"));
            for(long sequence:sequences){
                try(PreparedStatement p=c.prepareStatement("SELECT payload_bytes FROM "+table("normalized")+" WHERE pipeline_id=? AND stream_seq=?")){p.setString(1,settings.pipelineId);p.setLong(2,sequence);try(ResultSet rs=p.executeQuery()){if(rs.next())bytes+=rs.getLong(1);}}
                execute(c,"DELETE FROM "+table("normalized")+" WHERE pipeline_id=? AND stream_seq=?",settings.pipelineId,sequence);
                execute(c,"DELETE FROM "+table("log")+" WHERE pipeline_id=? AND stream_seq=?",settings.pipelineId,sequence);
                if(recovery&&settings.deliveryTiming)JdbcDeliveryTiming.collectOne(this,c,sequence);
            }
            if(!sequences.isEmpty()){
                if(!recovery&&settings.deliveryTiming)JdbcDeliveryTiming.collect(this,c,sequences.get(sequences.size()-1));
                execute(c,"UPDATE "+table("job")+" SET pending_bytes=? WHERE pipeline_id=?",(long)job.get("bytes")-bytes,settings.pipelineId);
            }
            c.commit();return sequences.size();
        }
    }
    @Override public void saveSchema(Lease lease,TableSchema schema)throws Exception{
        schema.validate();String payload=Codec.encode(schema),hash=Codec.sha256(payload);
        try(Connection c=connect()){c.setAutoCommit(false);fence(c,lease);
            try(PreparedStatement p=c.prepareStatement("SELECT payload_hash FROM "+table("schema")+" WHERE pipeline_id=? AND table_id=? AND schema_version=?")){
                p.setString(1,settings.pipelineId);p.setString(2,schema.id);p.setInt(3,schema.version);try(ResultSet rs=p.executeQuery()){
                    if(rs.next()){if(!hash.equals(rs.getString(1)))throw new IllegalStateException("SCHEMA_VERSION_CONTENT_CHANGED");c.commit();return;}
                }
            }
            execute(c,"INSERT INTO "+table("schema")+" (pipeline_id,table_id,schema_version,payload,payload_hash) VALUES (?,?,?,?,?)",settings.pipelineId,schema.id,schema.version,payload,hash);c.commit();
        }
    }
    @Override public List<TableSchema> schemas()throws Exception{
        try(Connection c=connect();PreparedStatement p=c.prepareStatement("SELECT payload,payload_hash FROM "+table("schema")+" WHERE pipeline_id=? ORDER BY table_id,schema_version")){
            p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){List<TableSchema> result=new ArrayList<>();while(rs.next()){
                String payload=rs.getString(1);if(!Codec.sha256(payload).equals(rs.getString(2)))throw new IllegalStateException("SCHEMA_CHECKSUM_MISMATCH");TableSchema schema=Codec.decode(payload,TableSchema.class);schema.validate();result.add(schema);
            }return result;}
        }
    }
    SnapshotState snapshot(Connection c,String scope,String tableId,boolean lock)throws SQLException{
        try(PreparedStatement p=c.prepareStatement("SELECT snapshot_id,schema_version,baseline,run_state,next_part,row_count,column_count,retained FROM "+table("snapshot")+" WHERE pipeline_id=? AND snapshot_scope=? AND table_id=?"+(lock?" FOR UPDATE":""))){
            p.setString(1,settings.pipelineId);p.setString(2,scope);p.setString(3,tableId);try(ResultSet rs=p.executeQuery()){
                if(!rs.next())return null;SnapshotState state=new SnapshotState();state.scope=scope;state.tableId=tableId;state.snapshotId=rs.getString(1);state.schemaVersion=rs.getInt(2);state.baseline=Codec.decode(rs.getString(3),SourcePosition.class);state.state=rs.getString(4);state.nextPart=rs.getInt(5);state.rows=rs.getLong(6);state.columnCount=rs.getInt(7);state.retained=rs.getInt(8)!=0;return state;
            }
        }
    }
    @Override public SnapshotState snapshot(String scope,String tableId)throws Exception{try(Connection c=connect()){return snapshot(c,scope,tableId,false);}}
    @Override public SnapshotState beginSnapshot(Lease lease,String scope,TableSchema schema,SourcePosition baseline)throws Exception{
        schema.validate();if(scope==null||!scope.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||baseline==null)throw new IllegalArgumentException("SNAPSHOT_IDENTITY_INVALID");
        saveSchema(lease,schema);
        try(Connection c=connect()){c.setAutoCommit(false);Map<String,Object> job=fence(c,lease);SnapshotState old=snapshot(c,scope,schema.id,true);
            if(old!=null&&"COMPLETE".equals(old.state)){
                if(old.schemaVersion!=schema.version)throw new IllegalStateException("SNAPSHOT_SCHEMA_MIGRATION_REQUIRED");c.commit();return old;
            }
            long oldBytes=0;
            try(PreparedStatement p=c.prepareStatement("SELECT COALESCE(SUM(payload_bytes),0) FROM "+table("snapshot_part")+" WHERE pipeline_id=? AND snapshot_scope=? AND table_id=?")){
                p.setString(1,settings.pipelineId);p.setString(2,scope);p.setString(3,schema.id);try(ResultSet rs=p.executeQuery()){rs.next();oldBytes=rs.getLong(1);}
            }
            execute(c,"DELETE FROM "+table("snapshot_part")+" WHERE pipeline_id=? AND snapshot_scope=? AND table_id=?",settings.pipelineId,scope,schema.id);
            execute(c,"DELETE FROM "+table("snapshot")+" WHERE pipeline_id=? AND snapshot_scope=? AND table_id=?",settings.pipelineId,scope,schema.id);
            SnapshotState state=new SnapshotState();state.scope=scope;state.tableId=schema.id;state.snapshotId=UUID.randomUUID().toString();state.schemaVersion=schema.version;state.columnCount=schema.columns.size();state.baseline=baseline;state.state="BUILDING";
            execute(c,"INSERT INTO "+table("snapshot")+" (pipeline_id,snapshot_scope,table_id,snapshot_id,schema_version,column_count,baseline,run_state,next_part,row_count,retained) VALUES (?,?,?,?,?,?,?,'BUILDING',0,0,1)",settings.pipelineId,scope,schema.id,state.snapshotId,schema.version,state.columnCount,Codec.encode(baseline));
            execute(c,"UPDATE "+table("job")+" SET snapshot_bytes=? WHERE pipeline_id=?",(long)job.get("snapshotBytes")-oldBytes,settings.pipelineId);c.commit();return state;
        }
    }
    @Override public void appendSnapshot(Lease lease,SnapshotState state,int part,List<List<Cell>> rows)throws Exception{
        if(part<0||rows==null||rows.isEmpty()||rows.size()>10000)throw new IllegalArgumentException("SNAPSHOT_PART_INVALID");
        for(List<Cell> row:rows){if(row==null)throw new IllegalArgumentException("SNAPSHOT_ROW_INVALID");for(Cell cell:row){if(cell==null||!cell.present)throw new IllegalArgumentException("SNAPSHOT_FIELD_MISSING");cell.validate();}}
        String payload=Codec.encode(rows),hash=Codec.sha256(payload);long bytes=payload.getBytes(StandardCharsets.UTF_8).length;
        try(Connection c=connect()){c.setAutoCommit(false);Map<String,Object> job=fence(c,lease);SnapshotState current=snapshot(c,state.scope,state.tableId,true);
            if(current==null||!current.snapshotId.equals(state.snapshotId)||!"BUILDING".equals(current.state))throw new IllegalStateException("SNAPSHOT_SESSION_CHANGED");
            for(List<Cell> row:rows)if(row.size()!=current.columnCount)throw new IllegalArgumentException("SNAPSHOT_COLUMN_COUNT_MISMATCH");
            if(part<current.nextPart){
                try(PreparedStatement p=c.prepareStatement("SELECT payload_hash FROM "+table("snapshot_part")+" WHERE pipeline_id=? AND snapshot_scope=? AND table_id=? AND part_no=?")){
                    p.setString(1,settings.pipelineId);p.setString(2,state.scope);p.setString(3,state.tableId);p.setInt(4,part);try(ResultSet rs=p.executeQuery()){if(!rs.next()||!hash.equals(rs.getString(1)))throw new IllegalStateException("SNAPSHOT_PART_CONTENT_CHANGED");}
                }c.commit();state.nextPart=current.nextPart;state.rows=current.rows;return;
            }
            if(part!=current.nextPart)throw new IllegalStateException("SNAPSHOT_PART_GAP");
            capacity(job,bytes);
            execute(c,"INSERT INTO "+table("snapshot_part")+" (pipeline_id,snapshot_scope,table_id,snapshot_id,part_no,payload,payload_hash,row_count,payload_bytes) VALUES (?,?,?,?,?,?,?,?,?)",settings.pipelineId,state.scope,state.tableId,state.snapshotId,part,payload,hash,rows.size(),bytes);
            int next=Math.addExact(part,1);long count=Math.addExact(current.rows,rows.size());
            execute(c,"UPDATE "+table("snapshot")+" SET next_part=?,row_count=? WHERE pipeline_id=? AND snapshot_scope=? AND table_id=?",next,count,settings.pipelineId,state.scope,state.tableId);
            execute(c,"UPDATE "+table("job")+" SET snapshot_bytes=? WHERE pipeline_id=?",Math.addExact((long)job.get("snapshotBytes"),bytes),settings.pipelineId);c.commit();state.nextPart=next;state.rows=count;
        }
    }
    @Override public void finishSnapshot(Lease lease,SnapshotState state)throws Exception{
        try(Connection c=connect()){c.setAutoCommit(false);fence(c,lease);SnapshotState current=snapshot(c,state.scope,state.tableId,true);
            if(current==null||!current.snapshotId.equals(state.snapshotId))throw new IllegalStateException("SNAPSHOT_SESSION_CHANGED");
            if(current.nextPart!=state.nextPart||current.rows!=state.rows)throw new IllegalStateException("SNAPSHOT_MANIFEST_CHANGED");
            execute(c,"UPDATE "+table("snapshot")+" SET run_state='COMPLETE' WHERE pipeline_id=? AND snapshot_scope=? AND table_id=?",settings.pipelineId,state.scope,state.tableId);c.commit();state.state="COMPLETE";
        }
    }
    @Override public List<SnapshotPart> snapshotParts(String scope,String tableId,String snapshotId,int fromPart,int limit)throws Exception{
        if(fromPart<0||limit<1||limit>1000)throw new IllegalArgumentException("SNAPSHOT_READ_LIMIT_INVALID");
        try(Connection c=connect()){
            SnapshotState state=snapshot(c,scope,tableId,false);if(state==null||!state.snapshotId.equals(snapshotId)||!"COMPLETE".equals(state.state))throw new IllegalStateException("SNAPSHOT_NOT_COMPLETE");
            if(!state.retained)throw new IllegalStateException("SNAPSHOT_ARTIFACT_RELEASED_REBUILD_REQUIRES_PLAN");
            if(fromPart>state.nextPart)throw new IllegalArgumentException("SNAPSHOT_CURSOR_AHEAD");
            try(PreparedStatement p=c.prepareStatement("SELECT part_no,payload,payload_hash FROM "+table("snapshot_part")+" WHERE pipeline_id=? AND snapshot_scope=? AND table_id=? AND snapshot_id=? AND part_no>=? ORDER BY part_no LIMIT ?")){
                p.setString(1,settings.pipelineId);p.setString(2,scope);p.setString(3,tableId);p.setString(4,snapshotId);p.setInt(5,fromPart);p.setInt(6,limit);
                try(ResultSet rs=p.executeQuery()){List<SnapshotPart> result=new ArrayList<>();int expected=fromPart;while(rs.next()){
                    if(rs.getInt(1)!=expected++)throw new IllegalStateException("SNAPSHOT_PART_GAP");String payload=rs.getString(2);if(!Codec.sha256(payload).equals(rs.getString(3)))throw new IllegalStateException("SNAPSHOT_CHECKSUM_MISMATCH");SnapshotPart chunk=new SnapshotPart();chunk.part=rs.getInt(1);chunk.rows=Codec.JSON.readValue(payload,new com.fasterxml.jackson.core.type.TypeReference<List<List<Cell>>>(){});result.add(chunk);
                }if(result.isEmpty()&&fromPart<state.nextPart)throw new IllegalStateException("SNAPSHOT_PART_GAP");return result;}
            }
        }
    }
    @Override public void acknowledgeSnapshot(Lease lease,String subscription,SnapshotState manifest)throws Exception{
        try(Connection c=connect()){c.setAutoCommit(false);fence(c,lease);subscription(c,subscription,true);SnapshotState state=snapshot(c,manifest.scope,manifest.tableId,true);
            if(state==null||!"COMPLETE".equals(state.state)||!state.snapshotId.equals(manifest.snapshotId)||state.nextPart!=manifest.nextPart||state.rows!=manifest.rows)throw new IllegalStateException("SNAPSHOT_MANIFEST_CHANGED");
            try(PreparedStatement p=c.prepareStatement("SELECT snapshot_id FROM "+table("snapshot_ack")+" WHERE pipeline_id=? AND snapshot_scope=? AND table_id=? AND subscription_id=?")){
                p.setString(1,settings.pipelineId);p.setString(2,state.scope);p.setString(3,state.tableId);p.setString(4,subscription);try(ResultSet rs=p.executeQuery()){if(rs.next()){if(!state.snapshotId.equals(rs.getString(1)))throw new IllegalStateException("SNAPSHOT_ACK_IDENTITY_CHANGED");c.commit();return;}}
            }
            execute(c,"INSERT INTO "+table("snapshot_ack")+" (pipeline_id,snapshot_scope,table_id,subscription_id,snapshot_id) VALUES (?,?,?,?,?)",settings.pipelineId,state.scope,state.tableId,subscription,state.snapshotId);c.commit();
        }
    }
    @Override public long collectSnapshots(Lease lease,int maximumParts)throws Exception{
        if(maximumParts<1||maximumParts>10000)throw new IllegalArgumentException("COLLECTION_LIMIT_INVALID");
        try(Connection c=connect()){c.setAutoCommit(false);Map<String,Object> job=fence(c,lease);List<Object[]> parts=new ArrayList<>();long bytes=0;
            String eligible="SELECT sp.snapshot_scope,sp.table_id,sp.snapshot_id,sp.part_no,sp.payload_bytes FROM "+table("snapshot_part")+" sp WHERE sp.pipeline_id=? AND NOT EXISTS (SELECT 1 FROM "+table("subscription")+" sub WHERE sub.pipeline_id=sp.pipeline_id AND NOT EXISTS (SELECT 1 FROM "+table("snapshot_ack")+" ack WHERE ack.pipeline_id=sub.pipeline_id AND ack.subscription_id=sub.subscription_id AND ack.snapshot_scope=sp.snapshot_scope AND ack.table_id=sp.table_id AND ack.snapshot_id=sp.snapshot_id)) ORDER BY sp.snapshot_scope,sp.table_id,sp.part_no LIMIT ?";
            try(PreparedStatement p=c.prepareStatement(eligible)){p.setString(1,settings.pipelineId);p.setInt(2,maximumParts);try(ResultSet rs=p.executeQuery()){while(rs.next()){parts.add(new Object[]{rs.getString(1),rs.getString(2),rs.getString(3),rs.getInt(4)});bytes=Math.addExact(bytes,rs.getLong(5));}}}
            for(Object[] part:parts){execute(c,"DELETE FROM "+table("snapshot_part")+" WHERE pipeline_id=? AND snapshot_scope=? AND table_id=? AND snapshot_id=? AND part_no=?",settings.pipelineId,part[0],part[1],part[2],part[3]);execute(c,"UPDATE "+table("snapshot")+" SET retained=0 WHERE pipeline_id=? AND snapshot_scope=? AND table_id=? AND snapshot_id=?",settings.pipelineId,part[0],part[1],part[2]);}
            if(!parts.isEmpty())execute(c,"UPDATE "+table("job")+" SET snapshot_bytes=? WHERE pipeline_id=?",Math.subtractExact((long)job.get("snapshotBytes"),bytes),settings.pipelineId);c.commit();return parts.size();
        }
    }
    @Override public List<SourcePosition> dependencies()throws Exception{
        try(Connection c=connect();PreparedStatement p=c.prepareStatement("SELECT baseline FROM "+table("snapshot")+" WHERE pipeline_id=?")){
            p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){List<SourcePosition> result=new ArrayList<>();while(rs.next())result.add(Codec.decode(rs.getString(1),SourcePosition.class));return result;}
        }
    }
    @Override public Map<String,Object> status()throws Exception{
        try(Connection c=connect()){Map<String,Object> result=new LinkedHashMap<>();
            long observedAt=now(c);
            try(PreparedStatement p=c.prepareStatement("SELECT epoch,owner,lease_until,next_seq,pending_bytes,snapshot_bytes,generation,normalized_seq,identity_bytes,run_state,error_generation,error_code,format_version FROM "+table("job")+" WHERE pipeline_id=?")){
                p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){if(!rs.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");result.put("epoch",rs.getLong(1));result.put("owner",rs.getString(2));result.put("leaseUntil",rs.getLong(3));result.put("lastSequence",rs.getLong(4)-1);result.put("pendingBytes",rs.getLong(5));result.put("snapshotBytes",rs.getLong(6));result.put("generation",rs.getString(7));result.put("normalizedSequence",rs.getLong(8));result.put("identityBytes",rs.getLong(9));result.put("run_state",rs.getString(10));result.put("errorGeneration",rs.getLong(11));result.put("errorCode",rs.getString(12));result.put("dataFormat",rs.getInt(13));}
            }
            String leaseState=result.get("owner")==null?"UNCLAIMED":
                    (long)result.get("leaseUntil")>observedAt?"CLAIMED":"EXPIRED";
            result.put("captureLeaseState",leaseState);
            if(assignedWriter((int)result.get("dataFormat")))
                result.put("writerPlacement",JournalWriterAssignment.current(this,c));
            if("ERROR".equals(result.get("run_state")))
                result.put("errorStage",pipelineFailurePhase(c,(long)result.get("errorGeneration")));
            if(hasTargetGaps((int)result.get("dataFormat")))result.putAll(JdbcSkipJournal.summary(this,c));
            if(hasSourceExclusions((int)result.get("dataFormat"))){
                JdbcSourceExclusion.verify(this,c);
                result.put("hasSourceExclusions",true);
                result.put("hasOpenGaps",true);
                result.put("hasGaps",true);
            }
            result.put("subscriptions",subscriptions(c));return result;
        }
    }
    void requireCaptureExclusion(Connection c,Map<String,Object> job,CaptureExclusionPlan plan)throws Exception{
        String captured=(String)job.get("captured");
        if(!settings.pipelineId.equals(plan.pipelineId)||!plan.generation.equals(job.get("generation"))
                ||plan.epoch!=(long)job.get("epoch")||plan.errorGeneration!=(long)job.get("errorGeneration")
                ||plan.afterSequence!=(long)job.get("next")-1||captured==null
                ||!plan.checkpointHash.equals(Codec.sha256(captured))||!"ERROR".equals(job.get("state"))
                ||!"SOURCE_DECODE".equals(pipelineFailurePhase(c,plan.errorGeneration))
                ||job.get("owner")!=null&&(long)job.get("lease")>now(c))
            throw new IllegalStateException("SOURCE_REVIEW_STALE");
        if(hasTargetGaps((int)job.get("format")))JdbcSkipJournal.requireNoPending(this,c,null);
    }
    public Map<String,Object> excludedCapture(String operation,String approvalHash)throws Exception{
        try(Connection c=connect()){
            c.setAutoCommit(false);
            int format=(int)locked(c).get("format");
            if(!hasSourceExclusions(format))return null;
            JdbcSourceExclusion.verify(this,c);
            return JdbcSourceExclusion.receipt(this,c,operation,approvalHash);
        }
    }
    public Map<String,Object> captureExclusions(String after,int limit)throws Exception{
        if(after==null||!after.isEmpty()&&!after.matches("[A-Za-z0-9_-]{8,128}")||limit<1||limit>25)
            throw new IllegalArgumentException("CAPTURE_EXCLUSION_PAGE_INVALID");
        try(Connection c=connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            int format;
            try(PreparedStatement p=c.prepareStatement("SELECT format_version FROM "+table("job")+" WHERE pipeline_id=?")){
                p.setString(1,settings.pipelineId);
                try(ResultSet rows=p.executeQuery()){
                    if(!rows.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");
                    format=rows.getInt(1);
                }
            }
            if(!supported(format))throw new IllegalStateException("JOURNAL_IDENTITY_MISMATCH");
            if(!hasSourceExclusions(format))return Collections.singletonMap("exclusions",Collections.emptyList());
            JdbcSourceExclusion.verify(this,c);
            return JdbcSourceExclusion.page(this,c,after,limit);
        }
    }
    public Map<String,Object> excludeCapture(CaptureExclusionPlan plan,SourcePosition position,String operation,
                                               String approvalHash,String actor,String reason,
                                               java.util.concurrent.Callable<Void> authorize)throws Exception{
        return JdbcSourceExclusion.commit(this,plan,position,operation,approvalHash,actor,reason,authorize);
    }
    public TableSchema latestSchema(String tableId)throws Exception{
        try(Connection c=connect();PreparedStatement p=c.prepareStatement("SELECT payload,payload_hash FROM "
                +table("schema")+" WHERE pipeline_id=? AND table_id=? ORDER BY schema_version DESC LIMIT 1")){
            p.setString(1,settings.pipelineId);p.setString(2,tableId);
            try(ResultSet rows=p.executeQuery()){
                if(!rows.next())return null;
                String payload=rows.getString(1);
                if(!Codec.sha256(payload).equals(rows.getString(2)))throw new IllegalStateException("SCHEMA_CHECKSUM_MISMATCH");
                TableSchema schema=Codec.decode(payload,TableSchema.class);schema.validate();return schema;
            }
        }
    }
    /** One incremental progress frame. First-gap lookups use the per-subscription primary key. */
    public Map<String,Object> deliveryProgress()throws Exception{
        try(Connection c=connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);
            Map<String,Object> result=deliveryProgress(c);c.rollback();return result;
        }
    }
    Map<String,Object> deliveryProgress(Connection c)throws Exception{
            int format;long captured;
            try(PreparedStatement p=c.prepareStatement("SELECT format_version,next_seq FROM "+table("job")+" WHERE pipeline_id=?")){
                p.setString(1,settings.pipelineId);try(ResultSet r=p.executeQuery()){if(!r.next())throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");format=r.getInt(1);captured=r.getLong(2)-1;}
            }
            if(!supported(format))throw new IllegalStateException("JOURNAL_IDENTITY_MISMATCH");verifyRecoveryTables(c,format);
            Map<String,Map<String,Object>> progress=new TreeMap<>();
            for(Subscription sub:subscriptions(c)){
                long prefix=sub.acknowledged;Long first=null;
                if(hasTargetGaps(format))try(PreparedStatement p=c.prepareStatement("SELECT g.stream_seq,g.plan_hash,p.stream_seq,p.subscription_id,p.plan_hash,p.run_state FROM "+table("gap")+" g LEFT JOIN "+table("skip_plan")+" p ON p.pipeline_id=g.pipeline_id AND p.operation_id=g.operation_id WHERE g.pipeline_id=? AND g.subscription_id=? ORDER BY g.stream_seq LIMIT 1")){
                    p.setString(1,settings.pipelineId);p.setString(2,sub.id);try(ResultSet r=p.executeQuery()){if(r.next()){
                        first=r.getLong(1);if(first<1||first>sub.acknowledged||r.getObject(3)==null||first!=r.getLong(3)||!sub.id.equals(r.getString(4))||!Objects.equals(r.getString(2),r.getString(5))||!"COMPLETED".equals(r.getString(6)))throw new IllegalStateException("GAP_PROGRESS_RECORD_CHANGED");prefix=first-1;
                    }}
                }
                if(sub.acknowledged<0||sub.acknowledged>captured)throw new IllegalStateException("GAP_PROGRESS_CHECKPOINT_INVALID");
                Map<String,Object> row=new LinkedHashMap<>();row.put("safeCoveredSequence",Long.toString(sub.acknowledged));row.put("contiguousDeliveredSequence",Long.toString(prefix));row.put("firstSkippedSequence",first==null?null:Long.toString(first));
                if(hasSourceExclusions(format)){
                    Long sourceGap=JdbcSourceExclusion.firstGap(this,c,sub.id);
                    if(sourceGap!=null){
                        if(sourceGap>captured)throw new IllegalStateException("CAPTURE_EXCLUSION_RECEIPT_CHANGED");
                        row.put("contiguousDeliveredSequence",Long.toString(Math.min(prefix,sourceGap)));
                        row.put("firstSourceExclusionAfterSequence",Long.toString(sourceGap));
                    }
                }
                progress.put(sub.id,row);
            }
            if(settings.deliveryTiming)JdbcDeliveryTiming.enrich(this,c,progress);
            Map<String,Object> result=new LinkedHashMap<>();result.put("capturedSequence",Long.toString(captured));result.put("subscriptions",progress);return result;
    }
    /** Bounded metadata preview for the console; never loads all business schemas or rows. */
    public Map<String,Object> snapshotSummary(int limit)throws Exception{
        if(limit<1||limit>1000)throw new IllegalArgumentException("SNAPSHOT_PREVIEW_LIMIT_INVALID");Map<String,Object> result=new LinkedHashMap<>();
        try(Connection c=connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);
            try(PreparedStatement p=c.prepareStatement("SELECT COUNT(*),COALESCE(SUM(CASE WHEN run_state='COMPLETE' THEN 1 ELSE 0 END),0) FROM "+table("snapshot")+" WHERE pipeline_id=? AND snapshot_scope='source'")){
                p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){rs.next();result.put("table_count",rs.getLong(1));result.put("completed_tables",rs.getLong(2));}
            }
            Map<String,Long> acknowledgments=new TreeMap<>();
            try(PreparedStatement p=c.prepareStatement("SELECT ack.subscription_id,COUNT(*) FROM "+table("snapshot_ack")+" ack JOIN "+table("snapshot")+" sp ON sp.pipeline_id=ack.pipeline_id AND sp.snapshot_scope=ack.snapshot_scope AND sp.table_id=ack.table_id AND sp.snapshot_id=ack.snapshot_id WHERE ack.pipeline_id=? AND ack.snapshot_scope='source' AND sp.run_state='COMPLETE' GROUP BY ack.subscription_id")){
                p.setString(1,settings.pipelineId);try(ResultSet rs=p.executeQuery()){while(rs.next())acknowledgments.put(rs.getString(1),rs.getLong(2));}
            }result.put("snapshotAcknowledgments",acknowledgments);
            List<Map<String,Object>> rows=new ArrayList<>();
            try(PreparedStatement p=c.prepareStatement("SELECT sc.payload,sp.row_count,sp.run_state FROM "+table("snapshot")+" sp JOIN "+table("schema")+" sc ON sc.pipeline_id=sp.pipeline_id AND sc.table_id=sp.table_id AND sc.schema_version=sp.schema_version WHERE sp.pipeline_id=? AND sp.snapshot_scope='source' ORDER BY sp.table_id LIMIT ?")){
                p.setString(1,settings.pipelineId);p.setInt(2,limit);try(ResultSet rs=p.executeQuery()){while(rs.next()){TableSchema schema=Codec.decode(rs.getString(1),TableSchema.class);Map<String,Object> row=new LinkedHashMap<>();row.put("table",schema.qualifiedName());row.put("copiedRows",rs.getLong(2));row.put("done","COMPLETE".equals(rs.getString(3)));rows.add(row);}}
            }result.put("tables",rows);c.commit();return result;
        }
    }
    @Override public boolean seedIdentities(Lease lease,TableSchema table,SnapshotState snapshot)throws Exception{return new JdbcNormalization(this).seed(lease,table,snapshot);}
    @Override public SourceTransaction normalize(Lease lease,Record record,Map<String,TableSchema> schemas,Map<String,SourcePosition> baselines,BaselineFilter filter)throws Exception{return new JdbcNormalization(this).normalize(lease,record,schemas,baselines,filter);}
    public static final class IdentityRow {public String rowId,hash,payload;}
    /** Keyset pages at a declared paused cut; never expose identities from a moving normalizer. */
    public List<IdentityRow> identities(String generation,long epoch,long sequence,String tableId,String after,int limit)throws Exception{return identities(generation,epoch,sequence,tableId,after,limit,4L*1024*1024);}
    /** Both row count and UTF8 payload bytes are bounded; continue from the last returned ID. */
    public List<IdentityRow> identities(String generation,long epoch,long sequence,String tableId,String after,int limit,long maximumBytes)throws Exception{
        if(tableId==null||after==null||limit<1||limit>1000||maximumBytes<1024||maximumBytes>32L*1024*1024)throw new IllegalArgumentException("VALIDATION_IDENTITY_PAGE_INVALID");
        try(Connection c=connect()){
            c.setAutoCommit(false);Map<String,Object> state=locked(c);
            if(!generation.equals(state.get("generation"))||(long)state.get("epoch")!=epoch||!"PAUSED".equals(state.get("state"))||(long)state.get("next")-1!=sequence||(long)state.get("normalized")!=sequence)throw new IllegalStateException("VALIDATION_BARRIER_MOVED");
            try(PreparedStatement p=c.prepareStatement("SELECT row_id,row_hash,payload_bytes FROM "+table("identity")+" WHERE pipeline_id=? AND table_id=? AND row_id>? ORDER BY row_id LIMIT ?")){
                p.setString(1,settings.pipelineId);p.setString(2,tableId);p.setString(3,after);p.setInt(4,limit);List<IdentityRow> result=new ArrayList<>();long bytes=0;
                try(ResultSet rows=p.executeQuery()){while(rows.next()){
                    long declared=rows.getLong(3);if(declared<0)throw new IllegalStateException("ROW_IDENTITY_CHECKSUM_MISMATCH");
                    if(declared>maximumBytes-bytes){if(result.isEmpty())throw new IllegalStateException("VALIDATION_IDENTITY_ROW_TOO_LARGE");break;}
                    IdentityRow row=new IdentityRow();row.rowId=rows.getString(1);row.hash=rows.getString(2);
                    try(PreparedStatement value=c.prepareStatement("SELECT CASE WHEN OCTET_LENGTH(payload)<=? THEN payload ELSE NULL END FROM "+table("identity")+" WHERE pipeline_id=? AND table_id=? AND row_id=?")){
                        value.setLong(1,maximumBytes-bytes);value.setString(2,settings.pipelineId);value.setString(3,tableId);value.setString(4,row.rowId);
                        try(ResultSet payload=value.executeQuery()){if(!payload.next()||(row.payload=payload.getString(1))==null)throw new IllegalStateException("VALIDATION_IDENTITY_ROW_TOO_LARGE");}
                    }
                    long actual=row.payload.getBytes(StandardCharsets.UTF_8).length;
                    if(actual!=declared||!Codec.sha256(row.payload).equals(row.hash))throw new IllegalStateException("ROW_IDENTITY_CHECKSUM_MISMATCH");
                    if(actual>maximumBytes-bytes)throw new IllegalStateException("VALIDATION_IDENTITY_ROW_TOO_LARGE");bytes+=actual;result.add(row);
                }}c.commit();return result;
            }
        }
    }
    @Override public void close()throws SQLException{
        if(anchor!=null)try{anchor.close();}
        catch(SQLException failure){
            // This keeper has never written data. H2 may try to reconnect a dead automatic server on close.
            if(!JdbcConnections.localAutomaticServer(settings.driver,settings.url)||!JdbcConnections.automaticServerTransition(failure))throw failure;
        }finally{anchor=null;}
    }
}
