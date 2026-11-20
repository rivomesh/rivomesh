package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.OracleDate;
import io.github.rivomesh.PostgresDate;
import io.github.rivomesh.PostgresClock;
import io.github.rivomesh.PostgresTimestamp;
import io.github.rivomesh.PipelineValidationBarrier;
import io.github.rivomesh.TargetOwnership;
import io.github.rivomesh.recovery.UniqueRestoreOperation;
import io.github.rivomesh.repair.*;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.Path;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/** Transactional targets. Every business write shares a local row lock and commit with its checkpoint. */
public final class JdbcSink implements SinkAdapter {
    public enum UniquePolicy { PRESERVE,DOWNGRADE_BUSINESS }
    public static final String ROW_ID="_rivomesh_row_id";
    public static final class Settings {
        public String id,pipelineId,generation,fingerprint,driver,url,username="",password="",metadataSchema="rivomesh_v3";
        public TargetProduct product;
        public boolean acknowledgeTextConversions;
        public UniquePolicy uniquePolicy=UniquePolicy.DOWNGRADE_BUSINESS;
        public Map<String,String> databases=new TreeMap<>();
    }
    private final Settings settings;
    private final Map<String,TableSchema> schemas=new HashMap<>();
    private Connection anchor;
    private boolean skipAware;
    private enum OpenMode { INITIALIZE, EXISTING, RECOVERY, ADOPTION_PREVIEW }
    public JdbcSink(Settings s)throws Exception{this(s,OpenMode.INITIALIZE);}
    /** Administrative reads/fences must never recreate lost target metadata. */
    public static JdbcSink existing(Settings s)throws Exception{return new JdbcSink(s,OpenMode.EXISTING);}
    public static PostgresAdoptionRead adoptionRead(Settings s,long maximumMillis)throws Exception{
        if(s.product!=TargetProduct.POSTGRESQL)throw new IllegalArgumentException("ADOPTION_TARGET_PRODUCT_UNSUPPORTED");
        JdbcSink sink=new JdbcSink(s,OpenMode.ADOPTION_PREVIEW);
        return new PostgresAdoptionRead(sink,s,sink.connect(),maximumMillis);
    }
    public static PostgresExistingAdoption adoptionBootstrap(Settings s, JdbcJournal journal, long maximumMillis)throws Exception{
        if(s.product!=TargetProduct.POSTGRESQL)throw new IllegalArgumentException("ADOPTION_TARGET_PRODUCT_UNSUPPORTED");
        JdbcSink sink=new JdbcSink(s,OpenMode.ADOPTION_PREVIEW);
        return new PostgresExistingAdoption(sink,s,sink.connect(),journal,maximumMillis);
    }
    private JdbcSink(Settings s,OpenMode mode)throws Exception{
        if(s==null||s.product==null||s.uniquePolicy==null||s.fingerprint==null||s.fingerprint.length()!=64||s.generation==null||s.generation.isEmpty()
                ||s.generation.length()>128||s.driver==null||s.url==null||s.databases==null||s.databases.isEmpty())throw new IllegalArgumentException("SINK_CONFIG_INVALID");
        for(String id:Arrays.asList(s.id,s.pipelineId))if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw new IllegalArgumentException("SINK_ID_INVALID");
        settings=s;q(s.metadataSchema);Set<String> destinations=new HashSet<>();
        for(Map.Entry<String,String> entry:s.databases.entrySet())if(entry.getKey()==null||entry.getKey().isEmpty()||q(entry.getValue())==null||entry.getValue().equalsIgnoreCase(s.metadataSchema)||!destinations.add(entry.getValue().toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("SINK_MAPPING_COLLISION");
        if(mode==OpenMode.ADOPTION_PREVIEW)return;
        anchor=connect();String product=anchor.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        if(!(s.product.mysqlDialect()&&(product.contains("mysql")||product.contains("oceanbase"))||s.product==TargetProduct.POSTGRESQL&&product.contains("postgresql")
                ||s.product==TargetProduct.GAUSS&&(product.contains("gauss")||product.contains("postgresql"))||s.product==TargetProduct.GBASE_8C&&product.equals("gbase8c")||s.product==TargetProduct.KINGBASE&&product.contains("kingbase")||s.product==TargetProduct.DM&&product.equals("dm dbms")||s.product==TargetProduct.H2&&product.contains("h2"))){anchor.close();throw new IllegalArgumentException("SINK_PRODUCT_MISMATCH");}
        try{if(mode==OpenMode.INITIALIZE)initialize();skipAware=skipColumns(anchor,mode==OpenMode.RECOVERY)==2;if(mode!=OpenMode.INITIALIZE)try(RepairConnectionGuard guard=new RepairConnectionGuard(anchor,30000)){validateMetadata(anchor);anchor.setAutoCommit(false);boundRecoveryLocks(anchor);locked(anchor,null);guard.verify();anchor.rollback();}}catch(Exception e){anchor.close();throw e;}
    }
    private Connection connect()throws SQLException{
        Connection c=JdbcConnections.open(settings.driver,settings.url,settings.username,settings.password);
        try{if(settings.product==TargetProduct.OCEANBASE)OceanBaseProfile.check(c);if(settings.product==TargetProduct.KINGBASE)KingbaseProfile.check(c);if(settings.product==TargetProduct.GBASE_8C)Gbase8cProfile.check(c);if(dm())DmProfile.check(c);return c;}
        catch(SQLException|RuntimeException failure){try{c.close();}catch(SQLException close){failure.addSuppressed(close);}throw failure;}
    }
    private boolean dm(){return settings.product==TargetProduct.DM;}
    private boolean shouldCreate(Connection c,String schema,String table)throws SQLException{return dm()?!DmProfile.tableExists(c,schema,table):settings.product!=TargetProduct.OCEANBASE||!OceanBaseProfile.tableExists(c,schema,table);}
    private boolean mysql(){return settings.product.mysqlDialect();}
    private String q(String name){
        if(name==null||name.isEmpty()||name.indexOf('\0')>=0||name.getBytes(StandardCharsets.UTF_8).length>(mysql()?64:63))throw new IllegalArgumentException("TARGET_IDENTIFIER_INVALID");
        return mysql()?"`"+name.replace("`","``")+"`":"\""+name.replace("\"","\"\"")+"\"";
    }
    private String metadata(String name){return q(settings.metadataSchema)+"."+q("rm3_sink_"+name);}
    private String destination(TableSchema table){String name=settings.databases.get(table.database);if(name==null)throw new IllegalArgumentException("DATABASE_MAPPING_MISSING");return name;}
    private String table(TableSchema table){return q(destination(table))+"."+q(table.name);}
    private static int sql(Connection c,String query,Object... args)throws SQLException{
        try(PreparedStatement p=c.prepareStatement(query)){for(int i=0;i<args.length;i++)p.setObject(i+1,args[i]);return p.executeUpdate();}
    }
    private void ddl(Connection c,String query)throws SQLException{
        if(dm())query=query.replaceFirst("^CREATE TABLE IF NOT EXISTS ","CREATE TABLE ");
        try(Statement st=c.createStatement()){st.execute(query);}
        catch(SQLException e){if(dm()&&e.getErrorCode()==-2124&&!query.startsWith("CREATE INDEX ")&&!query.startsWith("CREATE UNIQUE INDEX "))throw new SQLTransactionRollbackException("TARGET_CATALOG_RETRY","40001",e);if(Arrays.asList("42P06","42P07","42710","23505").contains(e.getSQLState())){if(c.getAutoCommit())return;throw new SQLTransactionRollbackException("TARGET_CATALOG_RETRY","40001",e);}throw e;}
    }
    private void createNamespace(Connection c,String name)throws SQLException{if(dm()){if(!DmProfile.schemaExists(c,name))ddl(c,"CREATE SCHEMA "+q(name));return;}if(settings.product==TargetProduct.OCEANBASE&&OceanBaseProfile.databaseExists(c,name))return;ddl(c,(mysql()?"CREATE DATABASE IF NOT EXISTS ":"CREATE SCHEMA IF NOT EXISTS ")+q(name));}
    private void initialize()throws Exception{
        try(Connection c=connect()){
            createMetadata(c);
            c.setAutoCommit(false);
            try{sql(c,"INSERT INTO "+metadata("job")+" (pipeline_id,subscription_id,fingerprint,generation,epoch,stream_seq) VALUES (?,?,?,?,0,0)",settings.pipelineId,settings.id,settings.fingerprint,settings.generation);c.commit();}
            catch(SQLException e){c.rollback();if(!"23505".equals(e.getSQLState())&&e.getErrorCode()!=1062&&!(dm()&&DmProfile.unique(e)))throw e;}
            locked(c,null);c.rollback();
        }
    }
    void createMetadata(Connection c)throws Exception{
        createNamespace(c,settings.metadataSchema);
        if(shouldCreate(c,settings.metadataSchema,"rm3_sink_job"))ddl(c,"CREATE TABLE IF NOT EXISTS "+metadata("job")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,fingerprint VARCHAR(64) NOT NULL,generation VARCHAR(128) NOT NULL,epoch BIGINT NOT NULL,stream_seq BIGINT NOT NULL,transaction_id VARCHAR(512),transaction_hash VARCHAR(64),PRIMARY KEY(pipeline_id,subscription_id))"+(mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":""));
        if(shouldCreate(c,settings.metadataSchema,"rm3_sink_table"))ddl(c,"CREATE TABLE IF NOT EXISTS "+metadata("table")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,schema_version INTEGER NOT NULL,schema_hash VARCHAR(64) NOT NULL,snapshot_id VARCHAR(36),snapshot_state VARCHAR(16),next_part INTEGER NOT NULL,row_count BIGINT NOT NULL,last_part_hash VARCHAR(64),PRIMARY KEY(pipeline_id,subscription_id,table_id))"+(mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":""));
        validateMetadata(c);
    }
    private void validateMetadata(Connection c)throws SQLException{
            if(mysql())try(PreparedStatement p=c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rm3_sink_job','rm3_sink_table')")){
                p.setString(1,settings.metadataSchema);try(ResultSet rs=p.executeQuery()){int count=0;while(rs.next()){count++;if(!"InnoDB".equalsIgnoreCase(rs.getString(1)))throw new IllegalStateException("SINK_METADATA_ENGINE_NOT_TRANSACTIONAL");}if(count!=2)throw new IllegalStateException("SINK_METADATA_TABLES_MISSING");}
            }
    }
    static JdbcSink forSkipRecovery(Settings settings)throws Exception{
        if(settings.product!=TargetProduct.POSTGRESQL&&settings.product!=TargetProduct.MYSQL&&settings.product!=TargetProduct.H2)throw new IllegalStateException("SKIP_TARGET_UNSUPPORTED");
        return new JdbcSink(settings,OpenMode.RECOVERY);
    }
    private int skipColumns(Connection c,boolean incompleteRecovery)throws SQLException{
        Set<String> found=skipColumnNames(c);if(found.size()==1&&!incompleteRecovery)throw new IllegalStateException("SINK_RECOVERY_METADATA_INCOMPLETE");return found.size();
    }
    private Set<String> skipColumnNames(Connection c)throws SQLException{
        Set<String> found=new HashSet<>();
        if(settings.product!=TargetProduct.POSTGRESQL&&settings.product!=TargetProduct.MYSQL&&settings.product!=TargetProduct.H2)return found;
        boolean lower=mysql()&&c.getMetaData().storesLowerCaseIdentifiers();
        try(ResultSet columns=c.getMetaData().getColumns(mysql()?settings.metadataSchema:c.getCatalog(),mysql()?null:settings.metadataSchema,"rm3_sink_job",null)){
            while(columns.next()){
                String schema=columns.getString(mysql()?"TABLE_CAT":"TABLE_SCHEM"),table=columns.getString("TABLE_NAME"),name=columns.getString("COLUMN_NAME");
                if(!(lower?settings.metadataSchema.equalsIgnoreCase(schema):settings.metadataSchema.equals(schema))||!(lower?"rm3_sink_job".equalsIgnoreCase(table):"rm3_sink_job".equals(table)))continue;
                if(Arrays.asList("skip_operation","skip_hash").contains(name))found.add(name);
            }
        }return found;
    }
    private Checkpoint locked(Connection c,Long epoch)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT fingerprint,generation,epoch,stream_seq,transaction_id,transaction_hash"+(skipAware?","+q("skip_operation")+","+q("skip_hash"):"")+" FROM "+metadata("job")+" WHERE pipeline_id=? AND subscription_id=? FOR UPDATE")){
            p.setString(1,settings.pipelineId);p.setString(2,settings.id);try(ResultSet rs=p.executeQuery()){
                if(!rs.next()||!settings.fingerprint.equals(rs.getString(1))||!settings.generation.equals(rs.getString(2)))throw new IllegalStateException("SINK_IDENTITY_MISMATCH");
                Checkpoint checkpoint=new Checkpoint();checkpoint.epoch=rs.getLong(3);checkpoint.sequence=rs.getLong(4);checkpoint.transactionId=rs.getString(5);checkpoint.transactionHash=rs.getString(6);
                if(skipAware){checkpoint.skipOperation=rs.getString(7);checkpoint.skipHash=rs.getString(8);if((checkpoint.skipOperation==null)!=(checkpoint.skipHash==null)||checkpoint.skipOperation!=null&&(!checkpoint.skipOperation.matches("[A-Za-z0-9_-]{8,128}")||!io.github.rivomesh.recovery.FailureEvidence.hash(checkpoint.skipHash)))throw new IllegalStateException("SINK_SKIP_CHECKPOINT_INVALID");}
                if(epoch!=null&&checkpoint.epoch!=epoch)throw new IllegalStateException("SINK_FENCED");return checkpoint;
            }
        }
    }
    @Override public String id(){return settings.id;}
    @Override public boolean includes(TableSchema table){return settings.databases.containsKey(table.database);}
    @Override public Consistency consistency(){return Consistency.TRANSACTIONAL;}
    @Override public Checkpoint activate(long epoch)throws Exception{
        if(epoch<1)throw new IllegalArgumentException("SINK_EPOCH_INVALID");
        try(Connection c=connect()){c.setAutoCommit(false);Checkpoint cp=locked(c,null);if(epoch<cp.epoch)throw new IllegalStateException("SINK_FENCED");
            if(cp.epoch!=epoch)sql(c,"UPDATE "+metadata("job")+" SET epoch=? WHERE pipeline_id=? AND subscription_id=?",epoch,settings.pipelineId,settings.id);c.commit();cp.epoch=epoch;return cp;}
    }
    @Override public Checkpoint checkpoint()throws Exception{try(Connection c=connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,30000)){c.setAutoCommit(false);Checkpoint cp=locked(c,null);guard.verify();c.rollback();return cp;}}
    Checkpoint verifyWriterCut(long maximumEpoch,long sequence,List<TableSchema> tables,
                               Map<String,Journal.SnapshotState> expected)throws Exception{
        try(Connection c=connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);boundRecoveryLocks(c);Checkpoint checkpoint=locked(c,null);
            requireWriterCut(c,checkpoint,maximumEpoch,sequence,tables,expected);
            guard.verify();c.rollback();return checkpoint;
        }
    }

    private void requireWriterCut(Connection c,Checkpoint checkpoint,long maximumEpoch,long sequence,
                                  List<TableSchema> tables,Map<String,Journal.SnapshotState> expected)throws Exception{
        boolean validReceipt=sequence==0?checkpoint.transactionId==null&&checkpoint.transactionHash==null:
                checkpoint.transactionId!=null&&io.github.rivomesh.recovery.FailureEvidence.hash(checkpoint.transactionHash);
        if(checkpoint.epoch<1||checkpoint.epoch>maximumEpoch||sequence!=checkpoint.sequence||!validReceipt)
            throw new IllegalStateException("WRITER_ASSIGNMENT_TARGET_CHECKPOINT_CHANGED");
        for(TableSchema table:tables){
            SnapshotCheckpoint actual=snapshotCheckpoint(c,table);
            Journal.SnapshotState original=expected.get(table.id);
            if(!"COMPLETE".equals(actual.state)||!original.snapshotId.equals(actual.snapshotId)
                    ||original.nextPart!=actual.nextPart||original.rows!=actual.rows)
                throw new IllegalStateException("WRITER_ASSIGNMENT_TARGET_SNAPSHOT_CHANGED");
            validateShape(c,table);
        }
    }

    /** The exact business checkpoint and snapshot must still be visible under the target writer lock. */
    Checkpoint fenceWriterCut(long parkedEpoch,Checkpoint prior,List<TableSchema> tables,
                              Map<String,Journal.SnapshotState> snapshots,JournalWriterAssignment.FenceAuthorization authorize)throws Exception{
        if(prior.epoch<1||parkedEpoch<=prior.epoch||parkedEpoch-prior.epoch!=1)
            throw new IllegalArgumentException("HANDOFF_TARGET_EPOCH_INVALID");
        try(Connection c=connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,5000)){
            c.setAutoCommit(false);boundRecoveryLocks(c);
            Checkpoint actual=locked(c,null);
            requireWriterCut(c,actual,parkedEpoch,prior.sequence,tables,snapshots);
            long observed=actual.epoch;
            if(observed!=prior.epoch&&observed!=parkedEpoch)throw new IllegalStateException("HANDOFF_TARGET_CUT_CHANGED");
            actual.epoch=prior.epoch;
            if(!Codec.encode(actual).equals(Codec.encode(prior)))throw new IllegalStateException("HANDOFF_TARGET_CUT_CHANGED");
            guard.verify();authorize.verify();
            if(observed!=parkedEpoch&&sql(c,"UPDATE "+metadata("job")+" SET epoch=? WHERE pipeline_id=? AND subscription_id=? AND epoch=?",
                    parkedEpoch,settings.pipelineId,settings.id,observed)!=1)
                throw new IllegalStateException("HANDOFF_TARGET_CUT_CHANGED");
            guard.verify();c.commit();actual.epoch=parkedEpoch;return actual;
        }
    }

    interface CheckpointAction<T>{T run(Checkpoint checkpoint)throws Exception;}
    /** Recovery caller must already hold a PAUSED pipeline's journal job/subscription locks. */
    <T>T withRecoveryCheckpoint(long epoch,Checkpoint expected,CheckpointAction<T> action)throws Exception{
        try(Connection c=connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);validateMetadata(c);Checkpoint actual=locked(c,epoch);
            if(!Codec.encode(actual).equals(Codec.encode(expected)))throw new IllegalStateException("FAILURE_CHECKPOINT_CHANGED");
            guard.verify();T result=action.run(actual);c.rollback();return result;
        }
    }
    private void skipScope(io.github.rivomesh.recovery.SkipPlan plan){
        plan.validate();if(!settings.pipelineId.equals(plan.pipelineId)||!settings.id.equals(plan.subscriptionId)||!settings.generation.equals(plan.generation)||!settings.fingerprint.equals(plan.fingerprint))throw new IllegalStateException("SKIP_TARGET_SCOPE_CHANGED");
        if(settings.product!=TargetProduct.POSTGRESQL&&settings.product!=TargetProduct.MYSQL&&settings.product!=TargetProduct.H2)throw new IllegalStateException("SKIP_TARGET_UNSUPPORTED");
    }
    private boolean skipTableExists(Connection c)throws Exception{
        try(Statement s=c.createStatement();ResultSet ignored=s.executeQuery("SELECT operation_id,plan_hash,payload_hash,payload FROM "+metadata("skip")+" WHERE 1=0")){return true;}
        catch(SQLException missing){if("42P01".equals(missing.getSQLState())||"42S02".equals(missing.getSQLState())||missing.getErrorCode()==1146)return false;throw missing;}
    }
    /** Source PREPARED intent and whole-pipeline fence must precede this additive native DDL. */
    void prepareSkipMetadata(io.github.rivomesh.recovery.SkipPlan plan)throws Exception{
        skipScope(plan);
        try(Connection c=connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            validateMetadata(c);Set<String> columns=skipColumnNames(c);boolean receiptExists=skipTableExists(c);
            if(!receiptExists&&!columns.isEmpty())throw new IllegalStateException("SINK_SKIP_METADATA_LOST");
            Checkpoint checkpoint=locked(c,plan.epoch);
            if(checkpoint.sequence!=plan.acknowledged&&checkpoint.sequence!=plan.sequence)throw new IllegalStateException("SKIP_NATIVE_CHECKPOINT_CHANGED");
            if(!receiptExists)ddl(c,"CREATE TABLE IF NOT EXISTS "+metadata("skip")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,operation_id VARCHAR(128) NOT NULL,stream_seq BIGINT NOT NULL,plan_hash VARCHAR(64) NOT NULL,payload_hash VARCHAR(64) NOT NULL,payload "+(mysql()?"LONGTEXT":settings.product==TargetProduct.H2?"CLOB":"TEXT")+" NOT NULL,PRIMARY KEY(pipeline_id,subscription_id,operation_id),UNIQUE(pipeline_id,subscription_id,stream_seq))"+(mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":""));
            // Receipt table first: every legitimate interrupted column migration keeps this table.
            if(!columns.contains("skip_operation"))addSkipColumn(c,"skip_operation",128);
            if(!columns.contains("skip_hash"))addSkipColumn(c,"skip_hash",64);
            skipAware=skipColumns(c,false)==2;deadline.verify();
        }
    }
    private void addSkipColumn(Connection c,String name,int length)throws Exception{
        try{ddl(c,"ALTER TABLE "+metadata("job")+" ADD COLUMN "+q(name)+" VARCHAR("+length+")");}
        catch(SQLException duplicate){if(!"42701".equals(duplicate.getSQLState())&&!"42S21".equals(duplicate.getSQLState())&&duplicate.getErrorCode()!=1060)throw duplicate;if(!skipColumnNames(c).contains(name))throw duplicate;}
    }
    private void skipStorage(Connection c)throws Exception{
        if(!skipAware||!skipTableExists(c))throw new IllegalStateException("SINK_SKIP_METADATA_LOST");validateMetadata(c);
        if(mysql())try(PreparedStatement p=c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME='rm3_sink_skip'")){
            p.setString(1,settings.metadataSchema);try(ResultSet r=p.executeQuery()){if(!r.next()||!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new IllegalStateException("SINK_SKIP_METADATA_NOT_TRANSACTIONAL");}
        }
    }
    private io.github.rivomesh.recovery.SkipReceipt skipReceipt(Connection c,io.github.rivomesh.recovery.SkipPlan plan)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT plan_hash,payload_hash,payload FROM "+metadata("skip")+" WHERE pipeline_id=? AND subscription_id=? AND operation_id=?")){
            p.setString(1,settings.pipelineId);p.setString(2,settings.id);p.setString(3,plan.id);try(ResultSet r=p.executeQuery()){
                if(!r.next())return null;if(!plan.hash().equals(r.getString(1)))throw new IllegalStateException("SKIP_REQUEST_CONFLICT");
                StringBuilder text=new StringBuilder();try(java.io.Reader input=r.getCharacterStream(3)){char[] b=new char[1024];int n;while((n=input.read(b))!=-1){if(text.length()+n>32768)throw new IllegalStateException("SINK_SKIP_RECEIPT_TOO_LARGE");text.append(b,0,n);}}
                String payload=text.toString();if(!Codec.sha256(payload).equals(r.getString(2)))throw new IllegalStateException("SINK_SKIP_RECEIPT_CHANGED");io.github.rivomesh.recovery.SkipReceipt receipt=Codec.decode(payload,io.github.rivomesh.recovery.SkipReceipt.class);receipt.validate(plan);if(receipt.executionRequest==null)throw new IllegalStateException("SINK_SKIP_RECEIPT_INVALID");return receipt;
            }
        }
    }
    private void skipTables(Connection c,io.github.rivomesh.recovery.SkipPlan plan,io.github.rivomesh.recovery.FailureEvidence evidence)throws Exception{
        for(String reference:plan.schemaVersions){TableSchema table=evidence.schemas.get(reference);if(table==null||!includes(table))throw new IllegalStateException("SKIP_HISTORICAL_SCHEMA_CHANGED");
            TargetOwnership.verify(c,destination(table),table.name,settings.pipelineId,settings.id,settings.generation);
            if(!"COMPLETE".equals(snapshotCheckpoint(c,table).state))throw new IllegalStateException("SINK_SNAPSHOT_INCOMPLETE");validateShape(c,table);RepairDependencies.verify(c,settings.product,destination(table),table.name,table(table));
        }
    }
    private void skippedCheckpoint(Checkpoint cp,io.github.rivomesh.recovery.SkipPlan plan,io.github.rivomesh.recovery.SkipReceipt receipt){
        if(cp.epoch!=plan.epoch||cp.sequence!=plan.sequence||!plan.transactionId.equals(cp.transactionId)||!plan.projectedHash.equals(cp.transactionHash)||!plan.id.equals(cp.skipOperation)||!receipt.hash().equals(cp.skipHash))throw new IllegalStateException("SKIP_NATIVE_CHECKPOINT_CHANGED");
    }
    <T>T withSkipPreflight(io.github.rivomesh.recovery.SkipPlan plan,io.github.rivomesh.recovery.FailureEvidence evidence,CheckpointAction<T> action)throws Exception{
        skipScope(plan);
        try(Connection c=connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);Checkpoint cp=locked(c,plan.epoch);validateMetadata(c);
            if(!Codec.encode(cp).equals(Codec.encode(plan.expectedCheckpoint)))throw new IllegalStateException("SKIP_NATIVE_CHECKPOINT_CHANGED");skipTables(c,plan,evidence);
            deadline.verify();T result=action.run(cp);c.rollback();return result;
        }
    }
    /** Native receipt and safe progress commit in one transaction; no business DML is executed. */
    io.github.rivomesh.recovery.SkipReceipt skip(io.github.rivomesh.recovery.SkipPlan plan,io.github.rivomesh.recovery.FailureEvidence evidence)throws Exception{
        return skip(plan,evidence,io.github.rivomesh.recovery.SkipResolution.create(plan.id,plan.id,"SKIP_COMMITTED",plan.createdBy,plan.reason));
    }
    io.github.rivomesh.recovery.SkipReceipt skip(io.github.rivomesh.recovery.SkipPlan plan,io.github.rivomesh.recovery.FailureEvidence evidence,io.github.rivomesh.recovery.SkipResolution execution)throws Exception{
        skipScope(plan);
        execution.validate();if(!plan.id.equals(execution.planId)||!"SKIP_COMMITTED".equals(execution.action))throw new IllegalArgumentException("SKIP_RESOLUTION_INVALID");
        try(Connection c=connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);Checkpoint cp=locked(c,plan.epoch);skipStorage(c);io.github.rivomesh.recovery.SkipReceipt prior=skipReceipt(c,plan);
            if(prior!=null){skippedCheckpoint(cp,plan,prior);c.rollback();return prior;}
            if(!Codec.encode(cp).equals(Codec.encode(plan.expectedCheckpoint)))throw new IllegalStateException("SKIP_NATIVE_CHECKPOINT_CHANGED");skipTables(c,plan,evidence);
            io.github.rivomesh.recovery.SkipReceipt receipt=new io.github.rivomesh.recovery.SkipReceipt();receipt.operationId=plan.id;receipt.planHash=plan.hash();receipt.pipelineId=plan.pipelineId;receipt.subscriptionId=plan.subscriptionId;receipt.generation=plan.generation;receipt.fingerprint=plan.fingerprint;receipt.transactionId=plan.transactionId;receipt.projectedHash=plan.projectedHash;receipt.epoch=plan.epoch;receipt.sequence=plan.sequence;receipt.committedAt=java.time.Instant.now().toString();receipt.executionRequest=execution.requestId;receipt.executedBy=execution.actor;receipt.executionReason=execution.reason;receipt.executionReasonHash=Codec.sha256(execution.reason);receipt.validate(plan);
            sql(c,"INSERT INTO "+metadata("skip")+" (pipeline_id,subscription_id,operation_id,stream_seq,plan_hash,payload_hash,payload) VALUES (?,?,?,?,?,?,?)",plan.pipelineId,plan.subscriptionId,plan.id,plan.sequence,plan.hash(),receipt.hash(),Codec.encode(receipt));
            sql(c,"UPDATE "+metadata("job")+" SET stream_seq=?,transaction_id=?,transaction_hash=?,"+q("skip_operation")+"=?,"+q("skip_hash")+"=? WHERE pipeline_id=? AND subscription_id=?",plan.sequence,plan.transactionId,plan.projectedHash,plan.id,receipt.hash(),settings.pipelineId,settings.id);
            deadline.verify();c.commit();return receipt;
        }
    }
    interface SkipReceiptAction<T>{T run(io.github.rivomesh.recovery.SkipReceipt receipt)throws Exception;}
    /** Read-only snapshot for the admin dialog. No DDL, activation, ACK or normalization. */
    Map<String,Object> skipProof(io.github.rivomesh.recovery.SkipPlan plan)throws Exception{
        skipScope(plan);try(Connection c=connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            int columns=skipColumns(c,true);boolean tableExists=skipTableExists(c);if(!tableExists&&columns>0)throw new IllegalStateException("SINK_SKIP_METADATA_LOST");
            c.setAutoCommit(false);Checkpoint cp=locked(c,plan.epoch);validateMetadata(c);io.github.rivomesh.recovery.SkipReceipt receipt=tableExists?skipReceipt(c,plan):null;
            String state;if(receipt!=null){if(columns!=2)throw new IllegalStateException("SINK_RECOVERY_METADATA_INCOMPLETE");skipStorage(c);skippedCheckpoint(cp,plan,receipt);state="COMMITTED";}
            else{if(!Codec.encode(cp).equals(Codec.encode(plan.expectedCheckpoint)))throw new IllegalStateException("SKIP_NATIVE_CHECKPOINT_CHANGED");state="ABSENT";}
            deadline.verify();Map<String,Object> result=new LinkedHashMap<>();result.put("state",state);result.put("metadataReady",columns==2&&tableExists);result.put("checkpoint",cp);result.put("receipt",receipt);c.rollback();return result;
        }
    }
    /** Holds native proof through the caller's journal completion or controlled cancellation. */
    <T>T resolveSkip(io.github.rivomesh.recovery.SkipPlan plan,boolean committed,SkipReceiptAction<T> action)throws Exception{
        skipScope(plan);
        try(Connection c=connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,30000)){
            c.setAutoCommit(false);Checkpoint cp=locked(c,plan.epoch);skipStorage(c);io.github.rivomesh.recovery.SkipReceipt receipt=skipReceipt(c,plan);
            if(committed){if(receipt==null)throw new IllegalStateException("SKIP_NATIVE_COMMIT_NOT_PROVEN");skippedCheckpoint(cp,plan,receipt);}
            else if(receipt!=null)throw new IllegalStateException("SKIP_ALREADY_COMMITTED");else if(!Codec.encode(cp).equals(Codec.encode(plan.expectedCheckpoint)))throw new IllegalStateException("SKIP_NATIVE_CHECKPOINT_CHANGED");
            deadline.verify();T result=action.run(receipt);c.rollback();return result;
        }
    }
    @Override public TargetRead validationRead(TableSchema schema,Checkpoint expected,long maximumMillis,int maximumRowBytes)throws Exception{
        if(expected==null||expected.epoch<1||expected.sequence<0||maximumMillis<1000||maximumMillis>86400000||maximumRowBytes<1024||maximumRowBytes>32*1024*1024)throw new IllegalArgumentException("VALIDATION_TARGET_READ_LIMIT_INVALID");
        TableSchema table=Codec.decode(Codec.encode(schema),TableSchema.class);table.validate();destination(table);
        Checkpoint checkpoint=Codec.decode(Codec.encode(expected),Checkpoint.class);
        return new JdbcTargetRead(connect(),table,settings.product,validationSql(table),maximumRowBytes,maximumMillis,c->{
            Checkpoint actual=locked(c,checkpoint.epoch);
            if(actual.sequence!=checkpoint.sequence||!Objects.equals(actual.transactionId,checkpoint.transactionId)||!Objects.equals(actual.transactionHash,checkpoint.transactionHash))throw new IllegalStateException("VALIDATION_TARGET_CHECKPOINT_MISMATCH");
            if(!"COMPLETE".equals(snapshotCheckpoint(c,table).state))throw new IllegalStateException("SINK_SNAPSHOT_INCOMPLETE");validateShape(c,table);
        });
    }
    String validationSql(TableSchema table){
        List<String> columns=new ArrayList<>();for(TableSchema.Column column:table.columns){String value=q(column.name);
            if(column.type==TableSchema.Type.TIME&&mysql())value="CAST("+value+" AS CHAR)";
            if(column.type==TableSchema.Type.TIME&&!mysql()&&settings.product!=TargetProduct.H2){
                String seconds=dm()?"(EXTRACT(DAY FROM "+value+")*86400+EXTRACT(HOUR FROM "+value+")*3600+EXTRACT(MINUTE FROM "+value+")*60+EXTRACT(SECOND FROM "+value+"))":"EXTRACT(EPOCH FROM "+value+")";
                value="CAST(CAST("+seconds+" AS NUMERIC(20,6))*1000000 AS NUMERIC(20,0))";
            }columns.add(value);
        }
        if(table.keyless())columns.add(q(ROW_ID));
        return "SELECT "+String.join(",",columns)+" FROM "+table(table);
    }
    private boolean compensationStorageExists(Connection c)throws SQLException {
        try (ResultSet tables = c.getMetaData().getTables(mysql() ? settings.metadataSchema : c.getCatalog(),
                mysql() ? null : settings.metadataSchema, "rm3_sink_compensation", new String[]{"TABLE"})) {
            while (tables.next()) if ("rm3_sink_compensation".equals(tables.getString("TABLE_NAME"))) return true;
            return false;
        }
    }

    private void prepareCompensationStorage()throws Exception {
        try (Connection c = connect(); RepairConnectionGuard guard = new RepairConnectionGuard(c, 30000)) {
            if (!compensationStorageExists(c)) {
                ddl(c, "CREATE TABLE IF NOT EXISTS " + metadata("compensation")
                        + " (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,"
                        + "operation_id VARCHAR(128) NOT NULL,request_hash VARCHAR(64) NOT NULL,payload_hash VARCHAR(64) NOT NULL,"
                        + "payload " + (mysql() ? "LONGTEXT" : settings.product == TargetProduct.H2 ? "CLOB" : "TEXT")
                        + " NOT NULL,PRIMARY KEY(pipeline_id,subscription_id,operation_id))"
                        + (mysql() ? " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin" : ""));
            }
            guard.verify();
        }
    }

    private void boundRecoveryLocks(Connection c)throws SQLException {
        // A disconnected JDBC client can leave a backend waiting on a lock.
        // Let the engine release its transaction before the client-side guard expires.
        try(Statement statement=c.createStatement()){
            if(settings.product==TargetProduct.POSTGRESQL){
                statement.execute("SET LOCAL lock_timeout = '20s'");
            }else if(settings.product==TargetProduct.MYSQL){
                statement.execute("SET SESSION innodb_lock_wait_timeout = 20");
                statement.execute("SET SESSION lock_wait_timeout = 20");
            }
        }
    }

    Map<String,Object> compensationReceipt(String operation, String requestHash)throws Exception {
        try (Connection c = connect(); RepairConnectionGuard guard = new RepairConnectionGuard(c, 30000)) {
            if (!compensationStorageExists(c)) return null;
            c.setAutoCommit(false);
            boundRecoveryLocks(c);
            locked(c, null);
            Map<String,Object> receipt = compensationReceipt(c, operation, requestHash);
            guard.verify();
            c.rollback();
            return receipt;
        }
    }

    private Map<String,Object> compensationReceipt(Connection c, String operation, String requestHash)throws Exception {
        String query = "SELECT request_hash,payload_hash,payload FROM " + metadata("compensation")
                + " WHERE pipeline_id=? AND subscription_id=? AND operation_id=?";
        try (PreparedStatement read = c.prepareStatement(query)) {
            read.setString(1, settings.pipelineId);
            read.setString(2, settings.id);
            read.setString(3, operation);
            try (ResultSet result = read.executeQuery()) {
                if (!result.next()) return null;
                String payload = JdbcGapVerificationLedger.bounded(result.getCharacterStream(3), 512 * 1024);
                if (!requestHash.equals(result.getString(1))) throw new IllegalStateException("COMPENSATION_REQUEST_CONFLICT");
                if (!Codec.sha256(payload).equals(result.getString(2))) throw new IllegalStateException("COMPENSATION_RECEIPT_CHANGED");
                @SuppressWarnings("unchecked") Map<String,Object> receipt = Codec.decode(payload, Map.class);
                if (result.next() || !operation.equals(receipt.get("operation"))
                        || !settings.generation.equals(receipt.get("generation"))
                        || !settings.fingerprint.equals(receipt.get("fingerprint"))
                        || !Arrays.asList("COMPENSATION_COMPLETED","CORRECTION_COMPLETED").contains(receipt.get("code")))
                    throw new IllegalStateException("COMPENSATION_RECEIPT_CHANGED");
                return receipt;
            }
        }
    }

    private List<Cell> compensationRow(Connection c, CompensationRows.Row row, RepairConnectionGuard guard)throws Exception {
        String predicate = keys(row.table).stream().map(key -> q(key) + "=?").collect(Collectors.joining(" AND "));
        try (PreparedStatement read = c.prepareStatement(validationSql(row.table) + " WHERE " + predicate + " FOR UPDATE")) {
            if (row.table.keyless()) read.setString(1, row.occurrence);
            else {
                int parameter = 1;
                for (String name : keys(row.table)) {
                    int column = row.table.column(name);
                    bind(read, parameter++, row.table.columns.get(column), row.key.get(column));
                }
            }
            try (ResultSet result = read.executeQuery()) {
                if (!result.next()) return null;
                TargetRead.Row actual = JdbcTargetRead.readRow(result, row.table, settings.product, row.edit==null?4*1024*1024:512*1024, guard::verify);
                if (result.next() || !row.identity.equals(RowValues.identity(row.table, actual.values, actual.rowId)))
                    throw new IllegalStateException("COMPENSATION_IDENTITY_AMBIGUOUS");
                return actual.values;
            }
        }
    }

    Map<String,Object> compensation(Collection<CompensationRows.Row> rows,
            io.github.rivomesh.recovery.FailureEvidenceStore.Envelope retained, boolean correction, String approvedHash,
            String operation, String requestHash, String actor, String reason,
            java.util.function.BooleanSupplier cancelled, CheckpointAction<Void> check)throws Exception {
        repairProduct();
        if(cancelled.getAsBoolean())throw new IllegalStateException("RECOVERY_CANCELLED");
        if (approvedHash != null) prepareCompensationStorage();
        Journal.FailureReview review = retained.value.review;
        try (Connection c = connect(); RepairConnectionGuard guard = new RepairConnectionGuard(c, 30000,cancelled)) {
            if (mysql()) c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            boundRecoveryLocks(c);
            Checkpoint checkpoint = locked(c, null);
            if (approvedHash != null) {
                Map<String,Object> prior = compensationReceipt(c, operation, requestHash);
                if (prior != null) return prior;
                RepairDependencies.verify(c, settings.product, settings.metadataSchema, "rm3_sink_compensation", metadata("compensation"));
                if (mysql()) {
                    try (PreparedStatement p = c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME='rm3_sink_compensation'")) {
                        p.setString(1, settings.metadataSchema);
                        try (ResultSet r = p.executeQuery()) {
                            if (!r.next() || !"InnoDB".equalsIgnoreCase(r.getString(1)))
                                throw new IllegalStateException("COMPENSATION_RECEIPT_ENGINE_INVALID");
                        }
                    }
                }
            }
            if (checkpoint.epoch != review.epoch) throw new IllegalStateException("COMPENSATION_TARGET_FENCE_REQUIRED");
            if (checkpoint.sequence != review.subscription.acknowledged)
                throw new IllegalStateException("COMPENSATION_CHECKPOINT_RECONCILIATION_REQUIRED");
            if (checkpoint.sequence == 0 ? checkpoint.transactionId != null || checkpoint.transactionHash != null || checkpoint.skipOperation != null
                    : checkpoint.transactionId == null || checkpoint.transactionId.isEmpty()
                      || !io.github.rivomesh.recovery.FailureEvidence.hash(checkpoint.transactionHash))
                throw new IllegalStateException("FAILURE_CHECKPOINT_CONTENT_MISMATCH");
            check.run(checkpoint);
            Map<String,Map<String,Object>> tables = new TreeMap<>();
            List<List<String>> images = new ArrayList<>();
            boolean showRows=correction && approvedHash==null;
            List<Map<String,Object>> details=showRows?new ArrayList<>():null;
            long detailBytes=0;
            Map<CompensationRows.Row,List<Cell>> desiredRows=new LinkedHashMap<>();
            List<CompensationRows.Row> delete = new ArrayList<>(), insert = new ArrayList<>();
            for (CompensationRows.Row row : rows) {
                guard.verify();
                TableSchema table = row.table;
                Map<String,Object> summary = tables.get(table.id);
                if (summary == null) {
                    if (!"COMPLETE".equals(snapshotCheckpoint(c, table).state))
                        throw new IllegalStateException("SINK_SNAPSHOT_INCOMPLETE");
                    validateShape(c, table);
                    RepairDependencies.verify(c, settings.product, destination(table), table.name, table(table));
                    summary = new LinkedHashMap<>();
                    summary.put("database", table.database);
                    summary.put("table", table.name);
                    summary.put("schemaVersion", table.version);
                    summary.put("identities", 0);
                    summary.put("deletes", 0);
                    summary.put("inserts", 0);
                    tables.put(table.id, summary);
                }
                List<Cell> actual = compensationRow(c, row, guard);
                List<Cell> desiredRow=row.desired(actual);
                desiredRows.put(row,desiredRow);
                String current=CompensationRows.hash(table,actual), desired=CompensationRows.hash(table,desiredRow);
                List<String> image=new ArrayList<>(Arrays.asList(table.id,Codec.sha256(row.identity),current,desired));
                if(correction)image.add(row.edit.name());
                images.add(image);
                if(correction){
                    Map<String,Object> detail=new LinkedHashMap<>();
                    detail.put("table",table.qualifiedName());
                    detail.put("action",row.edit.name());
                    detail.put("rowId",row.occurrence);
                    detail.put("before",correctionValues(table,actual));
                    detail.put("after",correctionValues(table,desiredRow));
                    detail.put("changed",!Objects.equals(current,desired));
                    detailBytes+=Codec.encode(detail).getBytes(StandardCharsets.UTF_8).length;
                    if(detailBytes>512*1024)throw new IllegalArgumentException("CORRECTION_PREVIEW_LIMIT");
                    if(showRows)details.add(detail);
                }
                summary.compute("identities", (key, count) -> (int)count + 1);
                if (Objects.equals(current, desired)) continue;
                if (actual != null) {
                    delete.add(row);
                    summary.compute("deletes", (key, count) -> (int)count + 1);
                }
                if (desiredRow != null) {
                    insert.add(row);
                    summary.compute("inserts", (key, count) -> (int)count + 1);
                }
            }
            Map<String,Object> result = new LinkedHashMap<>();
            result.put("evidenceId", retained.value.id);
            result.put("evidenceHash", retained.hash);
            result.put("pipelineId", settings.pipelineId);
            result.put("subscriptionId", settings.id);
            result.put("generation", settings.generation);
            result.put("fingerprint", settings.fingerprint);
            result.put("errorGeneration", review.subscription.errorGeneration);
            result.put("sequence", Long.toString(review.sequence));
            result.put("checkpoint", Codec.decode(Codec.encode(checkpoint), Map.class));
            result.put("tables", tables);
            String hash = Codec.sha256(Codec.encode(Arrays.asList(correction?"CORRECTION_V1":"COMPENSATION_V1", result, images)));
            result.put("hash", hash);
            guard.verify();
            if (approvedHash == null) {
                c.rollback();
                result.put("code", correction?"CORRECTION_PLAN_READY":"COMPENSATION_PLAN_READY");
                if(showRows)result.put("rows",details);
                return result;
            }
            if (!approvedHash.equals(hash)) throw new IllegalStateException(correction?"CORRECTION_PLAN_CHANGED":"COMPENSATION_PLAN_CHANGED");
            for (CompensationRows.Row row : delete) {
                guard.verify();
                Change removal = new Change();
                removal.operation = Change.Operation.DELETE;
                removal.before = row.key;
                removal.rowId = row.occurrence;
                change(c, row.table, removal);
            }
            for (CompensationRows.Row row : insert) {
                guard.verify();
                insert(c, row.table, desiredRows.get(row), row.occurrence);
            }
            for (CompensationRows.Row row : rows) {
                if (!CompensationRows.equal(row.table, desiredRows.get(row), compensationRow(c, row, guard)))
                    throw new IllegalStateException("COMPENSATION_POSTCHECK_FAILED");
            }
            result.put("code", correction?"CORRECTION_COMPLETED":"COMPENSATION_COMPLETED");
            result.put("operation", operation);
            result.put("actor", actor);
            result.put("reason", reason);
            result.put("completedAt", java.time.Instant.now().toString());
            String payload = Codec.encode(result);
            sql(c, "INSERT INTO " + metadata("compensation") + " VALUES (?,?,?,?,?,?)",
                    settings.pipelineId, settings.id, operation, requestHash, Codec.sha256(payload), payload);
            guard.verify();
            c.commit();
            return result;
        }
    }
    private static Map<String,String> correctionValues(TableSchema table,List<Cell> row){
        if(row==null)return null;
        Map<String,String> values=new LinkedHashMap<>();
        for(int i=0;i<row.size();i++)values.put(table.columns.get(i).name,row.get(i).value);
        return values;
    }
    private List<String> keys(TableSchema table){return table.keyless()?Collections.singletonList(ROW_ID):table.primaryKey.isEmpty()?table.identityKey:table.primaryKey;}
    public static void validateOracleMysqlProjection(TableSchema table){
        if(!"ORACLE".equals(table.sourceEngine))return;
        for(TableSchema.Column column:table.columns){
            if(column.type==TableSchema.Type.DECIMAL&&column.scale>30)
                throw new IllegalArgumentException("ORACLE_MYSQL_DECIMAL_SCALE_UNSUPPORTED");
        }
    }
    private boolean oracleMysql(TableSchema table){
        return settings.product==TargetProduct.MYSQL&&"ORACLE".equals(table.sourceEngine);
    }
    private String type(TableSchema.Column col){
        if(dm())return DmStorage.type(col);
        switch(col.type){
            case BOOLEAN:return "BOOLEAN";
            case INT64:return "BIGINT";
            case UINT64:return "DECIMAL(20,0)";
            case DECIMAL:if(col.precision<1||col.precision>1000||col.scale<0||col.scale>col.precision)throw new IllegalArgumentException("DECIMAL_RANGE_INVALID");return "DECIMAL("+col.precision+","+col.scale+")";
            case FLOAT32:return mysql()?"FLOAT":"REAL";
            case FLOAT64:return mysql()?"DOUBLE":"DOUBLE PRECISION";
            case BYTES:case GEOMETRY:
                if(settings.product==TargetProduct.MYSQL&&"RAW".equals(col.sourceType))
                    return "VARBINARY("+col.length+")";
                return mysql()?"LONGBLOB":settings.product==TargetProduct.H2?"BLOB":"BYTEA";
            case STRING:if(col.length>0&&col.length<=4000)return "VARCHAR("+(settings.product==TargetProduct.GAUSS?Math.multiplyExact(col.length,4):col.length)+")";return mysql()?"LONGTEXT":"TEXT";
            case DATE:return "DATE";
            case DATETIME:case TIMESTAMP:return mysql()?"DATETIME(6)":"TIMESTAMP(6)";
            case TIME:return mysql()||nativePostgresClock(col)?"TIME(6)":settings.product==TargetProduct.H2?"VARCHAR(32)":"INTERVAL";
            case JSON:return mysql()?"JSON":settings.product==TargetProduct.H2?"CLOB":"JSONB";
            default:throw new IllegalArgumentException("TARGET_TYPE_UNSUPPORTED");
        }
    }
    private String columnDefinitions(TableSchema table){
        if(TargetOwnership.TABLE.equalsIgnoreCase(table.name))throw new IllegalArgumentException("RESERVED_TARGET_TABLE_NAME");
        List<String> definitions=new ArrayList<>();for(TableSchema.Column col:table.columns){
            if(col.name.equalsIgnoreCase(ROW_ID))throw new IllegalArgumentException("RESERVED_IDENTITY_COLUMN");
            definitions.add(q(col.name)+" "+type(col)+(col.nullable?"":" NOT NULL"));
        }
        if(table.keyless())definitions.add(q(ROW_ID)+" VARCHAR(64) NOT NULL");
        definitions.add("PRIMARY KEY ("+keys(table).stream().map(this::q).collect(Collectors.joining(","))+")");return String.join(",",definitions);
    }
    private boolean registered(Connection c,TableSchema table)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT schema_version,schema_hash FROM "+metadata("table")+" WHERE pipeline_id=? AND subscription_id=? AND table_id=?")){
            p.setString(1,settings.pipelineId);p.setString(2,settings.id);p.setString(3,table.id);try(ResultSet rs=p.executeQuery()){
                if(!rs.next())return false;if(rs.getInt(1)!=table.version||!rs.getString(2).equals(Codec.sha256(Codec.encode(table))))throw new IllegalStateException("SINK_SCHEMA_MIGRATION_REQUIRED");return true;
            }
        }
    }
    private boolean exists(Connection c,TableSchema table)throws Exception{
        DatabaseMetaData m=c.getMetaData();try(ResultSet rs=m.getTables(mysql()?destination(table):c.getCatalog(),mysql()?null:destination(table),table.name,new String[]{"TABLE"})){while(rs.next())if(table.name.equals(rs.getString("TABLE_NAME")))return true;return false;}
    }
    void validateShape(Connection c,TableSchema table)throws Exception{
        validateShape(c,table,null);
    }
    void validateShape(Connection c,TableSchema table,String verifiedWriterTrigger)throws Exception{
        if(verifiedWriterTrigger!=null&&settings.product!=TargetProduct.POSTGRESQL)throw new IllegalArgumentException("TARGET_WRITER_PROFILE_UNSUPPORTED");
        // A zero-row select validates exact quoted names and order without loading data.
        try(Statement st=c.createStatement();ResultSet rs=st.executeQuery("SELECT * FROM "+table(table)+" WHERE 1=0")){
            ResultSetMetaData m=rs.getMetaData();if(m.getColumnCount()!=table.columns.size()+(table.keyless()?1:0))throw new IllegalStateException("TARGET_COLUMN_COUNT_MISMATCH");
            for(int i=0;i<table.columns.size();i++){
                TableSchema.Column col=table.columns.get(i);if(!col.name.equals(m.getColumnName(i+1)))throw new IllegalStateException("TARGET_COLUMN_ORDER_MISMATCH");
                if(col.nullable&&m.isNullable(i+1)==ResultSetMetaData.columnNoNulls)throw new IllegalStateException("TARGET_NULLABILITY_MISMATCH");
                int jdbc=m.getColumnType(i+1);String actual=m.getColumnTypeName(i+1).toLowerCase(Locale.ROOT);boolean matches;
                switch(col.type){
                    case BOOLEAN:matches=jdbc==Types.BOOLEAN||jdbc==Types.BIT||mysql()&&jdbc==Types.TINYINT;break;
                    case INT64:matches=jdbc==Types.BIGINT;break;
                    case UINT64:case DECIMAL:matches=dm()&&DmStorage.textDecimal(col)?jdbc==Types.VARCHAR&&m.getPrecision(i+1)>=col.precision+3:(jdbc==Types.DECIMAL||jdbc==Types.NUMERIC)&&m.getPrecision(i+1)>=(col.type==TableSchema.Type.UINT64?20:col.precision)&&m.getScale(i+1)==(col.type==TableSchema.Type.UINT64?0:col.scale);break;
                    case FLOAT32:matches=jdbc==Types.REAL||jdbc==Types.FLOAT;break;
                    case FLOAT64:matches=jdbc==Types.DOUBLE;break;
                    case STRING:matches=Arrays.asList(Types.VARCHAR,Types.LONGVARCHAR,Types.CLOB,Types.CHAR).contains(jdbc)&&(col.length<1?actual.contains("text")||jdbc==Types.CLOB||jdbc==Types.LONGVARCHAR:m.getPrecision(i+1)>=col.length||actual.contains("text"));break;
                    case BYTES:case GEOMETRY:matches=Arrays.asList(Types.BINARY,Types.VARBINARY,Types.LONGVARBINARY,Types.BLOB).contains(jdbc);break;
                    case JSON:matches=actual.equals(mysql()?"json":"jsonb")||(settings.product==TargetProduct.H2||dm())&&jdbc==Types.CLOB;break;
                    case DATE:matches=jdbc==Types.DATE||(settings.product==TargetProduct.GAUSS||dm())&&jdbc==Types.TIMESTAMP;break;
                    case DATETIME:case TIMESTAMP:
                        matches=jdbc==Types.TIMESTAMP&&temporalPrecision(c,table,col,m.getScale(i+1))>=6
                                &&(!OracleDate.isColumn(col)||actual.equals(mysql()?"datetime":"timestamp"));
                        break;
                    case TIME:
                        if(nativePostgresClock(col)||mysql()&&PostgresClock.isColumn(col))
                            matches=jdbc==Types.TIME&&actual.equals("time")&&temporalPrecision(c,table,col,m.getScale(i+1))==6;
                        else matches=mysql()?jdbc==Types.TIME:settings.product==TargetProduct.H2?jdbc==Types.VARCHAR:
                                dm()?actual.equals("interval day to second")&&m.getPrecision(i+1)>=2&&m.getScale(i+1)>=6:actual.equals("interval");
                        break;
                    default:matches=false;
                }
                if(dm()&&col.type==TableSchema.Type.STRING)matches=jdbc==Types.CLOB||jdbc==Types.VARCHAR&&col.length>0&&DmProfile.characterCapacity(c,destination(table),table.name,col.name)>=col.length;
                if(dm()&&(col.type==TableSchema.Type.BYTES||col.type==TableSchema.Type.GEOMETRY)&&jdbc!=Types.BLOB)matches=matches&&col.length>0&&m.getPrecision(i+1)>=col.length;
                if(oracleMysql(table)&&"RAW".equals(col.sourceType))
                    matches=jdbc==Types.VARBINARY&&m.getPrecision(i+1)>=col.length;
                if(!matches)throw new IllegalStateException("TARGET_COLUMN_TYPE_MISMATCH",new IllegalStateException("column="+col.name+", expected="+col.type+", actual="+actual+", jdbc="+jdbc+", precision="+m.getPrecision(i+1)+", scale="+m.getScale(i+1)));
                if(m.isAutoIncrement(i+1))throw new IllegalStateException("TARGET_IDENTITY_COLUMN_REQUIRES_PLAN");
            }
            if(table.keyless()&&(!ROW_ID.equals(m.getColumnName(m.getColumnCount()))||m.getColumnType(m.getColumnCount())!=Types.VARCHAR||m.getPrecision(m.getColumnCount())<64))throw new IllegalStateException("TARGET_ROW_ID_MISMATCH");
        }
        Map<Short,String> primary=new TreeMap<>();try(ResultSet rs=c.getMetaData().getPrimaryKeys(mysql()?destination(table):c.getCatalog(),mysql()?null:destination(table),table.name)){while(rs.next())primary.put(rs.getShort("KEY_SEQ"),rs.getString("COLUMN_NAME"));}
        if(!new ArrayList<>(primary.values()).equals(keys(table)))throw new IllegalStateException("TARGET_PRIMARY_KEY_MISMATCH");
        if(oracleMysql(table))validateOracleMysqlTextKeys(c,table);
        try(ResultSet rs=c.getMetaData().getImportedKeys(mysql()?destination(table):c.getCatalog(),mysql()?null:destination(table),table.name)){if(rs.next())throw new IllegalStateException("TARGET_FOREIGN_KEYS_REQUIRE_PLAN");}
        if(settings.product!=TargetProduct.H2)try(PreparedStatement p=c.prepareStatement(dm()?"SELECT 1 FROM ALL_TRIGGERS WHERE TABLE_OWNER=? AND TABLE_NAME=?":"SELECT 1 FROM information_schema.triggers WHERE event_object_schema=? AND event_object_table=?"+(verifiedWriterTrigger==null?"":" AND trigger_name<>?")+" LIMIT 1")){
            p.setString(1,destination(table));p.setString(2,table.name);if(verifiedWriterTrigger!=null)p.setString(3,verifiedWriterTrigger);
            try(ResultSet rs=p.executeQuery()){if(rs.next())throw new IllegalStateException("TARGET_TRIGGERS_REQUIRE_PLAN");}
        }
        if(mysql())try(PreparedStatement p=c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")){
            p.setString(1,destination(table));p.setString(2,table.name);try(ResultSet rs=p.executeQuery()){if(!rs.next()||!"InnoDB".equalsIgnoreCase(rs.getString(1)))throw new IllegalStateException("TARGET_ENGINE_NOT_TRANSACTIONAL");}
        }
    }
    private void validateOracleMysqlTextKeys(Connection c,TableSchema table)throws SQLException{
        String query="SELECT DATA_TYPE,COLLATION_NAME FROM information_schema.COLUMNS "
                +"WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?";
        try(PreparedStatement column=c.prepareStatement(query)){
            column.setString(1,destination(table));
            column.setString(2,table.name);
            for(String key:table.primaryKey){
                TableSchema.Column definition=table.columns.get(table.column(key));
                if(definition.type!=TableSchema.Type.STRING)continue;
                column.setString(3,key);
                try(ResultSet rows=column.executeQuery()){
                    if(!rows.next()||!"varchar".equalsIgnoreCase(rows.getString(1))
                            ||!"utf8mb4_0900_bin".equalsIgnoreCase(rows.getString(2)))
                        throw new IllegalStateException("ORACLE_MYSQL_TEXT_KEY_COLLATION_REQUIRES_PLAN");
                }
            }
        }
    }
    private boolean nativePostgresClock(TableSchema.Column column){
        return settings.product==TargetProduct.POSTGRESQL&&PostgresClock.isColumn(column);
    }
    private int temporalPrecision(Connection c,TableSchema table,TableSchema.Column column,int reported)throws SQLException{
        if(!mysql())return reported;
        // Connector/J reports numeric scale rather than DATETIME fractional precision in result metadata.
        try(PreparedStatement p=c.prepareStatement("SELECT DATETIME_PRECISION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?")){
            p.setString(1,destination(table));p.setString(2,table.name);p.setString(3,column.name);try(ResultSet rs=p.executeQuery()){return rs.next()?rs.getInt(1):-1;}
        }
    }
    @Override public synchronized void prepare(long epoch,TableSchema table)throws Exception{
        table.validate();
        if(dm())DmStorage.plan(table,settings.acknowledgeTextConversions);
        if(oracleMysql(table))validateOracleMysqlProjection(table);
        String definition=columnDefinitions(table);
        List<IndexPolicy.Decision> indexes=IndexPolicy.plan(table,settings.product,settings.uniquePolicy);
        for(IndexPolicy.Decision index:indexes)if("REQUIRES_PLAN".equals(index.action))throw new IllegalStateException("INDEX_CONVERSION_REQUIRES_PLAN");
        try(Connection c=connect()){
            c.setAutoCommit(false);locked(c,epoch);createNamespace(c,destination(table));TargetOwnership.initialize(c,destination(table),settings.product==TargetProduct.OCEANBASE);locked(c,epoch);TargetOwnership.claim(c,destination(table),table.name,settings.pipelineId,settings.id,settings.generation);
            boolean known=registered(c,table),existing=exists(c,table);if(known&&!existing)throw new IllegalStateException("TARGET_TABLE_MISSING_REBUILD_REQUIRED");
            if(!existing){
                createNamespace(c,destination(table));
                String storage=mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE="
                        +(oracleMysql(table)?"utf8mb4_0900_bin":"utf8mb4_bin"):"";
                ddl(c,"CREATE TABLE IF NOT EXISTS "+table(table)+" ("+definition+")"+storage);
            }
            // MySQL/DM DDL can commit; reacquire the local fence before registering any result.
            locked(c,epoch);validateShape(c,table);
            TargetOwnership.verify(c,destination(table),table.name,settings.pipelineId,settings.id,settings.generation);
            if(!known){
                try(Statement st=c.createStatement();ResultSet rs=st.executeQuery("SELECT 1 FROM "+table(table)+" LIMIT 1")){if(rs.next())throw new IllegalStateException("EXISTING_TARGET_REQUIRES_PLAN");}
            }
            createIndexes(c,table,indexes);
            locked(c,epoch);known=registered(c,table);
            if(!known){
                sql(c,"INSERT INTO "+metadata("table")+" (pipeline_id,subscription_id,table_id,schema_version,schema_hash,next_part,row_count) VALUES (?,?,?,?,?,0,0)",settings.pipelineId,settings.id,table.id,table.version,Codec.sha256(Codec.encode(table)));
            }
            c.commit();schemas.put(table.id,Codec.decode(Codec.encode(table),TableSchema.class));
        }
    }
    private void createIndexes(Connection c,TableSchema table,List<IndexPolicy.Decision> indexes)throws Exception{
        Map<String,List<String>> columns=new HashMap<>();Map<String,Boolean> unique=new HashMap<>();
        try(ResultSet rs=c.getMetaData().getIndexInfo(mysql()?destination(table):c.getCatalog(),mysql()?null:destination(table),table.name,false,false)){
            while(rs.next()){String name=rs.getString("INDEX_NAME"),column=rs.getString("COLUMN_NAME");if(name==null||column==null)continue;columns.computeIfAbsent(name,k->new ArrayList<>()).add(column);unique.put(name,!rs.getBoolean("NON_UNIQUE"));}
        }
        for(IndexPolicy.Decision index:indexes){
            if(columns.containsKey(index.targetName)){
                if(!columns.get(index.targetName).equals(index.columns)||unique.get(index.targetName)!=index.unique)throw new IllegalStateException("TARGET_INDEX_DEFINITION_MISMATCH");
            }else ddl(c,"CREATE "+(index.unique?"UNIQUE ":"")+"INDEX "+(dm()?q(destination(table))+".":"")+q(index.targetName)+" ON "+table(table)+" ("+index.columns.stream().map(this::q).collect(Collectors.joining(","))+")");
        }
        // Existing unique constraints cannot be silently dropped. Produce a separate migration plan.
        if(settings.uniquePolicy==UniquePolicy.DOWNGRADE_BUSINESS)for(String name:columns.keySet())if(unique.get(name)&&!columns.get(name).equals(keys(table))&&!columns.get(name).equals(table.identityKey)){
            String restored=settings.product==TargetProduct.POSTGRESQL
                    ?PostgresAdoptedUnique.runtimeConstraint(c,settings,destination(table),table):null;
            if(!name.equals(restored))throw new IllegalStateException("EXISTING_UNIQUE_REQUIRES_PLAN");
        }
    }
    private void bind(PreparedStatement p,int n,TableSchema.Column col,Cell value)throws Exception{
        if(value==null||!value.present)throw new IllegalArgumentException("ROW_IMAGE_INCOMPLETE");
        if(value.value==null){p.setNull(n,Types.NULL);return;}
        String v=value.value;
        if(dm()&&DmStorage.textDecimal(col)){p.setString(n,new BigDecimal(v).setScale(col.scale,java.math.RoundingMode.UNNECESSARY).toPlainString());return;}
        switch(col.type){
            case INT64:p.setLong(n,new BigDecimal(v).longValueExact());break;
            case UINT64:case DECIMAL:p.setBigDecimal(n,new BigDecimal(v));break;
            case FLOAT32:p.setFloat(n,Float.parseFloat(v));break;
            case FLOAT64:p.setDouble(n,Double.parseDouble(v));break;
            case BOOLEAN:if(!Arrays.asList("true","false","1","0").contains(v))throw new IllegalArgumentException("BOOLEAN_VALUE_INVALID");p.setBoolean(n,v.equals("true")||v.equals("1"));break;
            case BYTES:case GEOMETRY:p.setBytes(n,Base64.getDecoder().decode(v));break;
            case DATE:
                if(PostgresDate.isColumn(col))p.setObject(n,LocalDate.parse(v));
                else p.setDate(n,java.sql.Date.valueOf(v));
                break;
            case DATETIME:case TIMESTAMP:
                if(PostgresTimestamp.isColumn(col))p.setObject(n,PostgresTimestamp.localTime(v));
                else if((settings.product==TargetProduct.POSTGRESQL||settings.product==TargetProduct.MYSQL
                        ||settings.product==TargetProduct.KINGBASE)&&OracleDate.isColumn(col))p.setObject(n,OracleDate.localTime(v));
                else p.setTimestamp(n,Timestamp.valueOf(v));
                break;
            case JSON:if(mysql()||dm()||settings.product==TargetProduct.H2)p.setString(n,JsonValues.text(v));else p.setObject(n,JsonValues.text(v),Types.OTHER);break;
            case TIME:if(dm())p.setString(n,DmStorage.interval(v));else if(mysql()||settings.product==TargetProduct.H2)p.setString(n,v);else p.setObject(n,v,Types.OTHER);break;
            default:p.setString(n,v);
        }
    }
    private void image(TableSchema table,List<Cell> row){if(row==null||row.size()!=table.columns.size())throw new IllegalArgumentException("ROW_WIDTH_INVALID");for(Cell cell:row)if(cell==null||!cell.present)throw new IllegalArgumentException("ROW_IMAGE_INCOMPLETE");}
    private void insert(Connection c,TableSchema table,List<Cell> row,String rowId)throws Exception{
        image(table,row);List<String> columns=table.columns.stream().map(col->q(col.name)).collect(Collectors.toList());if(table.keyless())columns.add(q(ROW_ID));
        try(PreparedStatement p=c.prepareStatement("INSERT INTO "+table(table)+" ("+String.join(",",columns)+") VALUES ("+String.join(",",Collections.nCopies(columns.size(),"?"))+")")){
            for(int i=0;i<row.size();i++)bind(p,i+1,table.columns.get(i),row.get(i));if(table.keyless()){requireRowId(rowId);p.setString(row.size()+1,rowId);}if(p.executeUpdate()!=1)throw new IllegalStateException("TARGET_INSERT_COUNT_MISMATCH");
        }
    }
    private void requireRowId(String id){if(id==null||!id.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("ROW_IDENTITY_REQUIRED");}
    private void repairProduct(){if(settings.product!=TargetProduct.H2&&settings.product!=TargetProduct.MYSQL&&settings.product!=TargetProduct.POSTGRESQL)throw new IllegalArgumentException("REPAIR_TARGET_PROFILE_UNVERIFIED");}
    private boolean repairStorageExists(Connection c)throws SQLException{
        try(ResultSet tables=c.getMetaData().getTables(mysql()?settings.metadataSchema:c.getCatalog(),
                mysql()?null:settings.metadataSchema,"rm3_sink_repair",new String[]{"TABLE"})){
            while(tables.next())if("rm3_sink_repair".equals(tables.getString("TABLE_NAME")))return true;
            return false;
        }
    }
    /** Explicit preparation creates only receipts, never missing job/table/ownership metadata. */
    public void prepareRepair()throws Exception{
        repairProduct();try(Connection c=connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,600000)){
            c.setAutoCommit(false);locked(c,null);
            ddl(c,"CREATE TABLE IF NOT EXISTS "+metadata("repair")+" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,plan_id VARCHAR(36) NOT NULL,table_id VARCHAR(64) NOT NULL,plan_hash VARCHAR(64) NOT NULL,before_root VARCHAR(64) NOT NULL,after_root VARCHAR(64) NOT NULL,schema_hash VARCHAR(64) NOT NULL,epoch BIGINT NOT NULL,stream_seq BIGINT NOT NULL,transaction_id VARCHAR(512),transaction_hash VARCHAR(64),deleted_rows BIGINT NOT NULL,inserted_rows BIGINT NOT NULL,actor VARCHAR(128) NOT NULL,request_hash VARCHAR(64) NOT NULL,committed_at VARCHAR(40) NOT NULL,PRIMARY KEY(pipeline_id,subscription_id,plan_id,table_id))"+(mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":""));
            locked(c,null);validateRepairStorage(c);guard.verify();c.commit();
        }
    }
    private void validateRepairStorage(Connection c)throws SQLException{
        try(Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT pipeline_id,subscription_id,plan_id,table_id,plan_hash,before_root,after_root,schema_hash,epoch,stream_seq,transaction_id,transaction_hash,deleted_rows,inserted_rows,actor,request_hash,committed_at FROM "+metadata("repair")+" WHERE 1=0")){
            ResultSetMetaData m=rows.getMetaData();if(m.getColumnCount()!=17)throw new IllegalStateException("REPAIR_RECEIPT_SHAPE_INVALID");
            int[] lengths={128,128,36,64,64,64,64,64,0,0,512,64,0,0,128,64,40};for(int i=0;i<lengths.length;i++)if(lengths[i]==0?m.getColumnType(i+1)!=Types.BIGINT:m.getColumnType(i+1)!=Types.VARCHAR||m.getPrecision(i+1)<lengths[i])throw new IllegalStateException("REPAIR_RECEIPT_SHAPE_INVALID");
        }
        Map<Short,String> primary=new TreeMap<>();try(ResultSet keys=c.getMetaData().getPrimaryKeys(mysql()?settings.metadataSchema:c.getCatalog(),mysql()?null:settings.metadataSchema,"rm3_sink_repair")){while(keys.next())primary.put(keys.getShort("KEY_SEQ"),keys.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));}if(!new ArrayList<>(primary.values()).equals(Arrays.asList("pipeline_id","subscription_id","plan_id","table_id")))throw new IllegalStateException("REPAIR_RECEIPT_PRIMARY_KEY_INVALID");
        if(mysql())try(PreparedStatement p=c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rm3_sink_job','rm3_sink_table','rm3_sink_repair')")){p.setString(1,settings.metadataSchema);try(ResultSet r=p.executeQuery()){int count=0;while(r.next()){count++;if(!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new IllegalStateException("REPAIR_RECEIPT_ENGINE_INVALID");}if(count!=3)throw new IllegalStateException("SINK_METADATA_TABLES_MISSING");}}
    }
    public void checkRepairDependencies(TableSchema table,Checkpoint expected)throws Exception{
        repairProduct();try(Connection c=connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,600000)){c.setAutoCommit(false);Checkpoint actual=locked(c,expected.epoch);if(!Codec.encode(actual).equals(Codec.encode(expected)))throw new IllegalStateException("VALIDATION_TARGET_CHECKPOINT_MISMATCH");if(!"COMPLETE".equals(snapshotCheckpoint(c,table).state))throw new IllegalStateException("SINK_SNAPSHOT_INCOMPLETE");RepairDependencies.verify(c,settings.product,destination(table),table.name,table(table));validateShape(c,table);c.rollback();}
    }

    public record UniqueCheck(String tableId, String schema, String table, String sourceIndex, List<String> columns,
                              String state, Long duplicateGroups, Long conflictingRows) {}

    public record UniqueRestoreObservation(String schema, String table, List<String> columns, String ordinaryIndex,
                                           String proposedConstraint, String physicalHash, long rows, String root) {}

    public UniqueRestoreObservation planUniqueRestoration(Checkpoint expected, TableSchema model, String sourceIndex,
                                                          PausedRepairFence fence, RepairProbe probe,
                                                          int rowBytes, long maximumMillis) throws Exception {
        if (settings.product != TargetProduct.POSTGRESQL || expected == null) {
            throw new IllegalArgumentException("UNIQUE_RESTORE_TARGET_PROFILE_UNSUPPORTED");
        }
        IndexPolicy.Decision index = uniqueRestorationIndex(model, sourceIndex);
        try (Connection c = connect(); RepairConnectionGuard guard = new RepairConnectionGuard(c, maximumMillis)) {
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            try (JdbcTargetRead reader = new JdbcTargetRead(c, model, settings.product, validationSql(model),
                    rowBytes, maximumMillis, connection -> {
                        uniqueRestoreSession(connection, maximumMillis);
                        lockUniqueTable(connection, model, "SHARE");
                        verifyUniqueRestoreTarget(connection, expected, model, false);
                    })) {
                UniqueRestoreObservation observation = observeUniqueRestore(c, model, index, scanRepair(reader, probe));
                reader.verify();
                fence.verify();
                guard.verify();
                return observation;
            }
        }
    }

    public record UniqueRestoreExecution(UniqueRestoreOperation.Receipt receipt, boolean replayed) {}

    @FunctionalInterface
    public interface CheckUniqueApproval {
        void verify(UniqueRestoreObservation observed) throws Exception;
    }

    public UniqueRestoreExecution replayUniqueRestoration(TableSchema model, UniqueRestoreOperation.Approval approval,
                                                         long maximumMillis) throws Exception {
        verifyUniqueRestoreScope(model, approval);
        try (Connection c = connect(); RepairConnectionGuard guard = new RepairConnectionGuard(c, maximumMillis)) {
            c.setAutoCommit(false);
            uniqueRestoreSession(c, maximumMillis);
            locked(c, null);
            UniqueRestoreOperation.Receipt receipt = priorUniqueRestore(c, model, approval);
            guard.verify();
            c.rollback();
            return receipt == null ? null : new UniqueRestoreExecution(receipt, true);
        }
    }

    /** Cut, target checkpoint, table DDL and native operation receipt share one PostgreSQL transaction. */
    public UniqueRestoreExecution executeUniqueRestoration(JdbcJournal journal, PipelineValidationBarrier.Certificate cut,
                                                           TableSchema model, UniqueRestoreOperation.Approval approval,
                                                           RepairProbe probe, int rowBytes, long maximumMillis,
                                                           CheckUniqueApproval checkApproval) throws Exception {
        verifyUniqueRestoreScope(model, approval);
        if (!settings.url.equals(journal.settings.url) || !settings.driver.equals(journal.settings.driver)
                || !settings.pipelineId.equals(journal.settings.pipelineId)) {
            throw new IllegalStateException("UNIQUE_RESTORE_SHARED_DATABASE_REQUIRED");
        }
        Checkpoint expected = cut.targetCheckpoints.get(settings.id);
        if (expected == null) throw new IllegalStateException("VALIDATION_TARGET_CHECKPOINT_MISMATCH");
        IndexPolicy.Decision index = uniqueRestorationIndex(model, approval.sourceIndex());
        UniqueRestoreExecution acknowledged = null;
        try (Connection c = connect(); RepairConnectionGuard guard = new RepairConnectionGuard(c, maximumMillis)) {
            // A waiter must see the previous commit after acquiring the journal/target lock pair.
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.setAutoCommit(false);
            uniqueRestoreSession(c, maximumMillis);
            Map<String,Object> job = journal.locked(c);
            locked(c, null);
            UniqueRestoreOperation.Receipt prior = priorUniqueRestore(c, model, approval);
            if (prior != null) {
                guard.verify();
                c.rollback();
                return new UniqueRestoreExecution(prior, true);
            }
            requireUniqueRestoreCut(c, journal, job, cut);
            lockUniqueTable(c, model, "ACCESS EXCLUSIVE");
            verifyUniqueRestoreTarget(c, expected, model, true);
            PostgresUniqueRestoreEvidence.requireSafeDdlSession(c);
            try (JdbcTargetRead reader = new JdbcTargetRead(c, model, settings.product, validationSql(model),
                    rowBytes, maximumMillis, connection -> guard.verify())) {
                UniqueRestoreObservation observed = observeUniqueRestore(c, model, index, scanRepair(reader, probe));
                checkApproval.verify(observed);
                try (Statement sql = c.createStatement()) {
                    List<String> keys = observed.columns.stream().map(this::q).toList();
                    sql.execute("ALTER TABLE ONLY " + table(model) + " ADD CONSTRAINT " + q(observed.proposedConstraint)
                            + " UNIQUE NULLS DISTINCT (" + String.join(",", keys) + ") NOT DEFERRABLE");
                }
                String nativeHash = PostgresUniqueRestoreEvidence.restoredConstraint(c, observed.schema, observed.table,
                        observed.proposedConstraint, observed.columns);
                UniqueRestoreOperation.Receipt receipt = new UniqueRestoreOperation.Receipt(approval, observed.schema,
                        observed.table, observed.columns, observed.proposedConstraint, nativeHash, java.time.Instant.now().toString());
                new PostgresUniqueRestoreLedger(settings.metadataSchema).insert(c, receipt);
                reader.verify();
                guard.verify();
                checkApproval.verify(observed);
                guard.verify();
                try {
                    c.commit();
                    acknowledged = new UniqueRestoreExecution(receipt, false);
                } catch (SQLException uncertain) {
                    throw new IllegalStateException("UNIQUE_RESTORE_COMMIT_UNKNOWN");
                }
            }
        } catch (Exception failure) {
            if (acknowledged == null) throw failure;
        }
        return acknowledged;
    }

    private void requireUniqueRestoreCut(Connection c, JdbcJournal journal, Map<String,Object> job,
                                         PipelineValidationBarrier.Certificate cut) throws Exception {
        PausedRepairFence.requireCut(job, cut);
        if (!settings.fingerprint.equals(cut.fingerprint) || !settings.fingerprint.equals(job.get("fingerprint"))) {
            throw new IllegalStateException("JOURNAL_IDENTITY_MISMATCH");
        }
        Journal.Subscription state = journal.subscription(c, settings.id, true);
        String sequence = Long.toString(cut.sequence);
        Map<String,Object> progress = journal.deliveryProgress(c);
        if (state.acknowledged != cut.sequence || !"RUNNING".equals(state.state)
                || !sequence.equals(progress.get("capturedSequence"))
                || !(progress.get("subscriptions") instanceof Map<?,?> all) || all.size() != 1
                || !(all.get(settings.id) instanceof Map<?,?> sub) || !sequence.equals(sub.get("safeCoveredSequence"))) {
            throw new IllegalStateException("UNIQUE_RESTORE_TARGET_NOT_AT_CUT");
        }
        if (sub.get("firstSkippedSequence") != null || sub.get("firstSourceExclusionAfterSequence") != null) {
            throw new IllegalStateException("UNIQUE_RESTORE_EXCLUSIONS_RECORDED");
        }
    }

    private void verifyUniqueRestoreScope(TableSchema model, UniqueRestoreOperation.Approval approval) {
        if (settings.product != TargetProduct.POSTGRESQL || !settings.pipelineId.equals(approval.pipelineId())
                || !settings.id.equals(approval.targetId()) || !settings.generation.equals(approval.generation())
                || !settings.fingerprint.equals(approval.fingerprint()) || !model.id.equals(approval.tableId())
                || !includes(model)) throw new IllegalStateException("UNIQUE_RESTORE_SCOPE_INVALID");
    }

    private UniqueRestoreOperation.Receipt priorUniqueRestore(Connection c, TableSchema model,
                                                              UniqueRestoreOperation.Approval approval) throws Exception {
        UniqueRestoreOperation.Receipt receipt = new PostgresUniqueRestoreLedger(settings.metadataSchema).find(c, approval);
        if (receipt == null) return null;
        IndexPolicy.Decision index = uniqueRestorationIndex(model, approval.sourceIndex());
        lockUniqueTable(c, model, "ACCESS SHARE");
        if (!destination(model).equals(receipt.schema()) || !model.name.equals(receipt.table())
                || !constraintName(index).equals(receipt.constraint()) || !index.columns.equals(receipt.columns())
                || !"COMPLETE".equals(snapshotCheckpoint(c, model).state)
                || !receipt.nativeSha256().equals(PostgresUniqueRestoreEvidence.restoredConstraint(c,
                        receipt.schema(), receipt.table(), receipt.constraint(), receipt.columns()))) {
            throw new IllegalStateException("UNIQUE_RESTORE_NATIVE_CONSTRAINT_CHANGED");
        }
        return receipt;
    }

    private void uniqueRestoreSession(Connection c, long maximumMillis) throws Exception {
        try (Statement sql = c.createStatement()) {
            sql.execute("SET LOCAL search_path TO pg_catalog");
            sql.execute("SET LOCAL row_security TO off");
            sql.execute("SET LOCAL statement_timeout TO " + maximumMillis);
            sql.execute("SET LOCAL idle_in_transaction_session_timeout TO " + maximumMillis);
            sql.execute("SET LOCAL lock_timeout TO '2s'");
        }
    }

    private void lockUniqueTable(Connection c, TableSchema model, String lock) throws SQLException {
        try (Statement sql = c.createStatement()) {
            sql.execute("LOCK TABLE ONLY " + table(model) + " IN " + lock + " MODE");
        }
    }

    private void verifyUniqueRestoreTarget(Connection c, Checkpoint expected, TableSchema model,
                                           boolean lockOwnership) throws Exception {
        Checkpoint actual = locked(c, expected.epoch);
        if (!Codec.encode(actual).equals(Codec.encode(expected))) {
            throw new IllegalStateException("VALIDATION_TARGET_CHECKPOINT_MISMATCH");
        }
        if (lockOwnership) {
            TargetOwnership.lockAndVerify(c, destination(model), model.name, settings.pipelineId, settings.id, settings.generation);
        }
        if (!"COMPLETE".equals(snapshotCheckpoint(c, model).state)) {
            throw new IllegalStateException("SINK_SNAPSHOT_INCOMPLETE");
        }
        verifyUniqueReviewTable(c, model);
    }

    private UniqueRestoreObservation observeUniqueRestore(Connection c, TableSchema model, IndexPolicy.Decision index,
                                                          RepairProbe.Result rows) throws Exception {
        String schema = destination(model);
        String constraint = constraintName(index);
        String physicalHash = PostgresUniqueRestoreEvidence.observe(c, schema, model, index, constraint);
        if (uniqueConflicts(c, model, index, schema, table(model)).duplicateGroups() != 0) {
            throw new IllegalStateException("UNIQUE_RESTORE_DUPLICATES_FOUND");
        }
        return new UniqueRestoreObservation(schema, model.name, index.columns, index.targetName, constraint,
                physicalHash, rows.rows, rows.root);
    }

    private IndexPolicy.Decision uniqueRestorationIndex(TableSchema model, String sourceIndex) {
        return uniqueReviewPlans(model).stream()
                .filter(candidate -> candidate.sourceName.equals(sourceIndex) && "DOWNGRADE_UNIQUE".equals(candidate.action))
                .findFirst().orElseThrow(() -> new IllegalStateException("UNIQUE_RESTORE_INDEX_UNSUPPORTED"));
    }

    static String constraintName(IndexPolicy.Decision index) {
        return "rm_u_" + index.targetName.substring("rm_".length());
    }

    /** The caller holds the cut's journal fence. The PostgreSQL transaction only reads and takes locks. */
    public List<UniqueCheck> reviewUniqueConstraints(Checkpoint expected, List<TableSchema> tables,
                                                   PausedRepairFence fence, long maximumMillis) throws Exception {
        if (settings.product != TargetProduct.POSTGRESQL || expected == null || maximumMillis < 1000
                || maximumMillis > 120000 || tables.isEmpty() || tables.size() > 100) {
            throw new IllegalArgumentException("UNIQUE_REVIEW_SCOPE_INVALID");
        }
        Map<TableSchema,List<IndexPolicy.Decision>> plans = new LinkedHashMap<>();
        int planned = 0;
        for (TableSchema model : tables) {
            List<IndexPolicy.Decision> candidates = uniqueReviewPlans(model);
            planned += candidates.size();
            if (planned > 128) throw new IllegalStateException("UNIQUE_REVIEW_INDEX_LIMIT");
            if (!candidates.isEmpty()) plans.put(model, candidates);
        }
        List<UniqueCheck> results = new ArrayList<>();
        fence.verify();
        try (Connection c = connect(); RepairConnectionGuard guard = new RepairConnectionGuard(c, maximumMillis)) {
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            try (Statement statement = c.createStatement()) {
                statement.execute("SET LOCAL search_path TO pg_catalog");
                statement.execute("SET LOCAL row_security TO off");
                statement.execute("SET LOCAL lock_timeout TO '2s'");
                statement.execute("SET LOCAL statement_timeout TO " + maximumMillis);
                // Pin physical relations before the first MVCC query, including against non-MVCC-safe TRUNCATE.
                for (TableSchema model : plans.keySet()) {
                    guard.verify();
                    statement.execute("LOCK TABLE ONLY " + table(model) + " IN ACCESS SHARE MODE");
                }
            }
            Checkpoint actual = locked(c, expected.epoch);
            if (!Codec.encode(actual).equals(Codec.encode(expected))) {
                throw new IllegalStateException("VALIDATION_TARGET_CHECKPOINT_MISMATCH");
            }
            for (TableSchema model : tables) {
                guard.verify();
                fence.verify();
                if (!"COMPLETE".equals(snapshotCheckpoint(c, model).state)) {
                    throw new IllegalStateException("SINK_SNAPSHOT_INCOMPLETE");
                }
                List<IndexPolicy.Decision> candidates = plans.get(model);
                if (candidates == null) continue;
                String schema = destination(model);
                String relation = table(model);
                verifyUniqueReviewTable(c, model);
                for (IndexPolicy.Decision index : candidates) {
                    guard.verify();
                    if ("DOWNGRADE_UNIQUE".equals(index.action)) {
                        results.add(uniqueConflicts(c, model, index, schema, relation));
                    } else {
                        results.add(new UniqueCheck(model.id, schema, model.name, index.sourceName, index.columns,
                                "SPECIAL_INDEX_REQUIRES_PLAN", null, null));
                    }
                }
            }
            guard.verify();
            fence.verify();
            c.rollback();
        }
        return results;
    }

    private List<IndexPolicy.Decision> uniqueReviewPlans(TableSchema table) {
        Set<String> uniqueNames = new HashSet<>();
        for (TableSchema.Index index : table.indexes) {
            if (!index.primary && index.unique) uniqueNames.add(index.name);
        }
        return IndexPolicy.plan(table, settings.product, settings.uniquePolicy).stream()
                .filter(index -> uniqueNames.contains(index.sourceName) && !"CREATE".equals(index.action)).toList();
    }

    private void verifyUniqueReviewTable(Connection c, TableSchema model) throws Exception {
        String relationShape = """
                SELECT r.relkind='r' AND r.relpersistence='p' AND NOT r.relrowsecurity
                       AND NOT r.relispartition AND NOT r.relhassubclass
                       AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=r.oid)
                  FROM pg_catalog.pg_class r
                  JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
                 WHERE n.nspname=? AND r.relname=?
                """;
        try (PreparedStatement query = c.prepareStatement(relationShape)) {
            query.setString(1, destination(model));
            query.setString(2, model.name);
            try (ResultSet r = query.executeQuery()) {
                if (!r.next() || !r.getBoolean(1)) throw new IllegalStateException("UNIQUE_REVIEW_RELATION_REQUIRES_PLAN");
            }
        }
        validateShape(c, model);
    }

    private UniqueCheck uniqueConflicts(Connection c, TableSchema model, IndexPolicy.Decision index,
                                        String schema, String relation) throws SQLException {
        List<String> columns = index.columns.stream().map(this::q).toList();
        String nonNull = columns.stream().map(column -> column + " IS NOT NULL").collect(Collectors.joining(" AND "));
        String query = "SELECT COUNT(*),COALESCE(SUM(group_rows),0) FROM (SELECT COUNT(*) AS group_rows FROM "
                + relation + " WHERE " + nonNull + " GROUP BY " + String.join(",", columns)
                + " HAVING COUNT(*)>1) AS duplicates";
        try (Statement statement = c.createStatement(); ResultSet result = statement.executeQuery(query)) {
            result.next();
            long groups = result.getLong(1);
            return new UniqueCheck(model.id, schema, model.name, index.sourceName, index.columns,
                    groups == 0 ? "NO_DUPLICATES_OBSERVED" : "DUPLICATES_FOUND", groups, result.getLong(2));
        }
    }

    public static final class RepairReceipt {
        public String planId,tableId,planHash,beforeRoot,afterRoot,schemaHash,actor,requestHash,committedAt;
        public Checkpoint checkpoint;
        public long deleted,inserted;
        public boolean alreadyApplied;
    }
    private RepairReceipt receipt(Connection c,RepairPlan plan,RepairPlan.Table table)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT plan_hash,before_root,after_root,schema_hash,epoch,stream_seq,transaction_id,transaction_hash,deleted_rows,inserted_rows,actor,request_hash,committed_at FROM "+metadata("repair")+" WHERE pipeline_id=? AND subscription_id=? AND plan_id=? AND table_id=?")){
            p.setString(1,settings.pipelineId);p.setString(2,settings.id);p.setString(3,plan.id);p.setString(4,table.tableId);try(ResultSet r=p.executeQuery()){
                if(!r.next())return null;RepairReceipt receipt=new RepairReceipt();receipt.planId=plan.id;receipt.tableId=table.tableId;receipt.planHash=r.getString(1);receipt.beforeRoot=r.getString(2);receipt.afterRoot=r.getString(3);receipt.schemaHash=r.getString(4);receipt.checkpoint=new Checkpoint();receipt.checkpoint.epoch=r.getLong(5);receipt.checkpoint.sequence=r.getLong(6);receipt.checkpoint.transactionId=r.getString(7);receipt.checkpoint.transactionHash=r.getString(8);receipt.deleted=r.getLong(9);receipt.inserted=r.getLong(10);receipt.actor=r.getString(11);receipt.requestHash=r.getString(12);receipt.committedAt=r.getString(13);receipt.alreadyApplied=true;
                Checkpoint expected=plan.certificate.targetCheckpoints.get(settings.id);
                if(!Objects.equals(receipt.planHash,plan.hash)||!Objects.equals(receipt.beforeRoot,table.beforeRoot)||!Objects.equals(receipt.afterRoot,table.afterRoot)||!Objects.equals(receipt.schemaHash,table.schemaHash)||receipt.deleted!=table.deletes()||receipt.inserted!=table.inserts()||expected==null||!Codec.encode(receipt.checkpoint).equals(Codec.encode(expected)))throw new IllegalStateException("REPAIR_RECEIPT_CONFLICT");return receipt;
            }
        }
    }
    private void repairScope(RepairPlan plan,RepairPlan.Table table){
        repairProduct();plan.validate();
        if(table==null||!settings.id.equals(table.target)||!settings.fingerprint.equals(plan.certificate.fingerprint)||!settings.generation.equals(plan.certificate.generation)||!Objects.equals(Codec.encode(plan.tables.get(settings.id+"/"+table.tableId)),Codec.encode(table)))throw new IllegalArgumentException("REPAIR_SCOPE_INVALID");
    }
    /** Receipt resolution remains valid after resume; it does not assert current live equality. */
    public RepairReceipt repairReceipt(RepairPlan plan,RepairPlan.Table table)throws Exception{
        repairScope(plan,table);
        try(Connection c=connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,plan.maximumMillis)){
            c.setAutoCommit(false);locked(c,null);
            if(!repairStorageExists(c)){c.rollback();return null;}
            validateRepairStorage(c);RepairReceipt result=receipt(c,plan,table);guard.verify();c.rollback();return result;
        }
    }
    private static RepairProbe.Result scanRepair(JdbcTargetRead reader,RepairProbe probe)throws Exception{
        while(true){List<TargetRead.Row> rows=reader.next(128);if(rows.isEmpty())break;for(TargetRead.Row row:rows)probe.append(row);}reader.finishScan();return probe.finish();
    }
    /** Caller holds PausedRepairFence and the private validation-report writer lock. */
    public synchronized RepairReceipt repair(RepairPlan plan,RepairPlan.Table selected,ValidationStore store,Path scratch,String actor,String requestHash,PausedRepairFence fence)throws Exception{
        repairScope(plan,selected);RepairPlans.verifyReport(plan,store);
        if(fence==null||actor==null||actor.isEmpty()||actor.length()>128||requestHash==null||!requestHash.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("REPAIR_APPROVAL_INVALID");
        TableSchema table=store.manifest().schemas.get(selected.tableId);Checkpoint expected=plan.certificate.targetCheckpoints.get(settings.id);
        fence.verify();Connection c=connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,plan.maximumMillis);JdbcTargetRead reader=null;
        try{
            c.setAutoCommit(false);locked(c,null);RepairReceipt prior=receipt(c,plan,selected);if(prior!=null){c.rollback();return prior;}
            reader=new JdbcTargetRead(c,table,settings.product,validationSql(table),plan.maximumRowBytes,plan.maximumMillis,connection->{
                Checkpoint current=locked(connection,expected.epoch);if(!Codec.encode(current).equals(Codec.encode(expected)))throw new IllegalStateException("VALIDATION_TARGET_CHECKPOINT_MISMATCH");
                if(!"COMPLETE".equals(snapshotCheckpoint(connection,table).state))throw new IllegalStateException("SINK_SNAPSHOT_INCOMPLETE");RepairDependencies.verify(connection,settings.product,destination(table),table.name,table(table));validateShape(connection,table);
            });
            try(RepairProbe probe=new RepairProbe(scratch,table,plan.maximumProbeBytes,Math.max(1,Math.max(selected.beforeRows,selected.afterRows)),plan.maximumRowBytes)){
                RepairProbe.Result before=scanRepair(reader,probe);if(before.rows!=selected.beforeRows||!Objects.equals(before.root,selected.beforeRoot))throw new IllegalStateException("REPAIR_TARGET_DRIFTED");
                long deletes=0,inserts=0;
                // Delete all changed/extra identities first so reviewed business UNIQUE swaps can succeed.
                for(int phase=0;phase<2;phase++){
                    String after="";while(true){List<ValidationStore.Difference> page=store.differences(settings.id,table.id,after,100);if(page.isEmpty())break;
                        for(ValidationStore.Difference difference:page){reader.verify();fence.verify();after=difference.identityHash;ValidationStore.RowPair rows=store.differenceValues(settings.id,table.id,difference.identityHash,Math.min(64L*1024*1024,2L*plan.maximumRowBytes));
                            if(phase==0&&rows.actual!=null){Change deletion=new Change();deletion.operation=Change.Operation.DELETE;deletion.before=rows.actual;deletion.rowId=rows.actualRowId;change(c,table,deletion);deletes++;}
                            if(phase==1&&rows.expected!=null){insert(c,table,rows.expected,rows.expectedRowId);inserts++;}
                        }
                    }
                }
                if(deletes!=selected.deletes()||inserts!=selected.inserts())throw new IllegalStateException("REPAIR_DIFF_COUNTS_CHANGED");
                probe.reset();reader.rescan();RepairProbe.Result after=scanRepair(reader,probe);if(after.rows!=selected.afterRows||!Objects.equals(after.root,selected.afterRoot))throw new IllegalStateException("REPAIR_POSTCHECK_FAILED");
                reader.verify();fence.verify();Checkpoint unchanged=locked(c,expected.epoch);if(!Codec.encode(unchanged).equals(Codec.encode(expected)))throw new IllegalStateException("VALIDATION_TARGET_CHECKPOINT_MISMATCH");
                String now=java.time.Instant.now().toString();sql(c,"INSERT INTO "+metadata("repair")+" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",settings.pipelineId,settings.id,plan.id,table.id,plan.hash,selected.beforeRoot,selected.afterRoot,selected.schemaHash,expected.epoch,expected.sequence,expected.transactionId,expected.transactionHash,deletes,inserts,actor,requestHash,now);
                c.commit();RepairReceipt result=receipt(c,plan,selected);result.alreadyApplied=false;c.rollback();return result;
            }
        }finally{guard.close();if(reader!=null)reader.close();else c.close();}
    }
    private void change(Connection c,TableSchema table,Change change)throws Exception{
        if(change.operation==Change.Operation.INSERT||change.operation==Change.Operation.SNAPSHOT){insert(c,table,change.after,change.rowId);return;}
        if(change.operation!=Change.Operation.UPDATE&&change.operation!=Change.Operation.DELETE)throw new IllegalStateException("DDL_PLAN_REQUIRED");
        image(table,change.before);boolean update=change.operation==Change.Operation.UPDATE;if(update)image(table,change.after);
        String where=keys(table).stream().map(k->q(k)+"=?").collect(Collectors.joining(" AND "));
        String query=update?"UPDATE "+table(table)+" SET "+table.columns.stream().map(col->q(col.name)+"=?").collect(Collectors.joining(","))+" WHERE "+where:"DELETE FROM "+table(table)+" WHERE "+where;
        try(PreparedStatement p=c.prepareStatement(query)){
            int n=1;if(update)for(int i=0;i<table.columns.size();i++)bind(p,n++,table.columns.get(i),change.after.get(i));
            if(table.keyless()){requireRowId(change.rowId);p.setString(n,change.rowId);}else for(String key:keys(table)){int i=table.column(key);Cell cell=change.before.get(i);if(cell.value==null)throw new IllegalArgumentException("IDENTITY_IS_NULL");bind(p,n++,table.columns.get(i),cell);}
            if(p.executeUpdate()!=1)throw new IllegalStateException("TARGET_ROW_MISSING_OR_AMBIGUOUS");
        }
    }
    @Override public synchronized void apply(long epoch,long sequence,SourceTransaction tx)throws Exception{
        tx.validate();if(!settings.generation.equals(tx.streamGeneration))throw new IllegalStateException("STREAM_GENERATION_MISMATCH");String hash=Codec.sha256(Codec.encode(tx));
        try(Connection c=connect()){c.setAutoCommit(false);Checkpoint cp=locked(c,epoch);
            if(sequence==cp.sequence&&tx.id.equals(cp.transactionId)&&hash.equals(cp.transactionHash)){if(cp.skipOperation!=null)throw new IllegalStateException("SINK_TRANSACTION_WAS_SKIPPED");c.commit();return;}
            if(sequence!=Math.addExact(cp.sequence,1))throw new IllegalStateException("SINK_SEQUENCE_GAP_OR_REPLAY_CHANGED");
            Set<String> checked=new HashSet<>();
            for(Change change:tx.changes){if(change.operation==Change.Operation.BARRIER)continue;TableSchema table=schemas.get(change.tableId);if(table==null||table.version!=change.schemaVersion)throw new IllegalStateException("SINK_SCHEMA_NOT_PREPARED");
                if(checked.add(table.id)){TargetOwnership.verify(c,destination(table),table.name,settings.pipelineId,settings.id,settings.generation);if(!"COMPLETE".equals(snapshotCheckpoint(c,table).state))throw new IllegalStateException("SINK_SNAPSHOT_INCOMPLETE");}change(c,table,change);}
            sql(c,"UPDATE "+metadata("job")+" SET stream_seq=?,transaction_id=?,transaction_hash=?"+(skipAware?","+q("skip_operation")+"=NULL,"+q("skip_hash")+"=NULL":"")+" WHERE pipeline_id=? AND subscription_id=?",sequence,tx.id,hash,settings.pipelineId,settings.id);c.commit();
        }
    }
    private SnapshotCheckpoint snapshotCheckpoint(Connection c,TableSchema table)throws Exception{
        TargetOwnership.verify(c,destination(table),table.name,settings.pipelineId,settings.id,settings.generation);
        if(!registered(c,table))throw new IllegalStateException("SINK_SCHEMA_NOT_PREPARED");
        try(PreparedStatement p=c.prepareStatement("SELECT snapshot_id,snapshot_state,next_part,row_count FROM "+metadata("table")+" WHERE pipeline_id=? AND subscription_id=? AND table_id=? FOR UPDATE")){
            p.setString(1,settings.pipelineId);p.setString(2,settings.id);p.setString(3,table.id);try(ResultSet rs=p.executeQuery()){rs.next();SnapshotCheckpoint cp=new SnapshotCheckpoint();cp.snapshotId=rs.getString(1);cp.state=rs.getString(2);cp.nextPart=rs.getInt(3);cp.rows=rs.getLong(4);return cp;}
        }
    }
    @Override public synchronized SnapshotCheckpoint beginSnapshot(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception{
        if(!"COMPLETE".equals(snapshot.state)||!table.id.equals(snapshot.tableId)||snapshot.schemaVersion!=table.version)throw new IllegalArgumentException("SNAPSHOT_NOT_COMPLETE");
        try(Connection c=connect()){c.setAutoCommit(false);locked(c,epoch);SnapshotCheckpoint cp=snapshotCheckpoint(c,table);
            if(cp.snapshotId==null){try(Statement st=c.createStatement();ResultSet rs=st.executeQuery("SELECT 1 FROM "+table(table)+" LIMIT 1")){if(rs.next())throw new IllegalStateException("SNAPSHOT_TARGET_NOT_EMPTY");}
                sql(c,"UPDATE "+metadata("table")+" SET snapshot_id=?,snapshot_state='BUILDING' WHERE pipeline_id=? AND subscription_id=? AND table_id=?",snapshot.snapshotId,settings.pipelineId,settings.id,table.id);cp.snapshotId=snapshot.snapshotId;cp.state="BUILDING";
            }else if(!snapshot.snapshotId.equals(cp.snapshotId))throw new IllegalStateException("SNAPSHOT_REBASE_REQUIRES_PLAN");
            c.commit();return cp;}
    }
    @Override public synchronized void snapshotPart(long epoch,TableSchema table,String snapshotId,Journal.SnapshotPart part)throws Exception{
        if(part.part<0||part.rows==null||part.rows.isEmpty())throw new IllegalArgumentException("SNAPSHOT_PART_INVALID");String hash=Codec.sha256(Codec.encode(part.rows));
        try(Connection c=connect()){c.setAutoCommit(false);locked(c,epoch);SnapshotCheckpoint cp=snapshotCheckpoint(c,table);
            if(!snapshotId.equals(cp.snapshotId))throw new IllegalStateException("SNAPSHOT_SESSION_CHANGED");
            if(part.part==cp.nextPart-1){
                try(PreparedStatement p=c.prepareStatement("SELECT last_part_hash FROM "+metadata("table")+" WHERE pipeline_id=? AND subscription_id=? AND table_id=?")){p.setString(1,settings.pipelineId);p.setString(2,settings.id);p.setString(3,table.id);try(ResultSet rs=p.executeQuery()){rs.next();if(!hash.equals(rs.getString(1)))throw new IllegalStateException("SNAPSHOT_PART_CONTENT_CHANGED");}}
                c.commit();return;
            }
            if(part.part!=cp.nextPart||!"BUILDING".equals(cp.state))throw new IllegalStateException("SNAPSHOT_PART_GAP");
            int ordinal=0;for(List<Cell> row:part.rows)insert(c,table,row,Codec.sha256(snapshotId+":"+part.part+":"+ordinal++));
            sql(c,"UPDATE "+metadata("table")+" SET next_part=?,row_count=?,last_part_hash=? WHERE pipeline_id=? AND subscription_id=? AND table_id=?",Math.addExact(cp.nextPart,1),Math.addExact(cp.rows,part.rows.size()),hash,settings.pipelineId,settings.id,table.id);c.commit();
        }
    }
    @Override public synchronized void finishSnapshot(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception{
        try(Connection c=connect()){c.setAutoCommit(false);locked(c,epoch);SnapshotCheckpoint cp=snapshotCheckpoint(c,table);
            if(!"COMPLETE".equals(snapshot.state)||!snapshot.snapshotId.equals(cp.snapshotId)||cp.nextPart!=snapshot.nextPart||cp.rows!=snapshot.rows)throw new IllegalStateException("SNAPSHOT_MANIFEST_MISMATCH");
            sql(c,"UPDATE "+metadata("table")+" SET snapshot_state='COMPLETE' WHERE pipeline_id=? AND subscription_id=? AND table_id=?",settings.pipelineId,settings.id,table.id);c.commit();}
    }
    @Override public Map<String,Object> diagnostics()throws Exception{Map<String,Object> result=new LinkedHashMap<>();result.put("id",id());result.put("consistency",consistency().name());result.put("product",settings.product.name());result.put("checkpoint",checkpoint());result.put("uniquePolicy",settings.uniquePolicy.name());return result;}
    @Override public void close()throws SQLException{if(anchor!=null){anchor.close();anchor=null;}}
}
