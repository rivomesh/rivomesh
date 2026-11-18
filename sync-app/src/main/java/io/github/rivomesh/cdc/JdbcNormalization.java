package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.OracleDate;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

import static io.github.rivomesh.cdc.JdbcJournal.execute;

/** Durable multiset identities. Normalization and its identity changes commit exactly once in source order. */
final class JdbcNormalization {
    private final JdbcJournal journal;
    private final String pipeline;
    JdbcNormalization(JdbcJournal journal){this.journal=journal;pipeline=journal.settings.pipelineId;}
    private String t(String name){return journal.table(name);}
    void initialize(Connection c)throws Exception{
        String text=journal.text();
        try(Statement st=c.createStatement()){
            journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+t("normalized")+" (pipeline_id VARCHAR(128) NOT NULL,stream_seq BIGINT NOT NULL,raw_hash VARCHAR(64) NOT NULL,payload "+text+" NOT NULL,payload_hash VARCHAR(64) NOT NULL,payload_bytes BIGINT NOT NULL,PRIMARY KEY(pipeline_id,stream_seq))");
            journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+t("identity_seed")+" (pipeline_id VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,snapshot_id VARCHAR(36) NOT NULL,next_part INTEGER NOT NULL,run_state VARCHAR(16) NOT NULL,PRIMARY KEY(pipeline_id,table_id))");
            journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+t("identity")+" (pipeline_id VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,row_id VARCHAR(64) NOT NULL,row_hash VARCHAR(64) NOT NULL,payload "+text+" NOT NULL,payload_bytes BIGINT NOT NULL,PRIMARY KEY(pipeline_id,table_id,row_id))");
            boolean mysql=c.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT).contains("mysql");
            String name=mysql?"`rm3_identity_hash`":"\"rm3_identity_hash\"";
            if(mysql){
                boolean found=false;try(ResultSet indexes=c.getMetaData().getIndexInfo(journal.settings.namespace,null,"rm3_identity",false,false)){while(indexes.next())if("rm3_identity_hash".equals(indexes.getString("INDEX_NAME")))found=true;}
                if(!found)try{st.execute("CREATE INDEX "+name+" ON "+t("identity")+" (pipeline_id,table_id,row_hash,row_id)");}catch(SQLException e){if(e.getErrorCode()!=1061)throw e;}
            }else journal.ddl(c,"CREATE INDEX IF NOT EXISTS "+name+" ON "+t("identity")+" (pipeline_id,table_id,row_hash,row_id)");
        }
    }
    static String canonical(TableSchema table,List<Cell> row){
        if(row==null||row.size()!=table.columns.size())throw new IllegalArgumentException("ROW_WIDTH_INVALID");List<Cell> result=new ArrayList<>();
        for(int i=0;i<row.size();i++){
            Cell cell=row.get(i);if(cell==null||!cell.present)throw new IllegalArgumentException("ROW_IMAGE_INCOMPLETE");String value=cell.value;
            if(value!=null)switch(table.columns.get(i).type){
                case INT64:case UINT64:case DECIMAL:value=new BigDecimal(value).stripTrailingZeros().toPlainString();break;
                case FLOAT32:float f=Float.parseFloat(value);if(!Float.isFinite(f))throw new IllegalArgumentException("FLOAT_VALUE_INVALID");value=f==0?"0":Float.toHexString(f);break;
                case FLOAT64:double d=Double.parseDouble(value);if(!Double.isFinite(d))throw new IllegalArgumentException("FLOAT_VALUE_INVALID");value=d==0?"0":Double.toHexString(d);break;
                case JSON:value=JsonValues.canonical(value);break;
                case DATETIME:case TIMESTAMP:
                    value=(OracleDate.isColumn(table.columns.get(i))?OracleDate.localTime(value):Timestamp.valueOf(value).toLocalDateTime()).toString();
                    break;
                case DATE:value=java.sql.Date.valueOf(value).toString();break;
                case TIME:value=io.github.rivomesh.MysqlDuration.canonical(value);break;
                case BOOLEAN:if(!Arrays.asList("true","false","0","1").contains(value))throw new IllegalArgumentException("BOOLEAN_VALUE_INVALID");value=(value.equals("true")||value.equals("1"))?"true":"false";break;
                case BYTES:case GEOMETRY:value=Base64.getEncoder().encodeToString(Base64.getDecoder().decode(value));break;
                default:break;
            }
            result.add(Cell.value(value));
        }
        return Codec.encode(result);
    }
    private long insertIdentity(Connection c,TableSchema table,String rowId,List<Cell> row)throws Exception{
        String payload=canonical(table,row);long bytes=payload.getBytes(StandardCharsets.UTF_8).length;
        execute(c,"INSERT INTO "+t("identity")+" (pipeline_id,table_id,row_id,row_hash,payload,payload_bytes) VALUES (?,?,?,?,?,?)",pipeline,table.id,rowId,Codec.sha256(payload),payload,bytes);return bytes;
    }
    boolean seed(Journal.Lease lease,TableSchema table,Journal.SnapshotState snapshot)throws Exception{
        if(!table.id.equals(snapshot.tableId)||table.version!=snapshot.schemaVersion||!"COMPLETE".equals(snapshot.state))throw new IllegalArgumentException("SNAPSHOT_NOT_COMPLETE");
        if(!table.keyless())return true;
        Journal.SnapshotState manifest=journal.snapshot(snapshot.scope,table.id);
        if(manifest==null||!Codec.encode(snapshot).equals(Codec.encode(manifest)))throw new IllegalStateException("SNAPSHOT_MANIFEST_CHANGED");
        try(Connection c=journal.connect()){
            c.setAutoCommit(false);Map<String,Object> job=journal.fence(c,lease);int next=0;boolean exists=false;
            try(PreparedStatement p=c.prepareStatement("SELECT snapshot_id,next_part,run_state FROM "+t("identity_seed")+" WHERE pipeline_id=? AND table_id=? FOR UPDATE")){
                p.setString(1,pipeline);p.setString(2,table.id);try(ResultSet rs=p.executeQuery()){if(rs.next()){
                    exists=true;if(!snapshot.snapshotId.equals(rs.getString(1)))throw new IllegalStateException("IDENTITY_REBASE_REQUIRES_PLAN");next=rs.getInt(2);if("COMPLETE".equals(rs.getString(3))){c.commit();return true;}
                }}
            }
            if(!exists)execute(c,"INSERT INTO "+t("identity_seed")+" (pipeline_id,table_id,snapshot_id,next_part,run_state) VALUES (?,?,?,0,'BUILDING')",pipeline,table.id,snapshot.snapshotId);
            long bytes=0;
            if(next<snapshot.nextPart){
                Journal.SnapshotPart part=journal.snapshotParts(snapshot.scope,table.id,snapshot.snapshotId,next,1).get(0);int ordinal=0;
                for(List<Cell> row:part.rows)bytes=Math.addExact(bytes,insertIdentity(c,table,Codec.sha256(snapshot.snapshotId+":"+next+":"+ordinal++),row));next++;
            }
            journal.capacity(job,bytes);long total=Math.addExact((long)job.get("identityBytes"),bytes);
            boolean complete=next==snapshot.nextPart;
            execute(c,"UPDATE "+t("identity_seed")+" SET next_part=?,run_state=? WHERE pipeline_id=? AND table_id=?",next,complete?"COMPLETE":"BUILDING",pipeline,table.id);
            execute(c,"UPDATE "+t("job")+" SET identity_bytes=? WHERE pipeline_id=?",total,pipeline);c.commit();return complete;
        }
    }
    private void requireSeed(Connection c,TableSchema table)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT run_state FROM "+t("identity_seed")+" WHERE pipeline_id=? AND table_id=?")){
            p.setString(1,pipeline);p.setString(2,table.id);try(ResultSet rs=p.executeQuery()){if(!rs.next()||!"COMPLETE".equals(rs.getString(1)))throw new IllegalStateException("ROW_IDENTITIES_NOT_SEEDED");}
        }
    }
    private long identify(Connection c,TableSchema table,Change change)throws Exception{
        if(change.operation==Change.Operation.INSERT){change.rowId=Codec.sha256(change.eventId);return insertIdentity(c,table,change.rowId,change.after);}
        if(change.operation!=Change.Operation.UPDATE&&change.operation!=Change.Operation.DELETE)throw new IllegalStateException("DDL_PLAN_REQUIRED");
        String before=canonical(table,change.before);long oldBytes;
        try(PreparedStatement p=c.prepareStatement("SELECT row_id,payload,payload_bytes FROM "+t("identity")+" WHERE pipeline_id=? AND table_id=? AND row_hash=? ORDER BY row_id LIMIT 1 FOR UPDATE")){
            p.setString(1,pipeline);p.setString(2,table.id);p.setString(3,Codec.sha256(before));try(ResultSet rs=p.executeQuery()){
                if(!rs.next())throw new IllegalStateException("BEFORE_IMAGE_OCCURRENCE_MISSING");change.rowId=rs.getString(1);if(!before.equals(rs.getString(2)))throw new IllegalStateException("ROW_HASH_COLLISION");oldBytes=rs.getLong(3);
            }
        }
        execute(c,"DELETE FROM "+t("identity")+" WHERE pipeline_id=? AND table_id=? AND row_id=?",pipeline,table.id,change.rowId);
        return (change.operation==Change.Operation.UPDATE?insertIdentity(c,table,change.rowId,change.after):0)-oldBytes;
    }
    SourceTransaction normalize(Journal.Lease lease,Journal.Record record,Map<String,TableSchema> schemas,Map<String,SourcePosition> baselines,NormalizedJournal.BaselineFilter filter)throws Exception{
        String raw=Codec.encode(record.transaction),rawHash=Codec.sha256(raw);
        try(Connection c=journal.connect()){
            c.setAutoCommit(false);Map<String,Object> job=journal.fence(c,lease);
            long rawBytes=raw.getBytes(StandardCharsets.UTF_8).length,reserved;
            try(PreparedStatement p=c.prepareStatement("SELECT payload_hash,payload_bytes FROM "+t("log")+" WHERE pipeline_id=? AND stream_seq=?")){
                p.setString(1,pipeline);p.setLong(2,record.sequence);try(ResultSet rs=p.executeQuery()){if(!rs.next()||!rawHash.equals(rs.getString(1)))throw new IllegalStateException("RAW_TRANSACTION_CHANGED_OR_COLLECTED");reserved=rs.getLong(2)-rawBytes;if(reserved<0)throw new IllegalStateException("RAW_TRANSACTION_SIZE_INVALID");}
            }
            try(PreparedStatement p=c.prepareStatement("SELECT raw_hash,payload,payload_hash FROM "+t("normalized")+" WHERE pipeline_id=? AND stream_seq=?")){
                p.setString(1,pipeline);p.setLong(2,record.sequence);try(ResultSet rs=p.executeQuery()){if(rs.next()){
                    String payload=rs.getString(2);if(!rawHash.equals(rs.getString(1))||!Codec.sha256(payload).equals(rs.getString(3)))throw new IllegalStateException("NORMALIZED_TRANSACTION_CHECKSUM_MISMATCH");
                    SourceTransaction existing=Codec.decode(payload,SourceTransaction.class);existing.validate();c.commit();return existing;
                }}
            }
            if(record.sequence!=Math.addExact((long)job.get("normalized"),1))throw new IllegalStateException("NORMALIZATION_SEQUENCE_GAP");
            SourceTransaction normalized=Codec.decode(raw,SourceTransaction.class);List<Change> changes=new ArrayList<>();long delta=0;Set<String> seeds=new HashSet<>();
            for(Change change:normalized.changes){
                if(change.operation==Change.Operation.BARRIER){changes.add(change);continue;}
                TableSchema table=schemas.get(change.tableId);if(table==null)throw new IllegalStateException("HISTORICAL_SCHEMA_MISSING");
                SourcePosition baseline=baselines.get(table.id);if(baseline==null)throw new IllegalStateException("SNAPSHOT_BASELINE_MISSING");
                if(filter.includes(baseline,normalized.id))continue;
                if(table.version!=change.schemaVersion)throw new IllegalStateException("HISTORICAL_SCHEMA_VERSION_MISSING");
                if(change.operation==Change.Operation.DDL||change.operation==Change.Operation.SNAPSHOT)throw new IllegalStateException("DDL_OR_SNAPSHOT_PLAN_REQUIRED");
                if(table.keyless()){if(seeds.add(table.id))requireSeed(c,table);delta=Math.addExact(delta,identify(c,table,change));}
                changes.add(change);
            }
            normalized.changes=changes;normalized.validate();String payload=Codec.encode(normalized);long bytes=payload.getBytes(StandardCharsets.UTF_8).length;
            journal.capacity(job,Math.subtractExact(Math.addExact(delta,bytes),reserved));
            execute(c,"INSERT INTO "+t("normalized")+" (pipeline_id,stream_seq,raw_hash,payload,payload_hash,payload_bytes) VALUES (?,?,?,?,?,?)",pipeline,record.sequence,rawHash,payload,Codec.sha256(payload),bytes);
            execute(c,"UPDATE "+t("log")+" SET payload_bytes=? WHERE pipeline_id=? AND stream_seq=?",rawBytes,pipeline,record.sequence);
            execute(c,"UPDATE "+t("job")+" SET normalized_seq=?,identity_bytes=?,pending_bytes=? WHERE pipeline_id=?",record.sequence,Math.addExact((long)job.get("identityBytes"),delta),Math.addExact(Math.subtractExact((long)job.get("bytes"),reserved),bytes),pipeline);c.commit();return normalized;
        }
    }
}
