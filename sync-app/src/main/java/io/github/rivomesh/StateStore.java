package io.github.rivomesh;

import io.github.rivomesh.Model.*;
import com.fasterxml.jackson.core.type.TypeReference;
import java.sql.*;
import java.util.*;

/** Progress, data and ownership checks share a target transaction. Each session owns an immutable token. */
public final class StateStore {
    public static class JobBusyException extends IllegalStateException {
        JobBusyException() { super("任务由其他节点运行，等待租约释放或过期"); }
    }
    public static final class JobPausedException extends JobBusyException {}
    public static final class LeaseLostException extends IllegalStateException {
        LeaseLostException() { super("任务租约已丢失，旧会话停止写入"); }
    }
    private final Config config;
    private final String owner = UUID.randomUUID().toString();
    public StateStore(Config config) throws ClassNotFoundException {
        this.config = config; Class.forName(config.target.effectiveDriver());
    }
    public Connection connect() throws SQLException {
        Connection c = JdbcConnections.open(config.target.effectiveDriver(),config.target.url,config.target.username,config.target.password);
        try {
            if("POSTGRESQL".equals(config.target.type)&&!c.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT).contains("postgresql"))throw new SQLException("TARGET_PRODUCT_MISMATCH","0A000");
        } catch (SQLException e) { c.close(); throw e; }
        return c;
    }
    String name(String table) { return Sql.pg(config.target.metadataSchema) + "." + Sql.pg(table + "_v2"); }
    private void ddl(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
        catch (SQLException e) {
            // IF NOT EXISTS can race on PostgreSQL/openGauss catalogs during simultaneous cold starts.
            if (!Arrays.asList("42P06", "42P07", "42710", "23505").contains(e.getSQLState())) throw e;
        }
    }
    public void initialize() throws SQLException {
        try (Connection c = connect()) {
            ddl(c, "CREATE SCHEMA IF NOT EXISTS " + Sql.pg(config.target.metadataSchema));
            ddl(c, "CREATE SCHEMA IF NOT EXISTS " + Sql.pg(config.target.schema));
            try (PreparedStatement p = c.prepareStatement("SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name='sync_job'")) {
                p.setString(1, config.target.metadataSchema);
                try (ResultSet rs = p.executeQuery()) {
                    if (rs.next()) try (PreparedStatement legacy = c.prepareStatement("SELECT 1 FROM " + Sql.pg(config.target.metadataSchema) + ".sync_job WHERE job_id=?")) {
                        legacy.setString(1, config.jobId);
                        try (ResultSet old = legacy.executeQuery()) {
                            if (old.next()) throw new IllegalStateException("旧版 file/position 任务不能直接转换为 GTID；请保留旧状态并为新任务使用空目标 schema");
                        }
                    }
                }
            }
            ddl(c, "CREATE TABLE IF NOT EXISTS " + name("sync_job") + " (job_id varchar(128) PRIMARY KEY,format_version integer NOT NULL,"
                    + "fingerprint varchar(64) NOT NULL,source_uuid varchar(128) NOT NULL,source_address text,phase varchar(20) NOT NULL,"
                    + "run_state varchar(30) NOT NULL DEFAULT 'READY',capture_file text NOT NULL,capture_offset bigint NOT NULL,"
                    + "capture_gtid text NOT NULL,required_gtid text NOT NULL,applied_sequence bigint NOT NULL DEFAULT 0,catchup_sequence bigint,"
                    + "owner varchar(36),node_id varchar(128),lease_until timestamp,updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                    + "last_error text,error_code varchar(64),error_stage varchar(32),error_sqlstate varchar(16),error_vendor integer,"
                    + "error_table text,error_sequence bigint,error_gtid text,error_count bigint NOT NULL DEFAULT 0)");
            ddl(c, "CREATE TABLE IF NOT EXISTS " + name("sync_table") + " (job_id varchar(128) NOT NULL,table_name varchar(63) NOT NULL,progress text NOT NULL,PRIMARY KEY(job_id,table_name))");
            ddl(c, "CREATE TABLE IF NOT EXISTS " + name("sync_transaction") + " (sequence bigserial PRIMARY KEY,job_id varchar(128) NOT NULL,"
                    + "transaction_key varchar(512) NOT NULL,source_uuid varchar(128) NOT NULL,binlog_file text NOT NULL,binlog_offset bigint NOT NULL,"
                    + "gtid text NOT NULL,payload text NOT NULL,UNIQUE(job_id,transaction_key))");
            ddl(c, "CREATE INDEX IF NOT EXISTS sync_transaction_v2_job_sequence ON " + name("sync_transaction") + " (job_id,sequence)");
            ddl(c, "CREATE TABLE IF NOT EXISTS " + name("sync_error") + " (id bigserial PRIMARY KEY,job_id varchar(128) NOT NULL,node_id varchar(128),"
                    + "created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,code varchar(64),stage varchar(32),sqlstate varchar(16),vendor_code integer,"
                    + "table_name text,transaction_sequence bigint,gtid text,message text,retryable boolean NOT NULL)");
        }
    }
    public boolean exists() throws SQLException {
        try (Connection c = connect(); PreparedStatement p = c.prepareStatement("SELECT 1 FROM " + name("sync_job") + " WHERE job_id=?")) {
            p.setString(1, config.jobId); try (ResultSet rs = p.executeQuery()) { return rs.next(); }
        }
    }
    public void create(String identity, Position position, List<TableProgress> tables) throws Exception {
        create(identity, position, position.gtidSet, tables);
    }
    public void create(String identity, Position position, String required, List<TableProgress> tables) throws Exception {
        try (Connection c = connect()) {
            c.setAutoCommit(false);
            try {
                Sql.execute(c, "INSERT INTO " + name("sync_job") + " (job_id,format_version,fingerprint,source_uuid,phase,capture_file,capture_offset,capture_gtid,required_gtid) VALUES (?,2,?,?,'SNAPSHOT',?,?,?,?)",
                        config.jobId, config.fingerprint(), identity, position.file, position.offset, position.gtidSet, required);
            } catch (SQLException e) {
                c.rollback(); if ("23505".equals(e.getSQLState())) return; throw e;
            }
            for (TableProgress table : tables) Sql.execute(c, "INSERT INTO " + name("sync_table") + " (job_id,table_name,progress) VALUES (?,?,?)",
                    config.jobId, table.table.name, Model.JSON.writeValueAsString(table));
            c.commit();
        }
    }
    public void validateConfig() throws Exception {
        try (Connection c = connect(); PreparedStatement p = c.prepareStatement("SELECT format_version,fingerprint FROM " + name("sync_job") + " WHERE job_id=?")) {
            p.setString(1, config.jobId);
            try (ResultSet rs = p.executeQuery()) {
                if (!rs.next() || rs.getInt(1) != 2 || !config.fingerprint().equals(rs.getString(2)))
                    throw new IllegalStateException("任务配置或状态版本不匹配；源 clusterId、database、目标及读取选项必须保持一致");
            }
        }
    }
    public void validateIdentity(String identity, List<TableProgress> current) throws Exception {
        validateConfig(); // Physical source UUID is allowed to change after GTID validation.
        Map<String, String> expected = new TreeMap<>(), actual = new TreeMap<>();
        for (TableProgress table : tables()) expected.put(table.table.name, Model.JSON.writeValueAsString(table.table));
        for (TableProgress table : current) actual.put(table.table.name, Model.JSON.writeValueAsString(table.table));
        if (!expected.equals(actual)) throw new IllegalStateException("源表结构变化，任务暂停，不能跳过结构变更继续同步");
    }
    public void claim() throws SQLException {
        try (Connection c = connect()) {
            int count = Sql.execute(c, "UPDATE " + name("sync_job") + " SET owner=?,node_id=?,lease_until=CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'),"
                    + "updated_at=CURRENT_TIMESTAMP WHERE job_id=? AND run_state<>'PAUSED' AND (owner IS NULL OR lease_until<CURRENT_TIMESTAMP OR owner=?)",
                    owner, config.nodeId, config.leaseMillis, config.jobId, owner);
            if (count != 1) {
                if ("PAUSED".equals(runState())) throw new JobPausedException();
                throw new JobBusyException();
            }
        }
    }
    private void fence(Connection c) throws SQLException {
        if (Sql.execute(c, "UPDATE " + name("sync_job") + " SET lease_until=CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'),updated_at=CURRENT_TIMESTAMP"
                + " WHERE job_id=? AND owner=? AND lease_until>CURRENT_TIMESTAMP AND run_state<>'PAUSED'", config.leaseMillis, config.jobId, owner) != 1)
            throw new LeaseLostException();
    }
    public void heartbeat() throws SQLException { try (Connection c = connect()) { fence(c); } }
    public void release() throws SQLException {
        try (Connection c = connect()) { Sql.execute(c, "UPDATE " + name("sync_job") + " SET owner=NULL,node_id=NULL,lease_until=NULL WHERE job_id=? AND owner=?", config.jobId, owner); }
    }
    public void activate(String identity, String address) throws SQLException {
        try (Connection c = connect()) {
            c.setAutoCommit(false); fence(c);
            Sql.execute(c, "UPDATE " + name("sync_job") + " SET source_uuid=?,source_address=?,run_state='RUNNING' WHERE job_id=?", identity, address, config.jobId); c.commit();
        }
    }
    public void recordError(Throwable error) throws SQLException { recordFailure(error, "UNKNOWN"); }
    public void recordFailure(Throwable error, String stage) throws SQLException {
        SyncFailure detail = error instanceof SyncFailure ? (SyncFailure) error : null;
        SQLException sql = Sql.sqlCause(error);
        boolean retry = Sql.retryable(error);
        String code = detail != null ? detail.code : sql != null ? "SQL_ERROR" : "SYNC_ERROR";
        if (detail != null) stage = detail.stage;
        String message = detail != null ? detail.getMessage() : sql != null ? "数据库操作失败，SQLState=" + sql.getSQLState() + "，vendor=" + sql.getErrorCode() : error.getMessage();
        String state = retry ? code.startsWith("SOURCE_") ? "WAITING_SOURCE" : "RETRYING" : "PAUSED";
        try (Connection c = connect()) {
            c.setAutoCommit(false); fence(c);
            Sql.execute(c, "INSERT INTO " + name("sync_error") + " (job_id,node_id,code,stage,sqlstate,vendor_code,table_name,transaction_sequence,gtid,message,retryable) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    config.jobId, config.nodeId, code, stage, sql == null ? null : sql.getSQLState(), sql == null ? 0 : sql.getErrorCode(),
                    detail == null ? null : detail.table, detail == null ? 0 : detail.sequence, detail == null ? null : detail.gtid, message, retry);
            Sql.execute(c, "UPDATE " + name("sync_job") + " SET run_state=?,last_error=?,error_code=?,error_stage=?,error_sqlstate=?,error_vendor=?,error_table=?,"
                    + "error_sequence=?,error_gtid=?,error_count=error_count+1,owner=NULL,node_id=NULL,lease_until=NULL WHERE job_id=? AND owner=?",
                    state, message, code, stage, sql == null ? null : sql.getSQLState(), sql == null ? 0 : sql.getErrorCode(), detail == null ? null : detail.table,
                    detail == null ? 0 : detail.sequence, detail == null ? null : detail.gtid, config.jobId, owner);
            c.commit();
        }
    }
    public void resume() throws Exception {
        Map<String,Object> current = status();
        if (!"PAUSED".equals(current.get("run_state"))) throw new IllegalStateException("仅 PAUSED 任务可以恢复");
        resume(((Number) current.get("error_count")).longValue());
    }
    /** The error generation makes an uncertain/replayed UI request unable to resume a newer failure. */
    public void resume(long expectedErrorCount) throws Exception {
        validateConfig();
        try (Connection c = connect()) {
            c.setAutoCommit(false);
            try (PreparedStatement p = c.prepareStatement("SELECT run_state,error_count FROM " + name("sync_job") + " WHERE job_id=? FOR UPDATE")) {
                p.setString(1, config.jobId);
                try (ResultSet rs = p.executeQuery()) {
                    if (!rs.next() || rs.getLong(2) != expectedErrorCount) throw new IllegalStateException("ERROR_GENERATION_CHANGED");
                    if (!"PAUSED".equals(rs.getString(1))) { c.commit(); return; }
                }
            }
            Sql.execute(c, "UPDATE " + name("sync_job") + " SET run_state='READY',last_error=NULL,error_code=NULL,error_stage=NULL,owner=NULL,node_id=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE job_id=?", config.jobId);
            c.commit();
        }
    }
    /** Administrative pause takes the same row lock as business writes, fencing every service node. */
    public void pause() throws Exception {
        validateConfig();
        try (Connection c = connect()) {
            c.setAutoCommit(false);
            try (PreparedStatement p = c.prepareStatement("SELECT run_state FROM " + name("sync_job") + " WHERE job_id=? FOR UPDATE")) {
                p.setString(1, config.jobId);
                try (ResultSet rs = p.executeQuery()) {
                    if (!rs.next()) throw new IllegalStateException("任务不存在");
                    if ("PAUSED".equals(rs.getString(1))) { c.commit(); return; }
                }
            }
            Sql.execute(c, "INSERT INTO " + name("sync_error") + " (job_id,node_id,code,stage,message,retryable) VALUES (?,?,'OPERATOR_PAUSED','CONTROL','任务由操作员暂停',false)", config.jobId, config.nodeId);
            Sql.execute(c, "UPDATE " + name("sync_job") + " SET run_state='PAUSED',last_error='任务由操作员暂停',error_code='OPERATOR_PAUSED',error_stage='CONTROL',error_count=error_count+1,owner=NULL,node_id=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE job_id=?", config.jobId);
            c.commit();
        }
    }
    public String requiredGtids() throws SQLException { return textField("required_gtid"); }
    public String phase() throws SQLException { return textField("phase"); }
    public String runState() throws SQLException { return textField("run_state"); }
    private String textField(String field) throws SQLException {
        try (Connection c = connect(); PreparedStatement p = c.prepareStatement("SELECT " + field + " FROM " + name("sync_job") + " WHERE job_id=?")) {
            p.setString(1, config.jobId); try (ResultSet rs = p.executeQuery()) { if (!rs.next()) throw new IllegalStateException("任务不存在"); return rs.getString(1); }
        }
    }
    public Position captured() throws SQLException {
        try (Connection c = connect(); PreparedStatement p = c.prepareStatement("SELECT capture_file,capture_offset,capture_gtid,source_uuid,required_gtid FROM " + name("sync_job") + " WHERE job_id=?")) {
            p.setString(1, config.jobId);
            try (ResultSet rs = p.executeQuery()) {
                if (!rs.next()) throw new IllegalStateException("任务不存在");
                Position pos = new Position(rs.getString(1), rs.getLong(2)); pos.gtidSet = rs.getString(3); pos.sourceUuid = rs.getString(4); pos.requiredGtidSet = rs.getString(5); return pos;
            }
        }
    }
    public List<TableProgress> tables() throws Exception {
        return tables(0);
    }
    private List<TableProgress> tables(int limit) throws Exception {
        try (Connection c = connect(); PreparedStatement p = c.prepareStatement("SELECT progress FROM " + name("sync_table") + " WHERE job_id=? ORDER BY table_name" + (limit>0?" LIMIT ?":""))) {
            p.setString(1, config.jobId);
            if(limit>0)p.setInt(2,limit);
            try (ResultSet rs = p.executeQuery()) {
                List<TableProgress> result = new ArrayList<>(); while (rs.next()) result.add(Model.JSON.readValue(rs.getString(1), TableProgress.class)); return result;
            }
        }
    }
    private void saveTable(Connection c, TableProgress table) throws Exception {
        if (Sql.execute(c, "UPDATE " + name("sync_table") + " SET progress=? WHERE job_id=? AND table_name=?", Model.JSON.writeValueAsString(table), config.jobId, table.table.name) != 1)
            throw new IllegalStateException("表进度不存在");
    }
    public void prepare(TableProgress progress) throws Exception {
        try (Connection c = connect()) {
            c.setAutoCommit(false); fence(c);
            if (!progress.prepared) {
                try (Statement s = c.createStatement()) { s.execute(progress.table.createSql(config.target.schema)); }
                try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1 FROM " + progress.table.targetName(config.target.schema) + " LIMIT 1")) {
                    if (rs.next()) throw new IllegalStateException("首次导入要求目标表为空: " + progress.table.name);
                }
            }
            TargetOwnership.initialize(c,config.target.schema);
            TargetOwnership.claim(c,config.target.schema,progress.table.name,config.jobId,"legacy-v2",config.fingerprint());
            TargetWriter.validate(c, progress.table, config.target.schema);
            TableProgress next = Model.JSON.readValue(Model.JSON.writeValueAsString(progress), TableProgress.class); next.prepared = true;
            saveTable(c, next); c.commit(); progress.prepared = true;
        }
    }
    public void commitChunk(TableProgress progress, List<List<String>> rows) throws Exception { commitChunk(progress, rows, ""); }
    public void commitChunk(TableProgress progress, List<List<String>> rows, String readWatermark) throws Exception {
        TableProgress next = Model.JSON.readValue(Model.JSON.writeValueAsString(progress), TableProgress.class);
        try (Connection c = connect()) {
            c.setAutoCommit(false); fence(c);
            try (TargetWriter writer = new TargetWriter(c, config.target.schema)) { for (List<String> row : rows) writer.write(progress.table, row, false); }
            if (!rows.isEmpty()) next.lastKey = progress.table.keyOf(rows.get(rows.size() - 1));
            next.copiedRows += rows.size(); next.done = rows.size() < config.chunkSize;
            String required;
            try (PreparedStatement p = c.prepareStatement("SELECT required_gtid FROM " + name("sync_job") + " WHERE job_id=?")) {
                p.setString(1, config.jobId); try (ResultSet rs = p.executeQuery()) { rs.next(); required = rs.getString(1); }
            }
            Sql.execute(c, "UPDATE " + name("sync_job") + " SET required_gtid=? WHERE job_id=?", Gtids.union(required, readWatermark), config.jobId);
            saveTable(c, next); c.commit();
        }
        progress.lastKey = next.lastKey; progress.copiedRows = next.copiedRows; progress.done = next.done;
    }
    public void append(Transaction transaction) throws Exception {
        String key = transaction.gtid.isEmpty() ? transaction.position.sourceUuid + ":" + transaction.position.file + ":" + transaction.position.offset : transaction.gtid;
        try (Connection c = connect()) {
            c.setAutoCommit(false); fence(c);
            String captured, required;
            try (PreparedStatement p = c.prepareStatement("SELECT capture_gtid,required_gtid FROM " + name("sync_job") + " WHERE job_id=?")) {
                p.setString(1, config.jobId); try (ResultSet rs = p.executeQuery()) { rs.next(); captured = rs.getString(1); required = rs.getString(2); }
            }
            if (!transaction.gtid.isEmpty() && Gtids.contains(captured, transaction.gtid)) { c.commit(); return; }
            if (!transaction.changes.isEmpty()) Sql.execute(c, "INSERT INTO " + name("sync_transaction") + " (job_id,transaction_key,source_uuid,binlog_file,binlog_offset,gtid,payload) VALUES (?,?,?,?,?,?,?)",
                    config.jobId, key, transaction.position.sourceUuid, transaction.position.file, transaction.position.offset, transaction.gtid, Model.JSON.writeValueAsString(transaction.changes));
            String latest = Gtids.union(captured, transaction.position.gtidSet);
            Sql.execute(c, "UPDATE " + name("sync_job") + " SET capture_file=?,capture_offset=?,capture_gtid=?,required_gtid=? WHERE job_id=?",
                    transaction.position.file, transaction.position.offset, latest, Gtids.union(required, latest), config.jobId);
            c.commit();
        }
    }
    public void beginCatchup() throws SQLException {
        try (Connection c = connect()) {
            c.setAutoCommit(false); fence(c);
            Sql.execute(c, "UPDATE " + name("sync_job") + " SET phase='CATCHUP',catchup_sequence=(SELECT COALESCE(MAX(sequence),0) FROM " + name("sync_transaction") + " WHERE job_id=?) WHERE job_id=? AND phase='SNAPSHOT'", config.jobId, config.jobId);
            c.commit();
        }
    }
    public boolean applyNext(Map<String, Table> tables) throws Exception {
        long sequence = 0; String gtid = "", tableName = null;
        try (Connection c = connect()) {
            c.setAutoCommit(false); fence(c);
            List<Change> changes;
            try (PreparedStatement p = c.prepareStatement("SELECT sequence,gtid,payload FROM " + name("sync_transaction") + " WHERE job_id=? ORDER BY sequence LIMIT 1")) {
                p.setString(1, config.jobId);
                try (ResultSet rs = p.executeQuery()) {
                    if (!rs.next()) {
                        Sql.execute(c, "UPDATE " + name("sync_job") + " SET phase='STREAMING' WHERE job_id=? AND phase='CATCHUP'", config.jobId); c.commit(); return false;
                    }
                    sequence = rs.getLong(1); gtid = rs.getString(2); changes = Model.JSON.readValue(rs.getString(3), new TypeReference<List<Change>>() {});
                }
            }
            try (TargetWriter writer = new TargetWriter(c, config.target.schema)) {
                for (Change change : changes) {
                    tableName = change.table; Table table = tables.get(tableName);
                    if (table == null) throw new IllegalStateException("未知源表: " + tableName);
                    writer.write(table, change.row, change.delete);
                }
            }
            Sql.execute(c, "UPDATE " + name("sync_job") + " SET applied_sequence=?,phase=CASE WHEN phase='CATCHUP' AND catchup_sequence<=? THEN 'STREAMING' ELSE phase END WHERE job_id=?", sequence, sequence, config.jobId);
            Sql.execute(c, "DELETE FROM " + name("sync_transaction") + " WHERE job_id=? AND sequence=?", config.jobId, sequence); c.commit(); return true;
        } catch (SQLException e) {
            throw new SyncFailure("SQL_ERROR", "APPLY", Sql.retryable(e), "增量事务写入失败，SQLState=" + e.getSQLState(), e, tableName, sequence, gtid);
        }
    }
    public Map<String, Object> status() throws Exception {
        return status(0);
    }
    public Map<String, Object> status(int tableLimit) throws Exception {
        if(tableLimit<0||tableLimit>1000)throw new IllegalArgumentException("STATUS_LIMIT_INVALID");
        Map<String, Object> result = new LinkedHashMap<>();
        try (Connection c = connect(); PreparedStatement p = c.prepareStatement("SELECT job_id,phase,run_state,node_id,owner,lease_until,source_uuid,source_address,capture_file,capture_offset,capture_gtid,required_gtid,"
                + "applied_sequence,updated_at,last_error,error_code,error_stage,error_sqlstate,error_vendor,error_table,error_sequence,error_gtid,error_count,"
                + "(SELECT COUNT(*) FROM " + name("sync_transaction") + " WHERE job_id=?) AS pending_transactions,"
                + "(SELECT COUNT(*) FROM " + name("sync_table") + " WHERE job_id=?) AS table_count FROM " + name("sync_job") + " WHERE job_id=?")) {
            p.setString(1, config.jobId); p.setString(2, config.jobId);p.setString(3,config.jobId);
            try (ResultSet rs = p.executeQuery()) {
                if (!rs.next()) throw new IllegalStateException("任务不存在");
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) result.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
            }
        }
        List<Map<String, Object>> progress = new ArrayList<>();
        for (TableProgress t : tables(tableLimit)) {
            Map<String, Object> row = new LinkedHashMap<>(); row.put("table", t.table.name); row.put("copiedRows", t.copiedRows); row.put("done", t.done); row.put("lastKey", t.lastKey); progress.add(row);
        }
        result.put("tables", progress); return result;
    }
}
