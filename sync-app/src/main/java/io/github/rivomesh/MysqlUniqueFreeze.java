package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetRead;
import io.github.rivomesh.repair.RepairProbe;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** One pinned read on a MySQL server already frozen by the operator. */
final class MysqlUniqueFreeze implements AutoCloseable {
    private final MysqlSource source;
    private final Connection connection;
    private final SourcePosition cut;
    private final String marker = UUID.randomUUID().toString();
    private final ScheduledExecutorService watchdog;
    private final long deadline;

    MysqlUniqueFreeze(PipelineConfig config, SourcePosition cut, long maximumMillis) throws Exception {
        this(config, cut, maximumMillis, false);
    }

    static MysqlUniqueFreeze current(PipelineConfig config, long maximumMillis) throws Exception {
        return new MysqlUniqueFreeze(config, null, maximumMillis, true);
    }

    private MysqlUniqueFreeze(PipelineConfig config, SourcePosition cut, long maximumMillis, boolean observeCurrent) throws Exception {
        if (!"MYSQL".equals(config.effectiveSourceType()) || !"AUTO".equals(config.source.topology)
                || !config.source.nodes.isEmpty() || !observeCurrent && (cut == null || cut.getFormatVersion() != 1
                || !"MYSQL".equals(cut.getSourceType()) || !config.source.clusterId.equals(cut.getSourceId()))) {
            throw new IllegalArgumentException("UNIQUE_RESTORE_SOURCE_PROFILE_UNSUPPORTED");
        }
        source = new MysqlSource(config.legacy());
        connection = source.connect();
        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maximumMillis);
        watchdog = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "rivomesh-unique-restore-source-deadline");
            thread.setDaemon(true);
            return thread;
        });
        watchdog.schedule(this::abort, maximumMillis, TimeUnit.MILLISECONDS);
        try {
            try (PreparedStatement mark = connection.prepareStatement("SET @rivomesh_unique_freeze_session=?")) {
                mark.setQueryTimeout(3);
                mark.setString(1, marker);
                mark.execute();
            }
            this.cut = observeCurrent ? currentPosition(config.source.clusterId) : cut;
            verify();
            try (Statement gate = connection.createStatement()) {
                gate.setQueryTimeout(5);
                gate.execute("FLUSH TABLES WITH READ LOCK");
            }
            verify();
        } catch (Exception failure) {
            try {
                close();
            } catch (Exception close) {
                failure.addSuppressed(close);
            }
            throw failure;
        }
    }

    private SourcePosition currentPosition(String sourceId) throws SQLException {
        Model.Position position = source.position(connection);
        String serverUuid;
        try (Statement query = connection.createStatement()) {
            query.setQueryTimeout(3);
            try (ResultSet row = query.executeQuery("SELECT @@global.server_uuid,@rivomesh_unique_freeze_session")) {
                if (!row.next() || !marker.equals(row.getString(2))) {
                    throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_IDENTITY_CHANGED");
                }
                serverUuid = row.getString(1);
            }
        }
        return new SourcePosition(1, "MYSQL", sourceId, Map.of("gtidSet", position.gtidSet,
                "file", position.file, "offset", Long.toString(position.offset), "serverUuid", serverUuid));
    }

    SourcePosition boundary() {
        return cut;
    }

    String verify() throws SQLException {
        checkDeadline();
        Map<String,String> expected = cut.getPayload();
        String sqlMode;
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(3);
            try (ResultSet row = statement.executeQuery("SELECT VERSION(),@@global.server_uuid,@@global.read_only,"
                    + "@@global.super_read_only,@@global.log_bin,@@global.gtid_mode,@@global.performance_schema,"
                    + "@@session.sql_mode,@rivomesh_unique_freeze_session")) {
                if (!row.next() || !row.getString(1).matches("8\\.4\\.[0-9]+(?:[-+].*)?")
                        || !row.getBoolean(5) || !"ON".equals(row.getString(6)) || !row.getBoolean(7)) {
                    throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_PROFILE_UNSUPPORTED");
                }
                if (!row.getBoolean(3) || !row.getBoolean(4)) {
                    throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_NOT_FROZEN");
                }
                if (!row.getString(2).equals(expected.get("serverUuid")) || !marker.equals(row.getString(9))) {
                    throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_IDENTITY_CHANGED");
                }
                sqlMode = row.getString(8);
            }
            if (hasRows(statement, "SHOW REPLICA STATUS")
                    || hasRows(statement, "SELECT 1 FROM performance_schema.replication_group_members LIMIT 1")
                    || hasRows(statement, "SELECT 1 FROM performance_schema.replication_connection_configuration LIMIT 1")
                    || hasRows(statement, "SELECT 1 FROM performance_schema.replication_applier_configuration LIMIT 1")) {
                throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_REPLICATION_CONFIGURED");
            }
        }
        Model.Position observed = source.position(connection);
        if (!observed.gtidSet.equals(expected.get("gtidSet")) || !observed.file.equals(expected.get("file"))
                || !Long.toString(observed.offset).equals(expected.get("offset"))) {
            throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_CUT_CHANGED");
        }
        checkDeadline();
        return sqlMode;
    }

    void verifyDurable() throws SQLException {
        verify();
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(3);
            try (ResultSet row = statement.executeQuery("""
                    SELECT @@global.persisted_globals_load,@@global.read_only,@@global.super_read_only,
                           @rivomesh_unique_freeze_session,
                           (SELECT COUNT(*) FROM performance_schema.persisted_variables
                             WHERE VARIABLE_NAME IN ('read_only','super_read_only')
                               AND UPPER(VARIABLE_VALUE) IN ('ON','1'))
                    """)) {
                if (!row.next() || !marker.equals(row.getString(4))) {
                    throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_IDENTITY_CHANGED");
                }
                if (!row.getBoolean(1) || !row.getBoolean(2) || !row.getBoolean(3) || row.getInt(5) != 2) {
                    throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_FREEZE_NOT_PERSISTED");
                }
            }
        }
        checkDeadline();
    }

    TableSchema singleTable(String database) throws Exception {
        String sqlMode = verify();
        String name;
        try (PreparedStatement query = connection.prepareStatement("SELECT TABLE_NAME,ENGINE FROM information_schema.TABLES"
                + " WHERE BINARY TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' ORDER BY BINARY TABLE_NAME LIMIT 2")) {
            query.setQueryTimeout(3);
            query.setString(1, database);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next() || !"InnoDB".equals(rows.getString(2))) {
                    throw new IllegalStateException("ADOPTION_PLAN_SOURCE_SCOPE_UNSUPPORTED");
                }
                name = rows.getString(1);
                if (rows.next()) throw new IllegalStateException("ADOPTION_PLAN_SOURCE_SCOPE_UNSUPPORTED");
            }
        }
        TableSchema table = MysqlCdcSource.describe(connection, database, name, sqlMode);
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT EXISTS(SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
                               WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? AND CONSTRAINT_TYPE NOT IN ('PRIMARY KEY','UNIQUE'))
                    OR EXISTS(SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
                               WHERE BINARY REFERENCED_TABLE_SCHEMA=? AND BINARY REFERENCED_TABLE_NAME=?)
                    OR EXISTS(SELECT 1 FROM information_schema.TRIGGERS
                               WHERE BINARY EVENT_OBJECT_SCHEMA=? AND BINARY EVENT_OBJECT_TABLE=?)
                    OR EXISTS(SELECT 1 FROM information_schema.COLUMNS
                               WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=?
                                 AND (COLUMN_DEFAULT IS NOT NULL OR EXTRA<>''))
                """)) {
            query.setQueryTimeout(3);
            for (int i = 1; i <= 7; i += 2) {
                query.setString(i, database);
                query.setString(i + 1, name);
            }
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getBoolean(1)) throw new IllegalStateException("ADOPTION_PLAN_SOURCE_DDL_UNSUPPORTED");
            }
        }
        verifyTextUniqueKey(table);
        verify();
        return table;
    }

    private void verifyTextUniqueKey(TableSchema table) throws SQLException {
        for (TableSchema.Index index : table.indexes) {
            if (index.primary || !index.unique || index.columns.size() != 1) continue;
            TableSchema.Column key = table.columns.get(table.column(index.columns.get(0)));
            if (key.type != TableSchema.Type.STRING || key.sourceType == null || !key.sourceType.startsWith("varchar(")) continue;
            try (PreparedStatement query = connection.prepareStatement("""
                    SELECT PAD_ATTRIBUTE='NO PAD'
                           AND _utf8mb4'A' COLLATE utf8mb4_0900_bin <> _utf8mb4'a' COLLATE utf8mb4_0900_bin
                           AND _utf8mb4'a ' COLLATE utf8mb4_0900_bin <> _utf8mb4'a' COLLATE utf8mb4_0900_bin
                           AND CONVERT(X'c3a9' USING utf8mb4) COLLATE utf8mb4_0900_bin
                               <> CONVERT(X'65cc81' USING utf8mb4) COLLATE utf8mb4_0900_bin
                      FROM information_schema.COLLATIONS
                     WHERE BINARY COLLATION_NAME=? AND BINARY COLLATION_NAME='utf8mb4_0900_bin'
                       AND BINARY CHARACTER_SET_NAME=? AND BINARY CHARACTER_SET_NAME='utf8mb4'
                    """)) {
                query.setQueryTimeout(3);
                query.setString(1, key.collation);
                query.setString(2, key.charset);
                try (ResultSet row = query.executeQuery()) {
                    if (!row.next() || !row.getBoolean(1)) {
                        throw new IllegalStateException("ADOPTED_UNIQUE_SOURCE_TEXT_SEMANTICS_UNSUPPORTED");
                    }
                }
            }
            return;
        }
    }

    @FunctionalInterface
    interface RowVisitor { void visit(List<Cell> cells) throws Exception; }

    RepairProbe.Result scan(TableSchema table, RepairProbe probe) throws Exception {
        scanRows(table, 0, 0, values -> {
            TargetRead.Row row = new TargetRead.Row();
            row.values = values;
            probe.append(row);
        });
        return probe.finish();
    }

    void scanRows(TableSchema table, int maximumRawRowBytes, int maximumRows, RowVisitor visitor) throws Exception {
        String sqlMode = verify();
        try (PreparedStatement shape = connection.prepareStatement("SELECT 1 FROM information_schema.TABLES "
                + "WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? AND TABLE_TYPE='BASE TABLE' AND ENGINE='InnoDB'")) {
            shape.setQueryTimeout(3);
            shape.setString(1, table.database);
            shape.setString(2, table.name);
            try (ResultSet row = shape.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_SCHEMA_CHANGED");
            }
        }
        if (!Codec.encode(table).equals(Codec.encode(MysqlCdcSource.describe(connection, table.database, table.name, sqlMode)))) {
            throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_SCHEMA_CHANGED");
        }
        String relation = Sql.mysql(table.database) + "." + Sql.mysql(table.name);
        if (maximumRawRowBytes > 0) {
            String bytes = table.columns.stream().map(column -> "COALESCE(OCTET_LENGTH(CAST("
                    + Sql.mysql(column.name) + " AS CHAR CHARACTER SET utf8mb4)),0)").collect(Collectors.joining("+"));
            String sizeQuery = "SELECT COUNT(*),COALESCE(MAX(row_bytes),0) FROM (SELECT " + bytes
                    + " AS row_bytes FROM " + relation + " LIMIT ?) AS bounded";
            try (PreparedStatement oversized = connection.prepareStatement(sizeQuery)) {
                checkDeadline();
                oversized.setQueryTimeout((int) Math.max(1, TimeUnit.NANOSECONDS.toSeconds(deadline - System.nanoTime())));
                oversized.setInt(1, maximumRows + 1);
                try (ResultSet rows = oversized.executeQuery()) {
                    if (!rows.next() || rows.getLong(1) > maximumRows) throw new IllegalStateException("ADOPTION_SOURCE_ROW_LIMIT");
                    if (rows.getLong(2) > maximumRawRowBytes) throw new IllegalStateException("ADOPTION_ROW_TOO_LARGE");
                }
            }
        }
        String sql = "SELECT " + MysqlCdcSource.readColumns(table) + " FROM " + relation;
        try (PreparedStatement query = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            checkDeadline();
            query.setQueryTimeout((int) Math.max(1, TimeUnit.NANOSECONDS.toSeconds(deadline - System.nanoTime())));
            query.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rows = query.executeQuery()) {
                try {
                    while (rows.next()) {
                        checkDeadline();
                        List<Cell> values = new ArrayList<>();
                        for (int column = 0; column < table.columns.size(); column++) {
                            values.add(MysqlCdcSource.jdbc(table, column, rows, column + 1));
                        }
                        visitor.visit(values);
                    }
                } catch (Exception failure) {
                    abort();
                    throw failure;
                }
            }
        }
        verify();
    }

    private void checkDeadline() {
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("UNIQUE_RESTORE_SOURCE_FREEZE_EXPIRED");
        }
    }

    private static boolean hasRows(Statement statement, String sql) throws SQLException {
        try (ResultSet result = statement.executeQuery(sql)) {
            return result.next();
        }
    }

    private void abort() {
        try {
            connection.abort(Runnable::run);
        } catch (Exception unsupported) {
            try {
                connection.close();
            } catch (Exception ignored) {
                // The server releases the read gate if the physical connection closes.
            }
        }
    }

    @Override
    public void close() throws SQLException {
        watchdog.shutdownNow();
        connection.close();
    }
}
