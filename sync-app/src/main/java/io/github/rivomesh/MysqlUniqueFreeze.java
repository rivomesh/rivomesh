package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** One pinned read on a MySQL server already frozen by the operator. */
final class MysqlUniqueFreeze implements AutoCloseable {
    private final MysqlSource source;
    private final Connection connection;
    private final SourcePosition cut;
    private final String marker = UUID.randomUUID().toString();
    private final ScheduledExecutorService watchdog;
    private final long deadline;

    MysqlUniqueFreeze(PipelineConfig config, SourcePosition cut, long maximumMillis) throws Exception {
        if (!"MYSQL".equals(config.effectiveSourceType()) || !"AUTO".equals(config.source.topology)
                || !config.source.nodes.isEmpty() || cut == null || cut.getFormatVersion() != 1
                || !"MYSQL".equals(cut.getSourceType()) || !config.source.clusterId.equals(cut.getSourceId())) {
            throw new IllegalArgumentException("UNIQUE_RESTORE_SOURCE_PROFILE_UNSUPPORTED");
        }
        this.cut = cut;
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

    RepairProbe.Result scan(TableSchema table, RepairProbe probe) throws Exception {
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
        String sql = "SELECT " + MysqlCdcSource.readColumns(table) + " FROM "
                + Sql.mysql(table.database) + "." + Sql.mysql(table.name);
        try (PreparedStatement query = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            checkDeadline();
            query.setQueryTimeout((int) Math.max(1, TimeUnit.NANOSECONDS.toSeconds(deadline - System.nanoTime())));
            query.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rows = query.executeQuery()) {
                try {
                    while (rows.next()) {
                        checkDeadline();
                        TargetRead.Row row = new TargetRead.Row();
                        row.values = new ArrayList<>();
                        for (int column = 0; column < table.columns.size(); column++) {
                            row.values.add(MysqlCdcSource.jdbc(table, column, rows, column + 1));
                        }
                        probe.append(row);
                    }
                } catch (Exception failure) {
                    abort();
                    throw failure;
                }
            }
        }
        verify();
        return probe.finish();
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
