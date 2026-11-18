package io.github.rivomesh.cdc;

import io.github.rivomesh.Sql;
import io.github.rivomesh.TargetOwnership;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/** One physically read-only PostgreSQL view, without opening or creating sink metadata. */
public final class PostgresAdoptionRead implements AutoCloseable {
    private static final String SINK_JOB = "rm3_sink_job";
    private final JdbcSink sink;
    private final JdbcSink.Settings settings;
    private final Connection connection;
    private final RepairConnectionGuard timeout;
    private final long maximumMillis;
    private final boolean exclusive;
    private final Set<String> pinAttempts = new HashSet<>();
    private final Set<String> pinned = new HashSet<>();
    private boolean snapshot;

    PostgresAdoptionRead(JdbcSink sink, JdbcSink.Settings settings, Connection connection,
                         long maximumMillis) throws Exception {
        this(sink, settings, connection, maximumMillis, false);
    }

    PostgresAdoptionRead(JdbcSink sink, JdbcSink.Settings settings, Connection connection,
                         long maximumMillis, boolean exclusive) throws Exception {
        this.sink = sink;
        this.settings = settings;
        this.connection = connection;
        this.maximumMillis = maximumMillis;
        this.exclusive = exclusive;
        timeout = new RepairConnectionGuard(connection, maximumMillis);
        try {
            if (!"PostgreSQL".equals(connection.getMetaData().getDatabaseProductName())) {
                throw new IllegalArgumentException("ADOPTION_TARGET_PRODUCT_UNSUPPORTED");
            }
            connection.setAutoCommit(false);
            try (Statement sql = connection.createStatement()) {
                sql.execute(exclusive ? "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE"
                        : "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
                sql.execute("SET LOCAL search_path TO pg_catalog");
                sql.execute("SET LOCAL row_security TO off");
                sql.execute("SET LOCAL statement_timeout TO " + maximumMillis);
                sql.execute("SET LOCAL lock_timeout TO '2s'");
                sql.execute("SET LOCAL idle_in_transaction_session_timeout TO " + maximumMillis);
            }
        } catch (Exception failure) {
            try {
                close();
            } catch (Exception closing) {
                failure.addSuppressed(closing);
            }
            throw failure;
        }
    }

    /** Take the PostgreSQL snapshot before the caller releases the MySQL write gate. */
    public boolean pin(List<TableSchema> tables) throws Exception {
        if (snapshot) throw new IllegalStateException("ADOPTION_VIEW_ALREADY_PINNED");
        try (Statement sql = connection.createStatement()) {
            pinRelation(sql, settings.metadataSchema, SINK_JOB);
            for (TableSchema table : tables) {
                String schema = destination(table);
                pinRelation(sql, schema, TargetOwnership.TABLE);
                if (!table.primaryKey.isEmpty()) pinRelation(sql, schema, table.name);
            }
            try (ResultSet row = sql.executeQuery("SELECT pg_catalog.pg_is_in_recovery(),"
                    + " pg_catalog.current_setting('transaction_read_only'),"
                    + " pg_catalog.current_setting('transaction_isolation')")) {
                if (!row.next() || row.getBoolean(1) || !(exclusive ? "off" : "on").equals(row.getString(2))
                        || !(exclusive ? "read committed" : "repeatable read").equals(row.getString(3))) {
                    throw new IllegalStateException("ADOPTION_TARGET_VIEW_UNAVAILABLE");
                }
            }
        }
        snapshot = true;
        verify();
        return metadataTableExists(settings.metadataSchema, SINK_JOB)
                && exists("SELECT 1 FROM " + qualified(settings.metadataSchema, SINK_JOB)
                        + " WHERE pipeline_id=? AND subscription_id=?", settings.pipelineId, settings.id);
    }

    private void pinRelation(Statement sql, String schema, String table) throws Exception {
        verify();
        String name = qualified(schema, table);
        if (!pinAttempts.add(name)) return;
        Savepoint savepoint = connection.setSavepoint();
        try {
            sql.execute("LOCK TABLE ONLY " + name + " IN ACCESS SHARE MODE");
            pinned.add(name);
        } catch (SQLException failure) {
            // Missing tables, or relation kinds that PostgreSQL cannot lock as tables, are reported below.
            if (!Set.of("42P01", "3F000", "42809").contains(failure.getSQLState())) throw failure;
            connection.rollback(savepoint);
        }
        connection.releaseSavepoint(savepoint);
    }

    public String blocker(TableSchema table) throws Exception {
        return blocker(table, false);
    }

    private String blocker(TableSchema table, boolean adopted) throws Exception {
        if (!snapshot) throw new IllegalStateException("ADOPTION_VIEW_NOT_PINNED");
        verify();
        if (table.primaryKey.isEmpty()) return "ADOPTION_SOURCE_PRIMARY_KEY_REQUIRED";
        String schema = destination(table);
        if (TargetOwnership.TABLE.equalsIgnoreCase(table.name)
                || table.columns.stream().anyMatch(column -> JdbcSink.ROW_ID.equalsIgnoreCase(column.name))) {
            return "ADOPTION_RESERVED_TARGET_IDENTIFIER";
        }
        if (IndexPolicy.plan(table, settings.product, settings.uniquePolicy).stream()
                .anyMatch(index -> "REQUIRES_PLAN".equals(index.action))) {
            return "ADOPTION_INDEX_CONVERSION_REQUIRES_PLAN";
        }
        String properties = """
                SELECT r.oid,r.relkind::text,r.relpersistence::text,r.relrowsecurity,r.relispartition,
                       EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=r.oid OR i.inhparent=r.oid)
                  FROM pg_catalog.pg_class r
                  JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
                 WHERE n.nspname=? AND r.relname=?
                """;
        long oid;
        try (PreparedStatement query = connection.prepareStatement(properties)) {
            query.setString(1, schema);
            query.setString(2, table.name);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) return "ADOPTION_TARGET_TABLE_MISSING";
                if (!pinned.contains(qualified(schema, table.name))
                        || !"r".equals(row.getString(2)) || !"p".equals(row.getString(3))
                        || row.getBoolean(4) || row.getBoolean(5) || row.getBoolean(6)) {
                    return "ADOPTION_TARGET_RELATION_REQUIRES_PLAN";
                }
                oid = row.getLong(1);
            }
        }
        if (!adopted && metadataTableExists(schema, TargetOwnership.TABLE)
                && exists("SELECT 1 FROM " + qualified(schema, TargetOwnership.TABLE) + " WHERE table_name=?", table.name)) {
            return "ADOPTION_TARGET_ALREADY_OWNED";
        }
        if (exists("SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid=?::oid AND attnum>0 "
                + "AND NOT attisdropped AND (attgenerated<>'' OR attidentity<>'')", oid)) {
            return "ADOPTION_TARGET_GENERATION_REQUIRES_PLAN";
        }
        if (exists("SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=?::oid AND NOT tgisinternal", oid)
                || exists("SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class=?::oid", oid)) {
            return "ADOPTION_TARGET_TRIGGERS_OR_RULES_REQUIRE_PLAN";
        }
        if (exists("SELECT 1 FROM pg_catalog.pg_constraint WHERE (conrelid=?::oid AND contype<>'p')"
                + " OR confrelid=?::oid", oid, oid)
                || exists("SELECT 1 FROM pg_catalog.pg_index WHERE indrelid=?::oid AND indisunique AND NOT indisprimary", oid)) {
            return "ADOPTION_TARGET_CONSTRAINTS_REQUIRE_PLAN";
        }
        try {
            sink.validateShape(connection, table);
        } catch (IllegalStateException failure) {
            String code = failure.getMessage();
            if (code != null && code.matches("TARGET_[A-Z0-9_]+")) return code;
            throw failure;
        }
        return null;
    }

    private boolean metadataTableExists(String schema, String table) throws SQLException {
        String query = """
                SELECT r.relkind='r' AND r.relpersistence='p' AND NOT r.relrowsecurity
                       AND NOT r.relispartition AND NOT r.relhassubclass
                  FROM pg_catalog.pg_class r
                  JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
                 WHERE n.nspname=? AND r.relname=?
                """;
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) return false;
                if (!row.getBoolean(1) || !pinned.contains(qualified(schema, table))) {
                    throw new IllegalStateException("ADOPTION_TARGET_METADATA_REQUIRES_PLAN");
                }
                return true;
            }
        }
    }

    public void scan(TableSchema table, int rowBytes, Consumer<List<Cell>> visit) throws Exception {
        verify();
        try (PreparedStatement query = connection.prepareStatement(
                sink.validationSql(table), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            query.setQueryTimeout((int) (maximumMillis / 1000));
            query.setFetchSize(8);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    visit.accept(JdbcTargetRead.readRow(rows, table, settings.product, rowBytes, this::verify).values);
                }
            }
        }
        verify();
    }

    /** Exact native schema and unclaimed namespace evidence from the pinned target view. */
    public String planEvidence(TableSchema table, String journalNamespace) throws Exception {
        return planEvidence(table, journalNamespace, false);
    }

    String planEvidence(TableSchema table, String journalNamespace, boolean adopted) throws Exception {
        String blocker = blocker(table, adopted);
        if (blocker != null) throw new IllegalStateException(blocker);
        String evidence = PostgresAdoptionCatalog.inspect(connection, destination(table), table,
                settings.metadataSchema, journalNamespace, adopted);
        verify();
        return evidence;
    }

    private boolean exists(String sql, Object... values) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) query.setObject(i + 1, values[i]);
            try (ResultSet rows = query.executeQuery()) {
                return rows.next();
            }
        }
    }

    private String destination(TableSchema table) {
        return settings.databases.get(table.database);
    }

    private static String qualified(String schema, String table) {
        if (schema == null || table == null || table.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 63) {
            throw new IllegalArgumentException("ADOPTION_TARGET_IDENTIFIER_INVALID");
        }
        return Sql.pg(schema) + "." + Sql.pg(table);
    }

    public void verify() {
        timeout.verify();
    }

    @Override public void close() throws SQLException {
        try (Connection owned = connection) {
            if (!owned.isClosed() && !owned.getAutoCommit()) owned.rollback();
        } finally {
            timeout.close();
        }
    }
}
