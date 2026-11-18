package io.github.rivomesh.cdc;

import io.github.rivomesh.Sql;
import io.github.rivomesh.TargetOwnership;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.recovery.FailureEvidence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** First registration of an already matching table. The whole baseline is one PostgreSQL commit. */
public final class PostgresExistingAdoption implements AutoCloseable {
    private static final String PAUSE_REASON = "EXISTING_TARGET_ADOPTED";
    private static final String RECEIPT = "rm3_sink_adoption";
    private final JdbcSink sink;
    private final JdbcSink.Settings settings;
    private final JdbcJournal journal;
    private final Connection connection;
    private final PostgresAdoptionRead read;
    private final String targetSchema;
    private Approval lockedApproval;
    private Replay checkedReplay;

    enum BaselinePhase {
        ADOPTED("PAUSED", PAUSE_REASON, 1),
        TARGET_WRITER("TARGET_WRITER", "TARGET_WRITER_AUTHORIZED", 1),
        RETURNED("PAUSED", "TARGET_WRITER_REVOKED", 2);

        final String state;
        final String reason;
        final long errorGeneration;

        BaselinePhase(String state, String reason, long errorGeneration) {
            this.state = state;
            this.reason = reason;
            this.errorGeneration = errorGeneration;
        }
    }

    public record Approval(String table, String planSha256, String requestId, String actor) {
        public Approval {
            try {
                Sql.pg(table);
                FailureEvidence.uuid(requestId);
            } catch (IllegalArgumentException invalid) {
                throw failure("ADOPTION_APPROVAL_INVALID");
            }
            if (!FailureEvidence.hash(planSha256) || actor == null || !actor.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                    || TargetOwnership.TABLE.equalsIgnoreCase(table)) {
                throw failure("ADOPTION_APPROVAL_INVALID");
            }
        }
    }

    public record Receipt(Approval approval, String pipelineId, String targetId, String fingerprint,
                          String generation, String snapshot, String tableId, long rows, String sourceSha256,
                          String schemaSha256, String catalogSha256, String rowsSha256) {
        public Receipt {
            FailureEvidence.uuid(generation);
            FailureEvidence.uuid(snapshot);
            if (approval == null || rows < 1 || rows > 10_000 || !FailureEvidence.hash(fingerprint)
                    || !FailureEvidence.hash(tableId) || !FailureEvidence.hash(sourceSha256)
                    || !FailureEvidence.hash(schemaSha256) || !FailureEvidence.hash(catalogSha256)
                    || !FailureEvidence.hash(rowsSha256)) {
                throw failure("ADOPTION_RECEIPT_CHANGED");
            }
        }
    }

    public record Replay(Receipt receipt, TableSchema table) {}

    @FunctionalInterface
    public interface SourceFence { void verify() throws Exception; }

    PostgresExistingAdoption(JdbcSink sink, JdbcSink.Settings settings, Connection connection,
                            JdbcJournal journal, long maximumMillis) throws Exception {
        this.sink = sink;
        this.settings = settings;
        this.journal = journal;
        this.connection = connection;
        read = new PostgresAdoptionRead(sink, settings, connection, maximumMillis, true);
        try {
            if (settings.databases.size() != 1 || !settings.url.equals(journal.settings.url)
                    || !settings.driver.equals(journal.settings.driver) || !Objects.equals(settings.username, journal.settings.username)
                    || !settings.pipelineId.equals(journal.settings.pipelineId) || journal.settings.writerAssignment != null) {
                throw failure("ADOPTION_PLAN_ATOMIC_BOOTSTRAP_PROFILE_REQUIRED");
            }
            targetSchema = settings.databases.values().iterator().next();
        } catch (Exception error) {
            try { close(); } catch (Exception close) { error.addSuppressed(close); }
            throw error;
        }
    }

    public void lock(Approval approval) throws Exception {
        if (lockedApproval != null) throw failure("ADOPTION_TARGET_ALREADY_PINNED");
        try (Statement sql = connection.createStatement()) {
            sql.execute("LOCK TABLE ONLY " + qualify(targetSchema, approval.table) + " IN ACCESS EXCLUSIVE MODE");
        }
        read.verify();
        lockedApproval = approval;
    }

    public PostgresAdoptionRead observation() { return read; }

    public Replay replay() throws Exception {
        return replay(BaselinePhase.ADOPTED, null);
    }

    Replay replay(BaselinePhase phase, String verifiedWriterTrigger) throws Exception {
        Approval approval = requireLock();
        if (!existsRelation(settings.metadataSchema, RECEIPT)) return null;
        lockMetadata(settings.metadataSchema, RECEIPT);
        Receipt receipt;
        try (PreparedStatement sql = connection.prepareStatement("SELECT receipt_sha256,receipt FROM " + receiptTable()
                + " WHERE pipeline_id=? AND subscription_id=?")) {
            sql.setString(1, settings.pipelineId);
            sql.setString(2, settings.id);
            try (ResultSet rows = sql.executeQuery()) {
                if (!rows.next()) throw failure("ADOPTION_RECEIPT_MISSING");
                String hash = rows.getString(1);
                String body = JdbcGapVerificationLedger.bounded(rows.getCharacterStream(2), 16 * 1024);
                if (!Codec.sha256(body).equals(hash) || rows.next()) throw failure("ADOPTION_RECEIPT_CHANGED");
                receipt = Codec.decode(body, Receipt.class);
            }
        }
        if (!approval.equals(receipt.approval) || !settings.pipelineId.equals(receipt.pipelineId)
                || !settings.id.equals(receipt.targetId) || !settings.fingerprint.equals(receipt.fingerprint)) {
            throw failure("ADOPTION_APPROVAL_CONFLICT");
        }
        lockMetadata(targetSchema, TargetOwnership.TABLE);
        lockMetadata(settings.metadataSchema, "rm3_sink_job");
        lockMetadata(settings.metadataSchema, "rm3_sink_table");
        for (String name : List.of("job", "schema", "subscription", "snapshot", "snapshot_ack", "log", "receipt", "normalized", "snapshot_part")) {
            lockMetadata(journal.settings.namespace, "rm3_" + name);
        }
        TableSchema table = requireBaseline(receipt, phase);
        read.pin(List.of(table));
        if (!receipt.catalogSha256.equals(read.planEvidence(table, journal.settings.namespace, true, verifiedWriterTrigger))) {
            throw failure("ADOPTION_BOOTSTRAP_CATALOG_CHANGED");
        }
        checkedReplay = new Replay(receipt, table);
        return checkedReplay;
    }

    public PostgresWriterReadiness.Outcome prepareWriter(PostgresWriterReadiness.Approval approval,
                                                         SourceFence sourceFence) throws Exception {
        requireLock();
        if (checkedReplay == null) throw failure("WRITER_READINESS_ADOPTED_CUT_REQUIRED");
        return PostgresWriterReadiness.record(connection, settings, targetSchema, checkedReplay.receipt, approval, () -> {
            sourceFence.verify();
            read.verify();
        });
    }

    public PostgresWriterHandoff writerHandoff() throws Exception {
        requireLock();
        return new PostgresWriterHandoff(this, connection, settings, journal, targetSchema, lockedApproval.table);
    }

    public Receipt install(TableSchema table, SourcePosition cut, Map<String,Object> plan, String digest,
                           SourceFence source) throws Exception {
        Approval approval = requireLock();
        if (!approval.planSha256.equals(digest) || !digest.equals(Codec.sha256(Codec.encode(plan)))
                || !approval.table.equals(table.name) || table.primaryKey.size() != 1
                || !targetSchema.equals(settings.databases.get(table.database))) {
            throw failure("ADOPTION_PLAN_CHANGED");
        }
        String generation = UUID.randomUUID().toString();
        String snapshot = UUID.randomUUID().toString();
        String schema = Codec.encode(table);
        String baseline = Codec.encode(cut);
        if (!Codec.sha256(schema).equals(plan.get("sourceSchemaSha256"))
                || !Codec.sha256(baseline).equals(plan.get("sourcePositionSha256"))) {
            throw failure("ADOPTION_PLAN_CHANGED");
        }
        long rows = (Integer) plan.get("matchedRows");
        Receipt receipt = new Receipt(approval, settings.pipelineId, settings.id, settings.fingerprint, generation,
                snapshot, table.id, rows, Codec.sha256(baseline), Codec.sha256(schema),
                (String) plan.get("targetCatalogSha256"), (String) plan.get("matchedRowsSha256"));
        PostgresUniqueRestoreEvidence.requireSafeDdlSession(connection, "ADOPTION");
        try (Statement sql = connection.createStatement()) {
            // First creation under this transaction gives exclusive, database-wide namespace reservations.
            sql.execute("CREATE SCHEMA " + Sql.pg(journal.settings.namespace));
            sql.execute("CREATE SCHEMA " + Sql.pg(settings.metadataSchema));
            sql.execute("CREATE TABLE " + receiptTable() + " (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,"
                    + "receipt_sha256 VARCHAR(64) NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
        }
        TargetOwnership.createUnclaimedPostgres(connection, targetSchema);
        TargetOwnership.claim(connection, targetSchema, table.name, settings.pipelineId, settings.id, generation);
        journal.createStorage(connection);
        sink.createMetadata(connection);
        execute("INSERT INTO " + journal.table("job") + " (pipeline_id,format_version,fingerprint,generation,epoch,owner,lease_until,next_seq,"
                        + "captured,pending_bytes,snapshot_bytes,normalized_seq,identity_bytes,run_state,error_generation,error_code)"
                        + " VALUES (?,3,?,?,0,NULL,0,1,?,0,0,0,0,'PAUSED',1,?)",
                settings.pipelineId, settings.fingerprint, generation, baseline, PAUSE_REASON);
        execute("INSERT INTO " + journal.table("subscription") + " (pipeline_id,subscription_id,acknowledged,run_state,error_generation)"
                + " VALUES (?,?,0,'RUNNING',0)", settings.pipelineId, settings.id);
        execute("INSERT INTO " + journal.table("schema") + " (pipeline_id,table_id,schema_version,payload,payload_hash) VALUES (?,?,?,?,?)",
                settings.pipelineId, table.id, table.version, schema, receipt.schemaSha256);
        execute("INSERT INTO " + journal.table("snapshot") + " (pipeline_id,snapshot_scope,table_id,snapshot_id,schema_version,"
                        + "column_count,baseline,run_state,next_part,row_count,retained) VALUES (?,'source',?,?,?,?,?,'COMPLETE',0,?,0)",
                settings.pipelineId, table.id, snapshot, table.version, table.columns.size(), baseline, rows);
        execute("INSERT INTO " + journal.table("snapshot_ack") + " (pipeline_id,snapshot_scope,table_id,subscription_id,snapshot_id)"
                + " VALUES (?,'source',?,?,?)", settings.pipelineId, table.id, settings.id, snapshot);
        execute("INSERT INTO " + sinkTable("job") + " (pipeline_id,subscription_id,fingerprint,generation,epoch,stream_seq) VALUES (?,?,?,?,0,0)",
                settings.pipelineId, settings.id, settings.fingerprint, generation);
        execute("INSERT INTO " + sinkTable("table") + " (pipeline_id,subscription_id,table_id,schema_version,schema_hash,snapshot_id,"
                        + "snapshot_state,next_part,row_count) VALUES (?,?,?,?,?,?,'COMPLETE',0,?)",
                settings.pipelineId, settings.id, table.id, table.version, receipt.schemaSha256, snapshot, rows);
        String body = Codec.encode(receipt);
        execute("INSERT INTO " + receiptTable() + " VALUES (?,?,?,?)", settings.pipelineId, settings.id, Codec.sha256(body), body);
        requireBaseline(receipt, BaselinePhase.ADOPTED);
        if (!receipt.catalogSha256.equals(read.planEvidence(table, journal.settings.namespace, true))) {
            throw failure("ADOPTION_BOOTSTRAP_CATALOG_CHANGED");
        }
        source.verify();
        read.verify();
        try {
            connection.commit();
        } catch (SQLException error) {
            throw failure("ADOPTION_COMMIT_UNKNOWN");
        }
        return receipt;
    }

    private TableSchema requireBaseline(Receipt receipt, BaselinePhase phase) throws Exception {
        Map<String,Object> job = journal.locked(connection);
        if (!phase.state.equals(job.get("state")) || !phase.reason.equals(job.get("errorCode"))
                || !Long.valueOf(phase.errorGeneration).equals(job.get("errorGeneration")) || !Long.valueOf(0).equals(job.get("epoch"))
                || job.get("owner") != null || !Long.valueOf(0).equals(job.get("lease"))
                || !Long.valueOf(1).equals(job.get("next")) || !Long.valueOf(0).equals(job.get("normalized"))
                || !Long.valueOf(0).equals(job.get("bytes")) || !Long.valueOf(0).equals(job.get("snapshotBytes"))
                || !Long.valueOf(0).equals(job.get("identityBytes")) || !Integer.valueOf(3).equals(job.get("format"))
                || !receipt.generation.equals(job.get("generation")) || !receipt.fingerprint.equals(job.get("fingerprint"))
                || !(job.get("captured") instanceof String position) || !receipt.sourceSha256.equals(Codec.sha256(position))) {
            throw failure("ADOPTION_BOOTSTRAP_MOVED");
        }
        Journal.SnapshotState manifest = journal.snapshot(connection, "source", receipt.tableId, true);
        if (manifest == null || !receipt.snapshot.equals(manifest.snapshotId) || !"COMPLETE".equals(manifest.state)
                || manifest.nextPart != 0 || manifest.rows != receipt.rows || manifest.retained
                || !receipt.sourceSha256.equals(Codec.sha256(Codec.encode(manifest.baseline)))) {
            throw failure("ADOPTION_BOOTSTRAP_SNAPSHOT_CHANGED");
        }
        Journal.Subscription subscription = journal.subscription(connection, settings.id, true);
        if (subscription.acknowledged != 0 || !"RUNNING".equals(subscription.state)) throw failure("ADOPTION_BOOTSTRAP_MOVED");
        for (String name : List.of("schema", "subscription", "snapshot", "snapshot_ack")) {
            if (rowCount(journal.table(name)) != 1) throw failure("ADOPTION_BOOTSTRAP_CHECKPOINT_CHANGED");
        }
        for (String name : List.of("log", "receipt", "normalized", "snapshot_part")) {
            if (rowCount(journal.table(name)) != 0) throw failure("ADOPTION_BOOTSTRAP_CHECKPOINT_CHANGED");
        }
        requireRow("SELECT 1 FROM " + journal.table("snapshot_ack") + " WHERE pipeline_id=? AND subscription_id=?"
                        + " AND table_id=? AND snapshot_id=? AND snapshot_scope='source' FOR UPDATE",
                settings.pipelineId, settings.id, receipt.tableId, receipt.snapshot);
        requireRow("SELECT 1 FROM " + sinkTable("job") + " WHERE pipeline_id=? AND subscription_id=? AND generation=? AND fingerprint=?"
                        + " AND epoch=0 AND stream_seq=0 AND transaction_id IS NULL AND transaction_hash IS NULL FOR UPDATE",
                settings.pipelineId, settings.id, receipt.generation, receipt.fingerprint);
        requireRow("SELECT 1 FROM " + sinkTable("table") + " WHERE pipeline_id=? AND subscription_id=? AND table_id=?"
                        + " AND schema_hash=? AND schema_version=? AND snapshot_id=? AND snapshot_state='COMPLETE'"
                        + " AND row_count=? AND next_part=0 AND last_part_hash IS NULL FOR UPDATE",
                settings.pipelineId, settings.id, receipt.tableId, receipt.schemaSha256, manifest.schemaVersion, receipt.snapshot, receipt.rows);
        TargetOwnership.lockAndVerify(connection, targetSchema, receipt.approval.table, settings.pipelineId, settings.id, receipt.generation);
        try (PreparedStatement sql = connection.prepareStatement("SELECT payload,payload_hash FROM " + journal.table("schema")
                + " WHERE pipeline_id=? AND table_id=? AND schema_version=? FOR UPDATE")) {
            sql.setString(1, settings.pipelineId);
            sql.setString(2, receipt.tableId);
            sql.setInt(3, manifest.schemaVersion);
            try (ResultSet row = sql.executeQuery()) {
                if (!row.next() || !receipt.schemaSha256.equals(row.getString(2))) throw failure("ADOPTION_BOOTSTRAP_SCHEMA_CHANGED");
                String body = JdbcGapVerificationLedger.bounded(row.getCharacterStream(1), 2 * 1024 * 1024);
                if (!receipt.schemaSha256.equals(Codec.sha256(body))) throw failure("ADOPTION_BOOTSTRAP_SCHEMA_CHANGED");
                TableSchema table = Codec.decode(body, TableSchema.class);
                table.validate();
                if (!receipt.tableId.equals(table.id) || !receipt.approval.table.equals(table.name)
                        || !targetSchema.equals(settings.databases.get(table.database)) || manifest.columnCount != table.columns.size()) {
                    throw failure("ADOPTION_BOOTSTRAP_SCHEMA_CHANGED");
                }
                return table;
            }
        }
    }

    private void execute(String query, Object... arguments) throws SQLException {
        if (JdbcJournal.execute(connection, query, arguments) != 1) throw failure("ADOPTION_BOOTSTRAP_WRITE_FAILED");
    }

    private void requireRow(String query, Object... arguments) throws SQLException {
        try (PreparedStatement sql = connection.prepareStatement(query)) {
            for (int i = 0; i < arguments.length; i++) sql.setObject(i + 1, arguments[i]);
            try (ResultSet rows = sql.executeQuery()) {
                if (!rows.next() || rows.next()) throw failure("ADOPTION_BOOTSTRAP_CHECKPOINT_CHANGED");
            }
        }
    }

    private int rowCount(String relation) throws SQLException {
        try (PreparedStatement sql = connection.prepareStatement("SELECT 1 FROM " + relation + " WHERE pipeline_id=? LIMIT 2")) {
            sql.setString(1, settings.pipelineId);
            try (ResultSet rows = sql.executeQuery()) {
                if (!rows.next()) return 0;
                return rows.next() ? 2 : 1;
            }
        }
    }

    private boolean existsRelation(String schema, String relation) throws SQLException {
        try (PreparedStatement sql = connection.prepareStatement("""
                SELECT r.relkind='r' AND r.relpersistence='p' AND NOT r.relrowsecurity AND NOT r.relispartition
                       AND NOT r.relhassubclass AND NOT r.relhasrules
                       AND NOT EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=r.oid)
                       AND NOT EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=r.oid)
                  FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
                 WHERE n.nspname=? AND r.relname=?
                """)) {
            sql.setString(1, schema);
            sql.setString(2, relation);
            try (ResultSet rows = sql.executeQuery()) {
                if (!rows.next()) return false;
                if (!rows.getBoolean(1)) throw failure("ADOPTION_METADATA_INVALID");
                return true;
            }
        }
    }

    private void lockMetadata(String schema, String relation) throws SQLException {
        if (!existsRelation(schema, relation)) throw failure("ADOPTION_METADATA_MISSING");
        try (Statement sql = connection.createStatement()) {
            sql.execute("LOCK TABLE ONLY " + qualify(schema, relation) + " IN SHARE ROW EXCLUSIVE MODE");
        }
        if (!existsRelation(schema, relation)) throw failure("ADOPTION_METADATA_MISSING");
    }

    private Approval requireLock() {
        if (lockedApproval == null) throw failure("ADOPTION_TARGET_NOT_PINNED");
        read.verify();
        return lockedApproval;
    }

    private static String qualify(String schema, String relation) { return Sql.pg(schema) + "." + Sql.pg(relation); }
    private String receiptTable() { return qualify(settings.metadataSchema, RECEIPT); }
    private String sinkTable(String role) { return qualify(settings.metadataSchema, "rm3_sink_" + role); }
    private static IllegalStateException failure(String code) { return new IllegalStateException(code); }

    @Override
    public void close() throws SQLException { read.close(); }
}
