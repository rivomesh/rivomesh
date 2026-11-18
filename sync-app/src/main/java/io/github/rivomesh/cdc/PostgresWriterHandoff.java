package io.github.rivomesh.cdc;

import io.github.rivomesh.Sql;
import io.github.rivomesh.recovery.FailureEvidence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** First writer grant and write-free return for one previously adopted PostgreSQL table. */
public final class PostgresWriterHandoff {
    private static final String NAME = "rm3_sink_writer_handoff";
    private static final String CONTINUITY = "rm3_sink_writer_continuity";
    private final PostgresExistingAdoption adoption;
    private final Connection connection;
    private final JdbcSink.Settings settings;
    private final JdbcJournal journal;
    private final String schema;
    private final String table;
    private final String relation;
    private final String ledger;
    private final String continuity;
    private PostgresWriterReadiness.Receipt ready;
    private String readyHash;
    private PostgresExistingAdoption.Replay baseline;
    private Stored stored;

    public record Approval(String requestId, String actor) {
        public Approval {
            try {
                FailureEvidence.uuid(requestId);
            } catch (IllegalArgumentException invalid) {
                throw failure("TARGET_WRITER_APPROVAL_INVALID");
            }
            if (actor == null || !actor.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
                throw failure("TARGET_WRITER_APPROVAL_INVALID");
            }
        }
    }

    public record Activation(Approval approval, String readinessSha256, String role, String authoritySha256,
                             String witnessSha256) {}

    public record Receipt(Activation activation, Approval failback) {}

    public record Outcome(Receipt receipt, boolean replayed) {}

    private record Stored(PostgresExistingAdoption.BaselinePhase phase, boolean writeSeen, Receipt receipt) {}

    PostgresWriterHandoff(PostgresExistingAdoption adoption, Connection connection, JdbcSink.Settings settings,
                          JdbcJournal journal, String schema, String table) {
        this.adoption = adoption;
        this.connection = connection;
        this.settings = settings;
        this.journal = journal;
        this.schema = schema;
        this.table = table;
        relation = Sql.pg(schema) + "." + Sql.pg(table);
        ledger = Sql.pg(settings.metadataSchema) + "." + Sql.pg(NAME);
        continuity = Sql.pg(settings.metadataSchema) + "." + Sql.pg(CONTINUITY);
    }

    public PostgresExistingAdoption.Replay verifyBaseline(String readinessSha256, Approval approval,
                                                          boolean failback) throws Exception {
        if (!FailureEvidence.hash(readinessSha256) || baseline != null) throw failure("TARGET_WRITER_APPROVAL_INVALID");
        PostgresWriterReadiness.lockAuthority(connection, "TARGET_WRITER");
        ready = PostgresWriterReadiness.requireReceipt(connection, settings, readinessSha256);
        readyHash = readinessSha256;
        stored = readControl();
        PostgresExistingAdoption.BaselinePhase phase = stored == null ? PostgresExistingAdoption.BaselinePhase.ADOPTED : stored.phase;
        String trigger = null;
        if (stored == null) {
            if (failback) throw failure("TARGET_WRITER_ACTIVATION_REQUIRED");
        } else {
            Activation active = stored.receipt.activation;
            if (!active.readinessSha256.equals(readyHash) || !active.role.equals(ready.approval().role())) {
                throw failure("TARGET_WRITER_APPROVAL_CONFLICT");
            }
            if (!failback && (!approval.equals(active.approval) || stored.receipt.failback != null)) {
                throw failure("TARGET_WRITER_APPROVAL_CONFLICT");
            }
            if (failback && stored.receipt.failback != null && !approval.equals(stored.receipt.failback)) {
                throw failure("TARGET_WRITER_APPROVAL_CONFLICT");
            }
            if (stored.writeSeen) throw failure("TARGET_WRITER_WRITE_OCCURRED");
            if (phase == PostgresExistingAdoption.BaselinePhase.TARGET_WRITER) {
                requireContinuity(active.approval.requestId);
                verifyActive(active);
                trigger = PostgresTargetWriteWitness.TRIGGER;
            }
        }
        baseline = adoption.replay(phase, trigger);
        if (baseline == null || !ready.evidence().adoptionSha256().equals(Codec.sha256(Codec.encode(baseline.receipt())))) {
            throw failure("TARGET_WRITER_ADOPTION_CHANGED");
        }
        if (phase != PostgresExistingAdoption.BaselinePhase.TARGET_WRITER) verifyReadiness();
        return baseline;
    }

    public Outcome finish(Approval approval, boolean failback, PostgresExistingAdoption.SourceFence source) throws Exception {
        if (baseline == null) throw failure("TARGET_WRITER_BASELINE_REQUIRED");
        boolean replayed = stored != null && (!failback || stored.receipt.failback != null);
        Receipt receipt;
        if (replayed) {
            receipt = stored.receipt;
            source.verify();
            adoption.observation().verify();
            connection.rollback();
            return new Outcome(receipt, true);
        }
        if (!failback) {
            String role = ready.approval().role();
            verifyReadiness();
            try (Statement sql = connection.createStatement()) {
                sql.execute("CREATE TABLE " + ledger + " (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,"
                        + "phase VARCHAR(16) NOT NULL,write_seen BOOLEAN NOT NULL,receipt_sha256 VARCHAR(64) NOT NULL,"
                        + "receipt TEXT NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
                // PostgreSQL clears this marker on crash recovery, even if an asynchronous app commit was acknowledged and lost.
                sql.execute("CREATE UNLOGGED TABLE " + continuity + " (activation_request VARCHAR(36) PRIMARY KEY)");
            }
            execute("INSERT INTO " + continuity + " VALUES (?)", approval.requestId);
            PostgresTargetWriteWitness.install(connection, settings.metadataSchema, ledger, schema, table, settings.pipelineId, settings.id);
            executeDdl("GRANT INSERT, UPDATE, DELETE ON TABLE " + relation + " TO " + Sql.pg(role));
            Activation active = new Activation(approval, readyHash, role, activeAuthority(role),
                    PostgresTargetWriteWitness.inspect(connection, settings.metadataSchema, schema, table, role));
            receipt = new Receipt(active, null);
            String body = Codec.encode(receipt);
            execute("INSERT INTO " + ledger + " VALUES (?,?,'TARGET_WRITER',false,?,?)",
                    settings.pipelineId, settings.id, Codec.sha256(body), body);
            setPhase(PostgresExistingAdoption.BaselinePhase.ADOPTED, PostgresExistingAdoption.BaselinePhase.TARGET_WRITER);
        } else {
            Activation active = stored.receipt.activation;
            verifyActive(active);
            executeDdl("REVOKE INSERT, UPDATE, DELETE ON TABLE " + relation + " FROM " + Sql.pg(active.role));
            PostgresTargetWriteWitness.remove(connection, settings.metadataSchema, schema, table);
            verifyReadiness();
            executeDdl("DROP TABLE " + continuity);
            receipt = new Receipt(active, approval);
            String body = Codec.encode(receipt);
            execute("UPDATE " + ledger + " SET phase='RETURNED',receipt_sha256=?,receipt=? WHERE pipeline_id=?"
                    + " AND subscription_id=? AND phase='TARGET_WRITER' AND NOT write_seen",
                    Codec.sha256(body), body, settings.pipelineId, settings.id);
            setPhase(PostgresExistingAdoption.BaselinePhase.TARGET_WRITER, PostgresExistingAdoption.BaselinePhase.RETURNED);
        }
        Stored after = readControl();
        if (after == null || after.writeSeen || !receipt.equals(after.receipt)) throw failure("TARGET_WRITER_RECEIPT_CHANGED");
        if (!failback) requireContinuity(approval.requestId);
        source.verify();
        adoption.observation().verify();
        try {
            connection.commit();
        } catch (SQLException unknown) {
            throw failure("TARGET_WRITER_COMMIT_UNKNOWN");
        }
        return new Outcome(receipt, false);
    }

    private void verifyReadiness() throws Exception {
        if (!ready.evidence().equals(PostgresWriterReadiness.evidence(connection, schema, baseline.receipt(), ready.approval()))) {
            throw failure("WRITER_READINESS_EVIDENCE_CHANGED");
        }
    }

    private void verifyActive(Activation active) throws Exception {
        if (!active.witnessSha256.equals(PostgresTargetWriteWitness.inspect(connection, settings.metadataSchema, schema, table, active.role))) {
            throw failure("TARGET_WRITER_WITNESS_CHANGED");
        }
        if (!active.authoritySha256.equals(activeAuthority(active.role))) throw failure("TARGET_WRITER_AUTHORITY_CHANGED");
    }

    private String activeAuthority(String role) throws Exception {
        try (Statement sql = connection.createStatement()) { sql.execute("SELECT pg_stat_clear_snapshot()"); }
        try (PreparedStatement sql = connection.prepareStatement("""
                SELECT a.oid::text,r.oid::text,COALESCE(r.relacl::text,''),COALESCE(n.nspacl::text,''),
                       a.rolcanlogin AND NOT a.rolsuper AND NOT a.rolcreatedb AND NOT a.rolcreaterole
                           AND NOT a.rolreplication AND NOT a.rolbypassrls
                           AND NOT EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.roleid=a.oid OR m.member=a.oid),
                       has_table_privilege(a.oid,r.oid,'INSERT') AND has_table_privilege(a.oid,r.oid,'UPDATE')
                           AND has_table_privilege(a.oid,r.oid,'DELETE') AND NOT has_table_privilege(a.oid,r.oid,'TRUNCATE,TRIGGER,MAINTAIN'),
                       EXISTS(SELECT 1 FROM pg_stat_activity s WHERE s.usesysid=a.oid)
                           OR EXISTS(SELECT 1 FROM pg_prepared_xacts x WHERE x.owner=a.rolname),
                       COALESCE((SELECT string_agg(col.attnum::text||':'||col.attacl::text,',' ORDER BY col.attnum)
                                   FROM pg_attribute col WHERE col.attrelid=r.oid AND col.attacl IS NOT NULL), ''),
                       NOT has_schema_privilege(a.oid,?,'USAGE,CREATE')
                           AND NOT has_table_privilege(a.oid,to_regclass(?),'DELETE,TRUNCATE,TRIGGER,MAINTAIN')
                           AND NOT has_any_column_privilege(a.oid,to_regclass(?),'INSERT,UPDATE,REFERENCES')
                  FROM pg_roles a CROSS JOIN pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
                 WHERE a.rolname=? AND n.nspname=? AND r.relname=?
                """)) {
            sql.setString(1, settings.metadataSchema);
            sql.setString(2, ledger);
            sql.setString(3, ledger);
            sql.setString(4, role);
            sql.setString(5, schema);
            sql.setString(6, table);
            try (ResultSet row = sql.executeQuery()) {
                if (!row.next() || !ready.evidence().roleOid().equals(row.getString(1))
                        || !row.getBoolean(5) || !row.getBoolean(6) || !row.getBoolean(9)) {
                    throw failure("TARGET_WRITER_AUTHORITY_CHANGED");
                }
                if (row.getBoolean(7)) throw failure("TARGET_WRITER_ROLE_CONNECTED");
                return Codec.sha256(Codec.encode(List.of(row.getString(1), row.getString(2), row.getString(3),
                        row.getString(4), row.getString(8))));
            }
        }
    }

    private void setPhase(PostgresExistingAdoption.BaselinePhase from, PostgresExistingAdoption.BaselinePhase to) throws SQLException {
        execute("UPDATE " + journal.table("job") + " SET run_state=?,error_code=?,error_generation=? WHERE pipeline_id=?"
                        + " AND run_state=? AND error_code=? AND error_generation=? AND epoch=0 AND owner IS NULL AND lease_until=0",
                to.state, to.reason, to.errorGeneration, settings.pipelineId, from.state, from.reason, from.errorGeneration);
    }

    private Stored readControl() throws Exception {
        if (!nativeRelation(NAME, "p")) return null;
        try (Statement sql = connection.createStatement()) { sql.execute("LOCK TABLE ONLY " + ledger + " IN SHARE ROW EXCLUSIVE MODE"); }
        if (!nativeRelation(NAME, "p")) throw failure("TARGET_WRITER_LEDGER_MISSING");
        try (PreparedStatement query = connection.prepareStatement("SELECT phase,write_seen,receipt_sha256,receipt FROM " + ledger
                + " WHERE pipeline_id=? AND subscription_id=? FOR UPDATE")) {
            query.setString(1, settings.pipelineId);
            query.setString(2, settings.id);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) throw failure("TARGET_WRITER_RECEIPT_MISSING");
                PostgresExistingAdoption.BaselinePhase phase = PostgresExistingAdoption.BaselinePhase.valueOf(row.getString(1));
                boolean seen = row.getBoolean(2);
                String data = JdbcGapVerificationLedger.bounded(row.getCharacterStream(4), 16 * 1024);
                if (!Codec.sha256(data).equals(row.getString(3)) || row.next()) throw failure("TARGET_WRITER_RECEIPT_CHANGED");
                Receipt receipt = Codec.decode(data, Receipt.class);
                if (receipt.activation == null || phase == PostgresExistingAdoption.BaselinePhase.ADOPTED
                        || (phase == PostgresExistingAdoption.BaselinePhase.RETURNED) != (receipt.failback != null)) {
                    throw failure("TARGET_WRITER_RECEIPT_CHANGED");
                }
                return new Stored(phase, seen, receipt);
            }
        }
    }

    private void requireContinuity(String activation) throws SQLException {
        if (!nativeRelation(CONTINUITY, "u")) throw failure("TARGET_WRITER_HISTORY_UNCERTAIN");
        try (Statement sql = connection.createStatement()) { sql.execute("LOCK TABLE ONLY " + continuity + " IN SHARE MODE"); }
        if (!nativeRelation(CONTINUITY, "u")) throw failure("TARGET_WRITER_HISTORY_UNCERTAIN");
        try (Statement sql = connection.createStatement(); ResultSet row = sql.executeQuery(
                "SELECT activation_request FROM " + continuity + " LIMIT 2")) {
            if (!row.next() || !activation.equals(row.getString(1)) || row.next()) {
                throw failure("TARGET_WRITER_HISTORY_UNCERTAIN");
            }
        }
    }

    private boolean nativeRelation(String name, String persistence) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT r.relkind='r' AND r.relpersistence::text=? AND NOT r.relrowsecurity AND NOT r.relispartition
                           AND NOT r.relhassubclass AND NOT r.relhasrules
                           AND r.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
                           AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=r.oid)
                           AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=r.oid)
                  FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
                 WHERE n.nspname=? AND r.relname=?
                """)) {
            query.setString(1, persistence);
            query.setString(2, settings.metadataSchema);
            query.setString(3, name);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) return false;
                if (!row.getBoolean(1)) throw failure("TARGET_WRITER_LEDGER_INVALID");
                return true;
            }
        }
    }

    private void executeDdl(String query) throws SQLException {
        try (Statement sql = connection.createStatement()) { sql.execute(query); }
    }

    private void execute(String query, Object... arguments) throws SQLException {
        if (JdbcJournal.execute(connection, query, arguments) != 1) throw failure("TARGET_WRITER_STATE_CHANGED");
    }

    private static IllegalStateException failure(String code) { return new IllegalStateException(code); }
}
