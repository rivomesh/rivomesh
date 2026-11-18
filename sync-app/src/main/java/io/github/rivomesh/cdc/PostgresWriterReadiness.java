package io.github.rivomesh.cdc;

import io.github.rivomesh.Sql;
import io.github.rivomesh.recovery.FailureEvidence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

/** Durable authority observation for the adopted, still-paused PostgreSQL baseline. */
public final class PostgresWriterReadiness {
    private static final String NAME = "rm3_sink_writer_ready";

    private PostgresWriterReadiness() {}

    public record Approval(String role, String requestId, String actor) {
        public Approval {
            try {
                Sql.pg(role);
                FailureEvidence.uuid(requestId);
            } catch (IllegalArgumentException invalid) {
                throw failure("WRITER_READINESS_APPROVAL_INVALID");
            }
            if (actor == null || !actor.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
                throw failure("WRITER_READINESS_APPROVAL_INVALID");
            }
        }
    }

    public record Evidence(String adoptionSha256, String sourceCutSha256, String targetCatalogSha256,
                           String targetRowsSha256, String roleOid, String authoritySha256) {}

    public record Receipt(Approval approval, Evidence evidence, String evidenceSha256, String recordedAt) {}

    public record Outcome(Receipt receipt, boolean replayed) {}

    static Outcome record(Connection c, JdbcSink.Settings settings, String targetSchema,
                          PostgresExistingAdoption.Receipt adoption, Approval approval,
                          PostgresExistingAdoption.SourceFence verify) throws Exception {
        PostgresUniqueRestoreEvidence.requireSafeDdlSession(c, "WRITER_READINESS");
        try (Statement sql = c.createStatement()) {
            // Prevent native role/SET ROLE grant changes while computing and persisting authority evidence.
            sql.execute("LOCK TABLE pg_catalog.pg_authid IN SHARE MODE");
            sql.execute("LOCK TABLE pg_catalog.pg_auth_members IN SHARE MODE");
        }
        Evidence evidence = evidence(c, targetSchema, adoption, approval);
        String digest = Codec.sha256(Codec.encode(evidence));
        String relation = Sql.pg(settings.metadataSchema) + "." + Sql.pg(NAME);
        if (!ledger(c, settings.metadataSchema)) {
            try (Statement sql = c.createStatement()) {
                sql.execute("CREATE TABLE " + relation + " (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,"
                        + "receipt_sha256 VARCHAR(64) NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
            }
        }
        try (Statement sql = c.createStatement()) {
            sql.execute("LOCK TABLE ONLY " + relation + " IN SHARE ROW EXCLUSIVE MODE");
        }
        if (!ledger(c, settings.metadataSchema)) throw failure("WRITER_READINESS_LEDGER_MISSING");
        Receipt receipt = find(c, relation, settings);
        boolean replayed = receipt != null;
        if (replayed) {
            if (!approval.equals(receipt.approval)) throw failure("WRITER_READINESS_APPROVAL_CONFLICT");
            if (!evidence.equals(receipt.evidence) || !digest.equals(receipt.evidenceSha256)) {
                throw failure("WRITER_READINESS_EVIDENCE_CHANGED");
            }
        } else {
            receipt = new Receipt(approval, evidence, digest, Instant.now().toString());
            String body = Codec.encode(receipt);
            if (JdbcJournal.execute(c, "INSERT INTO " + relation + " VALUES (?,?,?,?)", settings.pipelineId, settings.id,
                    Codec.sha256(body), body) != 1 || !receipt.equals(find(c, relation, settings))) {
                throw failure("WRITER_READINESS_RECEIPT_CHANGED");
            }
        }
        if (!evidence.equals(evidence(c, targetSchema, adoption, approval))) throw failure("WRITER_READINESS_EVIDENCE_CHANGED");
        verify.verify();
        if (replayed) {
            c.rollback();
        } else {
            try {
                c.commit();
            } catch (SQLException unknown) {
                throw failure("WRITER_READINESS_COMMIT_UNKNOWN");
            }
        }
        return new Outcome(receipt, replayed);
    }

    private static Evidence evidence(Connection c, String schema, PostgresExistingAdoption.Receipt adoption,
                                     Approval approval) throws Exception {
        try (Statement statistics = c.createStatement()) {
            statistics.execute("SELECT pg_stat_clear_snapshot()");
        }
        String sql = """
                SELECT a.oid::text,t.oid::text,t.relowner::text,n.oid::text,
                       a.rolcanlogin AND NOT a.rolsuper AND NOT a.rolcreatedb AND NOT a.rolcreaterole
                           AND NOT a.rolreplication AND NOT a.rolbypassrls AND a.oid<>t.relowner
                           AND NOT EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.member=a.oid OR m.roleid=a.oid),
                       t.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
                           AND NOT EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.roleid=t.relowner),
                       has_schema_privilege(a.oid,n.oid,'USAGE') AND NOT has_schema_privilege(a.oid,n.oid,'CREATE')
                           AND has_database_privilege(a.oid,current_database(),'CONNECT')
                           AND NOT has_database_privilege(a.oid,current_database(),'CREATE')
                           AND has_table_privilege(a.oid,t.oid,'SELECT'),
                       has_table_privilege(a.oid,t.oid,'DELETE,TRUNCATE,TRIGGER,MAINTAIN')
                           OR has_any_column_privilege(a.oid,t.oid,'INSERT,UPDATE,REFERENCES')
                           OR EXISTS(SELECT 1 FROM aclexplode(COALESCE(t.relacl,acldefault('r',t.relowner))) acl
                                      WHERE acl.grantee<>t.relowner
                                        AND acl.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN'))
                           OR EXISTS(SELECT 1 FROM pg_attribute col CROSS JOIN LATERAL aclexplode(col.attacl) acl
                                      WHERE col.attrelid=t.oid AND col.attnum>0 AND NOT col.attisdropped
                                        AND acl.grantee<>t.relowner AND acl.privilege_type IN ('INSERT','UPDATE','REFERENCES')),
                       EXISTS(SELECT 1 FROM pg_stat_activity s WHERE s.usesysid=a.oid),
                       EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace pn ON pn.oid=p.pronamespace
                               WHERE p.prosecdef AND pn.nspname NOT IN ('pg_catalog','information_schema')
                                 AND has_schema_privilege(a.oid,pn.oid,'USAGE') AND has_function_privilege(a.oid,p.oid,'EXECUTE'))
                           OR EXISTS(SELECT 1 FROM pg_class v WHERE v.relkind='v'
                                      AND v.oid<>'pg_catalog.pg_settings'::regclass
                                      AND has_schema_privilege(a.oid,v.relnamespace,'USAGE')
                                      AND (has_table_privilege(a.oid,v.oid,'DELETE')
                                           OR has_any_column_privilege(a.oid,v.oid,'INSERT,UPDATE'))),
                       COALESCE(t.relacl::text,''),COALESCE(n.nspacl::text,''),
                       COALESCE((SELECT string_agg(col.attnum::text||':'||col.attacl::text,',' ORDER BY col.attnum)
                                   FROM pg_attribute col WHERE col.attrelid=t.oid AND col.attacl IS NOT NULL), '')
                  FROM pg_roles a CROSS JOIN pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
                 WHERE a.rolname=? AND n.nspname=? AND t.relname=?
                """;
        try (PreparedStatement query = c.prepareStatement(sql)) {
            query.setString(1, approval.role);
            query.setString(2, schema);
            query.setString(3, adoption.approval().table());
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !row.getBoolean(5)) throw failure("WRITER_READINESS_ROLE_UNSAFE");
                if (!row.getBoolean(6)) throw failure("WRITER_READINESS_OWNER_UNSAFE");
                if (!row.getBoolean(7)) throw failure("WRITER_READINESS_READER_GRANTS_REQUIRED");
                if (row.getBoolean(8)) throw failure("WRITER_READINESS_TARGET_WRITE_PRIVILEGE_PRESENT");
                if (row.getBoolean(9)) throw failure("WRITER_READINESS_ROLE_CONNECTED");
                if (row.getBoolean(10)) throw failure("WRITER_READINESS_INDIRECT_WRITE_PATH");
                String authority = Codec.sha256(Codec.encode(List.of(row.getString(1), row.getString(2), row.getString(3),
                        row.getString(4), row.getString(11), row.getString(12), row.getString(13))));
                return new Evidence(Codec.sha256(Codec.encode(adoption)), adoption.sourceSha256(), adoption.catalogSha256(),
                        adoption.rowsSha256(), row.getString(1), authority);
            }
        }
    }

    private static Receipt find(Connection c, String relation, JdbcSink.Settings settings) throws Exception {
        try (PreparedStatement query = c.prepareStatement("SELECT receipt_sha256,receipt FROM " + relation
                + " WHERE pipeline_id=? AND subscription_id=?")) {
            query.setString(1, settings.pipelineId);
            query.setString(2, settings.id);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) return null;
                String hash = row.getString(1);
                String data = JdbcGapVerificationLedger.bounded(row.getCharacterStream(2), 16 * 1024);
                if (!Codec.sha256(data).equals(hash) || row.next()) throw failure("WRITER_READINESS_RECEIPT_CHANGED");
                return Codec.decode(data, Receipt.class);
            }
        }
    }

    private static boolean ledger(Connection c, String schema) throws SQLException {
        try (PreparedStatement query = c.prepareStatement("""
                SELECT t.relkind='r' AND t.relpersistence='p' AND NOT t.relrowsecurity AND NOT t.relispartition
                          AND NOT t.relhassubclass AND NOT t.relhasrules
                          AND t.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
                          AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=t.oid)
                          AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=t.oid)
                  FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
                 WHERE n.nspname=? AND t.relname=?
                """)) {
            query.setString(1, schema);
            query.setString(2, NAME);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) return false;
                if (!row.getBoolean(1)) throw failure("WRITER_READINESS_LEDGER_INVALID");
                return true;
            }
        }
    }

    private static IllegalStateException failure(String code) { return new IllegalStateException(code); }
}
