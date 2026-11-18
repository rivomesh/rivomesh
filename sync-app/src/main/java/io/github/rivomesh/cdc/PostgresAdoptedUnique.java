package io.github.rivomesh.cdc;

import io.github.rivomesh.Sql;
import io.github.rivomesh.recovery.FailureEvidence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** Restores exactly the one business UNIQUE described by an existing-target adoption plan. */
public final class PostgresAdoptedUnique {
    private static final String NAME = "rm3_sink_adopted_unique";

    private PostgresAdoptedUnique() {}

    public record Approval(String requestId, String actor) {
        public Approval {
            try {
                FailureEvidence.uuid(requestId);
            } catch (IllegalArgumentException invalid) {
                throw failure("ADOPTED_UNIQUE_APPROVAL_INVALID");
            }
            if (actor == null || !actor.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
                throw failure("ADOPTED_UNIQUE_APPROVAL_INVALID");
            }
        }
    }

    public record Proposal(String sourceIndex, String ordinaryIndex, String constraint, List<String> columns) {}

    public record Receipt(Approval approval, String adoptionSha256, String generation, String tableId,
                          Proposal proposal, String ordinarySha256, String nativeSha256) {}

    public record Outcome(Receipt receipt, boolean replayed) {}

    @FunctionalInterface
    interface Check { void verify(Receipt receipt) throws Exception; }

    static IndexPolicy.Decision candidate(TableSchema table, JdbcSink.UniquePolicy uniquePolicy) {
        List<IndexPolicy.Decision> planned = IndexPolicy.plan(table, TargetProduct.POSTGRESQL, uniquePolicy);
        if (planned.isEmpty()) return null;
        IndexPolicy.Decision index = planned.getFirst();
        if (planned.size() != 1 || !"DOWNGRADE_UNIQUE".equals(index.action) || index.columns.size() != 1
                || table.primaryKey.contains(index.columns.getFirst())) {
            throw failure("ADOPTION_PLAN_SOURCE_INDEX_UNSUPPORTED");
        }
        TableSchema.Column key = table.columns.get(table.column(index.columns.getFirst()));
        if (key.type != TableSchema.Type.INT64 || !"bigint".equals(key.sourceType) || key.generationKind != null) {
            throw failure("ADOPTION_PLAN_SOURCE_INDEX_UNSUPPORTED");
        }
        return index;
    }

    static Proposal proposal(IndexPolicy.Decision candidate) {
        return candidate == null ? null : new Proposal(candidate.sourceName, candidate.targetName,
                JdbcSink.constraintName(candidate), List.copyOf(candidate.columns));
    }

    static Receipt verifyExisting(Connection c, JdbcSink.Settings settings, String schema, TableSchema table,
                                  PostgresExistingAdoption.Receipt adoption) throws Exception {
        Receipt receipt = find(c, settings);
        if (receipt == null || !table.id.equals(receipt.tableId)) return null;
        IndexPolicy.Decision index = candidate(table, settings.uniquePolicy);
        if (adoption == null || !receipt.adoptionSha256.equals(Codec.sha256(Codec.encode(adoption)))
                || !receipt.generation.equals(adoption.generation())
                || !receipt.proposal.equals(proposal(index))
                || !receipt.ordinarySha256.equals(PostgresUniqueRestoreEvidence.observe(c, schema, table, index,
                        receipt.proposal.constraint, true))
                || !receipt.nativeSha256.equals(PostgresUniqueRestoreEvidence.restoredConstraint(c, schema, table.name,
                        receipt.proposal.constraint, receipt.proposal.columns))) {
            throw failure("ADOPTED_UNIQUE_RECEIPT_CHANGED");
        }
        return receipt;
    }

    static String runtimeConstraint(Connection c, JdbcSink.Settings settings, String schema, TableSchema table) throws Exception {
        if (!ledgerExists(c, settings.metadataSchema)) return null;
        try (Statement sql = c.createStatement()) { sql.execute("SET LOCAL search_path TO pg_catalog"); }
        PostgresExistingAdoption.Receipt adoption = PostgresExistingAdoption.findReceipt(c, settings);
        if (adoption == null || !settings.generation.equals(adoption.generation())
                || !settings.fingerprint.equals(adoption.fingerprint())) throw failure("ADOPTED_UNIQUE_RECEIPT_CHANGED");
        Receipt receipt = verifyExisting(c, settings, schema, table, adoption);
        return receipt == null ? null : receipt.proposal.constraint;
    }

    static Outcome install(Connection c, JdbcSink.Settings settings, String schema, PostgresExistingAdoption.Replay baseline,
                           Approval approval, Check verify) throws Exception {
        TableSchema table = baseline.table();
        IndexPolicy.Decision index = candidate(table, settings.uniquePolicy);
        Proposal proposal = proposal(index);
        if (proposal == null) throw failure("ADOPTED_UNIQUE_CANDIDATE_REQUIRED");
        Receipt before = baseline.restoredUnique();
        if (before != null) {
            if (!approval.equals(before.approval)) throw failure("ADOPTED_UNIQUE_APPROVAL_CONFLICT");
            verify.verify(before);
            c.rollback();
            return new Outcome(before, true);
        }
        PostgresUniqueRestoreEvidence.requireSafeDdlSession(c, "ADOPTED_UNIQUE");
        String ordinaryHash = PostgresUniqueRestoreEvidence.observe(c, schema, table, index, proposal.constraint, false);
        String relation = relation(settings);
        try (Statement sql = c.createStatement()) {
            sql.execute("CREATE TABLE " + relation + " (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,"
                    + "receipt_sha256 VARCHAR(64) NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
            sql.execute("ALTER TABLE ONLY " + Sql.pg(schema) + "." + Sql.pg(table.name) + " ADD CONSTRAINT "
                    + Sql.pg(proposal.constraint) + " UNIQUE NULLS DISTINCT (" + Sql.pg(proposal.columns.getFirst()) + ") NOT DEFERRABLE");
        }
        String nativeHash = PostgresUniqueRestoreEvidence.restoredConstraint(c, schema, table.name, proposal.constraint, proposal.columns);
        Receipt receipt = new Receipt(approval, Codec.sha256(Codec.encode(baseline.receipt())), baseline.receipt().generation(),
                table.id, proposal, ordinaryHash, nativeHash);
        String body = Codec.encode(receipt);
        if (JdbcJournal.execute(c, "INSERT INTO " + relation + " VALUES (?,?,?,?)", settings.pipelineId, settings.id,
                Codec.sha256(body), body) != 1 || !receipt.equals(find(c, settings))) {
            throw failure("ADOPTED_UNIQUE_RECEIPT_CHANGED");
        }
        verify.verify(receipt);
        try {
            c.commit();
        } catch (SQLException unknown) {
            throw failure("ADOPTED_UNIQUE_COMMIT_UNKNOWN");
        }
        return new Outcome(receipt, false);
    }

    private static Receipt find(Connection c, JdbcSink.Settings settings) throws Exception {
        if (!ledgerExists(c, settings.metadataSchema)) return null;
        String relation = relation(settings);
        try (Statement sql = c.createStatement()) { sql.execute("LOCK TABLE ONLY " + relation + " IN ACCESS SHARE MODE"); }
        if (!ledgerExists(c, settings.metadataSchema)) throw failure("ADOPTED_UNIQUE_RECEIPT_CHANGED");
        try (PreparedStatement query = c.prepareStatement("SELECT receipt_sha256,receipt FROM " + relation
                + " WHERE pipeline_id=? AND subscription_id=?")) {
            query.setString(1, settings.pipelineId);
            query.setString(2, settings.id);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) throw failure("ADOPTED_UNIQUE_RECEIPT_CHANGED");
                String hash = row.getString(1);
                String body = JdbcGapVerificationLedger.bounded(row.getCharacterStream(2), 16 * 1024);
                if (!Codec.sha256(body).equals(hash) || row.next()) throw failure("ADOPTED_UNIQUE_RECEIPT_CHANGED");
                return Codec.decode(body, Receipt.class);
            }
        }
    }

    private static boolean ledgerExists(Connection c, String schema) throws SQLException {
        try (PreparedStatement query = c.prepareStatement("""
                SELECT r.relkind='r' AND r.relpersistence='p' AND NOT r.relrowsecurity AND NOT r.relispartition
                          AND NOT r.relhassubclass AND NOT r.relhasrules
                          AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=r.oid)
                          AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=r.oid)
                  FROM pg_catalog.pg_class r JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
                 WHERE n.nspname=? AND r.relname=?
                """)) {
            query.setString(1, schema);
            query.setString(2, NAME);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next()) return false;
                if (!row.getBoolean(1)) throw failure("ADOPTED_UNIQUE_LEDGER_INVALID");
                return true;
            }
        }
    }

    private static String relation(JdbcSink.Settings settings) { return Sql.pg(settings.metadataSchema) + "." + Sql.pg(NAME); }
    private static IllegalStateException failure(String code) { return new IllegalStateException(code); }
}
