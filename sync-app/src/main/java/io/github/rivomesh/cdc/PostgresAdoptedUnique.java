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
    private static final int MAXIMUM_TEXT_KEY_CHARACTERS = 320;

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
        TableSchema.Column key = keyColumn(table, index);
        boolean integer = key.type == TableSchema.Type.INT64 && "bigint".equals(key.sourceType);
        boolean text = key.type == TableSchema.Type.STRING && key.sourceType != null && key.sourceType.startsWith("varchar(")
                && key.length > 0 && key.length <= MAXIMUM_TEXT_KEY_CHARACTERS
                && "utf8mb4".equals(key.charset) && "utf8mb4_0900_bin".equals(key.collation);
        if ((!integer && !text) || key.generationKind != null) {
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
                || !receipt.ordinarySha256.equals(ordinaryHash(c, schema, table, index, true))
                || !receipt.nativeSha256.equals(restoredHash(c, schema, table, index))) {
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
        String ordinaryHash = ordinaryHash(c, schema, table, index, false);
        String relation = relation(settings);
        try (Statement sql = c.createStatement()) {
            sql.execute("CREATE TABLE " + relation + " (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,"
                    + "receipt_sha256 VARCHAR(64) NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
            sql.execute("ALTER TABLE ONLY " + Sql.pg(schema) + "." + Sql.pg(table.name) + " ADD CONSTRAINT "
                    + Sql.pg(proposal.constraint) + " UNIQUE NULLS DISTINCT (" + Sql.pg(proposal.columns.getFirst()) + ") NOT DEFERRABLE");
        }
        String nativeHash = restoredHash(c, schema, table, index);
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

    static String ordinaryHash(Connection c, String schema, TableSchema table, IndexPolicy.Decision index,
                               boolean restored) throws Exception {
        String hash = PostgresUniqueRestoreEvidence.observe(c, schema, table, index, JdbcSink.constraintName(index), restored);
        verifyTextIndex(c, schema, table, index, index.targetName);
        return hash;
    }

    private static String restoredHash(Connection c, String schema, TableSchema table, IndexPolicy.Decision index) throws Exception {
        String constraint = JdbcSink.constraintName(index);
        verifyTextIndex(c, schema, table, index, constraint);
        return PostgresUniqueRestoreEvidence.restoredConstraint(c, schema, table.name, constraint, index.columns);
    }

    private static TableSchema.Column keyColumn(TableSchema table, IndexPolicy.Decision index) {
        return table.columns.get(table.column(index.columns.getFirst()));
    }

    private static void verifyTextIndex(Connection c, String schema, TableSchema table, IndexPolicy.Decision index,
                                        String indexName) throws SQLException {
        TableSchema.Column key = keyColumn(table, index);
        if (key.type != TableSchema.Type.STRING) return;
        // PostgreSQL 17 bootstrap object IDs 950 and 3126 identify the stock C collation and B-tree text_ops.
        try (PreparedStatement query = c.prepareStatement("""
                SELECT current_setting('block_size')::int>=8192
                       AND a.atttypid='pg_catalog.varchar'::regtype AND a.atttypmod=?
                       AND coll.oid=950 AND coll.collprovider='c' AND coll.collencoding=-1 AND coll.collisdeterministic
                       AND coll.collcollate='C' AND coll.collctype='C'
                       AND i.indcollation[0]=coll.oid AND i.indclass[0]=3126
                       AND 'A'::varchar COLLATE pg_catalog."C" <> 'a'::varchar COLLATE pg_catalog."C"
                       AND 'a '::varchar COLLATE pg_catalog."C" <> 'a'::varchar COLLATE pg_catalog."C"
                       AND convert_from(decode('c3a9','hex'),'UTF8') COLLATE pg_catalog."C"
                           <> convert_from(decode('65cc81','hex'),'UTF8') COLLATE pg_catalog."C"
                  FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
                  JOIN pg_index i ON i.indrelid=t.oid JOIN pg_class idx ON idx.oid=i.indexrelid
                  JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=i.indkey[0] AND NOT a.attisdropped
                  JOIN pg_collation coll ON coll.oid=a.attcollation
                 WHERE n.nspname=? AND t.relname=? AND idx.relnamespace=n.oid AND idx.relname=? AND a.attname=?
                """)) {
            query.setInt(1, Math.toIntExact(key.length) + 4);
            query.setString(2, schema);
            query.setString(3, table.name);
            query.setString(4, indexName);
            query.setString(5, key.name);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !row.getBoolean(1)) throw failure("ADOPTED_UNIQUE_TARGET_TEXT_SEMANTICS_UNSUPPORTED");
            }
        }
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
