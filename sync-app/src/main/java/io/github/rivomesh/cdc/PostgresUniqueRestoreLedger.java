package io.github.rivomesh.cdc;

import io.github.rivomesh.Sql;
import io.github.rivomesh.recovery.UniqueRestoreOperation.Approval;
import io.github.rivomesh.recovery.UniqueRestoreOperation.Receipt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** The native receipt is created and inserted in the PostgreSQL constraint transaction. */
final class PostgresUniqueRestoreLedger {
    private static final String NAME = "rm3_sink_unique_restore";
    private final String schema;
    private final String relation;

    PostgresUniqueRestoreLedger(String schema) {
        this.schema = schema;
        relation = Sql.pg(schema) + "." + Sql.pg(NAME);
    }

    Receipt find(Connection c, Approval approval) throws Exception {
        if (relationOid(c) == null) return null;
        try (Statement sql = c.createStatement()) {
            sql.execute("LOCK TABLE ONLY " + relation + " IN ACCESS SHARE MODE");
        }
        validate(c);
        try (PreparedStatement sql = c.prepareStatement("SELECT receipt_sha256,receipt FROM " + relation
                + " WHERE pipeline_id=? AND subscription_id=? AND (request_id=? OR (generation=? AND table_id=? AND source_index=?))")) {
            sql.setString(1, approval.pipelineId());
            sql.setString(2, approval.targetId());
            sql.setString(3, approval.requestId());
            sql.setString(4, approval.generation());
            sql.setString(5, approval.tableId());
            sql.setString(6, approval.sourceIndex());
            try (ResultSet rows = sql.executeQuery()) {
                if (!rows.next()) return null;
                String body = JdbcGapVerificationLedger.bounded(rows.getCharacterStream(2), 16 * 1024);
                if (!Codec.sha256(body).equals(rows.getString(1)) || rows.next()) {
                    throw new IllegalStateException("UNIQUE_RESTORE_RECEIPT_CHANGED");
                }
                Receipt receipt = Codec.decode(body, Receipt.class);
                if (!approval.equals(receipt.approval())) throw new IllegalStateException("UNIQUE_RESTORE_APPROVAL_CONFLICT");
                return receipt;
            }
        }
    }

    void insert(Connection c, Receipt receipt) throws Exception {
        if (relationOid(c) == null) {
            try (Statement sql = c.createStatement()) {
                sql.execute("CREATE TABLE IF NOT EXISTS " + relation + """
                        (pipeline_id VARCHAR(128) NOT NULL, subscription_id VARCHAR(128) NOT NULL,
                         request_id VARCHAR(36) NOT NULL, generation VARCHAR(128) NOT NULL, table_id VARCHAR(64) NOT NULL,
                         source_index VARCHAR(64) NOT NULL, receipt_sha256 VARCHAR(64) NOT NULL, receipt TEXT NOT NULL,
                         PRIMARY KEY(pipeline_id,subscription_id,request_id),
                         UNIQUE(pipeline_id,subscription_id,generation,table_id,source_index))
                        """);
            }
        }
        validate(c);
        Approval approval = receipt.approval();
        String body = Codec.encode(receipt);
        try (PreparedStatement sql = c.prepareStatement("INSERT INTO " + relation + """
                (pipeline_id,subscription_id,request_id,generation,table_id,source_index,receipt_sha256,receipt)
                VALUES (?,?,?,?,?,?,?,?)
                """)) {
            sql.setString(1, approval.pipelineId());
            sql.setString(2, approval.targetId());
            sql.setString(3, approval.requestId());
            sql.setString(4, approval.generation());
            sql.setString(5, approval.tableId());
            sql.setString(6, approval.sourceIndex());
            sql.setString(7, Codec.sha256(body));
            sql.setString(8, body);
            if (sql.executeUpdate() != 1) throw new IllegalStateException("UNIQUE_RESTORE_RECEIPT_INSERT_FAILED");
        }
        if (!receipt.equals(find(c, approval))) throw new IllegalStateException("UNIQUE_RESTORE_RECEIPT_CHANGED");
    }

    private String relationOid(Connection c) throws Exception {
        try (PreparedStatement sql = c.prepareStatement("""
                SELECT r.oid::text, r.relkind='r' AND r.relpersistence='p' AND NOT r.relrowsecurity
                       AND NOT r.relispartition AND NOT r.relhassubclass AND NOT r.relhasrules
                       AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=r.oid)
                       AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=r.oid)
                  FROM pg_catalog.pg_class r JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
                 WHERE n.nspname=? AND r.relname=?
                """)) {
            sql.setString(1, schema);
            sql.setString(2, NAME);
            try (ResultSet rows = sql.executeQuery()) {
                if (!rows.next()) return null;
                if (!rows.getBoolean(2)) throw new IllegalStateException("UNIQUE_RESTORE_LEDGER_SHAPE_INVALID");
                return rows.getString(1);
            }
        }
    }

    private void validate(Connection c) throws Exception {
        String oid = relationOid(c);
        if (oid == null) throw new IllegalStateException("UNIQUE_RESTORE_LEDGER_MISSING");
        List<String> columns = new ArrayList<>();
        try (PreparedStatement sql = c.prepareStatement("""
                SELECT attname,format_type(atttypid,atttypmod),attnotnull,attgenerated='',attidentity=''
                  FROM pg_catalog.pg_attribute WHERE attrelid=?::oid AND attnum>0 AND NOT attisdropped
                 ORDER BY attnum
                """)) {
            sql.setString(1, oid);
            try (ResultSet rows = sql.executeQuery()) {
                while (rows.next()) {
                    if (!rows.getBoolean(3) || !rows.getBoolean(4) || !rows.getBoolean(5)) {
                        throw new IllegalStateException("UNIQUE_RESTORE_LEDGER_SHAPE_INVALID");
                    }
                    columns.add(rows.getString(1) + ":" + rows.getString(2));
                }
            }
        }
        if (!columns.equals(List.of("pipeline_id:character varying(128)", "subscription_id:character varying(128)",
                "request_id:character varying(36)", "generation:character varying(128)", "table_id:character varying(64)",
                "source_index:character varying(64)", "receipt_sha256:character varying(64)", "receipt:text"))) {
            throw new IllegalStateException("UNIQUE_RESTORE_LEDGER_SHAPE_INVALID");
        }
        Set<String> keys = new HashSet<>();
        try (PreparedStatement sql = c.prepareStatement("""
                SELECT pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid=?::oid
                """)) {
            sql.setString(1, oid);
            try (ResultSet rows = sql.executeQuery()) {
                while (rows.next()) keys.add(rows.getString(1));
            }
        }
        if (!keys.equals(Set.of("PRIMARY KEY (pipeline_id, subscription_id, request_id)",
                "UNIQUE (pipeline_id, subscription_id, generation, table_id, source_index)"))) {
            throw new IllegalStateException("UNIQUE_RESTORE_LEDGER_SHAPE_INVALID");
        }
    }
}
