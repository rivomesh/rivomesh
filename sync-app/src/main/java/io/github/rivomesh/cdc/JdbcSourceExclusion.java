package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.recovery.CaptureExclusionPlan;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Commits a source checkpoint and explicit gap receipt in one journal transaction. */
final class JdbcSourceExclusion {
    private JdbcSourceExclusion() {}

    static void verify(JdbcJournal journal, Connection c) throws SQLException {
        try (PreparedStatement p = c.prepareStatement("SELECT operation_id FROM " + journal.table("source_exclusion")
                + " WHERE pipeline_id=? LIMIT 1")) {
            p.setString(1, journal.settings.pipelineId);
            try (ResultSet rows = p.executeQuery()) {
                if (!rows.next()) throw new IllegalStateException("CAPTURE_EXCLUSION_LEDGER_LOST");
            }
        } catch (SQLException failure) {
            if (missingTable(failure)) throw new IllegalStateException("CAPTURE_EXCLUSION_LEDGER_LOST", failure);
            throw failure;
        }
        try (PreparedStatement p = c.prepareStatement("SELECT operation_id FROM " + journal.table("source_gap")
                + " WHERE pipeline_id=? LIMIT 1")) {
            p.setString(1, journal.settings.pipelineId);
            try (ResultSet rows = p.executeQuery()) {
                if (!rows.next()) throw new IllegalStateException("CAPTURE_EXCLUSION_LEDGER_LOST");
            }
        } catch (SQLException failure) {
            if (missingTable(failure)) throw new IllegalStateException("CAPTURE_EXCLUSION_LEDGER_LOST", failure);
            throw failure;
        }
        verifyTransactional(journal, c);
    }

    private static void verifyTransactional(JdbcJournal journal, Connection c) throws SQLException {
        if (journal.settings.driver.startsWith("com.mysql")) {
            try (PreparedStatement p = c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES"
                    + " WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rm3_job','rm3_source_exclusion','rm3_source_gap')")) {
                p.setString(1, journal.settings.namespace);
                try (ResultSet rows = p.executeQuery()) {
                    int count = 0;
                    while (rows.next()) {
                        if (!"InnoDB".equalsIgnoreCase(rows.getString(1)))
                            throw new IllegalStateException("CAPTURE_EXCLUSION_LEDGER_NOT_TRANSACTIONAL");
                        count++;
                    }
                    if (count != 3) throw new IllegalStateException("CAPTURE_EXCLUSION_LEDGER_LOST");
                }
            }
        }
    }

    private static boolean missingTable(SQLException failure) {
        String state = failure.getSQLState();
        return "42P01".equals(state) || "42S02".equals(state)
                || "42102".equals(state) || "42104".equals(state) || failure.getErrorCode() == 1146;
    }

    private static void ensure(JdbcJournal journal, Connection c) throws SQLException {
        journal.ddl(c, "CREATE TABLE IF NOT EXISTS " + journal.table("source_exclusion")
                + " (pipeline_id VARCHAR(128) NOT NULL,operation_id VARCHAR(128) NOT NULL,"
                + " transaction_id VARCHAR(128) NOT NULL,approval_hash VARCHAR(64) NOT NULL,"
                + " receipt_hash VARCHAR(64) NOT NULL,receipt " + journal.text() + " NOT NULL,"
                + " PRIMARY KEY(pipeline_id,operation_id),UNIQUE(pipeline_id,transaction_id))");
        journal.ddl(c, "CREATE TABLE IF NOT EXISTS " + journal.table("source_gap")
                + " (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,"
                + " after_seq BIGINT NOT NULL,operation_id VARCHAR(128) NOT NULL,receipt_hash VARCHAR(64) NOT NULL,"
                + " PRIMARY KEY(pipeline_id,subscription_id,after_seq,operation_id))");
    }

    static Map<String,Object> receipt(JdbcJournal journal, Connection c, String operation, String approvalHash)
            throws SQLException {
        try (PreparedStatement p = c.prepareStatement("SELECT approval_hash,receipt_hash,receipt FROM "
                + journal.table("source_exclusion") + " WHERE pipeline_id=? AND operation_id=?")) {
            p.setString(1, journal.settings.pipelineId);
            p.setString(2, operation);
            try (ResultSet rows = p.executeQuery()) {
                if (!rows.next()) return null;
                if (!approvalHash.equals(rows.getString(1)))
                    throw new IllegalStateException("CAPTURE_EXCLUSION_REQUEST_CONFLICT");
                return decodeReceipt(operation,rows.getString(3),rows.getString(2));
            }
        }
    }

    static Map<String,Object> page(JdbcJournal journal, Connection c, String after, int limit) throws SQLException {
        List<Map<String,Object>> receipts = new ArrayList<>();
        String next = null;
        try (PreparedStatement p = c.prepareStatement("SELECT operation_id,receipt,receipt_hash FROM "
                + journal.table("source_exclusion") + " WHERE pipeline_id=? AND operation_id>?"
                + " ORDER BY operation_id LIMIT ?")) {
            p.setString(1,journal.settings.pipelineId);
            p.setString(2,after);
            p.setInt(3,limit+1);
            try (ResultSet rows = p.executeQuery()) {
                while (rows.next()) {
                    if (receipts.size()==limit) {
                        next=(String)receipts.get(limit-1).get("operationId");
                        break;
                    }
                    receipts.add(decodeReceipt(rows.getString(1),rows.getString(2),rows.getString(3)));
                }
            }
        }
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("exclusions",receipts);
        result.put("nextCursor",next);
        return result;
    }

    private static Map<String,Object> decodeReceipt(String operation, String payload, String receiptHash) {
        if (payload==null || payload.length()>1024*1024 || !Codec.sha256(payload).equals(receiptHash))
            throw new IllegalStateException("CAPTURE_EXCLUSION_RECEIPT_CHANGED");
        @SuppressWarnings("unchecked") Map<String,Object> result = Codec.decode(payload,Map.class);
        if (!operation.equals(result.get("operationId")) || !"CAPTURE_TRANSACTION_EXCLUDED".equals(result.get("code")))
            throw new IllegalStateException("CAPTURE_EXCLUSION_RECEIPT_CHANGED");
        return result;
    }

    static Map<String,Object> commit(JdbcJournal journal, CaptureExclusionPlan plan, SourcePosition after,
                                     String operation, String approvalHash, String actor, String reason,
                                     java.util.concurrent.Callable<Void> authorize) throws Exception {
        try (Connection c = journal.connect()) {
            c.setAutoCommit(false);
            Map<String,Object> job = journal.locked(c);
            if (!JdbcJournal.hasSourceExclusions((int) job.get("format"))) {
                journal.requireCaptureExclusion(c, job, plan);
                c.rollback();
                c.setAutoCommit(true);
                ensure(journal, c);
                verifyTransactional(journal, c);
                c.setAutoCommit(false);
                job = journal.locked(c);
            }
            if (JdbcJournal.hasSourceExclusions((int) job.get("format"))) {
                verify(journal, c);
                Map<String,Object> existing = receipt(journal, c, operation, approvalHash);
                if (existing != null) {
                    c.rollback();
                    return existing;
                }
            }
            journal.requireCaptureExclusion(c, job, plan);
            // The proof identifies a physical commit, and the journal still holds its exact predecessor.
            // No log row, normal APPLY receipt or destination acknowledgment is created.
            Map<String,Object> resolution = new LinkedHashMap<>(plan.summary());
            resolution.put("code", "CAPTURE_TRANSACTION_EXCLUDED");
            resolution.put("operationId", operation);
            resolution.put("actor", actor);
            resolution.put("reason", reason);
            resolution.put("captureErrorGeneration", Math.addExact(plan.errorGeneration, 1));
            String payload = Codec.encode(resolution);
            if(payload.length()>1024*1024)throw new IllegalStateException("CAPTURE_EXCLUSION_RECEIPT_TOO_LARGE");
            String digest = Codec.sha256(payload);
            JdbcJournal.execute(c, "INSERT INTO " + journal.table("source_exclusion")
                    + " (pipeline_id,operation_id,transaction_id,approval_hash,receipt_hash,receipt) VALUES (?,?,?,?,?,?)",
                    plan.pipelineId, operation, plan.transactionId, approvalHash, digest, payload);
            for (String subscription : plan.affectedSubscriptions.keySet()) {
                JdbcJournal.execute(c, "INSERT INTO " + journal.table("source_gap")
                        + " (pipeline_id,subscription_id,after_seq,operation_id,receipt_hash) VALUES (?,?,?,?,?)",
                        plan.pipelineId, subscription, plan.afterSequence, operation, digest);
            }
            int format = JdbcJournal.withSourceExclusions((int) job.get("format"));
            JdbcJournal.execute(c, "UPDATE " + journal.table("job")
                    + " SET captured=?,format_version=?,run_state='PAUSED',error_generation=?,"
                    + " error_code='CAPTURE_TRANSACTION_EXCLUDED',epoch=epoch+1,owner=NULL,lease_until=0 WHERE pipeline_id=?",
                    Codec.encode(after), format, Math.addExact(plan.errorGeneration,1), plan.pipelineId);
            authorize.call();
            c.commit();
            return resolution;
        }
    }

    static Long firstGap(JdbcJournal journal, Connection c, String subscription) throws SQLException {
        try (PreparedStatement p = c.prepareStatement("SELECT gap.after_seq,gap.receipt_hash,receipt.receipt_hash FROM "
                + journal.table("source_gap") + " gap LEFT JOIN " + journal.table("source_exclusion") + " receipt"
                + " ON receipt.pipeline_id=gap.pipeline_id AND receipt.operation_id=gap.operation_id"
                + " WHERE gap.pipeline_id=? AND gap.subscription_id=? ORDER BY gap.after_seq,gap.operation_id LIMIT 1")) {
            p.setString(1, journal.settings.pipelineId);
            p.setString(2, subscription);
            try (ResultSet rows = p.executeQuery()) {
                if (!rows.next()) return null;
                long sequence = rows.getLong(1);
                if (sequence < 0 || !Objects.equals(rows.getString(2),rows.getString(3)))
                    throw new IllegalStateException("CAPTURE_EXCLUSION_RECEIPT_CHANGED");
                return sequence;
            }
        } catch (SQLException failure) {
            if (missingTable(failure)) throw new IllegalStateException("CAPTURE_EXCLUSION_LEDGER_LOST", failure);
            throw failure;
        }
    }

    static void requireNotExcluded(JdbcJournal journal, Connection c, String transactionId) throws SQLException {
        try (PreparedStatement p = c.prepareStatement("SELECT operation_id FROM " + journal.table("source_exclusion")
                + " WHERE pipeline_id=? AND transaction_id=?")) {
            p.setString(1, journal.settings.pipelineId);
            p.setString(2, transactionId);
            try (ResultSet rows = p.executeQuery()) {
                if (rows.next()) throw new IllegalStateException("CAPTURE_EXCLUDED_TRANSACTION_REAPPEARED");
            }
        }
    }
}
