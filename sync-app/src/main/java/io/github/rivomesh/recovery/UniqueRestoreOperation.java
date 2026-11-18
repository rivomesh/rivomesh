package io.github.rivomesh.recovery;

import io.github.rivomesh.Sql;

import java.time.Instant;
import java.util.List;

/** Logical identifiers and digests only. No row contents, connection strings or credentials. */
public final class UniqueRestoreOperation {
    private UniqueRestoreOperation() {}

    public record Approval(String pipelineId, String targetId, String generation, String fingerprint, String reportId,
                           String tableId, String sourceIndex, String planSha256, String requestId, String actor) {
        public Approval {
            FailureEvidence.uuid(reportId);
            FailureEvidence.uuid(requestId);
            if (!identifier(pipelineId) || !identifier(targetId) || !identifier(actor)
                    || generation == null || generation.isEmpty() || generation.length() > 128
                    || !FailureEvidence.hash(fingerprint) || !FailureEvidence.hash(tableId)
                    || !FailureEvidence.hash(planSha256) || sourceIndex == null || sourceIndex.isEmpty()
                    || sourceIndex.length() > 64 || sourceIndex.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("UNIQUE_RESTORE_APPROVAL_INVALID");
            }
        }
    }

    public record Receipt(Approval approval, String schema, String table, List<String> columns, String constraint,
                          String nativeSha256, String committedAt) {
        public Receipt {
            if (approval == null || !FailureEvidence.hash(nativeSha256) || columns == null || columns.isEmpty()) {
                throw new IllegalArgumentException("UNIQUE_RESTORE_RECEIPT_INVALID");
            }
            Sql.pg(schema); Sql.pg(table); Sql.pg(constraint);
            columns = List.copyOf(columns);
            for (String column : columns) Sql.pg(column);
            Instant.parse(committedAt);
        }
    }

    private static boolean identifier(String value) {
        return value != null && value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    }
}
