package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** The published shape supplied by pgoutput within the transaction that stopped capture. */
public record PostgresRelationEvidence(String checkpointSha256, String transactionFinalLsn,
                                       long relationOid, String namespace, String relation,
                                       String replicaIdentity, List<Column> columns,
                                       String recordedTableId, int recordedSchemaVersion, String recordedSchemaSha256) {
    public static final String FAILURE_CODE = "POSTGRESQL_RELATION_CHANGED_REQUIRES_PLAN";
    public static final int MAX_COLUMNS = 1600;
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern LSN = Pattern.compile("[0-9a-f]{1,8}/[0-9a-f]{1,8}");
    private static final long MAX_OID = 0xffffffffL;

    public record Column(String name, long typeOid, int typeModifier, boolean key) {
        public Column {
            if (!identifier(name) || typeOid < 1 || typeOid > MAX_OID) {
                throw new IllegalArgumentException("POSTGRESQL_RELATION_EVIDENCE_INVALID");
            }
        }
    }

    public PostgresRelationEvidence {
        if (!hash(checkpointSha256) || !hash(recordedSchemaSha256) || !validLsn(transactionFinalLsn)
                || relationOid < 1 || relationOid > MAX_OID || !identifier(namespace) || !identifier(relation)
                || replicaIdentity == null || replicaIdentity.length() != 1 || "dfin".indexOf(replicaIdentity.charAt(0)) < 0
                || recordedTableId == null || recordedTableId.isEmpty() || recordedTableId.length() > 64
                || recordedSchemaVersion < 1 || columns == null || columns.size() > MAX_COLUMNS) {
            throw new IllegalArgumentException("POSTGRESQL_RELATION_EVIDENCE_INVALID");
        }
        columns = List.copyOf(columns);
        Set<String> names = new HashSet<>();
        for (Column column : columns) {
            if (!names.add(column.name())) throw new IllegalArgumentException("POSTGRESQL_RELATION_EVIDENCE_INVALID");
        }
    }

    static boolean binds(PostgresRelationEvidence evidence, SourcePosition checkpoint) {
        if (evidence == null || checkpoint == null || !"POSTGRESQL".equals(checkpoint.getSourceType())) return false;
        String priorLsn = checkpoint.getPayload().get("lsn");
        return validLsn(priorLsn)
                && Long.compareUnsigned(parse(evidence.transactionFinalLsn), parse(priorLsn)) > 0
                && evidence.checkpointSha256.equals(Codec.sha256(Codec.encode(checkpoint)));
    }

    private static long parse(String lsn) {
        int separator = lsn.indexOf('/');
        return Long.parseLong(lsn, 0, separator, 16) << 32
                | Long.parseLong(lsn, separator + 1, lsn.length(), 16);
    }

    private static boolean validLsn(String value) {
        return value != null && LSN.matcher(value).matches();
    }

    private static boolean hash(String value) {
        return value != null && SHA256.matcher(value).matches();
    }

    private static boolean identifier(String value) {
        return value != null && !value.isEmpty() && value.length() <= 255 && value.indexOf('\0') < 0;
    }

    public static PostgresRelationEvidence failedRelation(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof Blocked observed) return observed.evidence;
        }
        return null;
    }

    public static final class Blocked extends PipelineFailure {
        private final PostgresRelationEvidence evidence;

        public Blocked(PostgresRelationEvidence evidence) {
            super(Phase.DDL, new IllegalStateException(FAILURE_CODE));
            this.evidence = evidence;
        }
    }
}
