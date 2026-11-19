package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;

import java.math.BigInteger;

/** A complete Oracle LogMiner row seen by a failed scan, recorded without SQL text. */
public record OracleDdlEvidence(String checkpointSha256, String archiveStartScn, String archiveEndScnExclusive,
                                String eventScn, String recordSetId, String statementSequence,
                                Kind kind, String decodedSqlSha256) {
    public enum Kind { USER_DDL, INTERNAL_DDL }

    public OracleDdlEvidence {
        if (!sha256(checkpointSha256) || !sha256(decodedSqlSha256)
                || !scn(archiveStartScn) || !scn(archiveEndScnExclusive) || !scn(eventScn)
                || number(archiveStartScn).compareTo(number(eventScn)) > 0
                || number(eventScn).compareTo(number(archiveEndScnExclusive)) >= 0
                || recordSetId == null || !recordSetId.matches("0x[0-9A-Fa-f]{6}\\.[0-9A-Fa-f]{8}\\.[0-9A-Fa-f]{4}")
                || statementSequence == null || !statementSequence.matches("0|[1-9][0-9]{0,19}")
                || kind == null) {
            throw new IllegalArgumentException("ORACLE_DDL_EVIDENCE_INVALID");
        }
    }

    static boolean binds(OracleDdlEvidence evidence, SourcePosition checkpoint) {
        if (evidence == null || checkpoint == null || !"ORACLE".equals(checkpoint.getSourceType())) {
            return false;
        }
        String scn = checkpoint.getPayload().get("scn");
        String archiveFloor = checkpoint.getPayload().get("archiveStartScn");
        return scn(scn) && evidence.archiveStartScn.equals(archiveFloor)
                && number(archiveFloor).compareTo(number(scn)) <= 0
                && number(scn).compareTo(number(evidence.eventScn)) < 0
                && evidence.checkpointSha256.equals(Codec.sha256(Codec.encode(checkpoint)));
    }

    private static boolean scn(String value) {
        return value != null && value.matches("[1-9][0-9]{0,19}");
    }

    private static boolean sha256(String value) {
        return value != null && value.matches("[0-9a-f]{64}");
    }

    private static BigInteger number(String value) {
        return new BigInteger(value);
    }
}
