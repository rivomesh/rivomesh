package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;
import java.util.Map;

/** Stores the most recent observed Oracle DDL failure under its error generation. */
final class JdbcOracleDdlEvidence {
    private static final String NAME = "oracle_ddl_failure";
    private static final List<String> MISSING_TABLE = List.of("42P01", "42S02", "42102", "42104");

    private JdbcOracleDdlEvidence() {}

    static void initialize(JdbcJournal journal, Connection connection) throws SQLException {
        journal.ddl(connection, "CREATE TABLE IF NOT EXISTS " + journal.table(NAME)
                + " (pipeline_id VARCHAR(128) PRIMARY KEY,error_generation BIGINT NOT NULL,payload "
                + journal.text() + " NOT NULL,payload_hash VARCHAR(64) NOT NULL)");
        journal.requirePhysicalEvidenceTable(connection, NAME);
    }

    static void record(JdbcJournal journal, Connection connection, long errorGeneration,
                       String captured, OracleDdlEvidence evidence) throws SQLException {
        try {
            if (captured == null || !OracleDdlEvidence.binds(evidence, Codec.decode(captured, SourcePosition.class))) {
                return;
            }
        } catch (IllegalArgumentException unreadable) {
            return;
        }
        String payload = Codec.encode(evidence);
        Savepoint beforeEvidence = connection.setSavepoint();
        try {
            JdbcJournal.execute(connection, "DELETE FROM " + journal.table(NAME) + " WHERE pipeline_id=?",
                    journal.settings.pipelineId);
            JdbcJournal.execute(connection, "INSERT INTO " + journal.table(NAME)
                            + " (pipeline_id,error_generation,payload,payload_hash) VALUES (?,?,?,?)",
                    journal.settings.pipelineId, errorGeneration, payload, Codec.sha256(payload));
        } catch (SQLException unavailable) {
            // The optional evidence is absent rather than committing a partial replacement or losing the DDL stop.
            connection.rollback(beforeEvidence);
        }
        connection.releaseSavepoint(beforeEvidence);
    }

    static Map<String, Object> review(JdbcJournal journal, long generation, SourcePosition checkpoint) throws SQLException {
        try (Connection connection = journal.connect();
             PreparedStatement query = connection.prepareStatement("SELECT payload,payload_hash FROM "
                     + journal.table(NAME) + " WHERE pipeline_id=? AND error_generation=?")) {
            query.setQueryTimeout(5);
            query.setString(1, journal.settings.pipelineId);
            query.setLong(2, generation);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    return unavailable("ORACLE_DDL_EVIDENCE_UNRECORDED");
                }
                String payload = rows.getString(1);
                String hash = rows.getString(2);
                if (payload == null || payload.length() > 2048 || !Codec.sha256(payload).equals(hash) || rows.next()) {
                    return unavailable("ORACLE_DDL_EVIDENCE_INVALID");
                }
                try {
                    OracleDdlEvidence evidence = Codec.decode(payload, OracleDdlEvidence.class);
                    if (!OracleDdlEvidence.binds(evidence, checkpoint)) {
                        return unavailable("ORACLE_DDL_EVIDENCE_INVALID");
                    }
                    return Map.of("state", "OBSERVED", "dictionaryMode", "ONLINE_CATALOG",
                            "fingerprint", hash, "event", evidence);
                } catch (IllegalArgumentException invalid) {
                    return unavailable("ORACLE_DDL_EVIDENCE_INVALID");
                }
            }
        } catch (SQLException missing) {
            if (MISSING_TABLE.contains(missing.getSQLState())) {
                return unavailable("ORACLE_DDL_EVIDENCE_UNRECORDED");
            }
            throw missing;
        }
    }

    private static Map<String, Object> unavailable(String code) {
        return Map.of("state", "UNAVAILABLE", "code", code);
    }
}
