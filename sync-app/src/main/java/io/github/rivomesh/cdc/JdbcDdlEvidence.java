package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

/** Stores the latest source observation under the fenced capture failure generation. */
final class JdbcDdlEvidence<T> {
    private static final List<String> MISSING_TABLE = List.of("42P01", "42S02", "42102", "42104");

    static final JdbcDdlEvidence<OracleDdlEvidence> ORACLE = new JdbcDdlEvidence<>(
            "oracle_ddl_failure", "ORACLE_DDL_EVIDENCE", OracleDdlEvidence.class, 2048,
            "dictionaryMode", "ONLINE_CATALOG", OracleDdlEvidence::binds);
    static final JdbcDdlEvidence<PostgresRelationEvidence> POSTGRESQL = new JdbcDdlEvidence<>(
            "postgres_relation_failure", "POSTGRESQL_RELATION_EVIDENCE", PostgresRelationEvidence.class,
            2 * 1024 * 1024, "protocol", "PGOUTPUT_V1_RELATION", PostgresRelationEvidence::binds);

    private final String name, errorCode, modeKey, mode;
    private final Class<T> type;
    private final int payloadLimit;
    private final BiPredicate<T, SourcePosition> binds;

    private JdbcDdlEvidence(String name, String errorCode, Class<T> type, int payloadLimit,
                            String modeKey, String mode, BiPredicate<T, SourcePosition> binds) {
        this.name = name;
        this.errorCode = errorCode;
        this.type = type;
        this.payloadLimit = payloadLimit;
        this.modeKey = modeKey;
        this.mode = mode;
        this.binds = binds;
    }

    void initialize(JdbcJournal journal, Connection connection) throws SQLException {
        journal.ddl(connection, "CREATE TABLE IF NOT EXISTS " + journal.table(name)
                + " (pipeline_id VARCHAR(128) PRIMARY KEY,error_generation BIGINT NOT NULL,payload "
                + journal.text() + " NOT NULL,payload_hash VARCHAR(64) NOT NULL)");
        journal.requirePhysicalEvidenceTable(connection, name);
    }

    void record(JdbcJournal journal, Connection connection, long errorGeneration,
                       String captured, T evidence) throws SQLException {
        try {
            if (captured == null || !binds.test(evidence, Codec.decode(captured, SourcePosition.class))) {
                return;
            }
        } catch (IllegalArgumentException unreadable) {
            return;
        }
        String payload = Codec.encode(evidence);
        if (payload.length() > payloadLimit) return;
        Savepoint beforeEvidence = connection.setSavepoint();
        try {
            JdbcJournal.execute(connection, "DELETE FROM " + journal.table(name) + " WHERE pipeline_id=?",
                    journal.settings.pipelineId);
            JdbcJournal.execute(connection, "INSERT INTO " + journal.table(name)
                            + " (pipeline_id,error_generation,payload,payload_hash) VALUES (?,?,?,?)",
                    journal.settings.pipelineId, errorGeneration, payload, Codec.sha256(payload));
        } catch (SQLException unavailable) {
            // The optional evidence is absent rather than committing a partial replacement or losing the DDL stop.
            connection.rollback(beforeEvidence);
        }
        connection.releaseSavepoint(beforeEvidence);
    }

    Map<String, Object> review(JdbcJournal journal, long generation, SourcePosition checkpoint) throws SQLException {
        try (Connection connection = journal.connect();
             PreparedStatement query = connection.prepareStatement("SELECT payload,payload_hash FROM "
                     + journal.table(name) + " WHERE pipeline_id=? AND error_generation=?")) {
            query.setQueryTimeout(5);
            query.setString(1, journal.settings.pipelineId);
            query.setLong(2, generation);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    return unavailable("_UNRECORDED");
                }
                String payload = rows.getString(1);
                String hash = rows.getString(2);
                if (payload == null || payload.length() > payloadLimit || !Codec.sha256(payload).equals(hash) || rows.next()) {
                    return unavailable("_INVALID");
                }
                try {
                    T evidence = Codec.decode(payload, type);
                    if (!binds.test(evidence, checkpoint)) {
                        return unavailable("_INVALID");
                    }
                    return Map.of("state", "OBSERVED", modeKey, mode,
                            "fingerprint", hash, "event", evidence);
                } catch (IllegalArgumentException invalid) {
                    return unavailable("_INVALID");
                }
            }
        } catch (SQLException missing) {
            if (MISSING_TABLE.contains(missing.getSQLState())) {
                return unavailable("_UNRECORDED");
            }
            throw missing;
        }
    }

    private Map<String, Object> unavailable(String code) {
        return Map.of("state", "UNAVAILABLE", "code", errorCode + code);
    }
}
