package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcSink;
import io.github.rivomesh.cdc.PostgresAdoptionRead;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;

import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Repeatable approval input for a future atomic PostgreSQL baseline bootstrap. */
final class PipelineAdoptionPlan {
    private static final int MAX_ROWS = 10_000;
    private static final int MAX_ROW_BYTES = 1024 * 1024;
    private static final long MAX_RETAINED_BYTES = 16L * 1024 * 1024;
    private static final long MAX_MILLIS = 30_000;

    private PipelineAdoptionPlan() {}

    static Map<String,Object> inspect(PipelineConfig config, String targetId, String expectedDigest) throws Exception {
        if (expectedDigest != null && !expectedDigest.matches("[a-f0-9]{64}")) {
            throw failure("ADOPTION_PLAN_DIGEST_INVALID");
        }
        PipelineConfig.Target target = config.targets.stream().filter(candidate -> candidate.id.equals(targetId))
                .findFirst().orElseThrow(() -> failure("ADOPTION_TARGET_UNKNOWN"));
        if (!"MYSQL".equals(config.effectiveSourceType()) || !"AUTO".equals(config.source.topology)
                || !config.source.nodes.isEmpty() || config.databases.size() != 1) {
            throw failure("ADOPTION_PLAN_SOURCE_PROFILE_UNSUPPORTED");
        }
        String database = config.databases.getFirst();
        String schema = target.mappings.get(database);
        if (config.targets.size() != 1 || target.product != TargetProduct.POSTGRESQL || schema == null
                || !"EXTERNAL".equals(config.journal.mode) || config.journal.product != TargetProduct.POSTGRESQL
                || !target.url.equals(config.journal.url) || config.journal.writerAssignment != null
                || schema.equalsIgnoreCase(config.journal.namespace)
                || target.metadataSchema.equalsIgnoreCase(config.journal.namespace)) {
            throw failure("ADOPTION_PLAN_ATOMIC_BOOTSTRAP_PROFILE_REQUIRED");
        }

        try (MysqlUniqueFreeze source = MysqlUniqueFreeze.current(config, MAX_MILLIS);
             PostgresAdoptionRead destination = JdbcSink.adoptionRead(config.targetSettings(target, "adoption-plan"), MAX_MILLIS)) {
            source.verifyDurable();
            TableSchema table = source.singleTable(database);
            requireShape(table);
            if (destination.pin(List.of(table))) throw failure("ADOPTION_TARGET_CHECKPOINT_EXISTS");
            String targetEvidence = destination.planEvidence(table, config.journal.namespace);

            PipelineAdoptionPreview.RowComparison rows = new PipelineAdoptionPreview.RowComparison(table,
                    MAX_ROWS, MAX_ROW_BYTES, MAX_RETAINED_BYTES);
            source.scanRows(table, MAX_ROW_BYTES, MAX_ROWS, rows::source);
            String rowRoot = rows.sourceDigest();
            destination.scan(table, MAX_ROW_BYTES, rows::target);
            if (rows.sourceRows() == 0) throw failure("ADOPTION_PLAN_SOURCE_EMPTY");
            if (!rows.matched()) throw failure("ADOPTION_PLAN_ROWS_DIFFER");
            destination.verify();
            source.verifyDurable();

            Map<String,Object> plan = new LinkedHashMap<>();
            plan.put("formatVersion", 1);
            plan.put("profile", "MYSQL84_POSTGRESQL17_UNCLAIMED_SINGLE_TABLE");
            plan.put("pipelineId", config.pipelineId);
            plan.put("configurationSha256", config.fingerprint());
            plan.put("sourceTable", table.name);
            plan.put("sourceSchema", database);
            plan.put("sourcePositionSha256", Codec.sha256(Codec.encode(source.boundary())));
            plan.put("sourceSchemaSha256", Codec.sha256(Codec.encode(table)));
            plan.put("targetId", targetId);
            plan.put("targetSchema", schema);
            plan.put("targetMetadataSchema", target.metadataSchema);
            plan.put("journalNamespace", config.journal.namespace);
            plan.put("targetCatalogSha256", targetEvidence);
            plan.put("matchedRows", rows.sourceRows());
            plan.put("matchedRowsSha256", rowRoot);
            String digest = Codec.sha256(Codec.encode(plan));
            if (expectedDigest != null && !expectedDigest.equals(digest)) throw failure("ADOPTION_PLAN_CHANGED");
            return Map.of("status", "ADOPTION_PLAN_OBSERVED", "observedAt", Instant.now().toString(),
                    "expectedDigestMatched", expectedDigest != null, "planSha256", digest, "plan", plan);
        } catch (SQLException error) {
            throw failure("SQL_" + error.getSQLState());
        } catch (IllegalArgumentException | IllegalStateException error) {
            String code = error.getMessage();
            if (code != null && code.startsWith("UNIQUE_RESTORE_SOURCE_")) {
                code = "ADOPTION_PLAN_" + code.substring("UNIQUE_RESTORE_".length());
            }
            throw failure(code);
        }
    }

    private static void requireShape(TableSchema table) {
        if (table.primaryKey.size() != 1 || table.indexes.stream().anyMatch(index -> !index.primary
                || !"BTREE".equalsIgnoreCase(index.method) || index.descending.stream().anyMatch(Boolean.TRUE::equals))) {
            throw failure("ADOPTION_PLAN_SOURCE_INDEX_UNSUPPORTED");
        }
        for (TableSchema.Column column : table.columns) {
            if (table.primaryKey.contains(column.name) && !"bigint".equals(column.sourceType)) {
                throw failure("ADOPTION_PLAN_SOURCE_INDEX_UNSUPPORTED");
            }
            boolean supported = switch (column.type) {
                case INT64, JSON -> true;
                case STRING -> column.sourceType.startsWith("varchar(") && column.length > 0 && column.length <= 4000
                        && "utf8mb4_0900_bin".equals(column.collation);
                default -> false;
            };
            if (!supported || column.generationKind != null) throw failure("ADOPTION_PLAN_SOURCE_TYPE_UNSUPPORTED");
        }
    }

    private static IllegalStateException failure(String code) {
        return new IllegalStateException(code != null && code.matches("[A-Z][A-Z0-9_]{2,95}")
                ? code : "ADOPTION_PLAN_FAILED");
    }
}
