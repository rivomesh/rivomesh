package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.JdbcSink;
import io.github.rivomesh.cdc.PostgresAdoptionRead;
import io.github.rivomesh.cdc.PostgresExistingAdoption;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;

import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Observation and approval of an existing PostgreSQL baseline. */
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
        PipelineConfig.Target target = profile(config, targetId);
        try (MysqlUniqueFreeze source = MysqlUniqueFreeze.current(config, MAX_MILLIS);
             PostgresAdoptionRead destination = JdbcSink.adoptionRead(config.targetSettings(target, "adoption-plan"), MAX_MILLIS)) {
            Observation observed = observe(config, target, source, destination, expectedDigest, null);
            return Map.of("status", "ADOPTION_PLAN_OBSERVED", "observedAt", Instant.now().toString(),
                    "expectedDigestMatched", expectedDigest != null, "planSha256", observed.digest, "plan", observed.plan);
        } catch (Exception error) {
            throw sanitized(error);
        }
    }

    static Map<String,Object> execute(PipelineConfig config, String targetId, String table, String digest,
                                      String requestId, String actor, boolean exclusiveFreeze) throws Exception {
        if (!exclusiveFreeze) throw failure("ADOPTION_EXCLUSIVE_FREEZE_ACK_REQUIRED");
        PipelineConfig.Target target = profile(config, targetId);
        PostgresExistingAdoption.Approval approval = new PostgresExistingAdoption.Approval(table, digest, requestId, actor);
        PostgresExistingAdoption.Receipt receipt = null;
        boolean replayed = false;
        try (JdbcJournal journal = new JdbcJournal(config.existingJournalSettings());
             PostgresExistingAdoption bootstrap = JdbcSink.adoptionBootstrap(
                     config.targetSettings(target, "adoption-install"), journal, MAX_MILLIS)) {
            bootstrap.lock(approval);
            PostgresExistingAdoption.Replay replay = bootstrap.replay();
            if (replay != null) {
                PipelineAdoptionPreview.RowComparison rows = comparison(replay.table());
                bootstrap.observation().scan(replay.table(), MAX_ROW_BYTES, rows::source);
                if (rows.sourceRows() != replay.receipt().rows() || !rows.sourceDigest().equals(replay.receipt().rowsSha256())) {
                    throw failure("ADOPTION_BOOTSTRAP_ROWS_CHANGED");
                }
                receipt = replay.receipt();
                replayed = true;
            } else {
                try (MysqlUniqueFreeze source = MysqlUniqueFreeze.current(config, MAX_MILLIS)) {
                    Observation observed = observe(config, target, source, bootstrap.observation(), digest, table);
                    receipt = bootstrap.install(observed.table, observed.cut, observed.plan, observed.digest,
                            () -> verifyExecutionFreeze(source));
                }
            }
        } catch (Exception error) {
            if (receipt == null) throw sanitized(error);
        }
        return Map.of("status", "EXISTING_TARGET_ADOPTED", "acknowledged", true, "replayed", replayed,
                "resumeErrorGeneration", 1, "receipt", receipt);
    }

    private static PipelineConfig.Target profile(PipelineConfig config, String targetId) {
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

        return target;
    }

    private record Observation(TableSchema table, SourcePosition cut, Map<String,Object> plan, String digest) {}

    private static Observation observe(PipelineConfig config, PipelineConfig.Target target, MysqlUniqueFreeze source,
                                       PostgresAdoptionRead destination, String expectedDigest, String lockedTable) throws Exception {
        String database = config.databases.getFirst();
        String schema = target.mappings.get(database);
        source.verifyDurable();
        TableSchema table = source.singleTable(database);
        requireShape(table);
        if (lockedTable != null && !lockedTable.equals(table.name)) throw failure("ADOPTION_PLAN_CHANGED");
        if (destination.pin(List.of(table))) throw failure("ADOPTION_TARGET_CHECKPOINT_EXISTS");
        String targetEvidence = destination.planEvidence(table, config.journal.namespace);
        PipelineAdoptionPreview.RowComparison rows = comparison(table);
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
        plan.put("targetId", target.id);
        plan.put("targetSchema", schema);
        plan.put("targetMetadataSchema", target.metadataSchema);
        plan.put("journalNamespace", config.journal.namespace);
        plan.put("targetCatalogSha256", targetEvidence);
        plan.put("matchedRows", rows.sourceRows());
        plan.put("matchedRowsSha256", rowRoot);
        String digest = Codec.sha256(Codec.encode(plan));
        if (expectedDigest != null && !expectedDigest.equals(digest)) throw failure("ADOPTION_PLAN_CHANGED");

        return new Observation(table, source.boundary(), plan, digest);
    }

    private static PipelineAdoptionPreview.RowComparison comparison(TableSchema table) {
        return new PipelineAdoptionPreview.RowComparison(table, MAX_ROWS, MAX_ROW_BYTES, MAX_RETAINED_BYTES);
    }

    private static void verifyExecutionFreeze(MysqlUniqueFreeze source) {
        try {
            source.verifyDurable();
        } catch (IllegalStateException rejection) {
            throw rejection;
        } catch (SQLException | RuntimeException lost) {
            throw failure("ADOPTION_SOURCE_FENCE_LOST");
        }
    }

    private static IllegalStateException sanitized(Exception error) {
        if (error instanceof SQLException sql) return failure("SQL_" + sql.getSQLState());
        if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        String code = error.getMessage();
        if (code != null && code.startsWith("UNIQUE_RESTORE_SOURCE_")) {
            code = "ADOPTION_PLAN_" + code.substring("UNIQUE_RESTORE_".length());
        }
        return failure(code);
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
