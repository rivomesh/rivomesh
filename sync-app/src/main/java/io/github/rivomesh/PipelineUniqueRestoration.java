package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.JdbcSink;
import io.github.rivomesh.cdc.PausedRepairFence;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;
import io.github.rivomesh.recovery.UniqueRestoreOperation;
import io.github.rivomesh.repair.RepairProbe;
import io.github.rivomesh.validation.ValidationStore;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Review and explicit restoration of one verified PostgreSQL business UNIQUE. */
final class PipelineUniqueRestoration {
    private static final long MAXIMUM_MILLIS = 30_000;
    private static final long MAXIMUM_BYTES = 16L * 1024 * 1024;
    private static final int MAXIMUM_ROWS = 10_000;
    private static final int MAXIMUM_ROW_BYTES = 1024 * 1024;

    private PipelineUniqueRestoration() {}

    static Map<String,Object> plan(PipelineConfig config, Path reports, String reportId, String targetId,
                                   String tableId, String sourceIndex, String expectedHash) throws Exception {
        PipelineConfig.Target target = target(config, targetId, tableId, sourceIndex, expectedHash);
        try (ValidationStore report = ValidationStore.open(reports, reportId)) {
            Review review = review(report, target, tableId);
            PipelineValidationBarrier.Certificate cut = review.manifest.certificate;
            PipelineValidationBarrier.verifyCertificate(config, cut);
            JdbcSink.UniqueRestoreObservation observation;
            try (JdbcJournal journal = new JdbcJournal(config.existingJournalSettings());
                 RepairProbe probe = probe(reports, reportId, review.table)) {
                journal.verifyIdentity(config.fingerprint(), config.subscribers());
                try (PausedRepairFence fence = new PausedRepairFence(journal, cut, MAXIMUM_MILLIS);
                     JdbcSink sink = JdbcSink.existing(config.targetSettings(target, cut.generation))) {
                    if (PipelineConstraintReview.exclusionsRecorded(journal, targetId, cut.sequence)) {
                        throw failure("UNIQUE_RESTORE_EXCLUSIONS_RECORDED");
                    }
                    try (MysqlUniqueFreeze source = new MysqlUniqueFreeze(config, cut.sourcePosition, MAXIMUM_MILLIS)) {
                        scanSource(source, probe, review);
                        observation = sink.planUniqueRestoration(cut.targetCheckpoints.get(targetId), review.table, sourceIndex,
                                fence, probe, MAXIMUM_ROW_BYTES, MAXIMUM_MILLIS);
                        verifyTargetRows(observation, review);
                        source.verify();
                        fence.verify();
                    }
                }
            }
            PipelineValidationBarrier.verifyCertificate(config, cut);
            Map<String,Object> approvedFields = planFields(config, reportId, targetId, sourceIndex, review, observation);
            String digest = verifyDigest(approvedFields, expectedHash);
            return Map.of("status", "RESTORE_PLAN_OBSERVED", "observedAt", Instant.now().toString(),
                    "plan", approvedFields, "planSha256", digest, "expectedDigestMatched", expectedHash != null);
        } catch (SyncFailure failure) {
            throw failure(failure.code);
        } catch (SQLException failure) {
            throw failure("SQL_" + failure.getSQLState());
        }
    }

    static Map<String,Object> execute(PipelineConfig config, Path reports, String reportId, String targetId,
                                      String tableId, String sourceIndex, String expectedHash, String requestId,
                                      String actor, boolean acknowledgedExclusiveFreeze) throws Exception {
        if (!acknowledgedExclusiveFreeze) throw failure("UNIQUE_RESTORE_EXCLUSIVE_FREEZE_ACK_REQUIRED");
        if (expectedHash == null) throw failure("UNIQUE_RESTORE_DIGEST_INVALID");
        PipelineConfig.Target target = target(config, targetId, tableId, sourceIndex, expectedHash);
        if (!"EXTERNAL".equals(config.journal.mode) || config.journal.product != TargetProduct.POSTGRESQL
                || config.targets.size() != 1 || !target.url.equals(config.journal.url)) {
            throw failure("UNIQUE_RESTORE_SHARED_DATABASE_REQUIRED");
        }
        JdbcSink.UniqueRestoreExecution result = null;
        try {
            Review review;
            try (ValidationStore report = ValidationStore.open(reports, reportId)) {
                review = review(report, target, tableId);
            }
            PipelineValidationBarrier.Certificate cut = review.manifest.certificate;
            if (!config.fingerprint().equals(cut.fingerprint)) throw failure("VALIDATION_CONFIG_MISMATCH");
            UniqueRestoreOperation.Approval approval = new UniqueRestoreOperation.Approval(config.pipelineId,
                    targetId, cut.generation, cut.fingerprint, reportId, tableId, sourceIndex, expectedHash, requestId, actor);
            try (JdbcSink sink = JdbcSink.existing(config.targetSettings(target, cut.generation))) {
                result = sink.replayUniqueRestoration(review.table, approval, MAXIMUM_MILLIS);
                if (result == null) {
                    PipelineValidationBarrier.verifyCertificate(config, cut);
                    try (JdbcJournal journal = new JdbcJournal(config.existingJournalSettings());
                         RepairProbe probe = probe(reports, reportId, review.table);
                         MysqlUniqueFreeze source = new MysqlUniqueFreeze(config, cut.sourcePosition, MAXIMUM_MILLIS)) {
                        source.verifyDurable();
                        scanSource(source, probe, review);
                        result = sink.executeUniqueRestoration(journal, cut, review.table, approval, probe,
                                MAXIMUM_ROW_BYTES, MAXIMUM_MILLIS, observed -> {
                                    verifyTargetRows(observed, review);
                                    verifyDigest(planFields(config, reportId, targetId, sourceIndex, review, observed), expectedHash);
                                    verifyExecutionFreeze(source);
                                });
                    }
                }
            }
        } catch (InterruptedException interruption) {
            Thread.currentThread().interrupt();
            if (result == null) throw failure("UNIQUE_RESTORE_INTERRUPTED");
        } catch (Exception rejected) {
            if (result == null) {
                if (rejected instanceof SyncFailure known) throw failure(known.code);
                if (rejected instanceof SQLException sql) throw failure("SQL_" + sql.getSQLState());
                if (rejected instanceof IllegalStateException || rejected instanceof IllegalArgumentException) {
                    throw failure(rejected.getMessage());
                }
                throw failure("UNIQUE_RESTORE_EXECUTION_FAILED");
            }
        }
        return Map.of("status", "UNIQUE_RESTORED", "acknowledged", true, "replayed", result.replayed(),
                "receipt", result.receipt());
    }

    private record Review(ValidationStore.Manifest manifest, TableSchema table, ValidationStore.TableResult expected) {}

    private static Review review(ValidationStore report, PipelineConfig.Target target, String tableId) throws Exception {
        ValidationStore.Manifest manifest = report.manifest();
        Set<String> scope = manifest.targetTables.get(target.id);
        TableSchema table = manifest.schemas.get(tableId);
        if (!"COMPLETE".equals(manifest.state) || scope == null || !scope.contains(tableId) || table == null
                || !target.mappings.containsKey(table.database) || table.primaryKey.isEmpty()) {
            throw failure("UNIQUE_RESTORE_REQUIRES_KEYED_REPORT_TABLE");
        }
        report.verifySeal();
        if (!PipelineConstraintReview.historicalMatch(manifest, target.id, scope)) {
            throw failure("UNIQUE_RESTORE_REQUIRES_MATCHED_REPORT");
        }
        ValidationStore.TableResult expected = manifest.results.get(target.id + "/" + tableId);
        if (expected.sourceRows < 0 || expected.sourceRows > MAXIMUM_ROWS || expected.sourceRoot == null
                || !expected.sourceRoot.equals(manifest.sourceRoots.get(tableId))) {
            throw failure("UNIQUE_RESTORE_REPORT_SCOPE_UNSUPPORTED");
        }
        return new Review(manifest, table, expected);
    }

    private static PipelineConfig.Target target(PipelineConfig config, String targetId, String tableId,
                                                 String sourceIndex, String expectedHash) {
        if (!tableId.matches("[a-f0-9]{64}") || sourceIndex.isEmpty() || sourceIndex.length() > 64) {
            throw failure("UNIQUE_RESTORE_SCOPE_INVALID");
        }
        if (expectedHash != null && !expectedHash.matches("[a-f0-9]{64}")) {
            throw failure("UNIQUE_RESTORE_DIGEST_INVALID");
        }
        if (!"MYSQL".equals(config.effectiveSourceType()) || !"AUTO".equals(config.source.topology)
                || !config.source.nodes.isEmpty()) throw failure("UNIQUE_RESTORE_SOURCE_PROFILE_UNSUPPORTED");
        PipelineConfig.Target target = config.targets.stream().filter(item -> item.id.equals(targetId))
                .findFirst().orElseThrow(() -> failure("UNIQUE_RESTORE_TARGET_UNKNOWN"));
        if (target.product != TargetProduct.POSTGRESQL) throw failure("UNIQUE_RESTORE_TARGET_PROFILE_UNSUPPORTED");
        return target;
    }

    private static RepairProbe probe(Path reports, String reportId, TableSchema table) throws Exception {
        return new RepairProbe(reports.resolve(reportId).resolve("unique-restore-scratch"),
                table, MAXIMUM_BYTES, MAXIMUM_ROWS, MAXIMUM_ROW_BYTES);
    }

    private static void verifyExecutionFreeze(MysqlUniqueFreeze source) {
        try {
            source.verifyDurable();
        } catch (IllegalStateException verifiedRejection) {
            throw verifiedRejection;
        } catch (SQLException | RuntimeException lost) {
            throw failure("UNIQUE_RESTORE_SOURCE_FENCE_LOST");
        }
    }

    private static void scanSource(MysqlUniqueFreeze source, RepairProbe probe, Review review) throws Exception {
        RepairProbe.Result current = source.scan(review.table, probe);
        if (current.rows != review.expected.sourceRows || !review.expected.sourceRoot.equals(current.root)) {
            throw failure("UNIQUE_RESTORE_SOURCE_CONTENT_CHANGED");
        }
        probe.reset();
    }

    private static void verifyTargetRows(JdbcSink.UniqueRestoreObservation observation, Review review) {
        if (observation.rows() != review.expected.targetRows || !review.expected.targetRoot.equals(observation.root())) {
            throw failure("UNIQUE_RESTORE_TARGET_CONTENT_CHANGED");
        }
    }

    private static Map<String,Object> planFields(PipelineConfig config, String reportId, String targetId, String sourceIndex,
                                                 Review review, JdbcSink.UniqueRestoreObservation observation) {
        PipelineValidationBarrier.Certificate cut = review.manifest.certificate;
        Map<String,Object> approvedFields = new LinkedHashMap<>();
        approvedFields.put("formatVersion", 2);
        approvedFields.put("pipelineId", config.pipelineId);
        approvedFields.put("reportId", reportId);
        approvedFields.put("targetId", targetId);
        approvedFields.put("tableId", review.table.id);
        approvedFields.put("sourceIndex", sourceIndex);
        approvedFields.put("manifestSeal", review.manifest.sourceSeal);
        approvedFields.put("cutSha256", Codec.sha256(Codec.encode(cut)));
        approvedFields.put("sourcePositionSha256", Codec.sha256(Codec.encode(cut.sourcePosition)));
        approvedFields.put("targetCheckpointSha256", Codec.sha256(Codec.encode(cut.targetCheckpoints.get(targetId))));
        approvedFields.put("epoch", cut.epoch);
        approvedFields.put("sequence", Long.toString(cut.sequence));
        approvedFields.put("journalMode", config.journal.mode);
        approvedFields.put("journalNamespace", config.journal.namespace);
        approvedFields.put("nullPolicy", "NULLS_DISTINCT");
        approvedFields.put("action", "ADD_UNIQUE_RETAIN_ORDINARY_INDEX");
        approvedFields.put("targetEvidence", observation);
        return approvedFields;
    }

    private static String verifyDigest(Map<String,Object> approvedFields, String expectedHash) {
        String digest = Codec.sha256(Codec.encode(approvedFields));
        if (expectedHash != null && !expectedHash.equals(digest)) throw failure("UNIQUE_RESTORE_PLAN_CHANGED");
        return digest;
    }

    private static IllegalStateException failure(String code) {
        return new IllegalStateException(code != null && code.matches("[A-Z][A-Z0-9_]{2,95}")
                ? code : "UNIQUE_RESTORE_PLAN_FAILED");
    }
}
