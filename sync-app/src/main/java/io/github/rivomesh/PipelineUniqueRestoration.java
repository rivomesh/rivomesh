package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.JdbcSink;
import io.github.rivomesh.cdc.PausedRepairFence;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;
import io.github.rivomesh.repair.RepairProbe;
import io.github.rivomesh.validation.ValidationStore;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Evidence for one prospective UNIQUE. It has no action that changes either database. */
final class PipelineUniqueRestoration {
    private static final long MAXIMUM_MILLIS = 30_000;
    private static final long MAXIMUM_BYTES = 16L * 1024 * 1024;
    private static final int MAXIMUM_ROWS = 10_000;
    private static final int MAXIMUM_ROW_BYTES = 1024 * 1024;

    private PipelineUniqueRestoration() {}

    static Map<String,Object> plan(PipelineConfig config, Path reports, String reportId, String targetId,
                                   String tableId, String sourceIndex, String expectedHash) throws Exception {
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

        try (ValidationStore report = ValidationStore.open(reports, reportId)) {
            ValidationStore.Manifest manifest = report.manifest();
            Set<String> scope = manifest.targetTables.get(targetId);
            TableSchema table = manifest.schemas.get(tableId);
            if (!"COMPLETE".equals(manifest.state) || scope == null || !scope.contains(tableId) || table == null
                    || !target.mappings.containsKey(table.database) || table.primaryKey.isEmpty()) {
                throw failure("UNIQUE_RESTORE_REQUIRES_KEYED_REPORT_TABLE");
            }
            report.verifySeal();
            if (!PipelineConstraintReview.historicalMatch(manifest, targetId, scope)) {
                throw failure("UNIQUE_RESTORE_REQUIRES_MATCHED_REPORT");
            }
            ValidationStore.TableResult expected = manifest.results.get(targetId + "/" + tableId);
            if (expected.sourceRows < 0 || expected.sourceRows > MAXIMUM_ROWS || expected.sourceRoot == null
                    || !expected.sourceRoot.equals(manifest.sourceRoots.get(tableId))) {
                throw failure("UNIQUE_RESTORE_REPORT_SCOPE_UNSUPPORTED");
            }
            PipelineValidationBarrier.Certificate cut = manifest.certificate;
            PipelineValidationBarrier.verifyCertificate(config, cut);
            JdbcSink.UniqueRestoreObservation observation;
            try (JdbcJournal journal = new JdbcJournal(config.existingJournalSettings());
                 RepairProbe probe = new RepairProbe(reports.resolve(reportId).resolve("unique-restore-scratch"),
                         table, MAXIMUM_BYTES, MAXIMUM_ROWS, MAXIMUM_ROW_BYTES)) {
                journal.verifyIdentity(config.fingerprint(), config.subscribers());
                try (PausedRepairFence fence = new PausedRepairFence(journal, cut, MAXIMUM_MILLIS);
                     JdbcSink sink = JdbcSink.existing(config.targetSettings(target, cut.generation))) {
                    if (PipelineConstraintReview.exclusionsRecorded(journal, targetId, cut.sequence)) {
                        throw failure("UNIQUE_RESTORE_EXCLUSIONS_RECORDED");
                    }
                    try (MysqlUniqueFreeze source = new MysqlUniqueFreeze(config, cut.sourcePosition, MAXIMUM_MILLIS)) {
                        RepairProbe.Result current = source.scan(table, probe);
                        if (current.rows != expected.sourceRows || !expected.sourceRoot.equals(current.root)) {
                            throw failure("UNIQUE_RESTORE_SOURCE_CONTENT_CHANGED");
                        }
                        probe.reset();
                        observation = sink.planUniqueRestoration(cut.targetCheckpoints.get(targetId), table, sourceIndex,
                                fence, probe, MAXIMUM_ROW_BYTES, MAXIMUM_MILLIS);
                        if (observation.rows() != expected.targetRows || !expected.targetRoot.equals(observation.root())) {
                            throw failure("UNIQUE_RESTORE_TARGET_CONTENT_CHANGED");
                        }
                        source.verify();
                        fence.verify();
                    }
                }
            }
            PipelineValidationBarrier.verifyCertificate(config, cut);
            Map<String,Object> approvedFields = new LinkedHashMap<>();
            approvedFields.put("formatVersion", 1);
            approvedFields.put("pipelineId", config.pipelineId);
            approvedFields.put("reportId", reportId);
            approvedFields.put("targetId", targetId);
            approvedFields.put("tableId", tableId);
            approvedFields.put("sourceIndex", sourceIndex);
            approvedFields.put("manifestSeal", manifest.sourceSeal);
            approvedFields.put("cutSha256", Codec.sha256(Codec.encode(cut)));
            approvedFields.put("sourcePositionSha256", Codec.sha256(Codec.encode(cut.sourcePosition)));
            approvedFields.put("targetCheckpointSha256", Codec.sha256(Codec.encode(cut.targetCheckpoints.get(targetId))));
            approvedFields.put("epoch", cut.epoch);
            approvedFields.put("sequence", Long.toString(cut.sequence));
            approvedFields.put("nullPolicy", "NULLS_DISTINCT");
            approvedFields.put("action", "ADD_UNIQUE_RETAIN_ORDINARY_INDEX");
            approvedFields.put("targetEvidence", observation);
            String digest = Codec.sha256(Codec.encode(approvedFields));
            if (expectedHash != null && !expectedHash.equals(digest)) {
                throw failure("UNIQUE_RESTORE_PLAN_CHANGED");
            }
            return Map.of("status", "RESTORE_PLAN_OBSERVED", "observedAt", Instant.now().toString(),
                    "plan", approvedFields, "planSha256", digest, "expectedDigestMatched", expectedHash != null);
        } catch (SyncFailure failure) {
            throw failure(failure.code);
        } catch (SQLException failure) {
            throw failure("SQL_" + failure.getSQLState());
        }
    }

    private static IllegalStateException failure(String code) {
        return new IllegalStateException(code != null && code.matches("[A-Z][A-Z0-9_]{2,95}")
                ? code : "UNIQUE_RESTORE_PLAN_FAILED");
    }
}
