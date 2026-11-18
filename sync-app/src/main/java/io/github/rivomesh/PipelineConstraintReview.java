package io.github.rivomesh;

import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.JdbcSink;
import io.github.rivomesh.cdc.PausedRepairFence;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;
import io.github.rivomesh.validation.ValidationStore;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Inspect business UNIQUE candidates while the existing common cut is still paused. */
final class PipelineConstraintReview {
    private static final long MAXIMUM_MILLIS = 120_000;

    private PipelineConstraintReview() {}

    static Map<String,Object> inspect(PipelineConfig config, Path reports, String reportId, String targetId) throws Exception {
        PipelineConfig.Target target = config.targets.stream().filter(item -> item.id.equals(targetId))
                .findFirst().orElseThrow(() -> reject("UNIQUE_REVIEW_TARGET_UNKNOWN"));
        if (target.product != TargetProduct.POSTGRESQL) throw reject("UNIQUE_REVIEW_TARGET_UNSUPPORTED");

        try (ValidationStore report = ValidationStore.open(reports, reportId)) {
            ValidationStore.Manifest manifest = report.manifest();
            if (!"COMPLETE".equals(manifest.state)) throw reject("UNIQUE_REVIEW_REQUIRES_COMPLETE_REPORT");
            report.verifySeal();
            PipelineValidationBarrier.Certificate cut = manifest.certificate;
            PipelineValidationBarrier.verifyCertificate(config, cut);
            List<TableSchema> tables = manifest.schemas.values().stream()
                    .filter(table -> target.mappings.containsKey(table.database)).toList();
            Set<String> tableIds = tables.stream().map(table -> table.id).collect(Collectors.toCollection(TreeSet::new));
            if (tableIds.isEmpty() || !tableIds.equals(manifest.targetTables.get(targetId))) {
                throw reject("UNIQUE_REVIEW_TARGET_SCOPE_MISMATCH");
            }
            boolean matchedOnReport = historicalMatch(manifest, targetId, tableIds);
            List<JdbcSink.UniqueCheck> findings;
            boolean exclusionsRecorded;
            try (JdbcJournal journal = new JdbcJournal(config.existingJournalSettings())) {
                journal.verifyIdentity(config.fingerprint(), config.subscribers());
                try (PausedRepairFence fence = new PausedRepairFence(journal, cut, MAXIMUM_MILLIS);
                     JdbcSink sink = JdbcSink.existing(config.targetSettings(target, cut.generation))) {
                    exclusionsRecorded = exclusionsRecorded(journal, targetId, cut.sequence);
                    findings = sink.reviewUniqueConstraints(cut.targetCheckpoints.get(targetId), tables, fence, MAXIMUM_MILLIS);
                    fence.verify();
                }
            }
            PipelineValidationBarrier.verifyCertificate(config, cut);
            Map<String,Object> result = new LinkedHashMap<>();
            result.put("reportId", reportId);
            result.put("targetId", targetId);
            result.put("reviewedAt", Instant.now().toString());
            result.put("epoch", cut.epoch);
            result.put("sequence", Long.toString(cut.sequence));
            result.put("historicalReportMatched", matchedOnReport);
            result.put("exclusionsRecorded", exclusionsRecorded);
            result.put("nullPolicy", "NULLS_DISTINCT");
            result.put("status", status(findings, matchedOnReport, exclusionsRecorded));
            result.put("indexes", findings);
            return result;
        } catch (SyncFailure failure) {
            throw reject(failure.code);
        } catch (SQLException failure) {
            throw reject("SQL_" + failure.getSQLState());
        }
    }

    private static boolean historicalMatch(ValidationStore.Manifest manifest, String target, Set<String> tableIds) {
        boolean matches = true;
        for (String id : tableIds) {
            ValidationStore.TableResult result = manifest.results.get(target + "/" + id);
            if (result == null || !target.equals(result.target) || !id.equals(result.tableId)) {
                throw reject("UNIQUE_REVIEW_REPORT_RESULTS_MISSING");
            }
            matches &= result.matches() && result.sourceRows == result.targetRows
                    && Objects.equals(result.sourceRoot, result.targetRoot);
        }
        return matches;
    }

    private static boolean exclusionsRecorded(JdbcJournal journal, String target, long sequence) throws Exception {
        String expected = Long.toString(sequence);
        Map<String,Object> progress = journal.deliveryProgress();
        if (!expected.equals(progress.get("capturedSequence")) || !(progress.get("subscriptions") instanceof Map<?,?> subscriptions)
                || !(subscriptions.get(target) instanceof Map<?,?> subscription)
                || !expected.equals(subscription.get("safeCoveredSequence"))) {
            throw reject("UNIQUE_REVIEW_TARGET_NOT_AT_CUT");
        }
        return subscription.get("firstSkippedSequence") != null
                || subscription.get("firstSourceExclusionAfterSequence") != null;
    }

    private static String status(List<JdbcSink.UniqueCheck> checks, boolean historicalMatch, boolean exclusionsRecorded) {
        if (checks.stream().anyMatch(check -> "DUPLICATES_FOUND".equals(check.state()))) return "DUPLICATES_FOUND";
        if (!historicalMatch || exclusionsRecorded
                || checks.stream().anyMatch(check -> "SPECIAL_INDEX_REQUIRES_PLAN".equals(check.state()))) {
            return "REQUIRES_RECONCILIATION";
        }
        return checks.isEmpty() ? "NO_DOWNGRADED_UNIQUE" : "NO_DUPLICATES_OBSERVED";
    }

    private static IllegalStateException reject(String code) {
        return new IllegalStateException(code != null && code.matches("[A-Z][A-Z0-9_]{2,95}")
                ? code : "UNIQUE_REVIEW_FAILED");
    }
}
