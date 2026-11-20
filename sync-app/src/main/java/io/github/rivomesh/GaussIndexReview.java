package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.IndexPolicy;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Bounded advisory review of the decisions used by the Gauss JDBC sink. */
final class GaussIndexReview {
    private static final int MAX_ENTRIES_PER_GROUP = 24;
    private static final int MAX_ENTRY_BYTES_PER_GROUP = 8 * 1024;

    private GaussIndexReview() {}

    static Map<String, Object> inspect(String sourceType, List<PipelineConfig.Target> targets, List<TableSchema> tables) {
        var gaussTargets = targets.stream().filter(target -> target.product == TargetProduct.GAUSS).toList();
        if (gaussTargets.isEmpty()) return Map.of();
        boolean postgresCatalog = "POSTGRESQL".equals(sourceType) && !tables.isEmpty()
                && tables.stream().allMatch(table -> "true".equals(table.properties.get(PostgresIndexCatalog.CAPTURED)));
        if (!"MYSQL".equals(sourceType) && !postgresCatalog) {
            return Map.of("state", "SOURCE_INDEX_METADATA_UNAVAILABLE", "sourceType", sourceType);
        }

        var blocked = new Details();
        var planned = new Details();
        long tableMappings = 0;
        long supported = 0;
        long downgradedUnique = 0;
        long replacedGsi = 0;
        long requiresPlan = 0;
        long distributionPlanRequired = 0;
        for (PipelineConfig.Target target : gaussTargets) {
            for (TableSchema table : tables) {
                String targetSchema = target.mappings.get(table.database);
                if (targetSchema == null) continue;
                tableMappings++;
                for (IndexPolicy.Decision decision : IndexPolicy.plan(table, target.product, target.uniquePolicy)) {
                    boolean needsPlan = "REQUIRES_PLAN".equals(decision.action);
                    switch (decision.action) {
                        case "CREATE" -> supported++;
                        case "DOWNGRADE_UNIQUE" -> downgradedUnique++;
                        case "REPLACE_GSI" -> replacedGsi++;
                        case "REQUIRES_PLAN" -> {
                            requiresPlan++;
                            if ("GLOBAL_IDENTITY_REQUIRES_DISTRIBUTION_PLAN".equals(decision.reason)) distributionPlanRequired++;
                        }
                        default -> throw new IllegalStateException("GAUSS_INDEX_REVIEW_UNKNOWN_ACTION");
                    }
                    var details = needsPlan ? blocked : planned;
                    if (details.full()) continue;
                    details.add(new Entry(target.id, targetSchema, table.database, table.schema, table.name,
                            decision.sourceName, needsPlan ? null : decision.targetName, decision.action,
                            decision.reason, needsPlan ? null : decision.unique, decision.columns));
                }
            }
        }

        var entries = new ArrayList<>(blocked.entries);
        entries.addAll(planned.entries);
        return Map.of("state", requiresPlan > 0 ? "REQUIRES_PLAN" : "REVIEWED", "sourceType", sourceType,
                "tableMappings", tableMappings,
                "indexCounts", Map.of("supported", supported, "downgradedUnique", downgradedUnique,
                        "replacedGsi", replacedGsi, "requiresPlan", requiresPlan,
                        "distributionPlanRequired", distributionPlanRequired),
                "omittedEntries", supported + downgradedUnique + replacedGsi + requiresPlan - entries.size(),
                "entries", entries);
    }

    private static final class Details {
        private final List<Entry> entries = new ArrayList<>();
        private int remainingBytes = MAX_ENTRY_BYTES_PER_GROUP - 2; // Leave room for JSON array brackets.

        private boolean full() {
            return entries.size() == MAX_ENTRIES_PER_GROUP || remainingBytes == 0;
        }

        private void add(Entry entry) {
            int bytes = Codec.encode(entry).getBytes(StandardCharsets.UTF_8).length + 1; // Include a separator.
            if (bytes <= remainingBytes) {
                entries.add(entry);
                remainingBytes -= bytes;
            }
        }
    }

    private record Entry(String targetId, String targetSchema, String sourceDatabase, String sourceSchema,
                         String sourceTable, String sourceIndex, String targetIndex, String action, String reason,
                         Boolean targetUnique, List<String> columns) {}
}
