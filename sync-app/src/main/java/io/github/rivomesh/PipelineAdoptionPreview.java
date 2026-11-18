package io.github.rivomesh;

import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcSink;
import io.github.rivomesh.cdc.PostgresAdoptionRead;
import io.github.rivomesh.cdc.RowValues;
import io.github.rivomesh.cdc.SourceAdapter;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Read-only preflight for assessing an existing, unowned SQL destination. */
final class PipelineAdoptionPreview {
    private static final int MAXIMUM_TABLES = 100;
    private static final int MAXIMUM_ROWS = 100_000;
    private static final int MAXIMUM_ROW_BYTES = 4 * 1024 * 1024;
    private static final long MAXIMUM_RETAINED_BYTES = 64L * 1024 * 1024;
    private static final long SOURCE_GATE_MILLIS = 15_000;
    private static final long READ_MILLIS = 120_000;

    private PipelineAdoptionPreview() {}

    static Map<String,Object> inspect(PipelineConfig config, String targetId) throws Exception {
        if (!"MYSQL".equals(config.effectiveSourceType())) throw rejected("ADOPTION_SOURCE_PRODUCT_UNSUPPORTED");
        PipelineConfig.Target target = config.targets.stream().filter(item -> item.id.equals(targetId))
                .findFirst().orElseThrow(() -> rejected("ADOPTION_TARGET_UNKNOWN"));
        if (target.product != TargetProduct.POSTGRESQL) throw rejected("ADOPTION_TARGET_PRODUCT_UNSUPPORTED");

        try (SourceAdapter source = PipelineSources.open(config, "adoption-preview")) {
            List<TableSchema> discovered = source.discover();
            if (discovered.size() > MAXIMUM_TABLES) throw rejected("ADOPTION_TABLE_LIMIT");
            List<TableSchema> selected = discovered.stream()
                    .filter(table -> target.mappings.containsKey(table.database)).toList();
            if (selected.isEmpty()) throw rejected("ADOPTION_TARGET_NO_MAPPED_TABLES");
            try (PostgresAdoptionRead destination = JdbcSink.adoptionRead(config.targetSettings(target, "adoption-preview"), READ_MILLIS);
                 SourceAdapter.ConsistentRead view = source.consistentRead(discovered, SOURCE_GATE_MILLIS, READ_MILLIS)) {
                boolean owned = destination.pin(selected);
                Instant observed = Instant.now();
                view.releaseWrites();
                Map<String,Object> result = new LinkedHashMap<>();
                result.put("targetId", targetId);
                result.put("observedAt", observed.toString());
                result.put("sourceBoundarySha256", Codec.sha256(Codec.encode(view.baseline())));
                result.put("mode", "ADOPTION_PREVIEW");
                List<Map<String,Object>> tables = new ArrayList<>();
                boolean blocked = false;
                boolean different = false;
                for (TableSchema table : selected) {
                    view.verify();
                    Map<String,Object> values = new LinkedHashMap<>();
                    values.put("sourceSchema", table.database);
                    values.put("targetSchema", target.mappings.get(table.database));
                    values.put("table", table.name);
                    String blocker = owned ? "ADOPTION_TARGET_CHECKPOINT_EXISTS" : destination.blocker(table);
                    if (blocker != null) {
                        blocked = true;
                        values.put("blocker", blocker);
                    } else {
                        RowComparison comparison = compare(view, destination, table);
                        different |= comparison.missing() != 0 || comparison.extra != 0 || comparison.changed != 0;
                        values.put("sourceRows", comparison.sourceRows);
                        values.put("targetRows", comparison.targetRows);
                        values.put("missing", comparison.missing());
                        values.put("extra", comparison.extra);
                        values.put("changed", comparison.changed);
                        values.put("matched", comparison.targetRows - comparison.changed - comparison.extra);
                    }
                    tables.add(values);
                }
                view.verify();
                destination.verify();
                result.put("status", blocked ? "BLOCKED" : different ? "DIFFERENCES_FOUND" : "MATCH_REQUIRES_VERIFIED_BASELINE");
                result.put("tables", tables);
                return result;
            }
        } catch (SyncFailure failure) {
            throw rejected(code(failure.code));
        } catch (SQLException failure) {
            throw rejected(code("SQL_" + failure.getSQLState()));
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw rejected(code(failure.getMessage()));
        }
    }

    private static RowComparison compare(SourceAdapter.ConsistentRead view, PostgresAdoptionRead destination,
                                         TableSchema table) throws Exception {
        RowComparison comparison = new RowComparison(table);
        try (SourceAdapter.Snapshot rows = view.table(table)) {
            for (List<List<Cell>> batch; !(batch = rows.next(1)).isEmpty();) comparison.source(batch.getFirst());
        }
        destination.scan(table, MAXIMUM_ROW_BYTES, comparison::target);
        return comparison;
    }

    private static final class RowComparison {
        private final TableSchema table;
        private final Map<String,String> expected = new HashMap<>();
        private long retainedBytes;
        private int sourceRows;
        private int targetRows;
        private int extra;
        private int changed;

        RowComparison(TableSchema table) {
            this.table = table;
        }

        void source(List<Cell> row) {
            if (++sourceRows > MAXIMUM_ROWS) throw rejected("ADOPTION_SOURCE_ROW_LIMIT");
            String canonical = canonical(row);
            String key = RowValues.identity(table, row, null);
            // UTF-16 worst case plus fixed map/row overhead.
            retainedBytes += 2L * (canonical.length() + key.length()) + 128;
            if (retainedBytes > MAXIMUM_RETAINED_BYTES) throw rejected("ADOPTION_COMPARISON_MEMORY_LIMIT");
            if (expected.putIfAbsent(key, canonical) != null) throw rejected("ADOPTION_SOURCE_IDENTITY_DUPLICATE");
        }

        void target(List<Cell> row) {
            if (++targetRows > MAXIMUM_ROWS) throw rejected("ADOPTION_TARGET_ROW_LIMIT");
            String canonical = canonical(row);
            String source = expected.remove(RowValues.identity(table, row, null));
            if (source == null) extra++;
            else if (!source.equals(canonical)) changed++;
        }

        int missing() {
            return expected.size();
        }

        private String canonical(List<Cell> row) {
            long bytes = 64L * row.size();
            for (Cell cell : row) if (cell.value != null) bytes += cell.value.getBytes(StandardCharsets.UTF_8).length;
            if (bytes > MAXIMUM_ROW_BYTES) throw rejected("ADOPTION_ROW_TOO_LARGE");
            String canonical = RowValues.canonical(table, row);
            if (canonical.getBytes(StandardCharsets.UTF_8).length > MAXIMUM_ROW_BYTES) {
                throw rejected("ADOPTION_ROW_TOO_LARGE");
            }
            return canonical;
        }
    }

    private static IllegalStateException rejected(String code) {
        return new IllegalStateException(code);
    }

    private static String code(String value) {
        return value != null && value.matches("[A-Z][A-Z0-9_]{2,95}") ? value : "ADOPTION_PREVIEW_FAILED";
    }
}
