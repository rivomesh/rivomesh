package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.TableSchema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/** Advisory comparison of existing journal metadata with the current MySQL catalog. */
final class MysqlSchemaReview {
    private static final int MAX_TABLE_DETAILS = 64;
    private static final int MAX_COLUMN_NAMES = 32;
    private static final String SCOPE = "CURRENT_MYSQL_CATALOG_VS_JOURNAL";

    private MysqlSchemaReview() {}

    static Map<String, Object> compare(List<TableSchema> history, List<TableSchema> catalog) {
        var recorded = new TreeMap<String, TableSchema>();
        for (TableSchema table : history) {
            recorded.merge(table.id, table, (first, next) -> first.version >= next.version ? first : next);
        }
        if (recorded.isEmpty()) {
            return Map.of("scope", SCOPE, "state", "NO_JOURNAL_SCHEMA");
        }
        var live = new TreeMap<String, TableSchema>();
        for (TableSchema table : catalog) {
            if (live.putIfAbsent(table.id, table) != null) {
                throw new IllegalStateException("SOURCE_SCHEMA_REVIEW_CATALOG_AMBIGUOUS");
            }
        }

        var ids = new TreeSet<>(recorded.keySet());
        ids.addAll(live.keySet());
        var details = new ArrayList<Difference>();
        int changed = 0;
        for (String id : ids) {
            TableSchema before = recorded.get(id);
            TableSchema now = live.get(id);
            Structure recordedShape = before == null ? null : structure(before);
            Structure liveShape = now == null ? null : structure(now);
            String oldHash = recordedShape == null ? null : hash(recordedShape);
            String liveHash = liveShape == null ? null : hash(liveShape);
            if (Objects.equals(oldHash, liveHash)) continue;
            changed++;
            if (details.size() >= MAX_TABLE_DETAILS) continue;
            TableSchema name = before == null ? now : before;
            details.add(new Difference(name.database, name.name,
                    before == null ? "TABLE_ADDED" : now == null ? "TABLE_MISSING" : "TABLE_CHANGED",
                    before == null ? null : before.version, oldHash, liveHash, columns(before, now),
                    before != null && now != null && (!before.primaryKey.equals(now.primaryKey)
                            || !before.identityKey.equals(now.identityKey))));
        }
        return Map.of("scope", SCOPE, "state", changed == 0 ? "NO_VISIBLE_CHANGE" : "CHANGES_VISIBLE",
                "recordedTables", recorded.size(), "visibleTables", live.size(), "changedTables", changed,
                "detailsLimited", changed > details.size(), "tables", details);
    }

    static Map<String, Object> unavailable(String code) {
        return Map.of("scope", SCOPE, "state", "UNAVAILABLE", "code", code);
    }

    private static ColumnChanges columns(TableSchema before, TableSchema now) {
        var oldColumns = columnHashes(before);
        var newColumns = columnHashes(now);
        var added = new TreeSet<>(newColumns.keySet());
        added.removeAll(oldColumns.keySet());
        var removed = new TreeSet<>(oldColumns.keySet());
        removed.removeAll(newColumns.keySet());
        var modified = new TreeSet<String>();
        for (var entry : oldColumns.entrySet()) {
            String current = newColumns.get(entry.getKey());
            if (current != null && !current.equals(entry.getValue())) modified.add(entry.getKey());
        }
        return new ColumnChanges(limited(added), limited(removed), limited(modified),
                added.size(), removed.size(), modified.size());
    }

    private static Map<String, String> columnHashes(TableSchema table) {
        var columns = new TreeMap<String, String>();
        if (table != null) {
            for (TableSchema.Column column : table.columns) columns.put(column.name, hash(column));
        }
        return columns;
    }

    private static List<String> limited(TreeSet<String> names) {
        return names.stream().limit(MAX_COLUMN_NAMES).toList();
    }

    private static Structure structure(TableSchema table) {
        return new Structure(table.sourceEngine, table.columns, table.primaryKey, table.identityKey,
                table.indexes.stream().sorted(Comparator.comparing(index -> index.name)).toList(),
                new TreeMap<>(table.properties));
    }

    private static String hash(Object value) {
        return Codec.sha256(Codec.encode(value));
    }

    private record Structure(String engine, List<TableSchema.Column> columns, List<String> primaryKey,
                             List<String> identityKey, List<TableSchema.Index> indexes, Map<String, String> properties) {}

    private record Difference(String database, String table, String change, Integer recordedVersion,
                              String recordedSha256, String liveSha256, ColumnChanges columns, boolean identityChanged) {}

    private record ColumnChanges(List<String> added, List<String> removed, List<String> modified,
                                 int addedCount, int removedCount, int modifiedCount) {}
}
