package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.PostgresRelationEvidence;
import io.github.rivomesh.cdc.TableSchema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/** Advisory comparison of recorded journal metadata with the current source catalog. */
final class SourceSchemaReview {
    private static final int MAX_TABLE_DETAILS = 64;
    private static final int MAX_COLUMN_NAMES = 32;
    enum Scope {
        CURRENT_MYSQL_CATALOG_VS_JOURNAL,
        CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL,
        CURRENT_ORACLE_PDB_CATALOG_VS_JOURNAL
    }

    private SourceSchemaReview() {}

    static Map<String, Object> compare(Scope scope, List<TableSchema> history, List<TableSchema> catalog) {
        var recorded = new TreeMap<String, TableSchema>();
        for (TableSchema table : history) {
            recorded.merge(table.id, table, (first, next) -> first.version >= next.version ? first : next);
        }
        if (recorded.isEmpty()) {
            return Map.of("scope", scope.name(), "state", "NO_JOURNAL_SCHEMA");
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
            boolean retained = before != null && now != null;
            details.add(new Difference(name.database, name.name,
                    before == null ? "TABLE_ADDED" : now == null ? "TABLE_MISSING" : "TABLE_CHANGED",
                    before == null ? null : before.version, oldHash, liveHash, columns(before, now),
                    retained && (!before.primaryKey.equals(now.primaryKey)
                            || !before.identityKey.equals(now.identityKey)),
                    retained && columnOrderChanged(before, now),
                    retained && !hash(recordedShape.indexes()).equals(hash(liveShape.indexes()))));
        }
        return Map.of("scope", scope.name(), "state", changed == 0 ? "NO_VISIBLE_CHANGE" : "CHANGES_VISIBLE",
                "recordedTables", recorded.size(), "visibleTables", live.size(), "changedTables", changed,
                "detailsLimited", changed > details.size(), "tables", details);
    }

    static Map<String, Object> unavailable(Scope scope, String code) {
        return Map.of("scope", scope.name(), "state", "UNAVAILABLE", "code", code);
    }

    static Map<String, Object> compareBlockedPostgresRelation(PostgresRelationEvidence event,
                                                              Map<Integer, TableSchema> publication) {
        TableSchema table = publication.get((int) event.relationOid());
        if (table == null) {
            for (var entry : publication.entrySet()) {
                if (sameRelationName(event, entry.getValue())) {
                    return Map.of("state", "RELATION_REPLACED", "currentRelationOid",
                            Integer.toUnsignedLong(entry.getKey()));
                }
            }
            return Map.of("state", "RELATION_NOT_PUBLISHED");
        }
        if (!sameRelationName(event, table)) return Map.of("state", "RELATION_RENAMED");
        // Publication discovery validates REPLICA IDENTITY FULL for all returned tables.
        if (!"f".equals(event.replicaIdentity())) return Map.of("state", "REPLICA_IDENTITY_CHANGED");
        if (event.columns().size() != table.columns.size()) return Map.of("state", "COLUMN_LAYOUT_CHANGED");
        for (int index = 0; index < table.columns.size(); index++) {
            var original = event.columns().get(index);
            String current = table.columns.get(index).name;
            if (!original.name().equals(current)
                    || !Long.toString(original.typeOid()).equals(table.properties.get("pg.oid." + current))
                    || !Integer.toString(original.typeModifier()).equals(table.properties.get("pg.modifier." + current))) {
                return Map.of("state", "COLUMN_LAYOUT_CHANGED");
            }
        }
        return Map.of("state", "MATCH");
    }

    private static boolean sameRelationName(PostgresRelationEvidence event, TableSchema table) {
        return event.namespace().equals(table.database) && event.relation().equals(table.name);
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
            for (TableSchema.Column column : table.columns) {
                columns.put(column.name, hash(new ColumnShape(column, table.properties.get("pg.modifier." + column.name))));
            }
        }
        return columns;
    }

    private static boolean columnOrderChanged(TableSchema before, TableSchema now) {
        var recorded = before.columns.stream().map(column -> column.name).toList();
        var live = now.columns.stream().map(column -> column.name).toList();
        var shared = new HashSet<>(live);
        shared.retainAll(new HashSet<>(recorded));
        return !recorded.stream().filter(shared::contains).toList().equals(live.stream().filter(shared::contains).toList());
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
                              String recordedSha256, String liveSha256, ColumnChanges columns, boolean identityChanged,
                              boolean columnOrderChanged, boolean indexesChanged) {}

    private record ColumnShape(TableSchema.Column column, String postgresTypeModifier) {}

    private record ColumnChanges(List<String> added, List<String> removed, List<String> modified,
                                 int addedCount, int removedCount, int modifiedCount) {}
}
