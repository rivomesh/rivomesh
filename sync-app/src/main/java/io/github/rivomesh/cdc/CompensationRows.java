package io.github.rivomesh.cdc;

import java.util.*;

/** Reconstruct the state at the start of one complete projected source transaction. */
final class CompensationRows {
    static final class Row {
        final TableSchema table;
        final String identity, occurrence;
        final List<Cell> key, expected;
        List<Cell> current;
        CorrectionBatch.Action edit;
        Map<Integer,Cell> writes;

        List<Cell> desired(List<Cell> actual) {
            if(edit==null)return expected;
            if(edit==CorrectionBatch.Action.INSERT){
                if(actual!=null)throw new IllegalStateException("CORRECTION_ROW_ALREADY_EXISTS");
                return key;
            }
            if(actual==null)throw new IllegalStateException("CORRECTION_ROW_MISSING");
            if(edit==CorrectionBatch.Action.DELETE)return null;
            List<Cell> updated=new ArrayList<>(actual);
            writes.forEach(updated::set);
            return updated;
        }

        Row(TableSchema table, String identity, String occurrence, List<Cell> key, boolean exists) {
            this.table = table;
            this.identity = identity;
            this.occurrence = occurrence;
            this.key = key;
            this.expected = exists ? key : null;
            this.current = expected;
        }
    }

    private final SortedMap<String, Row> rows = new TreeMap<>();

    static Collection<Row> reconstruct(SourceTransaction transaction, Map<String, TableSchema> schemas) {
        CompensationRows scope = new CompensationRows();
        Map<String, Integer> versions = new HashMap<>();
        for (Change change : transaction.changes) {
            if (change.operation == Change.Operation.BARRIER) continue;
            TableSchema table = schemas.get(change.tableId + "/" + change.schemaVersion);
            if (table == null) throw new IllegalStateException("HISTORICAL_SCHEMA_MISSING");
            Integer version = versions.putIfAbsent(table.id, table.version);
            if (version != null && version != table.version) throw new IllegalStateException("COMPENSATION_SCHEMA_CHANGED");
            switch (change.operation) {
                case INSERT:
                    scope.transition(table, change.rowId, change.after, null, change.after);
                    break;
                case DELETE:
                    scope.transition(table, change.rowId, change.before, change.before, null);
                    break;
                case UPDATE:
                    String before = RowValues.identity(table, change.before, change.rowId);
                    String after = RowValues.identity(table, change.after, change.rowId);
                    if (before.equals(after)) {
                        scope.transition(table, change.rowId, change.before, change.before, change.after);
                    } else {
                        scope.transition(table, change.rowId, change.before, change.before, null);
                        scope.transition(table, change.rowId, change.after, null, change.after);
                    }
                    break;
                default:
                    throw new IllegalStateException("COMPENSATION_ROW_OPERATION_REQUIRED");
            }
        }
        if (scope.rows.isEmpty()) throw new IllegalStateException("COMPENSATION_EMPTY_SCOPE");
        return scope.rows.values();
    }

    private void transition(TableSchema table, String occurrence, List<Cell> key, List<Cell> before, List<Cell> after) {
        String identity = RowValues.identity(table, key, occurrence);
        String reference = table.id + "/" + identity;
        Row row = rows.computeIfAbsent(reference, ignored -> new Row(table, identity, occurrence, key, before != null));
        if (rows.size() > 1000) throw new IllegalArgumentException("COMPENSATION_SCOPE_TOO_LARGE");
        if (!equal(table, row.current, before)) throw new IllegalStateException("COMPENSATION_IMAGE_CHAIN_CHANGED");
        if (after != null) RowValues.canonical(table, after);
        row.current = after;
    }

    static boolean equal(TableSchema table, List<Cell> first, List<Cell> second) {
        if (first == null || second == null) return first == second;
        return RowValues.canonical(table, first).equals(RowValues.canonical(table, second));
    }

    static String hash(TableSchema table, List<Cell> value) {
        return value == null ? null : Codec.sha256(RowValues.canonical(table, value));
    }
}
