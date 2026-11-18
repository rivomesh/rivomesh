package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.MysqlDuration;
import io.github.rivomesh.PostgresTimestamp;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/** Explicit, bounded target edits. Every selector names exactly one replication identity. */
public final class CorrectionBatch {
    public static final int MAX_ROWS = 50;
    public static final int MAX_BYTES = 128 * 1024;
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON=Codec.JSON.copy()
            .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);
    public List<Edit> edits = new ArrayList<>();

    public enum Action { INSERT, UPDATE, DELETE }

    public static final class Edit {
        public String tableId, rowId;
        public Action action;
        public Map<String,JsonNode> key = new TreeMap<>(), values = new TreeMap<>();
    }

    public void validate() {
        if (edits == null || edits.isEmpty() || edits.size() > MAX_ROWS
                || Codec.encode(this).getBytes(StandardCharsets.UTF_8).length > MAX_BYTES)
            throw invalid("CORRECTION_BATCH_LIMIT");
        for (Edit edit : edits) {
            if (edit == null || edit.action == null || edit.tableId == null || !edit.tableId.matches("[0-9a-f]{64}")
                    || edit.key == null || edit.values == null || edit.key.size() > 1024 || edit.values.size() > 1024)
                throw invalid("CORRECTION_ROW_INVALID");
            if (edit.rowId != null && !edit.rowId.matches("[0-9a-f]{64}"))
                throw invalid("CORRECTION_ROW_ID_INVALID");
        }
    }

    static List<String> keys(TableSchema table) {
        return table.primaryKey.isEmpty() ? table.identityKey : table.primaryKey;
    }

    Collection<CompensationRows.Row> bind(Map<String, TableSchema> tables) {
        validate();
        Map<String,CompensationRows.Row> ordered = new TreeMap<>();
        for (Edit edit : edits) {
            TableSchema table = tables.get(edit.tableId);
            if (table == null) throw invalid("CORRECTION_TABLE_OUTSIDE_FAILURE");
            List<String> keys = keys(table);
            List<Cell> selector = new ArrayList<>(Collections.nCopies(table.columns.size(), Cell.value(null)));
            Map<Integer,Cell> writes = new TreeMap<>();
            boolean insertion = edit.action == Action.INSERT;
            if (table.keyless() && (insertion || edit.rowId == null || !edit.key.isEmpty()))
                throw invalid("CORRECTION_KEYLESS_ROW_ID_REQUIRED");
            if (!table.keyless() && (edit.rowId != null
                    || !edit.key.keySet().equals(insertion ? Collections.emptySet() : new HashSet<>(keys))))
                throw invalid("CORRECTION_KEY_INVALID");
            if (edit.action == Action.DELETE ? !edit.values.isEmpty() : edit.values.isEmpty())
                throw invalid("CORRECTION_VALUES_INVALID");
            if (insertion && edit.values.size() != table.columns.size())
                throw invalid("CORRECTION_COMPLETE_INSERT_REQUIRED");
            for (String key : edit.key.keySet()) {
                int index = table.column(key);
                Cell cell = cell(table.columns.get(index), edit.key.get(key));
                if (cell.value == null) throw invalid("CORRECTION_KEY_INVALID");
                selector.set(index,cell);
            }
            for (Map.Entry<String,JsonNode> field : edit.values.entrySet()) {
                int index = columnIndex(table,field.getKey());
                if (!insertion && keys.contains(field.getKey())) throw invalid("CORRECTION_IDENTITY_EDIT");
                Cell value = cell(table.columns.get(index),field.getValue());
                if (insertion) selector.set(index,value);
                writes.put(index,value);
            }
            String identity = RowValues.identity(table, selector, edit.rowId);
            CompensationRows.Row row = new CompensationRows.Row(table,identity,edit.rowId,selector,false);
            row.edit = edit.action;
            row.writes = writes;
            if (ordered.putIfAbsent(table.id+"/"+identity,row) != null)
                throw invalid("CORRECTION_DUPLICATE_IDENTITY");
        }
        return ordered.values();
    }

    private static int columnIndex(TableSchema table,String name) {
        try{return table.column(name);}
        catch(IllegalArgumentException absent){throw invalid("CORRECTION_COLUMN_UNKNOWN");}
    }

    private static Cell cell(TableSchema.Column column, JsonNode node) {
        if (node != null && node.isNull()) {
            if (!column.nullable) throw invalid("CORRECTION_NULL_NOT_ALLOWED");
            return Cell.value(null);
        }
        if (node == null || !node.isTextual()) throw invalid("CORRECTION_TYPED_TEXT_REQUIRED");
        String value = node.textValue();
        if (value.length() > 65536 || !StandardCharsets.UTF_8.newEncoder().canEncode(value))
            throw invalid("CORRECTION_VALUE_LIMIT");
        try {
            switch (column.type) {
                case INT64: case UINT64: case DECIMAL:
                    // RowValues enforces range and scale. Restrict syntax before decimal parsing.
                    if (value.length() > 1024 || !value.matches("[+-]?[0-9]+(?:\\.[0-9]+)?"))
                        throw invalid("CORRECTION_NUMBER_INVALID");
                    TableSchema numeric = new TableSchema();
                    numeric.columns.add(column);
                    RowValues.canonical(numeric,Collections.singletonList(Cell.value(value)));
                    value=new java.math.BigDecimal(value).stripTrailingZeros().toPlainString();
                    break;
                case FLOAT32: case FLOAT64:
                    if (value.length()>128 || !value.matches("[+-]?[0-9]+(?:\\.[0-9]+)?(?:[Ee][+-]?[0-9]+)?"))
                        throw invalid("CORRECTION_NUMBER_INVALID");
                    java.math.BigDecimal original=new java.math.BigDecimal(value);
                    String stored=column.type==TableSchema.Type.FLOAT32
                            ? Float.toString(Float.parseFloat(value)) : Double.toString(Double.parseDouble(value));
                    if (original.compareTo(new java.math.BigDecimal(stored))!=0) throw invalid("CORRECTION_NUMBER_INVALID");
                    break;
                case BOOLEAN:
                    if (!"true".equals(value) && !"false".equals(value)) throw invalid("CORRECTION_BOOLEAN_INVALID");
                    break;
                case DATE:
                    if (!value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}") || LocalDate.parse(value).getYear()<1)
                        throw invalid("CORRECTION_DATE_INVALID");
                    break;
                case TIMESTAMP: case DATETIME:
                    if (!value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?")
                            || LocalDateTime.parse(value.replace(' ','T')).getYear()<1)
                        throw invalid("CORRECTION_DATE_INVALID");
                    value=value.replace('T',' ');
                    if(PostgresTimestamp.isColumn(column))value=PostgresTimestamp.format(PostgresTimestamp.localTime(value));
                    break;
                case TIME: value=MysqlDuration.canonical(value);break;
                case JSON: value=strictJson(value);break;
                case BYTES: value=Base64.getEncoder().encodeToString(Base64.getDecoder().decode(value));break;
                case GEOMETRY: throw invalid("CORRECTION_TYPE_UNSUPPORTED");
                case STRING:
                    if (column.length > 0 && value.codePointCount(0,value.length())>column.length)
                        throw invalid("CORRECTION_VALUE_LIMIT");
                    break;
                default: throw invalid("CORRECTION_TYPE_UNSUPPORTED");
            }
        } catch (IllegalArgumentException | java.time.DateTimeException bad) {
            String code=bad.getMessage();
            throw invalid(code!=null && code.matches("CORRECTION_[A-Z_]+") ? code : "CORRECTION_TYPED_VALUE_INVALID");
        }
        return Cell.value(value);
    }

    private static String strictJson(String value) {
        try{
            JsonNode node=JSON.readTree(value);
            if(node==null||node.isMissingNode())throw invalid("CORRECTION_JSON_INVALID");
            return JSON.writeValueAsString(node);
        }catch(com.fasterxml.jackson.core.JsonProcessingException bad){
            throw invalid("CORRECTION_JSON_INVALID");
        }
    }

    private static IllegalArgumentException invalid(String code) {
        return new IllegalArgumentException(code);
    }
}
