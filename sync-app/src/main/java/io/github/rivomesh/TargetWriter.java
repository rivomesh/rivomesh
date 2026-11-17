package io.github.rivomesh;

import io.github.rivomesh.Model.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** Statements are reused within one target transaction. Source keys are always written explicitly. */
final class TargetWriter implements AutoCloseable {
    private final Connection connection;
    private final String schema;
    private final Map<String, PreparedStatement[]> statements = new HashMap<>();
    TargetWriter(Connection connection, String schema) { this.connection = connection; this.schema = schema; }

    void write(Table table, List<String> row, boolean delete) throws SQLException {
        PreparedStatement[] pair = statements.get(table.name);
        if (pair == null) {
            String condition = table.keys().stream().map(k -> Sql.pg(k.name) + "=?").collect(Collectors.joining(" AND "));
            String columns = table.columns.stream().map(c -> Sql.pg(c.name)).collect(Collectors.joining(","));
            String values = table.columns.stream().map(c -> "?").collect(Collectors.joining(","));
            PreparedStatement remove = connection.prepareStatement("DELETE FROM " + table.targetName(schema) + " WHERE " + condition);
            try {
                pair = new PreparedStatement[]{remove, connection.prepareStatement("INSERT INTO " + table.targetName(schema) + " (" + columns + ") VALUES (" + values + ")")};
            } catch (SQLException e) { remove.close(); throw e; }
            statements.put(table.name, pair);
        }
        int index = 1;
        for (Column key : table.keys()) key.bind(pair[0], index++, row.get(table.columns.indexOf(key)));
        pair[0].executeUpdate();
        if (!delete) {
            for (int i = 0; i < table.columns.size(); i++) table.columns.get(i).bind(pair[1], i + 1, row.get(i));
            pair[1].executeUpdate();
        }
    }

    static void validate(Connection c, Table table, String schema) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM " + table.targetName(schema) + " WHERE 1=0")) {
            ResultSetMetaData meta = rs.getMetaData();
            if (meta.getColumnCount() != table.columns.size()) throw new IllegalStateException("目标字段数量不匹配: " + table.name);
            for (int i = 0; i < table.columns.size(); i++) {
                Column col = table.columns.get(i);
                int jdbc = meta.getColumnType(i + 1);
                boolean compatible;
                if (col.binary()) compatible = jdbc == Types.BINARY || jdbc == Types.VARBINARY || jdbc == Types.LONGVARBINARY;
                else if (col.numeric()) {
                    switch (col.targetType) {
                        case "smallint": compatible = jdbc == Types.SMALLINT; break;
                        case "integer": compatible = jdbc == Types.INTEGER; break;
                        case "bigint": compatible = jdbc == Types.BIGINT; break;
                        case "real": compatible = jdbc == Types.REAL; break;
                        case "double precision": compatible = jdbc == Types.DOUBLE || jdbc == Types.FLOAT; break;
                        default:
                            String[] size = col.targetType.substring(8, col.targetType.length() - 1).split(",");
                            compatible = (jdbc == Types.NUMERIC || jdbc == Types.DECIMAL) && meta.getPrecision(i + 1) >= Integer.parseInt(size[0])
                                    && meta.getScale(i + 1) == Integer.parseInt(size[1]);
                    }
                } else if(col.targetType.equals("jsonb"))compatible="jsonb".equalsIgnoreCase(meta.getColumnTypeName(i+1));
                else if (col.targetType.equals("date")) compatible = jdbc == Types.DATE || jdbc == Types.TIMESTAMP;
                else if (col.targetType.startsWith("timestamp")) compatible = jdbc == Types.TIMESTAMP && meta.getScale(i + 1) >= 6;
                else {
                    compatible = jdbc == Types.VARCHAR || jdbc == Types.LONGVARCHAR;
                    if (compatible && col.targetType.startsWith("varchar(")) compatible = meta.getPrecision(i + 1) >= Long.parseLong(col.targetType.substring(8, col.targetType.length() - 1));
                    if (compatible && col.targetType.equals("text")) compatible = meta.getColumnTypeName(i + 1).equalsIgnoreCase("text");
                }
                if (!col.name.equals(meta.getColumnName(i + 1)) || !compatible
                        || col.nullable != (meta.isNullable(i + 1) != ResultSetMetaData.columnNoNulls))
                    throw new IllegalStateException("目标字段不兼容: " + table.name + "." + col.name + "，期望 " + col.targetType + "，实际 " + meta.getColumnTypeName(i + 1));
            }
        }
        TreeMap<Integer, String> actual = new TreeMap<>();
        try (ResultSet rs = c.getMetaData().getPrimaryKeys(null, schema, table.name)) {
            while (rs.next()) actual.put(rs.getInt("KEY_SEQ"), rs.getString("COLUMN_NAME"));
        }
        List<String> expected = table.keys().stream().map(k -> k.name).collect(Collectors.toList());
        if (!expected.equals(new ArrayList<>(actual.values()))) throw new IllegalStateException("目标主键不匹配: " + table.name);
    }

    @Override public void close() throws SQLException {
        SQLException failure = null;
        for (PreparedStatement[] pair : statements.values()) for (PreparedStatement p : pair) {
            try { p.close(); } catch (SQLException e) { failure = e; }
        }
        if (failure != null) throw failure;
    }
}
