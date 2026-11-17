package io.github.rivomesh;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public final class Model {
    public static final ObjectMapper JSON = new ObjectMapper();
    private Model() {}

    public static final class Position {
        public String file;
        public long offset;
        public String gtidSet = "";
        public String requiredGtidSet = "";
        public String sourceUuid = "";
        public Position() {}
        public Position(String file, long offset) { this.file = file; this.offset = offset; }
        @Override public String toString() { return file + ":" + offset; }
    }

    public static final class Column {
        public String name;
        public String mysqlType;
        public String definition;
        public String charset;
        public String targetType;
        public boolean nullable;
        public int keyOrder;

        public boolean binary() { return targetType.equals("bytea"); }
        boolean numeric() { return targetType.matches("(smallint|integer|bigint|numeric.*|real|double precision)"); }
        public String fromJdbc(ResultSet rs, int index) throws SQLException {
            if(mysqlType!=null&&mysqlType.equals("json"))return io.github.rivomesh.cdc.JsonValues.text(rs.getString(index));
            if (binary()) {
                byte[] value = rs.getBytes(index);
                return value == null ? null : Base64.getEncoder().encodeToString(value);
            }
            if (targetType.equals("date")) {
                java.sql.Date value = rs.getDate(index);
                return value == null ? null : value.toString();
            }
            if (targetType.startsWith("timestamp")) {
                Timestamp value = rs.getTimestamp(index);
                return value == null ? null : value.toLocalDateTime().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
            }
            return rs.getString(index);
        }
        public String fromBinlog(Object value) {
            if (value == null) return null;
            if(mysqlType!=null&&mysqlType.equals("json"))return io.github.rivomesh.cdc.JsonValues.mysql(value);
            if ((mysqlType.equals("date") || mysqlType.equals("datetime") || mysqlType.equals("timestamp")) && value instanceof Long) {
                long micros = (Long) value;
                if (micros == Long.MIN_VALUE) throw new IllegalArgumentException("不支持零日期: " + name);
                java.time.LocalDateTime date = java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochSecond(
                        Math.floorDiv(micros, 1000000L), Math.floorMod(micros, 1000000L) * 1000), java.time.ZoneOffset.UTC);
                return mysqlType.equals("date") ? date.toLocalDate().toString()
                        : date.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
            }
            if (binary()) return Base64.getEncoder().encodeToString((byte[]) value);
            if (value instanceof byte[]) {
                String text = new String((byte[]) value, charset(charset));
                // MySQL returns CHAR values without trailing pad spaces to ordinary JDBC readers.
                if (mysqlType.equals("char")) {
                    int end = text.length(); while (end > 0 && text.charAt(end - 1) == ' ') end--;
                    text = text.substring(0, end);
                }
                return text;
            }
            if (value instanceof BigDecimal) return ((BigDecimal) value).toPlainString();
            if (value instanceof Number && definition.toLowerCase(Locale.ROOT).contains("unsigned") && ((Number) value).longValue() < 0) {
                int bits;
                switch (mysqlType) {
                    case "tinyint": bits = 8; break;
                    case "smallint": bits = 16; break;
                    case "mediumint": bits = 24; break;
                    case "int": bits = 32; break;
                    case "bigint": bits = 64; break;
                    default: return value.toString();
                }
                return BigInteger.valueOf(((Number) value).longValue()).add(BigInteger.ONE.shiftLeft(bits)).toString();
            }
            if (value instanceof java.util.Date) {
                String pattern = mysqlType.equals("date") ? "yyyy-MM-dd" : "yyyy-MM-dd HH:mm:ss";
                SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.ROOT);
                format.setTimeZone(TimeZone.getTimeZone("UTC"));
                String result = format.format(value);
                if (value instanceof Timestamp && !mysqlType.equals("date")) {
                    return result + String.format(Locale.ROOT, ".%06d", ((Timestamp) value).getNanos() / 1000);
                }
                if (!mysqlType.equals("date")) return result + String.format(Locale.ROOT, ".%03d", Math.floorMod(((java.util.Date) value).getTime(), 1000));
                return result;
            }
            return value.toString();
        }

        public void bind(PreparedStatement p, int index, String value) throws SQLException {
            if (value == null) { p.setNull(index, jdbcType()); return; }
            if(targetType.equals("jsonb"))p.setObject(index,value,Types.OTHER);
            else if (binary()) p.setBytes(index, Base64.getDecoder().decode(value));
            else if (numeric()) p.setBigDecimal(index, new BigDecimal(value));
            else if (targetType.equals("date")) p.setDate(index, java.sql.Date.valueOf(value));
            else if (targetType.startsWith("timestamp")) p.setTimestamp(index, Timestamp.valueOf(value));
            else p.setString(index, value);
        }
        int jdbcType() {
            if(targetType.equals("jsonb"))return Types.OTHER;
            if (binary()) return Types.BINARY;
            if (numeric()) return Types.NUMERIC;
            if (targetType.equals("date")) return Types.DATE;
            if (targetType.startsWith("timestamp")) return Types.TIMESTAMP;
            return Types.VARCHAR;
        }
    }

    static Charset charset(String name) {
        if (name == null || name.startsWith("utf8")) return java.nio.charset.StandardCharsets.UTF_8;
        if (name.equals("ascii")) return java.nio.charset.StandardCharsets.US_ASCII;
        // MySQL latin1 is Windows-1252, not ISO-8859-1.
        if (name.equals("latin1")) return Charset.forName("windows-1252");
        throw new IllegalArgumentException("暂不支持的源字符集: " + name);
    }

    public static final class Table {
        public String name;
        public List<Column> columns = new ArrayList<>();
        public List<Column> keys() {
            return columns.stream().filter(c -> c.keyOrder > 0).sorted(Comparator.comparingInt(c -> c.keyOrder)).collect(Collectors.toList());
        }
        public String sourceName(String database) { return Sql.mysql(database) + "." + Sql.mysql(name); }
        public String targetName(String schema) { return Sql.pg(schema) + "." + Sql.pg(name); }
        public String sourceColumns() { return columns.stream().map(c -> Sql.mysql(c.name)).collect(Collectors.joining(",")); }
        public String keyColumns() { return keys().stream().map(c -> Sql.mysql(c.name)).collect(Collectors.joining(",")); }
        public List<String> keyOf(List<String> row) {
            return keys().stream().map(c -> row.get(columns.indexOf(c))).collect(Collectors.toList());
        }
        public List<String> jdbcRow(ResultSet rs) throws SQLException {
            List<String> row = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) row.add(columns.get(i).fromJdbc(rs, i + 1));
            return row;
        }
        public List<String> binlogRow(Object[] raw) {
            if (raw.length != columns.size()) throw new IllegalStateException("binlog 字段数量不匹配: " + name);
            List<String> row = new ArrayList<>();
            for (int i = 0; i < raw.length; i++) {
                Column c = columns.get(i);
                row.add(c.fromBinlog(raw[i]));
            }
            return row;
        }
        public String createSql(String schema) {
            String definitions = columns.stream().map(c -> Sql.pg(c.name) + " " + c.targetType + (c.nullable ? "" : " NOT NULL")).collect(Collectors.joining(","));
            String pk = keys().stream().map(c -> Sql.pg(c.name)).collect(Collectors.joining(","));
            return "CREATE TABLE IF NOT EXISTS " + targetName(schema) + " (" + definitions + ",PRIMARY KEY (" + pk + "))";
        }
    }

    public static final class TableProgress {
        public Table table;
        public List<String> upperKey;
        public List<String> lastKey;
        public boolean done;
        public boolean prepared;
        public long copiedRows;
    }

    public static final class Change {
        public String table;
        public boolean delete;
        public List<String> row;
        public Change() {}
        public Change(String table, boolean delete, List<String> row) { this.table = table; this.delete = delete; this.row = row; }
    }

    public static final class Transaction {
        public long sequence;
        public String gtid = "";
        public Position position;
        public List<Change> changes = new ArrayList<>();
    }
}
