package io.github.rivomesh;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.DefaultSSLSocketFactory;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import io.github.rivomesh.Model.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public final class MysqlSource {
    private final Config config;
    private final Config.Endpoint endpoint;
    private final SourceSelector.Probe expected;
    public MysqlSource(Config config) { this(config, new Config.Endpoint(config.source.host, config.source.port), null); }
    MysqlSource(Config config, Config.Endpoint endpoint, SourceSelector.Probe expected) { this.config = config; this.endpoint = endpoint; this.expected = expected; }
    Config.Endpoint endpoint() { return endpoint; }
    String identity() { return expected == null ? null : expected.uuid; }
    long serverId() { return expected == null ? 0 : expected.serverId; }
    boolean isKnownNonGroupWriter() { return expected != null && !expected.groupMember; }

    BinaryLogClient binlogClient() {
        Config.Source settings = config.source;
        BinaryLogClient client = new BinaryLogClient(endpoint.host, endpoint.port, settings.username, settings.password);
        SSLMode mode = SSLMode.valueOf(settings.sslMode);
        client.setSSLMode(mode);
        if (mode == SSLMode.VERIFY_CA || mode == SSLMode.VERIFY_IDENTITY) {
            // Use the JDK's enabled TLS protocols and default trust managers for verified connections.
            client.setSslSocketFactory(new DefaultSSLSocketFactory("TLS"));
        }
        return client;
    }

    public Connection connect() throws SQLException {
        Config.Source s = config.source;
        String url = "jdbc:mysql://" + endpoint.host + ":" + endpoint.port + "/?" + s.jdbcOptions;
        Connection c = JdbcConnections.open("com.mysql.cj.jdbc.Driver", url, s.username, s.password);
        try (Statement statement = c.createStatement()) {
            statement.execute("SET time_zone = '+00:00'");
            if (expected != null) try (ResultSet rs = statement.executeQuery("SELECT @@global.server_uuid,@@global.read_only,@@global.super_read_only")) {
                rs.next();
                if (!expected.uuid.equals(rs.getString(1)) || rs.getBoolean(2) || rs.getBoolean(3))
                    throw new SQLTransientConnectionException("源节点角色或连接后端发生变化，重新发现主库", "08006");
            }
        }
        catch (SQLException e) { c.close(); throw e; }
        return c;
    }

    public String validateAndIdentity() throws SQLException {
        return SourceSelector.probe(config, endpoint).uuid;
    }

    public Position position() throws SQLException {
        try (Connection c = connect()) { return position(c); }
    }

    Position position(Connection connection) throws SQLException {
        try (Statement s = connection.createStatement()) {
            ResultSet status;
            try { status = s.executeQuery("SHOW BINARY LOG STATUS"); }
            catch (SQLException e) {
                if (!"42000".equals(e.getSQLState())) throw e;
                status = s.executeQuery("SHOW MASTER STATUS");
            }
            try (ResultSet rs = status) {
                if (!rs.next()) throw new IllegalStateException("无法获取源 binlog 位点");
                Position position = new Position(rs.getString(1), rs.getLong(2));
                position.gtidSet = Gtids.parse(rs.getString(5)).toString();
                position.sourceUuid = identity() == null ? "" : identity();
                return position;
            }
        }
    }

    public void verifyRetained(Position pos) throws SQLException {
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SHOW BINARY LOGS")) {
            while (rs.next()) if (rs.getString(1).equals(pos.file) && rs.getLong(2) >= pos.offset) return;
        }
        throw new IllegalStateException("恢复所需的 binlog 已丢失或被重置: " + pos + "，需要新建任务重新全量");
    }

    public List<TableProgress> discover() throws Exception {
        List<TableProgress> result = new ArrayList<>();
        try (Connection c = connect(); PreparedStatement p = c.prepareStatement(
                "SELECT TABLE_NAME,ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")) {
            p.setString(1, config.source.database);
            try (ResultSet rs = p.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (!"InnoDB".equalsIgnoreCase(rs.getString(2))) throw new IllegalArgumentException("仅支持 InnoDB 表: " + name);
                    TableProgress progress = new TableProgress();
                    progress.table = describe(c, name);
                    progress.upperKey = upperKey(c, progress.table);
                    result.add(progress);
                }
            }
        }
        if (result.isEmpty()) throw new IllegalArgumentException("源 database 不存在、无业务表或没有读取权限");
        return result;
    }

    private Table describe(Connection c, String name) throws SQLException {
        if(TargetOwnership.TABLE.equalsIgnoreCase(name))throw new IllegalArgumentException("RESERVED_TARGET_TABLE_NAME");
        Sql.pg(name);
        Table table = new Table(); table.name = name;
        try (PreparedStatement p = c.prepareStatement("SELECT c.COLUMN_NAME,c.DATA_TYPE,c.COLUMN_TYPE,c.CHARACTER_SET_NAME,c.IS_NULLABLE,"
                + "c.CHARACTER_MAXIMUM_LENGTH,c.NUMERIC_PRECISION,c.NUMERIC_SCALE,c.EXTRA,k.ORDINAL_POSITION "
                + "FROM information_schema.COLUMNS c LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON k.TABLE_SCHEMA=c.TABLE_SCHEMA "
                + "AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.CONSTRAINT_NAME='PRIMARY' "
                + "WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION")) {
            p.setString(1, config.source.database); p.setString(2, name);
            try (ResultSet rs = p.executeQuery()) {
                while (rs.next()) {
                    Column col = new Column();
                    col.name = rs.getString(1); Sql.pg(col.name);
                    col.mysqlType = rs.getString(2); col.definition = rs.getString(3);
                    col.charset = rs.getString(4); col.nullable = "YES".equals(rs.getString(5));
                    col.keyOrder = rs.getInt(10);
                    String extra = rs.getString(9).toUpperCase(Locale.ROOT);
                    if (extra.contains("VIRTUAL GENERATED") || extra.contains("STORED GENERATED") || extra.contains("INVISIBLE")) throw new IllegalArgumentException("暂不支持生成列或不可见列: " + name + "." + col.name);
                    col.targetType = mapType(col.mysqlType, col.definition, rs.getLong(6), rs.getInt(7), rs.getInt(8));
                    if (col.charset != null) Model.charset(col.charset);
                    table.columns.add(col);
                }
            }
        }
        if (table.keys().isEmpty()) throw new IllegalArgumentException("源表必须有主键（支持联合主键）: " + name);
        return table;
    }

    static String mapType(String type, String definition, long length, int precision, int scale) {
        boolean unsigned = definition.toLowerCase(Locale.ROOT).contains("unsigned");
        switch (type) {
            case "json": return "jsonb";
            case "tinyint": return "smallint";
            case "smallint": return unsigned ? "integer" : "smallint";
            case "mediumint": return "integer";
            case "int": return unsigned ? "bigint" : "integer";
            case "bigint": return unsigned ? "numeric(20,0)" : "bigint";
            case "decimal": return "numeric(" + precision + "," + scale + ")";
            case "float": return "real";
            case "double": return "double precision";
            case "year": return "smallint";
            case "date": return "date";
            case "datetime": case "timestamp": return "timestamp(6)";
            // MySQL length is in characters; GaussDB A mode enforces varchar length in bytes.
            // Reserve up to four UTF-8 bytes per character so valid source strings cannot be truncated.
            case "char": case "varchar": return "varchar(" + Math.multiplyExact(length, 4) + ")";
            case "tinytext": case "text": case "mediumtext": case "longtext": return "text";
            case "binary": case "varbinary": case "tinyblob": case "blob": case "mediumblob": case "longblob": return "bytea";
            default: throw new IllegalArgumentException("暂不支持的 MySQL 类型: " + definition);
        }
    }

    private List<String> upperKey(Connection c, Table table) throws SQLException {
        String order = table.keys().stream().map(k -> Sql.mysql(k.name) + " DESC").collect(Collectors.joining(","));
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT " + table.keyColumns() + " FROM "
                + table.sourceName(config.source.database) + " ORDER BY " + order + " LIMIT 1")) {
            if (!rs.next()) return null;
            List<String> result = new ArrayList<>();
            for (int i = 0; i < table.keys().size(); i++) result.add(table.keys().get(i).fromJdbc(rs, i + 1));
            return result;
        }
    }

    public List<List<String>> readChunk(TableProgress progress) throws SQLException {
        if (progress.upperKey == null) return Collections.emptyList();
        Table table = progress.table;
        String tuple = "(" + table.keyColumns() + ")";
        String placeholders = "(" + table.keys().stream().map(k -> "?").collect(Collectors.joining(",")) + ")";
        String sql = "SELECT " + table.sourceColumns() + " FROM " + table.sourceName(config.source.database) + " WHERE "
                + tuple + " <= " + placeholders + (progress.lastKey == null ? "" : " AND " + tuple + " > " + placeholders)
                + " ORDER BY " + table.keyColumns() + " LIMIT " + config.chunkSize;
        try (Connection c = connect(); PreparedStatement p = c.prepareStatement(sql)) {
            int index = 1;
            for (int i = 0; i < table.keys().size(); i++) table.keys().get(i).bind(p, index++, progress.upperKey.get(i));
            if (progress.lastKey != null) for (int i = 0; i < table.keys().size(); i++) table.keys().get(i).bind(p, index++, progress.lastKey.get(i));
            try (ResultSet rs = p.executeQuery()) {
                List<List<String>> rows = new ArrayList<>();
                while (rs.next()) rows.add(table.jdbcRow(rs));
                return rows;
            }
        }
    }
}
