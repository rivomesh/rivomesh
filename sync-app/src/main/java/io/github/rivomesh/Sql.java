package io.github.rivomesh;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class Sql {
    private Sql() {}
    public static String pg(String name) {
        if (name == null || name.isEmpty() || name.indexOf('\0') >= 0 || name.getBytes(StandardCharsets.UTF_8).length > 63)
            throw new IllegalArgumentException("GaussDB 标识符不能为空或超过 63 字节: " + name);
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
    public static String mysql(String name) {
        return "`" + name.replace("`", "``") + "`";
    }
    public static int execute(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement p = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) p.setObject(i + 1, args[i]);
            return p.executeUpdate();
        }
    }
    public static String targetErrorCode(SQLException error){
        if(error instanceof JdbcConnections.DriverFailure)return error.getMessage();
        // Keep DM's missing-table reason even when the driver is supplied at runtime.
        if (error.getErrorCode() == -2106) {
            for (Class<?> type = error.getClass(); type != SQLException.class; type = type.getSuperclass()) {
                if (type.getName().equals("dm.jdbc.driver.DMException")) return "TARGET_TABLE_MISSING_REBUILD_REQUIRED";
            }
        }
        String state=error.getSQLState();return state!=null&&state.matches("[A-Z0-9]{5}")?"SQL_"+state:"TARGET_SQL_ERROR";
    }
    public static boolean retryable(Throwable error) {
        if (error instanceof SyncFailure) return ((SyncFailure) error).retryable;
        if (error instanceof StateStore.JobBusyException || error instanceof StateStore.LeaseLostException || error instanceof java.util.concurrent.TimeoutException) return true;
        if (error instanceof IllegalStateException || error instanceof IllegalArgumentException) return false;
        for (Throwable e = error; e != null; e = e.getCause()) {
            if(e instanceof JdbcConnections.DriverFailure)return false;
            if(e instanceof SyncFailure)return ((SyncFailure)e).retryable;
            if (e instanceof com.github.shyiko.mysql.binlog.network.ServerException) {
                com.github.shyiko.mysql.binlog.network.ServerException server = (com.github.shyiko.mysql.binlog.network.ServerException) e;
                String state = server.getSqlState(); int code = server.getErrorCode();
                if ((state != null && state.startsWith("08")) || code == 1040 || code == 1053 || code == 3032
                        || code == 1158 || code == 1159 || code == 1160 || code == 1161) return true;
                // The previous dump socket can outlive its lease. Fencing prevents its target writes;
                // retry a duplicate-client disconnect, but never ignore other 1236 errors (e.g. purged GTIDs).
                if (code == 1236 && server.getMessage() != null && server.getMessage().contains("same server_uuid/server_id")) return true;
            }
            if (e instanceof SQLException) {
                String state = ((SQLException) e).getSQLState();
                if (state != null && (state.startsWith("08") || state.startsWith("40")
                        || state.startsWith("53") || state.equals("55P03") || state.equals("57014") || state.equals("57P01") || state.equals("57P02") || state.equals("57P03")||state.equals("HYT00")||state.equals("HYT01"))) return true;
                if (((SQLException) e).getErrorCode() == 1205 || ((SQLException) e).getErrorCode() == 1213) return true;
                // H2 automatic-server ownership can briefly transition between same-host CLI/worker JVMs.
                if(((SQLException)e).getErrorCode()==90020&&"90020".equals(((SQLException)e).getSQLState()))return true;
            }
            if (e instanceof java.io.IOException && !(e instanceof com.github.shyiko.mysql.binlog.network.ServerException)) return true;
        }
        return false;
    }
    static SQLException sqlCause(Throwable error) {
        for (Throwable e = error; e != null; e = e.getCause()) {
            if (e instanceof SQLException) return (SQLException) e;
            if (e instanceof com.github.shyiko.mysql.binlog.network.ServerException) {
                com.github.shyiko.mysql.binlog.network.ServerException server = (com.github.shyiko.mysql.binlog.network.ServerException) e;
                return new SQLException("MySQL replication server error", server.getSqlState(), server.getErrorCode());
            }
        }
        return null;
    }
}
