package io.github.rivomesh.cdc;

import java.sql.*;
import java.util.*;

/** Check every physical connection before catalog or checkpoint access. */
public final class KingbaseProfile {
    private KingbaseProfile(){}
    public static Map<String,String> check(Connection connection)throws SQLException{
        String product=connection.getMetaData().getDatabaseProductName(),version,mode,encoding;
        try(Statement sql=connection.createStatement()){
            sql.setQueryTimeout(10);
            try(ResultSet rows=sql.executeQuery("SELECT version()")){
                if(!rows.next())throw new SQLException("KINGBASE_IDENTITY_UNAVAILABLE","0A000");
                version=rows.getString(1);
            }
            validateProduct(product,version);
            try(ResultSet rows=sql.executeQuery("SHOW database_mode")){
                if(!rows.next())throw new SQLException("KINGBASE_MODE_UNAVAILABLE","0A000");mode=rows.getString(1);
            }
            validate(product,version,mode);
            try(ResultSet rows=sql.executeQuery("SHOW server_encoding")){
                if(!rows.next())throw new SQLException("KINGBASE_ENCODING_UNAVAILABLE","0A000");encoding=rows.getString(1);
                if(!"UTF8".equalsIgnoreCase(encoding))throw new IllegalArgumentException("KINGBASE_UTF8_REQUIRED");
            }
            sql.execute("SET TIME ZONE 'UTC'");
            // Persisted checkpoints must not be acknowledged with local WAL durability disabled.
            sql.execute("SET synchronous_commit = on");
            sql.execute("SET standard_conforming_strings = on");
            try(ResultSet rows=sql.executeQuery("SELECT CAST('' AS varchar) IS NULL")){
                if(!rows.next()||rows.getBoolean(1))throw new IllegalArgumentException("KINGBASE_EMPTY_STRING_SEMANTICS_UNSUPPORTED");
            }
            try(ResultSet rows=sql.executeQuery("SELECT CAST('a' AS text)=CAST('A' AS text),CAST('a' AS varchar)=CAST('a ' AS varchar)")){
                if(!rows.next()||rows.getBoolean(1)||rows.getBoolean(2))throw new IllegalArgumentException("KINGBASE_STRING_COMPARISON_REQUIRES_PLAN");
            }
            try(ResultSet rows=sql.executeQuery("SHOW transaction_read_only")){
                if(!rows.next()||!"off".equalsIgnoreCase(rows.getString(1)))throw new SQLTransientConnectionException("KINGBASE_WRITABLE_PRIMARY_REQUIRED","08006");
            }
        }
        Map<String,String> result=new LinkedHashMap<>();result.put("product",product);result.put("version",version);result.put("mode",mode);result.put("encoding",encoding);result.put("synchronousCommit","on");return result;
    }
    static void validate(String product,String version,String mode){
        validateProduct(product,version);
        if(!"pg".equalsIgnoreCase(mode))throw new IllegalArgumentException("KINGBASE_PG_MODE_REQUIRED");
    }
    private static void validateProduct(String product,String version){
        if(product==null||!product.toLowerCase(Locale.ROOT).contains("kingbase")||version==null||version.length()>2048||!version.toLowerCase(Locale.ROOT).contains("kingbase"))throw new IllegalArgumentException("KINGBASE_PRODUCT_REQUIRED");
    }
}
