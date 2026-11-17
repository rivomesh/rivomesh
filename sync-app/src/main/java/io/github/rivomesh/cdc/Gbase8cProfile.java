package io.github.rivomesh.cdc;

import java.sql.*;
import java.util.*;

/** Explicit 8c profile: 8a/8s are different products, not aliases of this adapter. */
public final class Gbase8cProfile {
    private Gbase8cProfile(){}
    public static Map<String,String> check(Connection connection)throws SQLException{
        String product=connection.getMetaData().getDatabaseProductName(),version,mode,encoding;
        try(Statement sql=connection.createStatement()){
            sql.setQueryTimeout(10);
            try(ResultSet rows=sql.executeQuery("SELECT version()")){
                if(!rows.next())throw new SQLException("GBASE8C_IDENTITY_UNAVAILABLE","0A000");
                version=rows.getString(1);
            }
            validateProduct(product,version);
            try(ResultSet rows=sql.executeQuery("SHOW sql_compatibility")){
                if(!rows.next())throw new SQLException("GBASE8C_MODE_UNAVAILABLE","0A000");mode=rows.getString(1);
            }
            validate(product,version,mode);
            try(ResultSet rows=sql.executeQuery("SHOW server_encoding")){
                if(!rows.next())throw new SQLException("GBASE8C_ENCODING_UNAVAILABLE","0A000");encoding=rows.getString(1);
                if(!"UTF8".equalsIgnoreCase(encoding))throw new IllegalArgumentException("GBASE8C_UTF8_REQUIRED");
            }
            sql.execute("SET TIME ZONE 'UTC'");
            sql.execute("SET synchronous_commit = on");
            sql.execute("SET standard_conforming_strings = on");
            try(ResultSet rows=sql.executeQuery("SELECT CAST('' AS varchar) IS NULL")){
                if(!rows.next()||rows.getBoolean(1))throw new IllegalArgumentException("GBASE8C_EMPTY_STRING_SEMANTICS_UNSUPPORTED");
            }
            try(ResultSet rows=sql.executeQuery("SELECT CAST('a' AS text)=CAST('A' AS text),CAST('a' AS varchar)=CAST('a ' AS varchar)")){
                if(!rows.next()||rows.getBoolean(1)||rows.getBoolean(2))throw new IllegalArgumentException("GBASE8C_STRING_COMPARISON_REQUIRES_PLAN");
            }
            try(ResultSet rows=sql.executeQuery("SHOW transaction_read_only")){
                if(!rows.next()||!"off".equalsIgnoreCase(rows.getString(1)))throw new SQLTransientConnectionException("GBASE8C_WRITABLE_PRIMARY_REQUIRED","08006");
            }
        }
        Map<String,String> result=new LinkedHashMap<>();result.put("product",product);result.put("version",version);result.put("mode",mode);result.put("encoding",encoding);result.put("synchronousCommit","on");return result;
    }
    static void validate(String product,String version,String mode){
        validateProduct(product,version);
        if(!"PG".equalsIgnoreCase(mode))throw new IllegalArgumentException("GBASE8C_PG_MODE_REQUIRED");
    }
    private static void validateProduct(String product,String version){
        if(!"GBase8c".equalsIgnoreCase(product)||version==null||version.length()>2048||!version.toLowerCase(Locale.ROOT).contains("gbase8c"))throw new IllegalArgumentException("GBASE8C_PRODUCT_REQUIRED");
    }
}
