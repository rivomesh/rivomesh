package io.github.rivomesh.cdc;

import java.sql.*;
import java.util.*;

/** Native DM profile. No global compatibility, durability or instance-role changes. */
public final class DmProfile {
    private DmProfile(){}
    public static boolean product(Connection c)throws SQLException{return "DM DBMS".equalsIgnoreCase(c.getMetaData().getDatabaseProductName());}
    public static Map<String,String> check(Connection c)throws SQLException{
        if(!product(c))throw new IllegalArgumentException("DM_PRODUCT_REQUIRED");
        String version=c.getMetaData().getDatabaseProductVersion(),role,status,commit;
        try(Statement sql=c.createStatement()){
            sql.setQueryTimeout(10);
            try(ResultSet r=sql.executeQuery("SELECT banner FROM v$version")){if(!r.next()||!r.getString(1).startsWith("DM Database Server"))throw new IllegalArgumentException("DM_PRODUCT_REQUIRED");}
            try(ResultSet r=sql.executeQuery("SELECT SF_GET_UNICODE_FLAG(),SF_GET_CASE_SENSITIVE_FLAG(),SF_GET_PARA_VALUE(1,'COMPATIBLE_MODE'),SF_GET_PARA_VALUE(2,'BLANK_PAD_MODE'),SF_GET_PARA_STRING_VALUE(1,'COMMIT_WRITE')")){
                if(!r.next())throw new SQLException("DM_PROFILE_UNAVAILABLE","0A000");commit=r.getString(5);validate(r.getInt(1),r.getInt(2),r.getInt(3),r.getInt(4),commit);
            }
            try(ResultSet r=sql.executeQuery("SELECT MODE$,STATUS$ FROM v$instance")){
                if(!r.next())throw new SQLException("DM_INSTANCE_UNAVAILABLE","08006");role=r.getString(1);status=r.getString(2);
                if(!Arrays.asList("NORMAL","PRIMARY").contains(role)||!"OPEN".equals(status))throw new SQLTransientConnectionException("DM_WRITABLE_PRIMARY_REQUIRED","08006");
            }
            sql.execute("ALTER SESSION SET TIME_ZONE='+00:00'");
            try(ResultSet r=sql.executeQuery("SELECT CASE WHEN CAST('' AS VARCHAR(10)) IS NULL THEN 1 ELSE 0 END")){
                if(!r.next()||r.getInt(1)!=0)throw new IllegalArgumentException("DM_EMPTY_STRING_SEMANTICS_UNSUPPORTED");
            }
        }
        Map<String,String> result=new LinkedHashMap<>();result.put("product","DM DBMS");result.put("version",version);result.put("compatibilityMode","0");result.put("encoding","UTF8");result.put("caseSensitive","1");result.put("blankPadMode","1");result.put("role",role);result.put("status",status);result.put("commitWrite",commit);return result;
    }
    static void validate(int encoding,int sensitive,int mode,int blank,String commit){
        if(encoding!=1)throw new IllegalArgumentException("DM_UTF8_REQUIRED");
        if(sensitive!=1)throw new IllegalArgumentException("DM_CASE_SENSITIVE_REQUIRED");
        if(mode!=0)throw new IllegalArgumentException("DM_NATIVE_MODE_REQUIRED");
        if(blank!=1)throw new IllegalArgumentException("DM_BLANK_PAD_MODE_REQUIRED");
        if(commit==null||!commit.replace(" ","").toUpperCase(Locale.ROOT).matches("(IMMEDIATE|BATCH),WAIT"))throw new IllegalArgumentException("DM_DURABLE_COMMIT_REQUIRED");
    }
    public static boolean schemaExists(Connection c,String schema)throws SQLException{
        try(ResultSet r=c.getMetaData().getSchemas()){while(r.next())if(schema.equals(r.getString("TABLE_SCHEM")))return true;return false;}
    }
    public static boolean tableExists(Connection c,String schema,String table)throws SQLException{
        try(ResultSet r=c.getMetaData().getTables(null,schema,table,new String[]{"TABLE"})){while(r.next())if(schema.equals(r.getString("TABLE_SCHEM"))&&table.equals(r.getString("TABLE_NAME")))return true;return false;}
    }
    public static boolean unique(SQLException failure){return failure.getErrorCode()==-6602;}
    public static long characterCapacity(Connection c,String schema,String table,String column)throws SQLException{
        try(PreparedStatement p=c.prepareStatement("SELECT DATA_LENGTH,CHAR_USED,CHAR_LENGTH FROM ALL_TAB_COLUMNS WHERE OWNER=? AND TABLE_NAME=? AND COLUMN_NAME=?")){
            p.setString(1,schema);p.setString(2,table);p.setString(3,column);try(ResultSet r=p.executeQuery()){
                if(!r.next())throw new SQLException("DM_COLUMN_DIRECTORY_UNAVAILABLE","0A000");return "C".equals(r.getString(2))?r.getLong(3):r.getLong(1)/4;
            }
        }
    }
}
