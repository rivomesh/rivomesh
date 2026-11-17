package io.github.rivomesh.cdc;

import java.sql.*;
import java.util.*;

/** MySQL-mode user tenant; an OceanBase target is never accepted from its wire version alone. */
public final class OceanBaseProfile {
    private OceanBaseProfile(){}
    public static Map<String,String> check(Connection connection)throws SQLException{
        String version,mode;long tenant;
        try(Statement sql=connection.createStatement()){
            sql.setQueryTimeout(10);
            try(ResultSet rows=sql.executeQuery("SELECT VERSION(),@@ob_compatibility_mode,effective_tenant_id()")){
                if(!rows.next())throw new SQLException("OCEANBASE_IDENTITY_UNAVAILABLE","0A000");
                version=rows.getString(1);mode=rows.getString(2);tenant=rows.getLong(3);
                validate(version,mode,tenant);
            }
            // Weak reads can return a stale checkpoint after an uncertain commit.
            sql.execute("SET ob_read_consistency='STRONG'");
        }
        Map<String,String> result=new LinkedHashMap<>();result.put("version",version);result.put("mode",mode);result.put("tenantId",Long.toString(tenant));result.put("readConsistency","STRONG");return result;
    }
    public static boolean databaseExists(Connection connection,String database)throws SQLException{
        try(PreparedStatement sql=connection.prepareStatement("SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=? LIMIT 1")){sql.setString(1,database);try(ResultSet rows=sql.executeQuery()){return rows.next();}}
    }
    public static boolean tableExists(Connection connection,String database,String table)throws SQLException{
        try(PreparedStatement sql=connection.prepareStatement("SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=? LIMIT 1")){sql.setString(1,database);sql.setString(2,table);try(ResultSet rows=sql.executeQuery()){return rows.next();}}
    }
    static void validate(String version,String mode,long tenant){
        if(version==null||version.length()>256||!version.toLowerCase(Locale.ROOT).contains("oceanbase")||!"MYSQL".equalsIgnoreCase(mode)||tenant<=1)throw new IllegalArgumentException("OCEANBASE_MYSQL_USER_TENANT_REQUIRED");
    }
}
