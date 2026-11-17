package io.github.rivomesh.cdc;

import java.sql.*;
import java.util.*;
import java.util.regex.*;

/** Reject side effects outside a reviewed table delta. No automatic dependency rewriting. */
final class RepairDependencies {
    private RepairDependencies(){}
    static void verify(Connection c,TargetProduct product,String schema,String name,String qualified)throws Exception{
        if(product==TargetProduct.MYSQL)mysqlVisibility(c);
        if(product==TargetProduct.POSTGRESQL){
            try(Statement lock=c.createStatement()){lock.execute("LOCK TABLE "+qualified+" IN SHARE ROW EXCLUSIVE MODE");}
            String query="SELECT t.relrowsecurity,t.relkind,EXISTS(SELECT 1 FROM pg_catalog.pg_trigger g WHERE g.tgrelid=t.oid AND NOT g.tgisinternal),EXISTS(SELECT 1 FROM pg_catalog.pg_constraint f WHERE f.contype='f' AND (f.conrelid=t.oid OR f.confrelid=t.oid)),EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=t.oid OR i.inhparent=t.oid),EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=t.oid AND r.rulename<>'_RETURN') FROM pg_catalog.pg_class t JOIN pg_catalog.pg_namespace n ON t.relnamespace=n.oid WHERE n.nspname=? AND t.relname=?";
            try(PreparedStatement p=c.prepareStatement(query)){p.setString(1,schema);p.setString(2,name);try(ResultSet r=p.executeQuery()){if(!r.next()||r.getBoolean(1)||!"r".equals(r.getString(2))||r.getBoolean(3)||r.getBoolean(4)||r.getBoolean(5)||r.getBoolean(6))throw new IllegalStateException("REPAIR_DEPENDENCY_PLAN_REQUIRED");}}
            return;
        }
        DatabaseMetaData metadata=c.getMetaData();
        try(ResultSet keys=metadata.getImportedKeys(product==TargetProduct.MYSQL?schema:c.getCatalog(),product==TargetProduct.MYSQL?null:schema,name)){if(keys.next())throw new IllegalStateException("REPAIR_DEPENDENCY_PLAN_REQUIRED");}
        try(ResultSet keys=metadata.getExportedKeys(product==TargetProduct.MYSQL?schema:c.getCatalog(),product==TargetProduct.MYSQL?null:schema,name)){if(keys.next())throw new IllegalStateException("REPAIR_DEPENDENCY_PLAN_REQUIRED");}
        try(PreparedStatement p=c.prepareStatement("SELECT 1 FROM INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?")){p.setString(1,schema);p.setString(2,name);try(ResultSet rows=p.executeQuery()){if(rows.next())throw new IllegalStateException("REPAIR_DEPENDENCY_PLAN_REQUIRED");}}
    }
    /** Direct grants only initially; role-only grants/partial revokes require a richer visibility plan. */
    private static void mysqlVisibility(Connection c)throws Exception{
        boolean select=false,trigger=false;Pattern global=Pattern.compile("^GRANT (.+) ON \\*\\.\\* TO .+$",Pattern.CASE_INSENSITIVE);
        try(Statement statement=c.createStatement();ResultSet rows=statement.executeQuery("SHOW GRANTS")){
            while(rows.next()){
                String grant=rows.getString(1);if(grant.toUpperCase(Locale.ROOT).startsWith("REVOKE "))throw new IllegalStateException("REPAIR_METADATA_VISIBILITY_REQUIRED");Matcher match=global.matcher(grant);if(!match.matches())continue;
                Set<String> privileges=new HashSet<>(Arrays.asList(match.group(1).toUpperCase(Locale.ROOT).split(",\\s*")));boolean all=privileges.contains("ALL PRIVILEGES");select|=all||privileges.contains("SELECT");trigger|=all||privileges.contains("TRIGGER");
            }
        }
        if(!select||!trigger)throw new IllegalStateException("REPAIR_METADATA_VISIBILITY_REQUIRED");
    }
}
