package io.github.rivomesh;

import java.sql.*;
import java.util.*;

/** Physical-schema ownership protects table writers even when different URLs alias one destination. */
public final class TargetOwnership {
    public static final String TABLE="_rivomesh_owner_v3";
    private TargetOwnership(){}
    private static boolean mysql(Connection c)throws SQLException{return c.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT).contains("mysql");}
    private static String q(Connection c,String name)throws SQLException{return mysql(c)?Sql.mysql(name):Sql.pg(name);}
    private static String table(Connection c,String schema)throws SQLException{return q(c,schema)+"."+q(c,TABLE);}
    public static void initialize(Connection c,String schema)throws SQLException{initialize(c,schema,false);}
    public static void initialize(Connection c,String schema,boolean avoidRedundantDdl)throws SQLException{
        boolean dm=io.github.rivomesh.cdc.DmProfile.product(c);
        // OceanBase can serialize even no-op DDL behind recovery. Keep ownership/engine checks below.
        if(dm?!io.github.rivomesh.cdc.DmProfile.tableExists(c,schema,TABLE):!avoidRedundantDdl||!io.github.rivomesh.cdc.OceanBaseProfile.tableExists(c,schema,TABLE))try(Statement st=c.createStatement()){st.execute((dm?"CREATE TABLE ":"CREATE TABLE IF NOT EXISTS ")+table(c,schema)+" (table_name VARCHAR(128) PRIMARY KEY,pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,generation VARCHAR(128) NOT NULL)"+(mysql(c)?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":""));}
        catch(SQLException e){if(dm&&e.getErrorCode()==-2124||Arrays.asList("42P07","42710","23505").contains(e.getSQLState()))throw new SQLTransactionRollbackException("TARGET_OWNERSHIP_CATALOG_RETRY","40001",e);throw e;}
        if(mysql(c))try(PreparedStatement p=c.prepareStatement("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")){
            p.setString(1,schema);p.setString(2,TABLE);try(ResultSet rs=p.executeQuery()){if(!rs.next()||!"InnoDB".equalsIgnoreCase(rs.getString(1)))throw new IllegalStateException("TARGET_OWNERSHIP_ENGINE_NOT_TRANSACTIONAL");}
        }
    }
    private static boolean matches(Connection c,String schema,String name,String pipeline,String subscription,String generation,boolean lock)throws SQLException{
        try(PreparedStatement p=c.prepareStatement("SELECT pipeline_id,subscription_id,generation FROM "+table(c,schema)+" WHERE table_name=?"+(lock?" FOR UPDATE":""))){
            p.setString(1,name);try(ResultSet rs=p.executeQuery()){if(!rs.next())return false;if(!pipeline.equals(rs.getString(1))||!subscription.equals(rs.getString(2))||!generation.equals(rs.getString(3)))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");return true;}
        }
    }
    public static void claim(Connection c,String schema,String name,String pipeline,String subscription,String generation)throws SQLException{
        if(TABLE.equalsIgnoreCase(name))throw new IllegalArgumentException("RESERVED_TARGET_TABLE_NAME");
        if(matches(c,schema,name,pipeline,subscription,generation,true))return;
        try(PreparedStatement p=c.prepareStatement("INSERT INTO "+table(c,schema)+" (table_name,pipeline_id,subscription_id,generation) VALUES (?,?,?,?)")){
            p.setString(1,name);p.setString(2,pipeline);p.setString(3,subscription);p.setString(4,generation);p.executeUpdate();
        }catch(SQLException e){if("23505".equals(e.getSQLState())||e.getErrorCode()==1062||io.github.rivomesh.cdc.DmProfile.product(c)&&io.github.rivomesh.cdc.DmProfile.unique(e))throw new IllegalStateException("DESTINATION_ALREADY_OWNED",e);throw e;}
    }
    public static void verify(Connection c,String schema,String name,String pipeline,String subscription,String generation)throws SQLException{
        if(!matches(c,schema,name,pipeline,subscription,generation,false))throw new IllegalStateException("DESTINATION_OWNERSHIP_LOST");
    }
    public static void lockAndVerify(Connection c,String schema,String name,String pipeline,String subscription,String generation)throws SQLException{
        if(!matches(c,schema,name,pipeline,subscription,generation,true))throw new IllegalStateException("DESTINATION_OWNERSHIP_LOST");
    }
}
