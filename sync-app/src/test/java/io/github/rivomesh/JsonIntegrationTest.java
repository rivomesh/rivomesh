package io.github.rivomesh;

import io.github.rivomesh.cdc.JsonValues;
import java.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.integration",matches="true")
class JsonIntegrationTest {
    @Test void snapshotAndBinaryJsonChangesPreserveNullsUnsignedNumbersAndOpaqueDecimals()throws Exception{
        Config config=ClusterHarness.config();
        try(ClusterHarness h=new ClusterHarness(config)){
            String source=Sql.mysql(config.source.database)+".json_docs",target=Sql.pg(config.target.schema)+".json_docs";
            ClusterHarness.sql("mysql","CREATE TABLE "+source+" (id INT PRIMARY KEY,document JSON) ENGINE=InnoDB");
            ClusterHarness.sql("mysql","INSERT INTO "+source+" VALUES (1,'{\"uint\":18446744073709551615,\"array\":[true,null,\"汉字🙂\"]}'),(2,NULL),(3,'null')");
            h.start("json-worker");h.await(()->"STREAMING".equals(h.store.phase())&&equal(h,source,target,1)&&equal(h,source,target,2)&&equal(h,source,target,3),60);
            assertNull(readTarget(h,target,2));assertEquals("null",readTarget(h,target,3));
            ClusterHarness.sql("mysql","UPDATE "+source+" SET document=JSON_SET(document,'$.decimal',CAST('123456789012345678901234567890.12345678901234567890' AS DECIMAL(60,20))) WHERE id=1");
            h.await(()->equal(h,source,target,1)&&readTarget(h,target,1).contains("123456789012345678901234567890.12345678901234567890"),30);
            h.kill("json-worker");
            ClusterHarness.sql("mysql","UPDATE "+source+" SET document='[1,2,3,null,false]' WHERE id=1");
            ClusterHarness.sql("mysql","DELETE FROM "+source+" WHERE id=2");
            h.start("json-worker");h.await(()->equal(h,source,target,1)&&count(h,target,"id=2")==0,45);
        }
    }
    private boolean equal(ClusterHarness h,String source,String target,int id)throws Exception{
        String value;
        try(Connection c=ClusterHarness.mysql("mysql");Statement s=c.createStatement();ResultSet rs=s.executeQuery("SELECT document FROM "+source+" WHERE id="+id)){if(!rs.next())return false;value=rs.getString(1);}
        return JsonValues.equivalent(value,readTarget(h,target,id));
    }
    private String readTarget(ClusterHarness h,String table,int id)throws Exception{
        try(Connection c=h.store.connect();Statement s=c.createStatement();ResultSet rs=s.executeQuery("SELECT document FROM "+table+" WHERE id="+id)){if(!rs.next())throw new IllegalStateException("not-yet-copied");return rs.getString(1);}
    }
    private int count(ClusterHarness h,String table,String condition)throws Exception{
        try(Connection c=h.store.connect();Statement s=c.createStatement();ResultSet rs=s.executeQuery("SELECT COUNT(*) FROM "+table+" WHERE "+condition)){rs.next();return rs.getInt(1);}
    }
}
