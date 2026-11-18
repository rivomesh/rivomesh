package io.github.rivomesh;

import io.github.rivomesh.cdc.PipelineFailure;
import io.github.rivomesh.cdc.SourceAdapter;
import io.github.rivomesh.cdc.SourceTransaction;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.mysql84.profile",matches="true")
class Mysql84ProfileTest {
    @Test void unsupportedServerAndSessionProfilesRefuseBeforePublishingAnUnsafeTransaction()throws Exception{
        Config config=ClusterHarness.config();
        String version=ClusterHarness.scalar("mysql","SELECT VERSION()");
        assertTrue(version.startsWith("8.4."));
        System.out.println("MYSQL_SOURCE_VERSION="+version);
        assertNotNull(new SourceSelector(config).select(null,""));
        rejectConfig(config,"binlog_row_image","'MINIMAL'","'FULL'","MYSQL_FULL_ROW_IMAGE_REQUIRED");
        rejectConfig(config,"binlog_row_value_options","'PARTIAL_JSON'","''","MYSQL_PARTIAL_JSON_UNSUPPORTED");
        rejectConfig(config,"binlog_transaction_compression","ON","OFF","MYSQL_COMPRESSED_TX_UNSUPPORTED");
        rejectConfig(config,"gtid_mode","ON_PERMISSIVE","ON","MYSQL_GTID_REQUIRED");
        String database=Sql.mysql(config.source.database);
        String table=database+".items";
        ClusterHarness.sql("mysql","CREATE DATABASE "+database);
        try{
            ClusterHarness.sql("mysql","CREATE TABLE "+table+" (id BIGINT PRIMARY KEY,payload JSON) ENGINE=InnoDB");
            ClusterHarness.sql("mysql","INSERT INTO "+table+" VALUES (1,JSON_OBJECT('padding',REPEAT('a',8192),'flag',1))");
            assertStreamRefuses(config,"SET SESSION binlog_row_value_options='PARTIAL_JSON'",
                    "UPDATE "+table+" SET payload=JSON_SET(payload,'$.flag',2) WHERE id=1","MYSQL_PARTIAL_JSON_UNSUPPORTED");
            assertStreamRefuses(config,"SET SESSION binlog_transaction_compression=ON",
                    "INSERT INTO "+table+" VALUES (2,JSON_OBJECT('padding',REPEAT('b',8192)))","MYSQL_COMPRESSED_TX_UNSUPPORTED");
            try(Connection writer=ClusterHarness.mysql("mysql"); Statement sql=writer.createStatement()){
                sql.execute("SET SESSION gtid_next='AUTOMATIC:rivomesh_profile'");
                sql.execute("INSERT INTO "+table+" VALUES (3,JSON_OBJECT('flag',3))");
                sql.execute("SET SESSION gtid_next='AUTOMATIC'");
            }
            assertEquals("MYSQL_TAGGED_GTID_UNSUPPORTED",assertThrows(IllegalArgumentException.class,
                    ()->new SourceSelector(config).select(null,"")).getMessage());
        }finally{
            ClusterHarness.sql("mysql","DROP DATABASE "+database);
        }
    }

    private void rejectConfig(Config config,String setting,String invalid,String restored,String code)throws Exception{
        ClusterHarness.sql("mysql","SET GLOBAL "+setting+"="+invalid);
        try{
            assertEquals(code,assertThrows(IllegalArgumentException.class,()->new SourceSelector(config).select(null,"")).getMessage());
        }finally{
            ClusterHarness.sql("mysql","SET GLOBAL "+setting+"="+restored);
        }
    }

    private void assertStreamRefuses(Config config,String sessionOption,String change,String code)throws Exception{
        List<SourceTransaction> delivered=new CopyOnWriteArrayList<>();
        try(MysqlCdcSource source=new MysqlCdcSource(config,Collections.singleton(config.source.database),"mysql84-profile")){
            source.discover();
            try(SourceAdapter.Stream stream=source.stream(source.position(),delivered::add);
                Connection writer=ClusterHarness.mysql("mysql");Statement sql=writer.createStatement()){
                sql.execute(sessionOption);
                sql.execute(change);
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
                PipelineFailure diagnostic=null;
                while(System.nanoTime()<deadline&&delivered.isEmpty()){
                    try{stream.check();}catch(PipelineFailure failure){diagnostic=failure;break;}
                    Thread.sleep(25);
                }
                assertTrue(delivered.isEmpty(),"The unsupported transaction reached the receiver");
                assertNotNull(diagnostic,"The stream did not identify the unsupported profile");
                assertEquals(PipelineFailure.Phase.SOURCE_DECODE,diagnostic.phase);
                assertEquals(code,diagnostic.getCause().getMessage());
            }
        }
    }
}
