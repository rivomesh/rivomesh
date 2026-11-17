package io.github.rivomesh;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rivomesh.cdc.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.clickhouse.integration",matches="true")
class ClickHousePipelineIntegrationTest {
    @TempDir Path directory;PipelineConfig config;Process worker;Path log;
    void start()throws Exception{Path file=directory.resolve("task.yml");new ObjectMapper(new YAMLFactory()).writeValue(file.toFile(),config);log=directory.resolve("worker-"+UUID.randomUUID()+".log");worker=new ProcessBuilder(System.getProperty("java.home")+"/bin/java","-Xmx256m","-jar",System.getProperty("sync.test.jar"),"run",file.toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();}
    void kill()throws Exception{if(worker!=null&&worker.isAlive()){worker.destroyForcibly();assertTrue(worker.waitFor(10,TimeUnit.SECONDS));}}
    interface Condition{boolean get()throws Exception;}
    void await(Condition condition)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);Exception last=null;while(System.nanoTime()<end){if(worker!=null&&!worker.isAlive())fail(new String(Files.readAllBytes(log),StandardCharsets.UTF_8));try{if(condition.get())return;}catch(Exception failure){last=failure;}Thread.sleep(150);}fail("Timed out "+(last==null?"":last.getClass().getName())+"\n"+new String(Files.readAllBytes(log),StandardCharsets.UTF_8));}
    ClickHouseHttpClient client()throws Exception{return new ClickHouseHttpClient(PipelineConfig.clickHouseHttp(config.targets.get(0)));}
    long count(String name,String where)throws Exception{try(ClickHouseHttpClient c=client()){return c.query("SELECT count() FROM "+ClickHouseMaterializer.q(config.targets.get(0).mappings.get(config.databases.get(0)))+"."+ClickHouseMaterializer.q(name)+" WHERE "+where).path("data").get(0).get(0).asLong();}}
    long pgCount(String where)throws Exception{PipelineConfig.Target t=config.targets.get(1);try(Connection c=JdbcConnections.open("org.postgresql.Driver",t.url,t.username,t.password);Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT count(*) FROM "+Sql.pg(t.mappings.get(config.databases.get(0)))+".items WHERE "+where)){r.next();return r.getLong(1);}}
    long last(JdbcJournal j)throws Exception{return ((Number)j.status().get("lastSequence")).longValue();}
    long ack(JdbcJournal j,String id)throws Exception{return j.subscriptions().stream().filter(s->s.id.equals(id)).findFirst().get().acknowledged;}
    @Test void mysqlSnapshotCdcIndependentOutageWorkerKillAndTargetLoss()throws Exception{
        Config base=ClusterHarness.config();String db=base.source.database;config=new PipelineConfig();config.pipelineId=db;config.source=base.source;config.databases=Collections.singletonList(db);config.journal.path=directory.resolve("journal").toString();config.chunkSize=1;config.leaseMillis=5000;config.heartbeatMillis=300;config.pollIntervalMillis=100;config.retryDelayMillis=300;
        PipelineConfig.Target ch=new PipelineConfig.Target();ch.id="clickhouse";ch.product=TargetProduct.CLICKHOUSE;ch.url="http://clickhouse:8123";ch.username="rivomesh";ch.password="ClickHouse@Test123";ch.metadataSchema=db+"_meta";ch.mappings.put(db,db+"_ch");ch.clickhouse=new PipelineConfig.ClickHouse();ch.clickhouse.secondaryIndexes=ClickHouseIndexPlan.Mode.DATA_SKIPPING;ch.clickhouse.requestTimeoutMillis=3000;config.targets.add(ch);
        PipelineConfig.Target pg=new PipelineConfig.Target();pg.id="postgres";pg.product=TargetProduct.POSTGRESQL;pg.url="jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10";pg.username="sync_test";pg.password=ClusterHarness.PASSWORD;pg.metadataSchema=db+"_pg_meta";pg.mappings.put(db,db+"_pg");config.targets.add(pg);config.validate();
        String items=Sql.mysql(db)+".items",duplicates=Sql.mysql(db)+".duplicates";ClusterHarness.sql("mysql","CREATE DATABASE "+Sql.mysql(db));
        try{
            ClusterHarness.sql("mysql","CREATE TABLE "+items+" (id INT PRIMARY KEY,document JSON,twice INT GENERATED ALWAYS AS(id*2) STORED,u BIGINT UNSIGNED,amount DECIMAL(65,30),payload BLOB,moment DATETIME(6),UNIQUE KEY uq_u(u),KEY ix_twice(twice)) ENGINE=InnoDB");
            ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,document,u,amount,payload,moment) VALUES (1,'{\"large\":18446744073709551615}',18446744073709551615,12345678901234567890123456789012345.123456789012345678901234567890,UNHEX('000180FF'),'1000-01-01 00:00:00.000001')");
            ClusterHarness.sql("mysql","CREATE TABLE "+duplicates+" (label VARCHAR(100)) ENGINE=InnoDB");ClusterHarness.sql("mysql","INSERT INTO "+duplicates+" VALUES ('same'),('same')");config.prepareLocalDirectory();
            try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
                start();await(()->count("items","id=1 AND twice=2 AND u=18446744073709551615 AND hex(payload)='000180FF'")==1&&count("duplicates","1")==2);await(()->((Number)journal.status().get("snapshotBytes")).longValue()==0);
                try(ClickHouseHttpClient c=client()){assertEquals(2,c.query("SELECT count() FROM system.data_skipping_indices WHERE database=\'"+ch.metadataSchema+"\'").path("data").get(0).get(0).asInt());}
                Map<String,Object> checked=PipelineEngine.check(config);assertNotNull(checked.get("clickhouseIndexPlans"));assertTrue(Codec.encode(checked).contains("CREATE_NON_UNIQUE_SKIPPING"));
                ClusterHarness.sql("mysql","UPDATE "+items+" SET id=2,u=42 WHERE id=1");ClusterHarness.sql("mysql","DELETE FROM "+duplicates+" LIMIT 1");await(()->count("items","id=2 AND twice=4")==1&&count("items","id=1")==0&&count("duplicates","1")==1);assertEquals(0,count("items","u=18446744073709551615"));assertEquals(1,count("items","u=42"));await(()->ack(journal,"clickhouse")==last(journal)&&ack(journal,"postgres")==last(journal));
                Path markers=Paths.get(System.getenv("RIVOMESH_CLICKHOUSE_RESTART_DIR"));Files.write(markers.resolve("stop.request"),new byte[]{1});await(()->Files.exists(markers.resolve("stop.done")));ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,document) VALUES (3,'null')");await(()->pgCount("id=3")==1);await(()->journal.subscriptions().stream().anyMatch(s->s.id.equals("clickhouse")&&s.state.equals("RUNNING")&&s.errorCode!=null));assertTrue(ack(journal,"clickhouse")<last(journal));Files.write(markers.resolve("start.request"),new byte[]{1});await(()->Files.exists(markers.resolve("start.done")));await(()->count("items","id=3")==1);await(()->ack(journal,"clickhouse")==last(journal));
                kill();ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,document) VALUES (4,NULL)");start();await(()->count("items","id=4")==1&&count("duplicates","1")==1);await(()->ack(journal,"clickhouse")==last(journal)&&ack(journal,"postgres")==last(journal));
                String physical,indexName;try(ClickHouseHttpClient c=client()){com.fasterxml.jackson.databind.JsonNode index=c.query("SELECT table,name FROM system.data_skipping_indices WHERE database='"+ch.metadataSchema+"' AND expr='u'").path("data");assertEquals(1,index.size());physical=index.get(0).get(0).asText();indexName=index.get(0).get(1).asText();c.execute("ALTER TABLE "+ClickHouseMaterializer.q(ch.metadataSchema)+"."+ClickHouseMaterializer.q(physical)+" DROP INDEX "+ClickHouseMaterializer.q(indexName));}ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,document) VALUES (5,'{}')");
                await(()->journal.subscriptions().stream().anyMatch(s->s.id.equals("clickhouse")&&s.state.equals("PAUSED")&&"CLICKHOUSE_TABLE_DEFINITION_CHANGED".equals(s.errorCode)));await(()->pgCount("id=5")==1&&ack(journal,"postgres")==last(journal));long error=journal.subscriptions().stream().filter(s->s.id.equals("clickhouse")).findFirst().get().errorGeneration;
                try(ClickHouseHttpClient c=client()){c.execute("ALTER TABLE "+ClickHouseMaterializer.q(ch.metadataSchema)+"."+ClickHouseMaterializer.q(physical)+" ADD INDEX "+ClickHouseMaterializer.q(indexName)+" u TYPE minmax GRANULARITY 1");}journal.resume("clickhouse",error);await(()->count("items","id=5")==1&&ack(journal,"clickhouse")==last(journal));
                try(ClickHouseHttpClient client=client()){client.execute("DROP VIEW "+ClickHouseMaterializer.q(ch.mappings.get(db))+".items SYNC");}ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,document) VALUES (6,'{}')");await(()->journal.subscriptions().stream().anyMatch(s->s.id.equals("clickhouse")&&s.state.equals("PAUSED")));await(()->pgCount("id=6")==1&&ack(journal,"postgres")==last(journal));assertTrue(ack(journal,"clickhouse")<last(journal));
                Map<String,Object> paused=PipelineControls.pause(config);@SuppressWarnings("unchecked")Map<String,String> fences=(Map<String,String>)paused.get("targetFences");assertEquals("UNCONFIRMED_IN_FLIGHT_MATERIALIZATION",fences.get("clickhouse"));assertEquals("FENCED",fences.get("postgres"));
            }
        }finally{kill();ClusterHarness.sql("mysql","DROP DATABASE IF EXISTS "+Sql.mysql(db));}
    }
}
