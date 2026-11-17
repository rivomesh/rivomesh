package io.github.rivomesh.acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.*;
import io.github.rivomesh.cdc.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Actual MySQL source, PostgreSQL/Hive targets, journal and fat-JAR worker processes. */
public final class HivePipelineAcceptance {
    static final String JAR="/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar";
    final Path directory=Paths.get(System.getenv("RIVOMESH_HIVE_DIR"));
    final String id="hive_"+UUID.randomUUID().toString().replace("-","").substring(0,20);
    final PipelineConfig config=new PipelineConfig();
    Path configuration;
    Process worker;
    int starts;
    interface Condition{boolean ready()throws Exception;}
    static void require(boolean condition,String message){if(!condition)throw new AssertionError(message);}
    void await(Condition condition)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(600);String last=null;while(System.nanoTime()<end){try{if(condition.ready())return;}catch(Exception failure){last=failure.getClass().getName();}Thread.sleep(1000);}throw new AssertionError("Timed out waiting for real pipeline; last="+last);}
    void source(String statement)throws Exception{try(Connection connection=JdbcConnections.open("com.mysql.cj.jdbc.Driver","jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000","root","Sync@Test123");Statement sql=connection.createStatement()){sql.execute(statement);}}
    Connection postgres()throws Exception{return JdbcConnections.open("org.postgresql.Driver","jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10","sync_test","Sync@Test123");}
    void pg(String statement)throws Exception{try(Connection connection=postgres();Statement sql=connection.createStatement()){sql.execute(statement);}}
    long pgCount(String table)throws Exception{try(Connection connection=postgres();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.pg(id+"_pg")+"."+Sql.pg(table))){rows.next();return rows.getLong(1);}}
    long hiveCount(String table)throws Exception{PipelineConfig.Target target=config.targets.get(0);try(HiveSqlClient sql=new HiveSqlClient(target.url,target.username,target.password,300,4*1024*1024,16*1024*1024)){return Long.parseLong(sql.query("SELECT count(*) AS n FROM "+HiveRows.q(id+"_copy")+"."+HiveRows.q(table)).get(0).get("n"));}}
    Journal.Subscription subscription(JdbcJournal journal,String id)throws Exception{return journal.subscriptions().stream().filter(item->item.id.equals(id)).findFirst().get();}
    void phase(String phase)throws Exception{System.out.println("HIVE_PIPELINE_PHASE: "+phase);Files.write(directory.resolve(phase+".request"),new byte[]{1});await(()->Files.exists(directory.resolve(phase+".done")));}
    void start()throws Exception{worker=new ProcessBuilder("java","-Xmx256m","-jar",JAR,"run",configuration.toString()).redirectErrorStream(true).redirectOutput(directory.resolve("worker-"+(++starts)+".log").toFile()).start();}
    void kill()throws Exception{if(worker!=null){worker.destroyForcibly();require(worker.waitFor(20,TimeUnit.SECONDS),"Worker SIGKILL was not confirmed");worker=null;}}
    void command(String... arguments)throws Exception{List<String> args=new ArrayList<>(Arrays.asList("java","-Xmx128m","-jar",JAR));args.addAll(Arrays.asList(arguments));Process operation=new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(directory.resolve("command-"+UUID.randomUUID()+".log").toFile()).start();require(operation.waitFor(180,TimeUnit.SECONDS),"CLI operation did not finish");require(operation.exitValue()==0,"CLI operation failed");}
    void insert(int value)throws Exception{source("INSERT INTO "+Sql.mysql(id)+".items(id,u,amount,payload,bytes,base_value) SELECT "+value+",u,amount,payload,bytes,"+(value*10)+" FROM "+Sql.mysql(id)+".items WHERE id=3");}
    void configure()throws Exception{
        Files.createDirectories(directory);Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"));config.pipelineId=id;config.nodeId="worker";config.source.clusterId=id;config.source.host="mysql";config.source.database=id;config.source.username="root";config.source.password="Sync@Test123";config.source.serverId=991716;config.databases=Collections.singletonList(id);config.chunkSize=1;config.pollIntervalMillis=100;config.retryDelayMillis=300;config.leaseMillis=15000;config.heartbeatMillis=300;config.journal.path=directory.resolve("journal").toString();config.journal.maxBytes=128*1024*1024;
        PipelineConfig.Target hive=new PipelineConfig.Target();hive.id="hive";hive.product=TargetProduct.HIVE;hive.url="jdbc:hive2://hive:10000/default;auth=noSasl";hive.username="hive";hive.password="";hive.metadataSchema=id+"_meta";hive.mappings.put(id,id+"_copy");hive.hive=new PipelineConfig.Hive();hive.hive.acknowledgeIndexOmission=true;hive.hive.queryTimeoutSeconds=300;config.targets.add(hive);
        PipelineConfig.Target pg=new PipelineConfig.Target();pg.id="postgres";pg.product=TargetProduct.POSTGRESQL;pg.url="jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10";pg.username="sync_test";pg.password="Sync@Test123";pg.metadataSchema=id+"_pg_meta";pg.mappings.put(id,id+"_pg");config.targets.add(pg);config.validate();config.prepareLocalDirectory();configuration=directory.resolve("task.yml");Files.write(configuration,new ObjectMapper(new YAMLFactory()).writeValueAsBytes(config));Files.setPosixFilePermissions(configuration,PosixFilePermissions.fromString("rw-------"));
    }
    void verifyTypes()throws Exception{
        PipelineConfig.Target target=config.targets.get(0);try(HiveSqlClient sql=new HiveSqlClient(target.url,target.username,target.password,300,4*1024*1024,16*1024*1024)){
            Map<String,String> row=sql.query("SELECT CAST(u AS STRING) AS u,CAST(amount AS STRING) AS amount,payload,bytes,calc FROM "+HiveRows.q(id+"_copy")+".items WHERE id=1").get(0);require(row.get("u").equals("18446744073709551615"),"Hive unsigned precision lost");require(row.get("amount").equals("12345678901234567890123456789012345.123456789012345678901234567890"),"Hive decimal precision lost");require(row.get("payload").contains("18446744073709551615"),"Hive JSON precision lost");require(row.get("bytes").equals("AP+A"),"Hive binary encoding changed");require(row.get("calc").equals("14"),"Hive generated value changed");
        }
        try(Connection connection=postgres();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT u::text,amount::text,payload->>'n',encode(bytes,'hex'),calc FROM "+Sql.pg(id+"_pg")+".items WHERE id=1")){require(rows.next()&&rows.getString(1).equals("18446744073709551615")&&rows.getString(3).equals("18446744073709551615")&&rows.getString(4).equals("00ff80")&&rows.getLong(5)==14,"PostgreSQL fan-out types changed");}
    }
    void run()throws Exception{
        configure();source("CREATE DATABASE "+Sql.mysql(id));String items=Sql.mysql(id)+".items",duplicates=Sql.mysql(id)+".duplicates";
        try{
            source("CREATE TABLE "+items+" (id BIGINT PRIMARY KEY,u BIGINT UNSIGNED,amount DECIMAL(65,30),payload JSON,bytes VARBINARY(100),base_value INT,calc INT GENERATED ALWAYS AS (base_value*2) STORED,UNIQUE KEY uq_base(base_value)) ENGINE=InnoDB");source("CREATE TABLE "+duplicates+" (value INT,payload JSON) ENGINE=InnoDB");source("INSERT INTO "+items+"(id,u,amount,payload,bytes,base_value) VALUES(1,18446744073709551615,12345678901234567890123456789012345.123456789012345678901234567890,JSON_OBJECT('n',CAST(18446744073709551615 AS UNSIGNED)),0x00ff80,7)");source("INSERT INTO "+duplicates+" VALUES(7,JSON_OBJECT('same',true)),(7,JSON_OBJECT('same',true))");command("check",configuration.toString());
            Map<String,Object> review=PipelineEngine.check(config);List<?> plans=(List<?>)review.get("hiveIndexPlans");require(plans!=null&&plans.size()==2,"Native check omitted Hive per-table plans");boolean reviewedUnique=false;
            for(Object entry:plans){Map<?,?> item=(Map<?,?>)entry;HiveIndexPlan plan=(HiveIndexPlan)item.get("plan");for(HiveIndexPlan.Decision decision:plan.decisions)if(decision.sourceUnique&&!decision.sourceName.equals("PRIMARY")){require("OMIT_BY_ACKNOWLEDGEMENT".equals(decision.action),"Native source UNIQUE reported a fake target index");reviewedUnique=true;}if(String.valueOf(item.get("table")).endsWith(".items"))require("STRING".equals(((Map<?,?>)item.get("columnStorage")).get("amount")),"Wide source decimal plan lost storage disclosure");}
            require(reviewedUnique,"Actual MySQL UNIQUE was not discovered/reviewed");
            try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
                start();await(()->pgCount("items")==1&&hiveCount("items")==1&&hiveCount("duplicates")==2);verifyTypes();System.out.println("HIVE_PIPELINE_SNAPSHOT_ACCEPTED");String generation=(String)journal.status().get("generation"),snapshot=journal.snapshot("source",TableSchema.identity(id,"","items")).snapshotId;
                source("UPDATE "+items+" SET id=3,base_value=9 WHERE id=1");source("DELETE FROM "+duplicates+" LIMIT 1");await(()->pgCount("duplicates")==1&&hiveCount("duplicates")==1);await(()->subscription(journal,"hive").acknowledged==((Number)journal.status().get("lastSequence")).longValue());
                System.out.println("HIVE_PIPELINE_CDC_ACCEPTED");phase("stop");insert(4);await(()->pgCount("items")==2&&subscription(journal,"postgres").acknowledged>subscription(journal,"hive").acknowledged);phase("start");await(()->hiveCount("items")==2&&subscription(journal,"hive").acknowledged==subscription(journal,"postgres").acknowledged);
                System.out.println("HIVE_PIPELINE_OUTAGE_RECOVERY_ACCEPTED");kill();insert(5);start();await(()->pgCount("items")==3&&hiveCount("items")==3);require(generation.equals(journal.status().get("generation"))&&snapshot.equals(journal.snapshot("source",TableSchema.identity(id,"","items")).snapshotId),"Worker restart repeated/reset snapshot identity");
                System.out.println("HIVE_PIPELINE_WORKER_RESTART_ACCEPTED");pg("ALTER TABLE "+Sql.pg(id+"_pg")+".items ADD CONSTRAINT block_next CHECK(id<6)");insert(6);await(()->"PAUSED".equals(subscription(journal,"postgres").state)&&hiveCount("items")==4);require(pgCount("items")==3,"Failed SQL transaction was committed");long error=subscription(journal,"postgres").errorGeneration;pg("ALTER TABLE "+Sql.pg(id+"_pg")+".items DROP CONSTRAINT block_next");command("resume",configuration.toString(),"postgres",Long.toString(error));await(()->pgCount("items")==4&&subscription(journal,"postgres").acknowledged==subscription(journal,"hive").acknowledged);
                PipelineConfig.Target target=config.targets.get(0);try(HiveSqlClient sql=new HiveSqlClient(target.url,target.username,target.password,300,4*1024*1024,16*1024*1024)){sql.execute("DROP VIEW "+HiveRows.q(id+"_copy")+".items");}insert(7);await(()->pgCount("items")==5&&"PAUSED".equals(subscription(journal,"hive").state));require("HIVE_TABLE_MISSING_REBUILD_REQUIRED".equals(subscription(journal,"hive").errorCode),"Missing view did not isolate its target");Map<String,Object> paused=PipelineControls.pause(config);Map<?,?> fences=(Map<?,?>)paused.get("targetFences");require("FENCED".equals(fences.get("postgres"))&&String.valueOf(fences.get("hive")).startsWith("UNCONFIRMED"),"Pause overstated the materialized fence");
            }
            System.out.println("HIVE_PIPELINE_ACCEPTED: actual MySQL8.4 to PostgreSQL17+Hive4.0.1, snapshot/CDC/uint64/decimal/JSON/binary/generated/keyless, Hive outage with independent SQL progress/catchup, worker SIGKILL resume, SQL error repair/retry while Hive continues, missing-view isolation and honest pause fences");
        }finally{kill();source("DROP DATABASE IF EXISTS "+Sql.mysql(id));}
    }
    public static void main(String[] args)throws Exception{new HivePipelineAcceptance().run();}
}
