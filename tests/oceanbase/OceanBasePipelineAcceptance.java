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

/** Actual MySQL source, PostgreSQL/OceanBase targets, journal and fat-JAR worker processes. */
public final class OceanBasePipelineAcceptance {
    static final String JAR="/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar";
    final Path directory=Paths.get(System.getenv("RIVOMESH_OB_DIR"));
    final String id="ob_"+UUID.randomUUID().toString().replace("-","").substring(0,20);
    final PipelineConfig config=new PipelineConfig();
    Path configuration;
    Process worker;
    int starts;
    interface Condition{boolean ready()throws Exception;}
    static void require(boolean condition,String message){if(!condition)throw new AssertionError(message);}
    void await(Condition condition)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(360);String last=null;while(System.nanoTime()<end){try{if(condition.ready())return;}catch(Exception failure){last=failure.getClass().getName();}Thread.sleep(100);}throw new AssertionError("Timed out waiting for real pipeline; last="+last);}
    void source(String statement)throws Exception{try(Connection connection=JdbcConnections.open("com.mysql.cj.jdbc.Driver","jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000","root","Sync@Test123");Statement sql=connection.createStatement()){sql.execute(statement);}}
    Connection postgres()throws Exception{return JdbcConnections.open("org.postgresql.Driver","jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10","sync_test","Sync@Test123");}
    void pg(String statement)throws Exception{try(Connection connection=postgres();Statement sql=connection.createStatement()){sql.execute(statement);}}
    long pgCount(String table)throws Exception{try(Connection connection=postgres();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.pg(id+"_pg")+"."+Sql.pg(table))){rows.next();return rows.getLong(1);}}
    Connection oceanbase()throws Exception{PipelineConfig.Target target=config.targets.get(0);return JdbcConnections.open("com.mysql.cj.jdbc.Driver",target.url,target.username,target.password);}
    void ob(String statement)throws Exception{PipelineConfig.Target target=config.targets.get(0);try(Connection c=JdbcConnections.open("com.mysql.cj.jdbc.Driver",target.url.replace("socketTimeout=60000","socketTimeout=120000"),target.username,target.password);Statement sql=c.createStatement()){sql.execute(statement);}}
    long obCount(String table)throws Exception{try(Connection c=oceanbase();Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.mysql(id+"_copy")+"."+Sql.mysql(table))){rows.next();return rows.getLong(1);}}
    Journal.Subscription subscription(JdbcJournal journal,String id)throws Exception{return journal.subscriptions().stream().filter(item->item.id.equals(id)).findFirst().get();}
    void phase(String phase)throws Exception{Files.write(directory.resolve(phase+".request"),new byte[]{1});await(()->Files.exists(directory.resolve(phase+".done")));}
    void start()throws Exception{worker=new ProcessBuilder("java","-Xmx256m","-jar",JAR,"run",configuration.toString()).redirectErrorStream(true).redirectOutput(directory.resolve("worker-"+(++starts)+".log").toFile()).start();}
    void kill()throws Exception{if(worker!=null){worker.destroyForcibly();require(worker.waitFor(20,TimeUnit.SECONDS),"Worker SIGKILL was not confirmed");worker=null;}}
    void command(String... arguments)throws Exception{List<String> args=new ArrayList<>(Arrays.asList("java","-Xmx128m","-jar",JAR));args.addAll(Arrays.asList(arguments));Process operation=new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(directory.resolve("command-"+UUID.randomUUID()+".log").toFile()).start();require(operation.waitFor(45,TimeUnit.SECONDS),"CLI operation did not finish");require(operation.exitValue()==0,"CLI operation failed");}
    void insert(int value)throws Exception{source("INSERT INTO "+Sql.mysql(id)+".items(id,u,amount,payload,bytes,base_value) SELECT "+value+",u,amount,payload,bytes,"+(value*10)+" FROM "+Sql.mysql(id)+".items WHERE id=3");}
    void configure()throws Exception{
        Files.createDirectories(directory);Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"));config.pipelineId=id;config.nodeId="worker";config.source.clusterId=id;config.source.host="mysql";config.source.database=id;config.source.username="root";config.source.password="Sync@Test123";config.source.serverId=991716;config.databases=Collections.singletonList(id);config.chunkSize=1;config.pollIntervalMillis=100;config.retryDelayMillis=300;config.leaseMillis=5000;config.heartbeatMillis=300;config.journal.path=directory.resolve("journal").toString();config.journal.maxBytes=128*1024*1024;
        PipelineConfig.Target ob=new PipelineConfig.Target();ob.id="oceanbase";ob.product=TargetProduct.OCEANBASE;ob.url="jdbc:mysql://oceanbase:2881/?useSSL=false&connectTimeout=3000&socketTimeout=60000";ob.username="root@test";ob.password="Sync@Test123";ob.metadataSchema=id+"_meta";ob.mappings.put(id,id+"_copy");config.targets.add(ob);
        PipelineConfig.Target pg=new PipelineConfig.Target();pg.id="postgres";pg.product=TargetProduct.POSTGRESQL;pg.url="jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10";pg.username="sync_test";pg.password="Sync@Test123";pg.metadataSchema=id+"_pg_meta";pg.mappings.put(id,id+"_pg");config.targets.add(pg);config.validate();config.prepareLocalDirectory();configuration=directory.resolve("task.yml");Files.write(configuration,new ObjectMapper(new YAMLFactory()).writeValueAsBytes(config));Files.setPosixFilePermissions(configuration,PosixFilePermissions.fromString("rw-------"));
    }
    void verifyTypes()throws Exception{
        try(Connection connection=oceanbase();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT CAST(u AS CHAR),CAST(amount AS CHAR),JSON_UNQUOTE(JSON_EXTRACT(payload,'$.n')),HEX(bytes),calc FROM "+Sql.mysql(id+"_copy")+".items WHERE id=1")){
            require(rows.next()&&rows.getString(1).equals("18446744073709551615")&&rows.getString(2).equals("12345678901234567890123456789012345.123456789012345678901234567890")&&rows.getString(3).equals("18446744073709551615")&&rows.getString(4).equals("00FF80")&&rows.getLong(5)==14,"OceanBase exact numeric/JSON/binary/generated values changed");
        }
        try(Connection connection=postgres();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT u::text,amount::text,payload->>'n',encode(bytes,'hex'),calc FROM "+Sql.pg(id+"_pg")+".items WHERE id=1")){require(rows.next()&&rows.getString(1).equals("18446744073709551615")&&rows.getString(3).equals("18446744073709551615")&&rows.getString(4).equals("00ff80")&&rows.getLong(5)==14,"PostgreSQL fan-out types changed");}
    }
    void nativeContract()throws Exception{
        try(Connection c=oceanbase();Statement sql=c.createStatement()){
            sql.execute("SET ob_read_consistency='WEAK'");Map<String,String> profile=OceanBaseProfile.check(c);require(profile.get("mode").equals("MYSQL"),"Native tenant mode mismatch");try(ResultSet result=sql.executeQuery("SELECT @@ob_read_consistency")){require(result.next()&&result.getString(1).equalsIgnoreCase("STRONG"),"Weak checkpoint reads were not overridden");}
            System.out.println("OCEANBASE_NATIVE_PROFILE: "+profile);
        }
        JdbcSink.Settings settings=config.targetSettings(config.targets.get(0),"native-generation");settings.pipelineId=id+"-contract";settings.metadataSchema=id+"_contract_meta";settings.databases=new TreeMap<>();settings.databases.put(id,id+"_contract");settings.databases.put(id+"_second",id+"_contract_second");
        TableSchema table=new TableSchema();table.database=id;table.name="contract_rows";table.id=TableSchema.identity(id,"",table.name);table.primaryKey=Collections.singletonList("id");
        TableSchema.Column column=new TableSchema.Column();column.name="id";column.type=TableSchema.Type.INT64;column.nullable=false;table.columns=Collections.singletonList(column);
        TableSchema second=Codec.decode(Codec.encode(table),TableSchema.class);second.database=id+"_second";second.id=TableSchema.identity(second.database,"",second.name);
        SourceTransaction first=new SourceTransaction();first.id="native-first";first.streamGeneration=settings.generation;first.position=new io.github.rivomesh.api.SourcePosition(1,"MYSQL","source",Collections.singletonMap("gtid","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1"));first.changes=Arrays.asList(nativeInsert(table,1),nativeInsert(second,1));
        try(JdbcSink target=new JdbcSink(settings)){
            target.activate(1);for(TableSchema schema:Arrays.asList(table,second)){target.prepare(1,schema);Journal.SnapshotState snapshot=new Journal.SnapshotState();snapshot.tableId=schema.id;snapshot.schemaVersion=schema.version;snapshot.snapshotId=UUID.randomUUID().toString();snapshot.state="COMPLETE";target.beginSnapshot(1,schema,snapshot);target.finishSnapshot(1,schema,snapshot);}
            JdbcSink.Settings alias=Codec.decode(Codec.encode(settings),JdbcSink.Settings.class);alias.pipelineId=id+"-alias";alias.id="alias";
            try(JdbcSink other=new JdbcSink(alias)){other.activate(1);try{other.prepare(1,table);throw new AssertionError("Aliased destination accepted a second owner");}catch(IllegalStateException expected){require(expected.getMessage().equals("DESTINATION_ALREADY_OWNED"),"Wrong owner guard");}}
            target.apply(1,1,first);target.apply(1,1,first);
            SourceTransaction broken=new SourceTransaction();broken.id="native-broken";broken.streamGeneration=settings.generation;broken.position=new io.github.rivomesh.api.SourcePosition(1,"MYSQL","source",Collections.singletonMap("gtid","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:2"));broken.changes=Arrays.asList(nativeInsert(table,2),nativeInsert(second,1));
            try{target.apply(1,2,broken);throw new AssertionError("Duplicate-PK transaction was accepted");}catch(SQLException expected){require(!Sql.retryable(expected),"Duplicate PK was treated as transient");}
            require(target.checkpoint().sequence==1,"Failed native transaction advanced checkpoint");
            try(Connection c=oceanbase();Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.mysql(id+"_contract")+".contract_rows")){require(rows.next()&&rows.getLong(1)==1,"Failed native transaction partially committed");}
            target.activate(2);try{target.apply(1,2,broken);throw new AssertionError("Old epoch could still write");}catch(IllegalStateException expected){require(expected.getMessage().equals("SINK_FENCED"),"Wrong old-epoch guard");}
        }
        try(JdbcSink target=new JdbcSink(settings)){require(target.activate(3).sequence==1,"Native reopen lost checkpoint");target.prepare(3,table);target.apply(3,1,first);}
        System.out.println("OCEANBASE_NATIVE_CONTRACT_ACCEPTED: strong reads, alias ownership, atomic business/checkpoint rollback, replay, epoch fence and reopen");
    }
    static Change nativeInsert(TableSchema table,int id){Change change=new Change();change.eventId=table.id+":native-"+id;change.tableId=table.id;change.operation=Change.Operation.INSERT;change.after=Collections.singletonList(Cell.value(Integer.toString(id)));return change;}
    void run()throws Exception{
        configure();nativeContract();source("CREATE DATABASE "+Sql.mysql(id));String items=Sql.mysql(id)+".items",duplicates=Sql.mysql(id)+".duplicates";
        try{
            source("CREATE TABLE "+items+" (id BIGINT PRIMARY KEY,u BIGINT UNSIGNED,amount DECIMAL(65,30),payload JSON,bytes VARBINARY(100),base_value INT,calc INT GENERATED ALWAYS AS (base_value*2) STORED,UNIQUE KEY uq_base(base_value)) ENGINE=InnoDB");source("CREATE TABLE "+duplicates+" (value INT,payload JSON) ENGINE=InnoDB");source("INSERT INTO "+items+"(id,u,amount,payload,bytes,base_value) VALUES(1,18446744073709551615,12345678901234567890123456789012345.123456789012345678901234567890,JSON_OBJECT('n',CAST(18446744073709551615 AS UNSIGNED)),0x00ff80,7)");source("INSERT INTO "+duplicates+" VALUES(7,JSON_OBJECT('same',true)),(7,JSON_OBJECT('same',true))");command("check",configuration.toString());
            try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
                start();await(()->pgCount("items")==1&&obCount("items")==1&&obCount("duplicates")==2);verifyTypes();String generation=(String)journal.status().get("generation"),snapshot=journal.snapshot("source",TableSchema.identity(id,"","items")).snapshotId;
                source("UPDATE "+items+" SET id=3,base_value=9 WHERE id=1");source("DELETE FROM "+duplicates+" LIMIT 1");await(()->pgCount("duplicates")==1&&obCount("duplicates")==1);await(()->subscription(journal,"oceanbase").acknowledged==((Number)journal.status().get("lastSequence")).longValue());
                phase("stop");insert(4);await(()->pgCount("items")==2&&subscription(journal,"postgres").acknowledged>subscription(journal,"oceanbase").acknowledged);phase("start");await(()->obCount("items")==2&&subscription(journal,"oceanbase").acknowledged==subscription(journal,"postgres").acknowledged);
                kill();insert(5);start();await(()->pgCount("items")==3&&obCount("items")==3);require(generation.equals(journal.status().get("generation"))&&snapshot.equals(journal.snapshot("source",TableSchema.identity(id,"","items")).snapshotId),"Worker restart repeated/reset snapshot identity");
                pg("ALTER TABLE "+Sql.pg(id+"_pg")+".items ADD CONSTRAINT block_next CHECK(id<6)");insert(6);await(()->"PAUSED".equals(subscription(journal,"postgres").state)&&obCount("items")==4);require(pgCount("items")==3,"Failed SQL transaction was committed");long error=subscription(journal,"postgres").errorGeneration;pg("ALTER TABLE "+Sql.pg(id+"_pg")+".items DROP CONSTRAINT block_next");command("resume",configuration.toString(),"postgres",Long.toString(error));await(()->pgCount("items")==4&&subscription(journal,"postgres").acknowledged==subscription(journal,"oceanbase").acknowledged);
                ob("ALTER TABLE "+Sql.mysql(id+"_copy")+".items ADD CONSTRAINT block_ob CHECK(id<7)");long acknowledged=subscription(journal,"oceanbase").acknowledged;insert(7);await(()->pgCount("items")==5&&"PAUSED".equals(subscription(journal,"oceanbase").state));require(obCount("items")==4&&subscription(journal,"oceanbase").acknowledged==acknowledged,"Failed OceanBase transaction or checkpoint committed");long obError=subscription(journal,"oceanbase").errorGeneration;kill();start();await(()->"PAUSED".equals(subscription(journal,"oceanbase").state));require(obCount("items")==4,"Restart bypassed SQL pause");ob("ALTER TABLE "+Sql.mysql(id+"_copy")+".items DROP CONSTRAINT block_ob");command("resume",configuration.toString(),"oceanbase",Long.toString(obError));await(()->obCount("items")==5&&subscription(journal,"oceanbase").acknowledged==subscription(journal,"postgres").acknowledged);
                ob("DROP TABLE "+Sql.mysql(id+"_copy")+".items");insert(8);await(()->pgCount("items")==6&&"PAUSED".equals(subscription(journal,"oceanbase").state));require(Arrays.asList("TARGET_TABLE_MISSING_REBUILD_REQUIRED","SQL_42S02").contains(subscription(journal,"oceanbase").errorCode),"Missing table did not isolate its target");Map<String,Object> paused=PipelineControls.pause(config);Map<?,?> fences=(Map<?,?>)paused.get("targetFences");require("FENCED".equals(fences.get("postgres"))&&"FENCED".equals(fences.get("oceanbase")),"Transactional target fences unconfirmed");
            }
            System.out.println("OCEANBASE_PIPELINE_ACCEPTED: actual MySQL8.4 to PostgreSQL17+OceanBase4.3.5 MySQL tenant, snapshot/CDC/uint64/decimal/JSON/binary/generated/keyless, target outage with independent SQL progress/catchup, worker SIGKILL resume, independent SQL error rollback/repair/retry, persistent pause, missing-table isolation and transactional fences");
        }finally{kill();source("DROP DATABASE IF EXISTS "+Sql.mysql(id));}
    }
    public static void main(String[] args)throws Exception{new OceanBasePipelineAcceptance().run();}
}
