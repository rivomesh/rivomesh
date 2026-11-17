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

/** Actual MySQL source, PostgreSQL/Gbase8c targets, journal and fat-JAR worker processes. */
public final class Gbase8cPipelineAcceptance {
    static final String JAR="/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar";
    final Path directory=Paths.get(System.getenv("RIVOMESH_GBASE8C_DIR"));
    final String id="gb_"+UUID.randomUUID().toString().replace("-","").substring(0,20);
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
    Connection gbase8c()throws Exception{PipelineConfig.Target target=config.targets.get(0);return JdbcConnections.open("com.gbase8c.Driver",target.url,target.username,target.password);}
    void ob(String statement)throws Exception{PipelineConfig.Target target=config.targets.get(0);try(Connection c=JdbcConnections.open("com.gbase8c.Driver",target.url,target.username,target.password);Statement sql=c.createStatement()){sql.execute(statement);}}
    long obCount(String table)throws Exception{try(Connection c=gbase8c();Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.pg(id+"_copy")+"."+Sql.pg(table))){rows.next();return rows.getLong(1);}}
    Journal.Subscription subscription(JdbcJournal journal,String id)throws Exception{return journal.subscriptions().stream().filter(item->item.id.equals(id)).findFirst().get();}
    void phase(String phase)throws Exception{Files.write(directory.resolve(phase+".request"),new byte[]{1});await(()->Files.exists(directory.resolve(phase+".done")));}
    void start()throws Exception{worker=new ProcessBuilder("java","-Xmx256m","-jar",JAR,"run",configuration.toString()).redirectErrorStream(true).redirectOutput(directory.resolve("worker-"+(++starts)+".log").toFile()).start();}
    void kill()throws Exception{if(worker!=null){worker.destroyForcibly();require(worker.waitFor(20,TimeUnit.SECONDS),"Worker SIGKILL was not confirmed");worker=null;}}
    void command(String... arguments)throws Exception{List<String> args=new ArrayList<>(Arrays.asList("java","-Xmx128m","-jar",JAR));args.addAll(Arrays.asList(arguments));Process operation=new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(directory.resolve("command-"+UUID.randomUUID()+".log").toFile()).start();require(operation.waitFor(45,TimeUnit.SECONDS),"CLI operation did not finish");require(operation.exitValue()==0,"CLI operation failed");}
    void insert(int value)throws Exception{source("INSERT INTO "+Sql.mysql(id)+".items(id,u,amount,payload,bytes,base_value) SELECT "+value+",u,amount,payload,bytes,"+(value*10)+" FROM "+Sql.mysql(id)+".items WHERE id=3");}
    void versions()throws Exception{
        try(Connection c=JdbcConnections.open("com.mysql.cj.jdbc.Driver","jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true","root","Sync@Test123");Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT VERSION()")){require(r.next(),"Source version unavailable");System.out.println("GBASE8C_FIXTURE_MYSQL_VERSION: "+r.getString(1));}
        try(Connection c=postgres();Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT VERSION()")){require(r.next(),"PG version unavailable");System.out.println("GBASE8C_FIXTURE_POSTGRES_VERSION: "+r.getString(1));}
    }
    void configure()throws Exception{
        Files.createDirectories(directory);Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"));config.pipelineId=id;config.nodeId="worker";config.source.clusterId=id;config.source.host="mysql";config.source.database=id;config.source.username="root";config.source.password="Sync@Test123";config.source.serverId=991716;config.databases=Collections.singletonList(id);config.chunkSize=1;config.pollIntervalMillis=100;config.retryDelayMillis=300;config.leaseMillis=5000;config.heartbeatMillis=300;config.journal.path=directory.resolve("journal").toString();config.journal.maxBytes=128*1024*1024;
        PipelineConfig.Target ob=new PipelineConfig.Target();ob.id="gbase8c";ob.product=TargetProduct.GBASE_8C;ob.url="jdbc:gbase8c://gbase8c:5432/postgres?connectTimeout=3&socketTimeout=15";ob.username="rivomesh_writer";ob.password="Sync@Test123";ob.metadataSchema=id+"_meta";ob.mappings.put(id,id+"_copy");config.targets.add(ob);
        PipelineConfig.Target pg=new PipelineConfig.Target();pg.id="postgres";pg.product=TargetProduct.POSTGRESQL;pg.url="jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10";pg.username="sync_test";pg.password="Sync@Test123";pg.metadataSchema=id+"_pg_meta";pg.mappings.put(id,id+"_pg");config.targets.add(pg);config.validate();config.prepareLocalDirectory();configuration=directory.resolve("task.yml");Files.write(configuration,new ObjectMapper(new YAMLFactory()).writeValueAsBytes(config));Files.setPosixFilePermissions(configuration,PosixFilePermissions.fromString("rw-------"));
    }
    boolean textKeyChanged()throws Exception{
        try(Connection c=gbase8c();PreparedStatement p=c.prepareStatement("SELECT \"value\" FROM "+Sql.pg(id+"_copy")+".\"TextKeys\" WHERE \"id\"=?")){p.setString(1,"a  ");try(ResultSet r=p.executeQuery()){return r.next()&&r.getInt(1)==5&&!r.next();}}
    }
    void verifyTypes()throws Exception{
        try(Connection connection=gbase8c();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT u::text,amount::text,payload->>'n',encode(bytes,'hex'),calc FROM "+Sql.pg(id+"_copy")+".items WHERE id=1")){
            require(rows.next()&&rows.getString(1).equals("18446744073709551615")&&rows.getString(2).equals("12345678901234567890123456789012345.123456789012345678901234567890")&&rows.getString(3).equals("18446744073709551615")&&rows.getString(4).equals("00ff80")&&rows.getLong(5)==14,"Gbase8c exact numeric/JSON/binary/generated values changed");
        }
        try(Connection connection=postgres();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT u::text,amount::text,payload->>'n',encode(bytes,'hex'),calc FROM "+Sql.pg(id+"_pg")+".items WHERE id=1")){require(rows.next()&&rows.getString(1).equals("18446744073709551615")&&rows.getString(3).equals("18446744073709551615")&&rows.getString(4).equals("00ff80")&&rows.getLong(5)==14,"PostgreSQL fan-out types changed");}
    }
    boolean durationNegative()throws Exception{try(Connection c=gbase8c();Statement sql=c.createStatement();ResultSet r=sql.executeQuery("SELECT extract(epoch from t6)<0 FROM "+Sql.pg(id+"_copy")+".durations")){return r.next()&&r.getBoolean(1);}}
    boolean duplicateMicrosecond()throws Exception{try(Connection c=gbase8c();Statement sql=c.createStatement();ResultSet r=sql.executeQuery("SELECT CAST(extract(epoch from clock) AS NUMERIC(20,6)) FROM "+Sql.pg(id+"_copy")+".duration_duplicates")){return r.next()&&new java.math.BigDecimal("-0.000001").compareTo(r.getBigDecimal(1))==0;}}
    void verifyDurations(boolean negative)throws Exception{
        try(Connection c=JdbcConnections.open("com.mysql.cj.jdbc.Driver","jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true","root","Sync@Test123");Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT CAST(t1 AS CHAR),TIME_TO_SEC(t1),MICROSECOND(t1) FROM "+Sql.mysql(id)+".durations")){require(r.next(),"Source TIME probe missing");System.out.println("GBASE8C_TIME_SOURCE_PROBE: native="+r.getString(1)+",TIME_TO_SEC="+r.getString(2)+",MICROSECOND="+r.getString(3));}
        // Integer microseconds: TIME_TO_SEC loses fractions, and default SQL division rounds to four places.
        List<String> sourceFields=new ArrayList<>(),targetFields=new ArrayList<>();for(int i=0;i<=6;i++){sourceFields.add("TIMESTAMPDIFF(MICROSECOND,'2000-01-01 00:00:00.000000',ADDTIME('2000-01-01 00:00:00.000000',t"+i+"))");targetFields.add("CAST(CAST(extract(epoch from t"+i+") AS NUMERIC(20,6))*1000000 AS NUMERIC(20,0))");}
        try(Connection source=JdbcConnections.open("com.mysql.cj.jdbc.Driver","jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true","root","Sync@Test123");Connection target=gbase8c();Statement a=source.createStatement();Statement b=target.createStatement();ResultSet expected=a.executeQuery("SELECT "+String.join(",",sourceFields)+" FROM "+Sql.mysql(id)+".durations");ResultSet actual=b.executeQuery("SELECT "+String.join(",",targetFields)+" FROM "+Sql.pg(id+"_copy")+".durations")){
            require(expected.next()&&actual.next(),"Duration matrix missing");for(int i=1;i<=7;i++){require(expected.getBigDecimal(i).compareTo(actual.getBigDecimal(i))==0,"TIME precision/sign mismatch fsp="+(i-1)+" expected="+expected.getBigDecimal(i)+" actual="+actual.getBigDecimal(i));require((actual.getBigDecimal(i).signum()<0)==negative,"Duration sign changed");}
        }
    }
    void verifyAdditionalTypes()throws Exception{
        try(Connection c=gbase8c();Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT id,note,to_char(dt,'YYYY-MM-DD HH24:MI:SS.US'),CAST(extract(epoch from clock) AS NUMERIC(20,6)),day::text,to_char(ts,'YYYY-MM-DD HH24:MI:SS.US') FROM "+Sql.pg(id+"_copy")+".typed ORDER BY id")){
            require(rows.next()&&"".equals(rows.getString(2))&&"2026-11-15 12:34:56.123456".equals(rows.getString(3))&&new java.math.BigDecimal("-90062.123456").compareTo(rows.getBigDecimal(4))==0&&"2026-11-15".equals(rows.getString(5))&&rows.getString(3).equals(rows.getString(6)),"Empty strings or temporal precision changed");
            require(rows.next()&&"报价'☃".equals(rows.getString(2))&&rows.getObject(3)==null&&rows.getObject(4)==null,"Unicode/quote/NULL changed");
        }
        try(Connection c=gbase8c();ResultSet indexes=c.getMetaData().getIndexInfo(c.getCatalog(),id+"_copy","items",false,false)){
            boolean found=false;while(indexes.next())if("base_value".equals(indexes.getString("COLUMN_NAME"))){found=true;require(indexes.getBoolean("NON_UNIQUE"),"Business UNIQUE was not converted");}require(found,"Business secondary index was lost");
        }
        System.out.println("GBASE8C_NATIVE_TYPES_ACCEPTED: exact65digit decimal/uint64/JSON/binary/generated, empty strings, Unicode/quotes, NULL, microseconds, negative duration and ordinary index metadata");
    }
    void nativeContract()throws Exception{
        try(Connection c=gbase8c();Statement sql=c.createStatement()){
            sql.execute("SET synchronous_commit=off");Map<String,String> profile=Gbase8cProfile.check(c);require(profile.get("mode").equalsIgnoreCase("PG"),"Native mode mismatch");
            try(ResultSet result=sql.executeQuery("SHOW synchronous_commit")){require(result.next()&&result.getString(1).equalsIgnoreCase("on"),"Asynchronous checkpoint durability was not overridden");}
            sql.execute("SET default_transaction_read_only=on");try{Gbase8cProfile.check(c);throw new AssertionError("Read-only endpoint accepted");}catch(SQLTransientConnectionException expected){require("08006".equals(expected.getSQLState()),"Read-only endpoint was not retryable");}finally{sql.execute("SET default_transaction_read_only=off");}
            System.out.println("GBASE8C_NATIVE_PROFILE: "+profile);
            try(ResultSet result=sql.executeQuery("SELECT extract(epoch from interval '25:01:02.1'),CAST(extract(epoch from interval '25:01:02.1') AS NUMERIC(20,6))")){require(result.next(),"Interval readback unavailable");System.out.println("GBASE8C_INTERVAL_READBACK: rawType="+result.getMetaData().getColumnTypeName(1)+",raw="+result.getString(1)+",numeric6="+result.getString(2));}

        }
        try(Connection c=postgres()){
            try{Gbase8cProfile.check(c);throw new AssertionError("PostgreSQL was accepted as Gbase8c");}catch(IllegalArgumentException expected){require(expected.getMessage().equals("GBASE8C_PRODUCT_REQUIRED"),"Wrong vendor guard");}
        }
        String incompatible=id+"_oracle_mode";
        try(Connection c=gbase8c();Statement sql=c.createStatement()){sql.execute("CREATE DATABASE "+Sql.pg(incompatible)+" DBCOMPATIBILITY='A'");}
        try{
            try(Connection c=JdbcConnections.open("com.gbase8c.Driver","jdbc:gbase8c://gbase8c:5432/"+incompatible+"?connectTimeout=3&socketTimeout=15","rivomesh_writer","Sync@Test123")){
                try{Gbase8cProfile.check(c);throw new AssertionError("Oracle compatibility mode was accepted");}catch(IllegalArgumentException expected){require(expected.getMessage().equals("GBASE8C_PG_MODE_REQUIRED"),"Wrong compatibility guard");}
            }
        }finally{try(Connection c=gbase8c();Statement sql=c.createStatement()){sql.execute("DROP DATABASE "+Sql.pg(incompatible));}}
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
            try(Connection c=gbase8c();Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.pg(id+"_contract")+".contract_rows")){require(rows.next()&&rows.getLong(1)==1,"Failed native transaction partially committed");}
            target.activate(2);try{target.apply(1,2,broken);throw new AssertionError("Old epoch could still write");}catch(IllegalStateException expected){require(expected.getMessage().equals("SINK_FENCED"),"Wrong old-epoch guard");}
        }
        try(JdbcSink target=new JdbcSink(settings)){require(target.activate(3).sequence==1,"Native reopen lost checkpoint");target.prepare(3,table);target.apply(3,1,first);}
        System.out.println("GBASE8C_NATIVE_CONTRACT_ACCEPTED: PG/UTF8/session profile, alias ownership, atomic business/checkpoint rollback, replay, epoch fence and reopen");
    }
    static Change nativeInsert(TableSchema table,int id){Change change=new Change();change.eventId=table.id+":native-"+id;change.tableId=table.id;change.operation=Change.Operation.INSERT;change.after=Collections.singletonList(Cell.value(Integer.toString(id)));return change;}
    void run()throws Exception{
        configure();versions();nativeContract();source("CREATE DATABASE "+Sql.mysql(id));String items=Sql.mysql(id)+".items",duplicates=Sql.mysql(id)+".duplicates";
        try{
            source("CREATE TABLE "+items+" (id BIGINT PRIMARY KEY,u BIGINT UNSIGNED,amount DECIMAL(65,30),payload JSON,bytes VARBINARY(100),base_value INT,calc INT GENERATED ALWAYS AS (base_value*2) STORED,UNIQUE KEY uq_base(base_value)) ENGINE=InnoDB");source("CREATE TABLE "+duplicates+" (value INT,payload JSON) ENGINE=InnoDB");source("INSERT INTO "+items+"(id,u,amount,payload,bytes,base_value) VALUES(1,18446744073709551615,12345678901234567890123456789012345.123456789012345678901234567890,JSON_OBJECT('n',CAST(18446744073709551615 AS UNSIGNED)),0x00ff80,7)");source("INSERT INTO "+duplicates+" VALUES(7,JSON_OBJECT('same',true)),(7,JSON_OBJECT('same',true))");source("CREATE TABLE "+Sql.mysql(id)+".typed (id BIGINT PRIMARY KEY,note VARCHAR(100),dt DATETIME(6),clock TIME(6),day DATE,ts TIMESTAMP(6)) ENGINE=InnoDB");
            source("INSERT INTO "+Sql.mysql(id)+".typed VALUES(1,'','2026-11-15 12:34:56.123456','-25:01:02.123456','2026-11-15','2026-11-15 12:34:56.123456'),(2,CONVERT(0xe68aa5e4bbb727e29883 USING utf8mb4),NULL,NULL,NULL,NULL)");source("CREATE TABLE "+Sql.mysql(id)+".durations (id BIGINT PRIMARY KEY,t0 TIME,t1 TIME(1),t2 TIME(2),t3 TIME(3),t4 TIME(4),t5 TIME(5),t6 TIME(6)) ENGINE=InnoDB");
            source("INSERT INTO "+Sql.mysql(id)+".durations VALUES(1,'25:01:02','25:01:02.1','25:01:02.12','25:01:02.123','25:01:02.1234','25:01:02.12345','25:01:02.123456')");
            source("CREATE TABLE "+Sql.mysql(id)+".duration_duplicates (clock TIME(6),note VARCHAR(20)) ENGINE=InnoDB");source("INSERT INTO "+Sql.mysql(id)+".duration_duplicates VALUES('-25:01:02.123456','same'),('-25:01:02.123456','same')");
            source("CREATE TABLE "+Sql.mysql(id)+".TextKeys (id VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin PRIMARY KEY, value INT) ENGINE=InnoDB");source("INSERT INTO "+Sql.mysql(id)+".TextKeys VALUES('a',1),('a ',2),('A',3),(CONVERT(0xf09f9880 USING utf8mb4),4)");command("check",configuration.toString());
            try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
                start();await(()->pgCount("items")==1&&obCount("items")==1&&obCount("duplicates")==2);await(()->obCount("typed")==2);await(()->obCount("durations")==1&&obCount("duration_duplicates")==2&&obCount("TextKeys")==4);verifyTypes();verifyAdditionalTypes();verifyDurations(false);System.out.println("GBASE8C_SNAPSHOT_ACCEPTED");String generation=(String)journal.status().get("generation"),snapshot=journal.snapshot("source",TableSchema.identity(id,"","items")).snapshotId;
                source("UPDATE "+items+" SET id=3,base_value=9 WHERE id=1");source("DELETE FROM "+duplicates+" LIMIT 1");await(()->pgCount("duplicates")==1&&obCount("duplicates")==1);await(()->subscription(journal,"gbase8c").acknowledged==((Number)journal.status().get("lastSequence")).longValue());
                source("UPDATE "+Sql.mysql(id)+".durations SET t0='-25:01:02',t1='-25:01:02.1',t2='-25:01:02.12',t3='-25:01:02.123',t4='-25:01:02.1234',t5='-25:01:02.12345',t6='-25:01:02.123456' WHERE id=1");
                await(()->durationNegative());verifyDurations(true);
                source("DELETE FROM "+Sql.mysql(id)+".duration_duplicates LIMIT 1");await(()->obCount("duration_duplicates")==1&&pgCount("duration_duplicates")==1);
                source("UPDATE "+Sql.mysql(id)+".duration_duplicates SET clock='-00:00:00.000001'");await(()->duplicateMicrosecond());
                source("UPDATE "+Sql.mysql(id)+".TextKeys SET id='a  ',value=5 WHERE id='a '");source("DELETE FROM "+Sql.mysql(id)+".TextKeys WHERE id='A'");await(()->obCount("TextKeys")==3&&textKeyChanged());
                System.out.println("GBASE8C_CDC_KEYLESS_ACCEPTED: allTIME precisions including negative fractions, keyless duration identity/delete/update");phase("stop");insert(4);await(()->pgCount("items")==2&&subscription(journal,"postgres").acknowledged>subscription(journal,"gbase8c").acknowledged);phase("start");await(()->obCount("items")==2&&subscription(journal,"gbase8c").acknowledged==subscription(journal,"postgres").acknowledged);
                System.out.println("GBASE8C_OUTAGE_CATCHUP_ACCEPTED");kill();insert(5);start();await(()->pgCount("items")==3&&obCount("items")==3);require(generation.equals(journal.status().get("generation"))&&snapshot.equals(journal.snapshot("source",TableSchema.identity(id,"","items")).snapshotId),"Worker restart repeated/reset snapshot identity");
                System.out.println("GBASE8C_WORKER_RESTART_ACCEPTED");pg("ALTER TABLE "+Sql.pg(id+"_pg")+".items ADD CONSTRAINT block_next CHECK(id<6)");insert(6);await(()->"PAUSED".equals(subscription(journal,"postgres").state)&&obCount("items")==4);require(pgCount("items")==3,"Failed SQL transaction was committed");long error=subscription(journal,"postgres").errorGeneration;pg("ALTER TABLE "+Sql.pg(id+"_pg")+".items DROP CONSTRAINT block_next");command("resume",configuration.toString(),"postgres",Long.toString(error));await(()->pgCount("items")==4&&subscription(journal,"postgres").acknowledged==subscription(journal,"gbase8c").acknowledged);
                System.out.println("GBASE8C_POSTGRES_REPAIR_ACCEPTED");ob("ALTER TABLE "+Sql.pg(id+"_copy")+".items ADD CONSTRAINT block_ob CHECK(id<7)");long acknowledged=subscription(journal,"gbase8c").acknowledged;insert(7);await(()->pgCount("items")==5&&"PAUSED".equals(subscription(journal,"gbase8c").state));require(obCount("items")==4&&subscription(journal,"gbase8c").acknowledged==acknowledged,"Failed Gbase8c transaction or checkpoint committed");long obError=subscription(journal,"gbase8c").errorGeneration;kill();start();await(()->"PAUSED".equals(subscription(journal,"gbase8c").state));require(obCount("items")==4,"Restart bypassed SQL pause");ob("ALTER TABLE "+Sql.pg(id+"_copy")+".items DROP CONSTRAINT block_ob");command("resume",configuration.toString(),"gbase8c",Long.toString(obError));await(()->obCount("items")==5&&subscription(journal,"gbase8c").acknowledged==subscription(journal,"postgres").acknowledged);
                System.out.println("GBASE8C_NATIVE_SQL_REPAIR_ACCEPTED");ob("DROP TABLE "+Sql.pg(id+"_copy")+".items");insert(8);await(()->pgCount("items")==6&&"PAUSED".equals(subscription(journal,"gbase8c").state));require(Arrays.asList("TARGET_TABLE_MISSING_REBUILD_REQUIRED","SQL_42P01").contains(subscription(journal,"gbase8c").errorCode),"Missing table did not isolate its target");Map<String,Object> paused=PipelineControls.pause(config);Map<?,?> fences=(Map<?,?>)paused.get("targetFences");require("FENCED".equals(fences.get("postgres"))&&"FENCED".equals(fences.get("gbase8c")),"Transactional target fences unconfirmed");
            }
            System.out.println("GBASE8C_PIPELINE_ACCEPTED: actual MySQL8.4 to PostgreSQL17+Gbase8cV6_S1.3.0B19 PG mode, snapshot/CDC/uint64/decimal/JSON/binary/generated/keyless, target outage with independent SQL progress/catchup, worker SIGKILL resume, independent SQL error rollback/repair/retry, persistent pause, missing-table isolation and transactional fences");
        }finally{kill();source("DROP DATABASE IF EXISTS "+Sql.mysql(id));}
    }
    public static void main(String[] args)throws Exception{new Gbase8cPipelineAcceptance().run();}
}
