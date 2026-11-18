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

/** Actual MySQL source, PostgreSQL/Dm targets, journal and fat-JAR worker processes. */
public final class DmPipelineAcceptance {
    static final String JAR=setting("RIVOMESH_DM_ARTIFACT","/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar");
    static final String MYSQL_DRIVER=System.getenv("RIVOMESH_DM_MYSQL_DRIVER");
    static final String DM_DRIVER=System.getenv("RIVOMESH_DM_JDBC_DRIVER");
    static final String MYSQL_ENDPOINT=setting("RIVOMESH_DM_MYSQL_ENDPOINT","mysql:3306");
    static final String MYSQL_URL="jdbc:mysql://"+MYSQL_ENDPOINT+"/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000";
    static final String POSTGRES_URL="jdbc:postgresql://"+setting("RIVOMESH_DM_POSTGRES_ENDPOINT","postgres:5432")
            +"/sync_test?connectTimeout=3&socketTimeout=10";
    static final String DM_URL="jdbc:dm://"+setting("RIVOMESH_DM_TARGET_ENDPOINT","dm:5236")+"?connectTimeout=3000&socketTimeout=15000";
    static final String JAVA=Path.of(System.getProperty("java.home"),"bin","java").toString();
    static String setting(String name,String fallback){String value=System.getenv(name);return value==null?fallback:value;}
    final Path directory=Paths.get(System.getenv("RIVOMESH_DM_DIR"));
    final String id="dm_"+UUID.randomUUID().toString().replace("-","").substring(0,20);
    final PipelineConfig config=new PipelineConfig();
    Path configuration;
    Process worker;
    int starts;
    interface Condition{boolean ready()throws Exception;}
    static void require(boolean condition,String message){if(!condition)throw new AssertionError(message);}
    void await(Condition condition)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(360);String last=null;while(System.nanoTime()<end){try{if(condition.ready())return;}catch(Exception failure){last=failure.getClass().getName();}Thread.sleep(100);}throw new AssertionError("Timed out waiting for real pipeline; last="+last);}
    void source(String statement)throws Exception{try(Connection connection=JdbcConnections.open("com.mysql.cj.jdbc.Driver",MYSQL_URL,"root","Sync@Test123");Statement sql=connection.createStatement()){sql.execute(statement);}}
    Connection postgres()throws Exception{return JdbcConnections.open("org.postgresql.Driver",POSTGRES_URL,"sync_test","Sync@Test123");}
    void pg(String statement)throws Exception{try(Connection connection=postgres();Statement sql=connection.createStatement()){sql.execute(statement);}}
    long pgCount(String table)throws Exception{try(Connection connection=postgres();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.pg(id+"_pg")+"."+Sql.pg(table))){rows.next();return rows.getLong(1);}}
    Connection dm()throws Exception{PipelineConfig.Target target=config.targets.get(0);return JdbcConnections.open("dm.jdbc.driver.DmDriver",target.url,target.username,target.password);}
    void ob(String statement)throws Exception{PipelineConfig.Target target=config.targets.get(0);try(Connection c=JdbcConnections.open("dm.jdbc.driver.DmDriver",target.url,target.username,target.password);Statement sql=c.createStatement()){sql.execute(statement);}}
    long obCount(String table)throws Exception{try(Connection c=dm();Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.pg(id+"_copy")+"."+Sql.pg(table))){rows.next();return rows.getLong(1);}}
    Journal.Subscription subscription(JdbcJournal journal,String id)throws Exception{return journal.subscriptions().stream().filter(item->item.id.equals(id)).findFirst().get();}
    void phase(String phase)throws Exception{Files.write(directory.resolve(phase+".request"),new byte[]{1});await(()->Files.exists(directory.resolve(phase+".done")));}
    List<String> launch(String heap,boolean includeDm,String...arguments){
        List<String> command=new ArrayList<>(List.of(JAVA,heap));
        if(MYSQL_DRIVER==null)command.addAll(List.of("-jar",JAR));
        else{
            String separator=System.getProperty("path.separator");
            String classpath=JAR+separator+MYSQL_DRIVER+(includeDm?separator+DM_DRIVER:"");
            command.addAll(List.of("-cp",classpath,Main.class.getName()));
        }
        command.addAll(Arrays.asList(arguments));
        return command;
    }
    void start()throws Exception{worker=new ProcessBuilder(launch("-Xmx256m",true,"run",configuration.toString())).redirectErrorStream(true).redirectOutput(directory.resolve("worker-"+(++starts)+".log").toFile()).start();}
    void kill()throws Exception{if(worker!=null){worker.destroyForcibly();require(worker.waitFor(20,TimeUnit.SECONDS),"Worker SIGKILL was not confirmed");worker=null;}}
    void command(String... arguments)throws Exception{Process operation=new ProcessBuilder(launch("-Xmx128m",true,arguments)).redirectErrorStream(true).redirectOutput(directory.resolve("command-"+UUID.randomUUID()+".log").toFile()).start();require(operation.waitFor(45,TimeUnit.SECONDS),"CLI operation did not finish");require(operation.exitValue()==0,"CLI operation failed");}
    void externalDrivers()throws Exception{
        require((MYSQL_DRIVER==null)==(DM_DRIVER==null),"Supply both external drivers");
        if(MYSQL_DRIVER==null)return;
        Class<?>[] drivers={Class.forName("com.mysql.cj.jdbc.Driver"),Class.forName("dm.jdbc.driver.DmDriver")};
        String[] files={MYSQL_DRIVER,DM_DRIVER};
        for(int i=0;i<drivers.length;i++){
            Path actual=Path.of(drivers[i].getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
            require(actual.equals(Path.of(files[i]).toRealPath()),"Driver loaded from unexpected JAR");
        }
        try(Connection connection=dm();Statement sql=connection.createStatement()){
            try{
                sql.executeQuery("SELECT * FROM "+Sql.pg(id+"_contract")+".\"external_fixture_missing\"");
                throw new AssertionError("Missing-table SQL unexpectedly accepted");
            }catch(SQLException nativeError){
                require(nativeError.getErrorCode()==-2106,"Unexpected Dameng missing-table vendor code");
                require("TARGET_TABLE_MISSING_REBUILD_REQUIRED".equals(Sql.targetErrorCode(nativeError)),
                        "External Dameng missing-table diagnosis changed");
            }
        }
        System.out.println("DM_EXTERNAL_ORIGIN_AND_NATIVE_ERROR_ACCEPTED");
    }
    void verifyDriverRequired()throws Exception{
        if(DM_DRIVER==null)return;
        Path log=directory.resolve("missing-driver.log");
        Process without=new ProcessBuilder(launch("-Xmx128m",false,"check",configuration.toString()))
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        if(!without.waitFor(45,TimeUnit.SECONDS)){
            without.destroyForcibly();
            throw new AssertionError("Missing-driver preflight did not finish");
        }
        String result=Files.readString(log);
        require(without.exitValue()!=0&&result.contains("JDBC_DRIVER_UNAVAILABLE")&&result.contains("dm.jdbc.driver.DmDriver"),
                "Packaged preflight did not refuse an absent Dameng driver");
        System.out.println("DM_EXTERNAL_MISSING_DRIVER_ACCEPTED");
    }
    void insert(int value)throws Exception{source("INSERT INTO "+Sql.mysql(id)+".items(id,u,amount,payload,bytes,base_value) SELECT "+value+",u,amount,payload,bytes,"+(value*10)+" FROM "+Sql.mysql(id)+".items WHERE id=3");}
    void versions()throws Exception{
        try(Connection c=JdbcConnections.open("com.mysql.cj.jdbc.Driver",MYSQL_URL,"root","Sync@Test123");Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT VERSION()")){require(r.next(),"Source version unavailable");System.out.println("DM_FIXTURE_MYSQL_VERSION: "+r.getString(1));}
        try(Connection c=postgres();Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT VERSION()")){require(r.next(),"PG version unavailable");System.out.println("DM_FIXTURE_POSTGRES_VERSION: "+r.getString(1));}
    }
    void configure()throws Exception{
        Files.createDirectories(directory);Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"));config.pipelineId=id;config.nodeId="worker";config.source.clusterId=id;config.source.host=MYSQL_ENDPOINT.substring(0,MYSQL_ENDPOINT.lastIndexOf(':'));config.source.port=Integer.parseInt(MYSQL_ENDPOINT.substring(MYSQL_ENDPOINT.lastIndexOf(':')+1));config.source.database=id;config.source.username="root";config.source.password="Sync@Test123";config.source.serverId=991716;config.databases=Collections.singletonList(id);config.chunkSize=1;config.pollIntervalMillis=100;config.retryDelayMillis=300;config.leaseMillis=5000;config.heartbeatMillis=300;config.journal.path=directory.resolve("journal").toString();config.journal.maxBytes=128*1024*1024;
        PipelineConfig.Target ob=new PipelineConfig.Target();ob.id="dm";ob.product=TargetProduct.DM;ob.url=DM_URL;ob.username="SYSDBA";ob.password="Sync@Test123";ob.dm=new PipelineConfig.Dm();ob.dm.acknowledgeTextConversions=true;ob.metadataSchema=id+"_meta";ob.mappings.put(id,id+"_copy");config.targets.add(ob);
        PipelineConfig.Target pg=new PipelineConfig.Target();pg.id="postgres";pg.product=TargetProduct.POSTGRESQL;pg.url=POSTGRES_URL;pg.username="sync_test";pg.password="Sync@Test123";pg.metadataSchema=id+"_pg_meta";pg.mappings.put(id,id+"_pg");config.targets.add(pg);config.validate();config.prepareLocalDirectory();configuration=directory.resolve("task.yml");Files.write(configuration,new ObjectMapper(new YAMLFactory()).writeValueAsBytes(config));Files.setPosixFilePermissions(configuration,PosixFilePermissions.fromString("rw-------"));
    }
    void verifyTypes()throws Exception{
        try(Connection connection=dm();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT \"u\",\"amount\",\"payload\",\"bytes\",\"calc\" FROM "+Sql.pg(id+"_copy")+".\"items\" WHERE \"id\"=1")){
            require(rows.next()&&rows.getBigDecimal(1).toPlainString().equals("18446744073709551615")&&rows.getString(2).equals("12345678901234567890123456789012345.123456789012345678901234567890")&&JsonValues.canonical(rows.getString(3)).equals(JsonValues.canonical("{\"n\":18446744073709551615}"))&&Arrays.equals(rows.getBytes(4),new byte[]{0,(byte)255,(byte)128})&&rows.getLong(5)==14,"DM exact numeric/JSON/binary/generated values changed");
        }
        try(Connection connection=postgres();Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT u::text,amount::text,payload->>'n',encode(bytes,'hex'),calc FROM "+Sql.pg(id+"_pg")+".items WHERE \"id\"=1")){require(rows.next()&&rows.getString(1).equals("18446744073709551615")&&rows.getString(3).equals("18446744073709551615")&&rows.getString(4).equals("00ff80")&&rows.getLong(5)==14,"PostgreSQL fan-out types changed");}
    }
    boolean textKeyChanged()throws Exception{
        try(Connection c=dm();PreparedStatement p=c.prepareStatement("SELECT \"value\" FROM "+Sql.pg(id+"_copy")+".\"TextKeys\" WHERE \"id\"=?")){p.setString(1,"a  ");try(ResultSet r=p.executeQuery()){return r.next()&&r.getInt(1)==5&&!r.next();}}
    }
    boolean durationNegative()throws Exception{try(Connection c=dm();Statement sql=c.createStatement();ResultSet r=sql.executeQuery("SELECT CASE WHEN \"t6\" < INTERVAL '0 00:00:00' DAY TO SECOND THEN 1 ELSE 0 END FROM "+Sql.pg(id+"_copy")+".\"durations\"")){return r.next()&&r.getBoolean(1);}}
    boolean duplicateMicrosecond()throws Exception{try(Connection c=dm();Statement sql=c.createStatement();ResultSet r=sql.executeQuery("SELECT CAST((EXTRACT(DAY FROM \"clock\")*86400+EXTRACT(HOUR FROM \"clock\")*3600+EXTRACT(MINUTE FROM \"clock\")*60+EXTRACT(SECOND FROM \"clock\")) AS NUMERIC(20,6)) FROM "+Sql.pg(id+"_copy")+".\"duration_duplicates\"")){return r.next()&&new java.math.BigDecimal("-0.000001").compareTo(r.getBigDecimal(1))==0;}}
    void verifyDurations(boolean negative)throws Exception{
        try(Connection c=JdbcConnections.open("com.mysql.cj.jdbc.Driver",MYSQL_URL,"root","Sync@Test123");Statement q=c.createStatement();ResultSet r=q.executeQuery("SELECT CAST(t1 AS CHAR),TIME_TO_SEC(t1),MICROSECOND(t1) FROM "+Sql.mysql(id)+".durations")){require(r.next(),"Source TIME probe missing");System.out.println("DM_TIME_SOURCE_PROBE: native="+r.getString(1)+",TIME_TO_SEC="+r.getString(2)+",MICROSECOND="+r.getString(3));}
        // Integer microseconds: TIME_TO_SEC loses fractions, and default SQL division rounds to four places.
        List<String> sourceFields=new ArrayList<>(),targetFields=new ArrayList<>();for(int i=0;i<=6;i++){sourceFields.add("TIMESTAMPDIFF(MICROSECOND,'2000-01-01 00:00:00.000000',ADDTIME('2000-01-01 00:00:00.000000',t"+i+"))");targetFields.add("CAST(CAST((EXTRACT(DAY FROM \"t"+i+"\")*86400+EXTRACT(HOUR FROM \"t"+i+"\")*3600+EXTRACT(MINUTE FROM \"t"+i+"\")*60+EXTRACT(SECOND FROM \"t"+i+"\")) AS NUMERIC(20,6))*1000000 AS NUMERIC(20,0))");}
        try(Connection source=JdbcConnections.open("com.mysql.cj.jdbc.Driver",MYSQL_URL,"root","Sync@Test123");Connection target=dm();Statement a=source.createStatement();Statement b=target.createStatement();ResultSet expected=a.executeQuery("SELECT "+String.join(",",sourceFields)+" FROM "+Sql.mysql(id)+".durations");ResultSet actual=b.executeQuery("SELECT "+String.join(",",targetFields)+" FROM "+Sql.pg(id+"_copy")+".\"durations\"")){
            require(expected.next()&&actual.next(),"Duration matrix missing");for(int i=1;i<=7;i++){require(expected.getBigDecimal(i).compareTo(actual.getBigDecimal(i))==0,"TIME precision/sign mismatch fsp="+(i-1)+" expected="+expected.getBigDecimal(i)+" actual="+actual.getBigDecimal(i));require((actual.getBigDecimal(i).signum()<0)==negative,"Duration sign changed");}
        }
    }
    void verifyAdditionalTypes()throws Exception{
        try(Connection c=dm();Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT \"id\",\"note\",to_char(\"dt\",'YYYY-MM-DD HH24:MI:SS.FF6'),CAST((EXTRACT(DAY FROM \"clock\")*86400+EXTRACT(HOUR FROM \"clock\")*3600+EXTRACT(MINUTE FROM \"clock\")*60+EXTRACT(SECOND FROM \"clock\")) AS NUMERIC(20,6)),to_char(\"day\",'YYYY-MM-DD'),to_char(\"ts\",'YYYY-MM-DD HH24:MI:SS.FF6') FROM "+Sql.pg(id+"_copy")+".\"typed\" ORDER BY \"id\"")){
            require(rows.next()&&"".equals(rows.getString(2))&&"2026-11-15 12:34:56.123456".equals(rows.getString(3))&&new java.math.BigDecimal("-90062.123456").compareTo(rows.getBigDecimal(4))==0&&"2026-11-15".equals(rows.getString(5))&&rows.getString(3).equals(rows.getString(6)),"Empty strings or temporal precision changed");
            require(rows.next()&&"报价'☃😀".equals(rows.getString(2))&&rows.getObject(3)==null&&rows.getObject(4)==null,"Unicode/quote/NULL changed");
        }
        try(Connection c=dm();ResultSet indexes=c.getMetaData().getIndexInfo(c.getCatalog(),id+"_copy","items",false,false)){
            boolean found=false;while(indexes.next())if("base_value".equals(indexes.getString("COLUMN_NAME"))){found=true;require(indexes.getBoolean("NON_UNIQUE"),"Business UNIQUE was not converted");}require(found,"Business secondary index was lost");
        }
        System.out.println("DM_NATIVE_TYPES_ACCEPTED: exact65digit decimal/uint64/JSON/binary/generated, empty strings, Unicode/quotes, NULL, microseconds, negative duration and ordinary index metadata");
    }
    void nativeContract()throws Exception{
        try(Connection c=dm();Statement sql=c.createStatement()){
            Map<String,String> profile=DmProfile.check(c);require(profile.get("compatibilityMode").equals("0"),"Native compatibility mode mismatch");System.out.println("DM_NATIVE_PROFILE: "+profile);
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
            try(Connection c=dm();Statement sql=c.createStatement();ResultSet rows=sql.executeQuery("SELECT count(*) FROM "+Sql.pg(id+"_contract")+".\"contract_rows\"")){require(rows.next()&&rows.getLong(1)==1,"Failed native transaction partially committed");}
            target.activate(2);try{target.apply(1,2,broken);throw new AssertionError("Old epoch could still write");}catch(IllegalStateException expected){require(expected.getMessage().equals("SINK_FENCED"),"Wrong old-epoch guard");}
        }
        try(JdbcSink target=new JdbcSink(settings)){require(target.activate(3).sequence==1,"Native reopen lost checkpoint");target.prepare(3,table);target.apply(3,1,first);}
        System.out.println("DM_NATIVE_CONTRACT_ACCEPTED: native-mode/UTF8/session profile, alias ownership, atomic business/checkpoint rollback, replay, epoch fence and reopen");
    }
    void existingLayouts()throws Exception{
        JdbcSink.Settings settings=config.targetSettings(config.targets.get(0),"layout-generation");settings.pipelineId=id+"-layout";settings.metadataSchema=id+"_layout_meta";settings.databases=new TreeMap<>();settings.databases.put(id,id+"_layout");
        ob("CREATE SCHEMA "+Sql.pg(id+"_layout"));
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);
            for(int i=0;i<3;i++){
                TableSchema table=new TableSchema();table.database=id;table.name="layout_"+i;table.id=TableSchema.identity(id,"",table.name);table.primaryKey=Collections.singletonList("id");
                TableSchema.Column key=new TableSchema.Column();key.name="id";key.type=TableSchema.Type.INT64;key.nullable=false;
                TableSchema.Column value=new TableSchema.Column();value.name="value";value.type=i==0?TableSchema.Type.STRING:i==1?TableSchema.Type.BYTES:TableSchema.Type.TIME;value.length=100;table.columns=Arrays.asList(key,value);
                String target=Sql.pg(id+"_layout")+"."+Sql.pg(table.name),bad=i==0?"VARCHAR(100)":i==1?"VARBINARY(20)":"INTERVAL DAY(2) TO SECOND(0)";
                ob("CREATE TABLE "+target+" (\"id\" BIGINT PRIMARY KEY,\"value\" "+bad+")");
                try{sink.prepare(1,table);throw new AssertionError("Incompatible empty target accepted: "+bad);}catch(IllegalStateException expected){require("TARGET_COLUMN_TYPE_MISMATCH".equals(expected.getMessage()),"Wrong layout guard: "+expected.getMessage());}
                ob("DROP TABLE "+target);ob("CREATE TABLE "+target+" (\"id\" BIGINT PRIMARY KEY,\"value\" "+DmStorage.type(value)+")");sink.prepare(1,table);require(sink.checkpoint().sequence==0,"Shape review advanced business progress");
            }
        }
        System.out.println("DM_EXISTING_LAYOUTS_ACCEPTED: UTF8 character/byte capacity, binary capacity, interval microseconds refuse incompatible empty targets; repaired layouts accepted without advancing progress");
    }
    static Change nativeInsert(TableSchema table,int id){Change change=new Change();change.eventId=table.id+":native-"+id;change.tableId=table.id;change.operation=Change.Operation.INSERT;change.after=Collections.singletonList(Cell.value(Integer.toString(id)));return change;}
    void run()throws Exception{
        configure();versions();nativeContract();externalDrivers();existingLayouts();source("CREATE DATABASE "+Sql.mysql(id));String items=Sql.mysql(id)+".items",duplicates=Sql.mysql(id)+".duplicates";
        try{
            source("CREATE TABLE "+items+" (id BIGINT PRIMARY KEY,u BIGINT UNSIGNED,amount DECIMAL(65,30),payload JSON,bytes VARBINARY(100),base_value INT,calc INT GENERATED ALWAYS AS (base_value*2) STORED,UNIQUE KEY uq_base(base_value)) ENGINE=InnoDB");source("CREATE TABLE "+duplicates+" (value INT,payload JSON) ENGINE=InnoDB");source("INSERT INTO "+items+"(id,u,amount,payload,bytes,base_value) VALUES(1,18446744073709551615,12345678901234567890123456789012345.123456789012345678901234567890,JSON_OBJECT('n',CAST(18446744073709551615 AS UNSIGNED)),0x00ff80,7)");source("INSERT INTO "+duplicates+" VALUES(7,JSON_OBJECT('same',true)),(7,JSON_OBJECT('same',true))");source("CREATE TABLE "+Sql.mysql(id)+".typed (id BIGINT PRIMARY KEY,note VARCHAR(100),dt DATETIME(6),clock TIME(6),day DATE,ts TIMESTAMP(6)) ENGINE=InnoDB");
            source("INSERT INTO "+Sql.mysql(id)+".typed VALUES(1,'','2026-11-15 12:34:56.123456','-25:01:02.123456','2026-11-15','2026-11-15 12:34:56.123456'),(2,CONVERT(0xe68aa5e4bbb727e29883f09f9880 USING utf8mb4),NULL,NULL,NULL,NULL)");source("CREATE TABLE "+Sql.mysql(id)+".durations (id BIGINT PRIMARY KEY,t0 TIME,t1 TIME(1),t2 TIME(2),t3 TIME(3),t4 TIME(4),t5 TIME(5),t6 TIME(6)) ENGINE=InnoDB");
            source("INSERT INTO "+Sql.mysql(id)+".durations VALUES(1,'25:01:02','25:01:02.1','25:01:02.12','25:01:02.123','25:01:02.1234','25:01:02.12345','25:01:02.123456')");
            source("CREATE TABLE "+Sql.mysql(id)+".TextKeys (id VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin PRIMARY KEY, value INT) ENGINE=InnoDB");source("INSERT INTO "+Sql.mysql(id)+".TextKeys VALUES('a',1),('a ',2),('A',3),(CONVERT(0xf09f9880 USING utf8mb4),4)");
            source("CREATE TABLE "+Sql.mysql(id)+".duration_duplicates (clock TIME(6),note VARCHAR(20)) ENGINE=InnoDB");source("INSERT INTO "+Sql.mysql(id)+".duration_duplicates VALUES('-25:01:02.123456','same'),('-25:01:02.123456','same')");verifyDriverRequired();command("check",configuration.toString());
            try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
                start();await(()->pgCount("items")==1&&obCount("items")==1&&obCount("duplicates")==2);await(()->obCount("typed")==2);await(()->obCount("durations")==1&&obCount("duration_duplicates")==2&&obCount("TextKeys")==4);verifyTypes();verifyAdditionalTypes();verifyDurations(false);System.out.println("DM_SNAPSHOT_ACCEPTED");String generation=(String)journal.status().get("generation"),snapshot=journal.snapshot("source",TableSchema.identity(id,"","items")).snapshotId;
                source("UPDATE "+items+" SET id=3,base_value=9 WHERE id=1");source("DELETE FROM "+duplicates+" LIMIT 1");await(()->pgCount("duplicates")==1&&obCount("duplicates")==1);await(()->subscription(journal,"dm").acknowledged==((Number)journal.status().get("lastSequence")).longValue());
                source("UPDATE "+Sql.mysql(id)+".durations SET t0='-25:01:02',t1='-25:01:02.1',t2='-25:01:02.12',t3='-25:01:02.123',t4='-25:01:02.1234',t5='-25:01:02.12345',t6='-25:01:02.123456' WHERE id=1");
                await(()->durationNegative());verifyDurations(true);
                source("DELETE FROM "+Sql.mysql(id)+".duration_duplicates LIMIT 1");await(()->obCount("duration_duplicates")==1&&pgCount("duration_duplicates")==1);
                source("UPDATE "+Sql.mysql(id)+".duration_duplicates SET clock='-00:00:00.000001'");await(()->duplicateMicrosecond());
                source("UPDATE "+Sql.mysql(id)+".TextKeys SET id='a  ',value=5 WHERE id='a '");source("DELETE FROM "+Sql.mysql(id)+".TextKeys WHERE id='A'");await(()->obCount("TextKeys")==3&&textKeyChanged());
                System.out.println("DM_CDC_KEYLESS_ACCEPTED: allTIME precisions including negative fractions, keyless duration identity/delete/update");phase("stop");insert(4);await(()->pgCount("items")==2&&subscription(journal,"postgres").acknowledged>subscription(journal,"dm").acknowledged);phase("start");await(()->obCount("items")==2&&subscription(journal,"dm").acknowledged==subscription(journal,"postgres").acknowledged);
                System.out.println("DM_OUTAGE_CATCHUP_ACCEPTED");kill();insert(5);start();await(()->pgCount("items")==3&&obCount("items")==3);require(generation.equals(journal.status().get("generation"))&&snapshot.equals(journal.snapshot("source",TableSchema.identity(id,"","items")).snapshotId),"Worker restart repeated/reset snapshot identity");
                System.out.println("DM_WORKER_RESTART_ACCEPTED");pg("ALTER TABLE "+Sql.pg(id+"_pg")+".items ADD CONSTRAINT block_next CHECK(\"id\"<6)");insert(6);await(()->"PAUSED".equals(subscription(journal,"postgres").state)&&obCount("items")==4);require(pgCount("items")==3,"Failed SQL transaction was committed");long error=subscription(journal,"postgres").errorGeneration;pg("ALTER TABLE "+Sql.pg(id+"_pg")+".items DROP CONSTRAINT block_next");command("resume",configuration.toString(),"postgres",Long.toString(error));await(()->pgCount("items")==4&&subscription(journal,"postgres").acknowledged==subscription(journal,"dm").acknowledged);
                System.out.println("DM_POSTGRES_REPAIR_ACCEPTED");ob("ALTER TABLE "+Sql.pg(id+"_copy")+".\"items\" ADD CONSTRAINT block_ob CHECK(\"id\"<7)");long acknowledged=subscription(journal,"dm").acknowledged;insert(7);await(()->pgCount("items")==5&&"PAUSED".equals(subscription(journal,"dm").state));require(obCount("items")==4&&subscription(journal,"dm").acknowledged==acknowledged,"Failed Dm transaction or checkpoint committed");long obError=subscription(journal,"dm").errorGeneration;kill();start();await(()->"PAUSED".equals(subscription(journal,"dm").state));require(obCount("items")==4,"Restart bypassed SQL pause");ob("ALTER TABLE "+Sql.pg(id+"_copy")+".\"items\" DROP CONSTRAINT block_ob");command("resume",configuration.toString(),"dm",Long.toString(obError));await(()->obCount("items")==5&&subscription(journal,"dm").acknowledged==subscription(journal,"postgres").acknowledged);
                System.out.println("DM_NATIVE_SQL_REPAIR_ACCEPTED");ob("DROP TABLE "+Sql.pg(id+"_copy")+".\"items\"");insert(8);await(()->pgCount("items")==6&&"PAUSED".equals(subscription(journal,"dm").state));require(Arrays.asList("TARGET_TABLE_MISSING_REBUILD_REQUIRED","SQL_42P01","SQL_42S02","SQL_42000").contains(subscription(journal,"dm").errorCode),"Missing table did not isolate its target");Map<String,Object> paused=PipelineControls.pause(config);Map<?,?> fences=(Map<?,?>)paused.get("targetFences");require("FENCED".equals(fences.get("postgres"))&&"FENCED".equals(fences.get("dm")),"Transactional target fences unconfirmed");
            }
            System.out.println("DM_PIPELINE_ACCEPTED: actual MySQL8.4 to PostgreSQL17+DM8.1.4.80 native mode, snapshot/CDC/uint64/decimal/JSON/binary/generated/keyless, target outage with independent SQL progress/catchup, worker SIGKILL resume, independent SQL error rollback/repair/retry, persistent pause, missing-table isolation and transactional fences");
        }finally{kill();source("DROP DATABASE IF EXISTS "+Sql.mysql(id));}
    }
    public static void main(String[] args)throws Exception{new DmPipelineAcceptance().run();}
}
