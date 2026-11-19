package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Actual release/JDBC/HTTP/native target receipts. No alternate production classes on the fixture classpath. */
public final class DorisMaterializerAcceptance {
    static void require(boolean condition,String message){if(!condition)throw new AssertionError(message);}
    static TableSchema.Column column(String name,TableSchema.Type type,boolean nullable){TableSchema.Column c=new TableSchema.Column();c.name=name;c.type=type;c.nullable=nullable;return c;}
    static TableSchema table(String name,boolean keyless){TableSchema table=new TableSchema();table.database="sales";table.name=name;table.id=TableSchema.identity("sales","",name);table.columns=Arrays.asList(column("id",TableSchema.Type.INT64,false),column("value",TableSchema.Type.STRING,true));if(!keyless){table.primaryKey=Collections.singletonList("id");TableSchema.Index ordinary=new TableSchema.Index();ordinary.name="ordinary";ordinary.method="BTREE";ordinary.columns=Arrays.asList("id","value");TableSchema.Index unique=new TableSchema.Index();unique.name="business_unique";unique.method="GSI";unique.unique=true;unique.columns=Collections.singletonList("value");table.indexes=Arrays.asList(ordinary,unique);}return table;}
    static SourcePosition position(int n){return new SourcePosition(1,"MYSQL","source",Collections.singletonMap("gtid","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-"+n));}
    static List<Cell> row(long id,String text){return Arrays.asList(Cell.value(Long.toString(id)),Cell.value(text));}
    static Change change(TableSchema table,String event,Change.Operation operation,List<Cell> before,List<Cell> after){Change c=new Change();c.tableId=table.id;c.eventId=event;c.operation=operation;c.before=before;c.after=after;return c;}
    static SourceTransaction transaction(String generation,int n,Change... changes){SourceTransaction tx=new SourceTransaction();tx.id="tx-"+n;tx.streamGeneration=generation;tx.position=position(n);tx.changes=Arrays.asList(changes);return tx;}
    static MessageSink sink(JdbcJournal journal,Journal.Lease lease,DorisMaterializer.Settings s)throws Exception{return new MessageSink(s.subscriptionId,s.mappings,new JdbcPublicationStore(journal,s.subscriptionId,lease),()->new DorisMaterializer(s),4*1024*1024);}
    static Journal.SnapshotState snapshot(JdbcJournal journal,Journal.Lease lease,MessageSink sink,TableSchema table,List<List<Cell>> rows)throws Exception{Journal.SnapshotState state=journal.beginSnapshot(lease,"source",table,position(1));journal.appendSnapshot(lease,state,0,rows);journal.finishSnapshot(lease,state);sink.prepare(lease.epoch,table);sink.beginSnapshot(lease.epoch,table,state);sink.snapshotPart(lease.epoch,table,state.snapshotId,journal.snapshotParts("source",table.id,state.snapshotId,0,1).get(0));sink.finishSnapshot(lease.epoch,table,state);return state;}
    static void phase(Path directory,String phase)throws Exception{Files.write(directory.resolve(phase+".request"),new byte[]{1});long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(180);while(!Files.exists(directory.resolve(phase+".done"))&&System.nanoTime()<end)Thread.sleep(100);require(Files.exists(directory.resolve(phase+".done")),"Native phase did not complete: "+phase);}
    static long count(DorisSqlClient sql,String name)throws Exception{return Long.parseLong(sql.query("SELECT count(*) AS n FROM "+name).get(0).get("n"));}
    static void modeCase(Path directory,String mode)throws Exception{
        String suffix=UUID.randomUUID().toString().replace("-",""),generation=UUID.randomUUID().toString(),pipeline="mode-"+suffix;
        DorisMaterializer.Settings s=new DorisMaterializer.Settings();s.pipelineId=pipeline;s.subscriptionId="doris";s.generation=generation;s.fingerprint=Codec.sha256("mode-config");s.metadataDatabase="meta_"+suffix;s.mappings.put("sales","copy_"+suffix);s.replicas=1;s.buckets=1;s.jdbcUrl="jdbc:mysql://doris:9030/?useSSL=false&allowPublicKeyRetrieval=true&sessionVariables="+java.net.URLEncoder.encode("sql_mode='"+mode+"'","UTF-8");s.username="root";s.password="";s.http=new DorisHttpClient.Settings();s.http.url="http://doris:8040";s.http.username=s.username;s.http.password=s.password;
        JdbcJournal.Settings local=new JdbcJournal.Settings();local.driver="org.h2.Driver";local.pipelineId=pipeline;local.namespace="journal";local.url="jdbc:h2:file:"+directory.resolve("mode-"+suffix)+";WRITE_DELAY=0";local.maxPendingBytes=64*1024*1024;
        TableSchema table=table("quoted'\\table",false);String qualified=DorisRows.q(s.mappings.get("sales"))+"."+DorisRows.q(table.name),value="quotes '\\ snow 雪 emoji \ud83d\ude03";
        try(JdbcJournal journal=new JdbcJournal(local)){
            journal.initialize(s.fingerprint,generation,Collections.singletonList(s.subscriptionId));Journal.Lease lease=journal.claim("owner",600000);
            try(DorisSqlClient sql=new DorisSqlClient(s.jdbcUrl,s.username,s.password,30000);MessageSink target=sink(journal,lease,s)){
                require(sql.query("SELECT @@sql_mode AS mode").get(0).get("mode").contains("NO_BACKSLASH_ESCAPES")==mode.contains("NO_BACKSLASH_ESCAPES"),"Requested SQL mode was not active");
                String predicate="x' OR 1=1 -- \\雪";require(sql.query("SELECT CAST(? AS STRING) AS value",predicate).get(0).get("value").equals(predicate),"Prepared parameter changed with SQL mode");
                target.activate(lease.epoch);snapshot(journal,lease,target,table,Collections.singletonList(row(1,value)));require(sql.query("SELECT value FROM "+qualified).get(0).get("value").equals(value),"Mode snapshot changed text");
                target.apply(lease.epoch,1,transaction(generation,1,change(table,"mode-update",Change.Operation.UPDATE,row(1,value),row(1,value+" updated"))));require(target.checkpoint().sequence==1,"Mode checkpoint not confirmed");
            }
            try(DorisSqlClient sql=new DorisSqlClient(s.jdbcUrl,s.username,s.password,30000);MessageSink target=sink(journal,lease,s)){
                target.activate(lease.epoch);target.prepare(lease.epoch,table);require(sql.query("SELECT value FROM "+qualified).get(0).get("value").equals(value+" updated"),"Mode recovery lost text");require(target.checkpoint().sequence==1,"Mode recovery lost checkpoint");
            }
        }
        System.out.println("DORIS_SQL_MODE_ACCEPTED: "+(mode.isEmpty()?"EMPTY":mode)+"; binary prepared values, quoted/backslash table, Unicode snapshot/CDC/reopen");
    }
    public static void main(String[] args)throws Exception{
        String suffix=UUID.randomUUID().toString().replace("-",""),generation=UUID.randomUUID().toString(),pipeline="native-"+suffix;Path directory=Paths.get(System.getenv("RIVOMESH_DORIS_DIR"));
        DorisMaterializer.Settings s=new DorisMaterializer.Settings();s.pipelineId=pipeline;s.subscriptionId="doris";s.generation=generation;s.fingerprint=Codec.sha256("native-config");s.metadataDatabase="meta_"+suffix;s.mappings.put("sales","copy_"+suffix);s.replicas=1;s.buckets=1;s.jdbcUrl="jdbc:mysql://doris:9030/?useSSL=false&allowPublicKeyRetrieval=true";s.username="root";s.password="";s.http=new DorisHttpClient.Settings();s.http.url="http://doris:8040";s.http.username=s.username;s.http.password=s.password;
        JdbcJournal.Settings local=new JdbcJournal.Settings();local.driver="org.h2.Driver";local.pipelineId=pipeline;local.namespace="journal";local.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0";local.maxPendingBytes=64*1024*1024;
        TableSchema keyed=table("items",false),duplicates=table("duplicates",true);String name=DorisRows.q(s.mappings.get("sales"));
        try(JdbcJournal journal=new JdbcJournal(local)){
            journal.initialize(s.fingerprint,generation,Collections.singletonList(s.subscriptionId));Journal.Lease lease=journal.claim("owner",600000);
            try(DorisSqlClient sql=new DorisSqlClient(s.jdbcUrl,s.username,s.password,30000);MessageSink target=sink(journal,lease,s)){
                DorisMaterializer.check(s);require(sql.query("SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME='"+s.metadataDatabase+"'").isEmpty(),"Check created target objects");target.activate(lease.epoch);
                snapshot(journal,lease,target,keyed,Collections.singletonList(row(1,"first")));Journal.SnapshotState duplicateSnapshot=snapshot(journal,lease,target,duplicates,Arrays.asList(row(7,"same"),row(7,"same")));require(count(sql,name+".duplicates")==2,"Keyless snapshot collapsed identical rows");
                Change update=change(keyed,"move-key",Change.Operation.UPDATE,row(1,"first"),row(3,"new")),remove=change(duplicates,"delete-one",Change.Operation.DELETE,row(7,"same"),null);remove.rowId=Codec.sha256(duplicateSnapshot.snapshotId+":0:0");SourceTransaction tx=transaction(generation,1,update,remove);target.apply(lease.epoch,1,tx);target.apply(lease.epoch,1,tx);
                require(count(sql,name+".items")==1&&sql.query("SELECT id FROM "+name+".items").get(0).get("id").equals("3"),"PK move/replay failed");require(count(sql,name+".duplicates")==1,"Delete removed the wrong keyless multiplicity");require(target.checkpoint().sequence==1,"Checkpoint preceded confirmed materialization");
                String physical=DorisMaterializer.dataName(s,keyed);String ddl=sql.query("SHOW CREATE TABLE "+DorisRows.q(s.metadataDatabase)+"."+DorisRows.q(physical)).get(0).get("Create Table");require(ddl.contains("USING INVERTED")&&!ddl.contains("business_unique"),"Index conversion not observed");
                sql.execute("INSERT INTO "+DorisRows.q(s.metadataDatabase)+"."+DorisRows.q(physical)+" SELECT _rivomesh_key,0,0,id,'stale' FROM "+DorisRows.q(s.metadataDatabase)+"."+DorisRows.q(physical)+" WHERE id=3 AND _rivomesh_deleted=0");require(sql.query("SELECT value FROM "+name+".items").get(0).get("value").equals("new"),"Stale native version overwrote newer data");
                TableSchema numeric=new TableSchema();numeric.database="sales";numeric.name="numeric_types";numeric.id=TableSchema.identity("sales","",numeric.name);TableSchema.Column amount=column("amount",TableSchema.Type.DECIMAL,true);amount.precision=65;amount.scale=30;numeric.columns=Arrays.asList(column("id",TableSchema.Type.UINT64,false),amount,column("json_value",TableSchema.Type.JSON,true),column("binary_value",TableSchema.Type.BYTES,true));numeric.primaryKey=Collections.singletonList("id");String exact="12345678901234567890123456789012345.123456789012345678901234567890",json="{\"n\":18446744073709551615}";snapshot(journal,lease,target,numeric,Collections.singletonList(Arrays.asList(Cell.value("18446744073709551615"),Cell.value(exact),Cell.value(json),Cell.value("AP+A"))));Map<String,String> numbers=sql.query("SELECT CAST(id AS STRING) AS id,CAST(amount AS STRING) AS amount,json_value,binary_value FROM "+name+".numeric_types").get(0);require(numbers.get("id").equals("18446744073709551615")&&numbers.get("amount").equals(exact)&&numbers.get("json_value").equals(json)&&numbers.get("binary_value").equals("AP+A"),"Native numeric/JSON/binary precision changed");
            }
            phase(directory,"restart");
            try(DorisSqlClient sql=new DorisSqlClient(s.jdbcUrl,s.username,s.password,30000);MessageSink target=sink(journal,lease,s)){
                target.activate(lease.epoch);target.prepare(lease.epoch,keyed);target.prepare(lease.epoch,duplicates);target.apply(lease.epoch,2,transaction(generation,2,change(keyed,"after-restart",Change.Operation.INSERT,null,row(4,"after restart"))));require(count(sql,name+".items")==2,"Restart lost confirmed rows");require(target.checkpoint().sequence==2,"Restart did not advance checkpoint");
            }
            phase(directory,"restore");
            try(DorisSqlClient sql=new DorisSqlClient(s.jdbcUrl,s.username,s.password,30000)){require(count(sql,name+".items")==1,"Restore is not an older nonempty target");}
            try(MessageSink target=sink(journal,lease,s)){target.activate(lease.epoch);try{target.prepare(lease.epoch,keyed);throw new AssertionError("Old nonempty native backup reused a newer checkpoint");}catch(IllegalStateException failure){require("DORIS_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED".equals(failure.getMessage()),"Wrong rollback guard: "+failure.getMessage());}require(target.checkpoint().sequence==2,"Rollback changed the saved checkpoint");}
        }
        modeCase(directory,"");modeCase(directory,"NO_BACKSLASH_ESCAPES");
        System.out.println("DORIS_MATERIALIZER_ACCEPTED: actual release Java 21/JDBC/Stream Load, auto MoW schema/views, nonunique BTREE/GSI indexes, PK move/replay, keyless multiplicity/delete, server/client restart and actual older nonempty native backup refusal");
    }
}
