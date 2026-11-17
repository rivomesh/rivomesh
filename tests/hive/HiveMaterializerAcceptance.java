package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Actual release/isolated JDBC/native target receipts. No alternate production classes on the fixture classpath. */
public final class HiveMaterializerAcceptance {
    static void require(boolean condition,String message){if(!condition)throw new AssertionError(message);}
    static TableSchema.Column column(String name,TableSchema.Type type,boolean nullable){TableSchema.Column c=new TableSchema.Column();c.name=name;c.type=type;c.nullable=nullable;return c;}
    static TableSchema table(String name,boolean keyless){TableSchema table=new TableSchema();table.database="sales";table.name=name;table.id=TableSchema.identity("sales","",name);table.columns=Arrays.asList(column("id",TableSchema.Type.INT64,false),column("value",TableSchema.Type.STRING,true));if(!keyless){table.primaryKey=Collections.singletonList("id");TableSchema.Index ordinary=new TableSchema.Index();ordinary.name="ordinary";ordinary.method="BTREE";ordinary.columns=Arrays.asList("id","value");TableSchema.Index unique=new TableSchema.Index();unique.name="business_unique";unique.method="GSI";unique.unique=true;unique.columns=Collections.singletonList("value");table.indexes=Arrays.asList(ordinary,unique);}return table;}
    static SourcePosition position(int n){return new SourcePosition(1,"MYSQL","source",Collections.singletonMap("gtid","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-"+n));}
    static List<Cell> row(long id,String text){return Arrays.asList(Cell.value(Long.toString(id)),Cell.value(text));}
    static Change change(TableSchema table,String event,Change.Operation operation,List<Cell> before,List<Cell> after){Change c=new Change();c.tableId=table.id;c.eventId=event;c.operation=operation;c.before=before;c.after=after;return c;}
    static SourceTransaction transaction(String generation,int n,Change... changes){SourceTransaction tx=new SourceTransaction();tx.id="tx-"+n;tx.streamGeneration=generation;tx.position=position(n);tx.changes=Arrays.asList(changes);return tx;}
    static MessageSink sink(JdbcJournal journal,Journal.Lease lease,HiveMaterializer.Settings s)throws Exception{return new MessageSink(s.subscriptionId,s.mappings,new JdbcPublicationStore(journal,s.subscriptionId,lease),()->new HiveMaterializer(s),4*1024*1024);}
    static MessageSink uncertainSink(JdbcJournal journal,Journal.Lease lease,HiveMaterializer.Settings s,java.util.concurrent.atomic.AtomicInteger remaining)throws Exception{
        PublicationStore delegate=new JdbcPublicationStore(journal,s.subscriptionId,lease);
        PublicationStore uncertain=(PublicationStore)java.lang.reflect.Proxy.newProxyInstance(PublicationStore.class.getClassLoader(),new Class<?>[]{PublicationStore.class},(proxy,method,arguments)->{
            // Real native publication has completed; lose confirmation before Journal progress is saved.
            if(method.getName().equals("progress")&&arguments!=null&&arguments.length==3&&remaining.get()>0&&remaining.decrementAndGet()==0)throw new java.io.IOException("INJECTED_HIVE_CONFIRMATION_LOSS");
            try{return method.invoke(delegate,arguments);}catch(java.lang.reflect.InvocationTargetException failure){throw failure.getCause();}
        });
        return new MessageSink(s.subscriptionId,s.mappings,uncertain,()->new HiveMaterializer(s),4*1024*1024);
    }
    static Journal.SnapshotState snapshot(JdbcJournal journal,Journal.Lease lease,MessageSink sink,TableSchema table,List<List<Cell>> rows)throws Exception{Journal.SnapshotState state=journal.beginSnapshot(lease,"source",table,position(1));journal.appendSnapshot(lease,state,0,rows);journal.finishSnapshot(lease,state);sink.prepare(lease.epoch,table);sink.beginSnapshot(lease.epoch,table,state);sink.snapshotPart(lease.epoch,table,state.snapshotId,journal.snapshotParts("source",table.id,state.snapshotId,0,1).get(0));sink.finishSnapshot(lease.epoch,table,state);return state;}
    static void phase(Path directory,String phase)throws Exception{Files.write(directory.resolve(phase+".request"),new byte[]{1});long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(300);while(!Files.exists(directory.resolve(phase+".done"))&&System.nanoTime()<end)Thread.sleep(100);require(Files.exists(directory.resolve(phase+".done")),"Native phase did not complete: "+phase);}
    static long count(HiveSqlClient sql,String name)throws Exception{return Long.parseLong(sql.query("SELECT count(*) AS n FROM "+name).get(0).get("n"));}
    public static void main(String[] args)throws Exception{
        String suffix=UUID.randomUUID().toString().replace("-",""),generation=UUID.randomUUID().toString(),pipeline="native-"+suffix;Path directory=Paths.get(System.getenv("RIVOMESH_HIVE_DIR"));
        HiveMaterializer.Settings s=new HiveMaterializer.Settings();s.pipelineId=pipeline;s.subscriptionId="hive";s.generation=generation;s.fingerprint=Codec.sha256("native-config");s.metadataDatabase="meta_"+suffix;s.mappings.put("sales","copy_"+suffix);s.acknowledgeIndexOmission=true;s.jdbcUrl="jdbc:hive2://hive:10000/default;auth=noSasl";s.username="hive";s.password="";
        JdbcJournal.Settings local=new JdbcJournal.Settings();local.driver="org.h2.Driver";local.pipelineId=pipeline;local.namespace="journal";local.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0";local.maxPendingBytes=64*1024*1024;
        TableSchema keyed=table("items",false),duplicates=table("duplicates",true);String name=HiveRows.q(s.mappings.get("sales"));
        try(JdbcJournal journal=new JdbcJournal(local)){
            journal.initialize(s.fingerprint,generation,Collections.singletonList(s.subscriptionId));Journal.Lease lease=journal.claim("owner",1800000);java.util.concurrent.atomic.AtomicInteger loseConfirmation=new java.util.concurrent.atomic.AtomicInteger();
            try(HiveSqlClient sql=new HiveSqlClient(s.jdbcUrl,s.username,s.password,300,4*1024*1024,16*1024*1024);MessageSink target=uncertainSink(journal,lease,s,loseConfirmation)){
                try(HiveMaterializer unsafe=new HiveMaterializer(s)){
                    java.lang.reflect.Field field=HiveMaterializer.class.getDeclaredField("sql");field.setAccessible(true);HiveSqlClient connection=(HiveSqlClient)field.get(unsafe);connection.execute("SET hive.query.results.cache.enabled=true");
                    try{unsafe.prepare(Collections.emptySet());throw new AssertionError("Unsafe cached-query profile was accepted");}catch(IllegalStateException rejected){require("HIVE_QUERY_CACHE_DISABLED_REQUIRED".equals(rejected.getMessage()),"Wrong query-cache guard: "+rejected.getMessage());}
                }
                System.out.println("HIVE_UNSAFE_CACHE_PROFILE_REFUSED");HiveMaterializer.check(s);require(sql.query("SHOW DATABASES LIKE '"+s.metadataDatabase+"'").isEmpty(),"Check created target objects");target.activate(lease.epoch);
                snapshot(journal,lease,target,keyed,Collections.singletonList(row(1,"first")));Journal.SnapshotState duplicateSnapshot=snapshot(journal,lease,target,duplicates,Arrays.asList(row(7,"same"),row(7,"same")));require(count(sql,name+".duplicates")==2,"Keyless snapshot collapsed identical rows");
                Change update=change(keyed,"move-key",Change.Operation.UPDATE,row(1,"first"),row(3,"new")),remove=change(duplicates,"delete-one",Change.Operation.DELETE,row(7,"same"),null);remove.rowId=Codec.sha256(duplicateSnapshot.snapshotId+":0:0");SourceTransaction tx=transaction(generation,1,update,remove);loseConfirmation.set(2);
                try{target.apply(lease.epoch,1,tx);throw new AssertionError("Confirmation loss was not injected after native rows");}catch(java.io.IOException lost){require("INJECTED_HIVE_CONFIRMATION_LOSS".equals(lost.getMessage()),"Wrong injected failure");}
                require(target.checkpoint().sequence==0,"Uncertain native publication advanced transaction checkpoint");require(count(sql,name+".items")==1&&count(sql,name+".duplicates")==1,"Failure occurred before native rows became visible");
                target.apply(lease.epoch,1,tx);target.apply(lease.epoch,1,tx);System.out.println("HIVE_UNCERTAIN_PUBLICATION_REPLAY_ACCEPTED");
                require(count(sql,name+".items")==1&&sql.query("SELECT id FROM "+name+".items").get(0).get("id").equals("3"),"PK move/replay failed");require(count(sql,name+".duplicates")==1,"Delete removed the wrong keyless multiplicity");require(target.checkpoint().sequence==1,"Checkpoint preceded confirmed materialization");
                String physical="rm3_data_"+Codec.sha256("rivomesh:"+Codec.sha256(s.pipelineId+":"+s.subscriptionId+":"+s.generation)+":"+keyed.id).substring(0,48),qualified=HiveRows.q(s.metadataDatabase)+"."+HiveRows.q(physical);String nativeKey=sql.query("SELECT `_rivomesh_key` AS rmkey FROM "+qualified+" WHERE id=3 AND `_rivomesh_deleted`=0 LIMIT 1").get(0).get("rmkey");
                sql.execute("INSERT INTO "+qualified+" SELECT '"+nativeKey+"',CAST(0 AS DECIMAL(38,0)),CAST(0 AS TINYINT),CAST(3 AS BIGINT),"+HiveRows.text("stale"));require(sql.query("SELECT value FROM "+name+".items").get(0).get("value").equals("new"),"Stale native history revived/overwrote newer data");
                TableSchema numeric=new TableSchema();numeric.database="sales";numeric.name="numeric_types";numeric.id=TableSchema.identity("sales","",numeric.name);TableSchema.Column amount=column("amount",TableSchema.Type.DECIMAL,true);amount.precision=65;amount.scale=30;numeric.columns=Arrays.asList(column("id",TableSchema.Type.UINT64,false),amount,column("json_value",TableSchema.Type.JSON,true),column("binary_value",TableSchema.Type.BYTES,true),column("MixedCase",TableSchema.Type.INT64,true));numeric.primaryKey=Collections.singletonList("id");String exact="12345678901234567890123456789012345.123456789012345678901234567890",json="{\"n\":18446744073709551615}";snapshot(journal,lease,target,numeric,Collections.singletonList(Arrays.asList(Cell.value("18446744073709551615"),Cell.value(exact),Cell.value(json),Cell.value("AP+A"),Cell.value("42"))));Map<String,String> numbers=sql.query("SELECT CAST(id AS STRING) AS id,CAST(amount AS STRING) AS amount,json_value,binary_value,MixedCase AS mixedcase FROM "+name+".numeric_types").get(0);require(numbers.get("id").equals("18446744073709551615")&&numbers.get("amount").equals(exact)&&numbers.get("json_value").equals(json)&&numbers.get("binary_value").equals("AP+A")&&numbers.get("mixedcase").equals("42"),"Native numeric/JSON/binary precision changed");
            }
            System.out.println("HIVE_NATIVE_DATA_ACCEPTED");phase(directory,"restart");
            try(HiveSqlClient sql=new HiveSqlClient(s.jdbcUrl,s.username,s.password,300,4*1024*1024,16*1024*1024);MessageSink target=sink(journal,lease,s)){
                target.activate(lease.epoch);target.prepare(lease.epoch,keyed);target.prepare(lease.epoch,duplicates);target.apply(lease.epoch,2,transaction(generation,2,change(keyed,"after-restart",Change.Operation.INSERT,null,row(4,"after restart"))));require(count(sql,name+".items")==2,"Restart lost confirmed rows");require(target.checkpoint().sequence==2,"Restart did not advance checkpoint");
            }
            System.out.println("HIVE_NATIVE_RESTART_ACCEPTED");phase(directory,"restore");
            try(HiveSqlClient sql=new HiveSqlClient(s.jdbcUrl,s.username,s.password,300,4*1024*1024,16*1024*1024)){require(count(sql,name+".items")==1,"Restore is not an older nonempty target");}
            try(MessageSink target=sink(journal,lease,s)){target.activate(lease.epoch);try{target.prepare(lease.epoch,keyed);throw new AssertionError("Old nonempty native backup reused a newer checkpoint");}catch(IllegalStateException failure){require("HIVE_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED".equals(failure.getMessage()),"Wrong rollback guard: "+failure.getMessage());}require(target.checkpoint().sequence==2,"Rollback changed the saved checkpoint");}
        }
        System.out.println("HIVE_MATERIALIZER_ACCEPTED: actual release Java8/isolated JDBC/native ACID ORC history/views, PK move/replay, keyless multiplicity/delete, injected native confirmation loss/replay, exact uint64/65digit-decimal/JSON/base64, stale-version refusal, server/client restart and actual older nonempty warehouse+metastore backup refusal");
    }
}
