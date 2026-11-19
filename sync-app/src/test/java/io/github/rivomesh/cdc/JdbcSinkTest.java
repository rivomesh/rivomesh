package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class JdbcSinkTest {
    @TempDir Path directory;
    JdbcSink.Settings settings(){
        JdbcSink.Settings s=new JdbcSink.Settings();s.id="target";s.pipelineId="pipeline";s.generation="generation";s.fingerprint=Codec.sha256("config");s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("target")+";WRITE_DELAY=0";s.product=TargetProduct.H2;s.databases.put("sales","sales_replica");return s;
    }
    @Test void administrativeOpenNeverRecreatesMissingCheckpointMetadata()throws Exception{
        JdbcSink.Settings s=settings();assertThrows(SQLException.class,()->JdbcSink.existing(s));
        try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);ResultSet schemas=c.getMetaData().getSchemas()){while(schemas.next())assertNotEquals(s.metadataSchema,schemas.getString("TABLE_SCHEM"));}
        try(JdbcSink created=new JdbcSink(s)){created.activate(7);}
        try(JdbcSink existing=JdbcSink.existing(s)){assertEquals(7,existing.checkpoint().epoch);assertEquals(0,existing.checkpoint().sequence);}
        try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);Statement sql=c.createStatement()){sql.execute("DROP TABLE \""+s.metadataSchema+"\".\"rm3_sink_job\"");}
        assertThrows(SQLException.class,()->JdbcSink.existing(s));
        try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);ResultSet tables=c.getMetaData().getTables(null,s.metadataSchema,"rm3_sink_job",null)){assertFalse(tables.next());}
    }
    @Test void skipCapabilitiesCannotBeInferredFromAnotherSchemaMatchingTheJdbcPattern()throws Exception{
        JdbcSink.Settings s=settings();
        try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);Statement sql=c.createStatement()){
            sql.execute("CREATE SCHEMA \"rivomeshXv3\"");sql.execute("CREATE TABLE \"rivomeshXv3\".\"rm3_sink_job\" (\"skip_operation\" VARCHAR(128),\"skip_hash\" VARCHAR(64))");
        }
        try(JdbcSink sink=new JdbcSink(s)){assertEquals(0,sink.activate(2).sequence);assertNull(sink.checkpoint().skipOperation);}
    }
    static TableSchema schema(boolean keyless){
        TableSchema t=new TableSchema();t.id=TableSchema.identity("sales","","items");t.database="sales";t.name=keyless?"duplicates":"items";
        for(String name:Arrays.asList("id","email","document","amount")){
            TableSchema.Column c=new TableSchema.Column();c.name=name;c.type=name.equals("id")?TableSchema.Type.INT64:name.equals("email")?TableSchema.Type.STRING:name.equals("document")?TableSchema.Type.JSON:TableSchema.Type.DECIMAL;c.length=100;c.precision=35;c.scale=15;c.nullable=!name.equals("id");t.columns.add(c);
        }
        if(!keyless)t.primaryKey.add("id");
        TableSchema.Index index=new TableSchema.Index();index.name="unique_email";index.unique=true;index.columns.add("email");t.indexes.add(index);return t;
    }
    static List<Cell> row(long id,String email){return Arrays.asList(Cell.value(""+id),Cell.value(email),Cell.value("{\"integer\":18446744073709551615,\"null\":null}"),Cell.value("12345678901234567890.123456789012345"));}
    static Change insert(TableSchema table,long id,String email){Change change=new Change();change.operation=Change.Operation.INSERT;change.eventId="event:"+id;change.tableId=table.id;change.after=row(id,email);return change;}
    static SourceTransaction transaction(int n,Change... changes){SourceTransaction tx=new JournalTest().transaction(n);tx.changes=new ArrayList<>(Arrays.asList(changes));return tx;}
    static Journal.SnapshotState snapshot(TableSchema t){Journal.SnapshotState s=new Journal.SnapshotState();s.tableId=t.id;s.schemaVersion=t.version;s.snapshotId=UUID.randomUUID().toString();s.state="COMPLETE";return s;}
    static void emptySnapshot(JdbcSink sink,long epoch,TableSchema t)throws Exception{sink.prepare(epoch,t);Journal.SnapshotState s=snapshot(t);sink.beginSnapshot(epoch,t,s);sink.finishSnapshot(epoch,t,s);}
    static String q(JdbcSink.Settings s,String value){return s.product==TargetProduct.MYSQL?"`"+value+"`":"\""+value+"\"";}
    static long count(JdbcSink.Settings s,String table)throws Exception{try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);Statement st=c.createStatement();ResultSet rs=st.executeQuery("SELECT COUNT(*) FROM "+q(s,s.databases.get("sales"))+"."+q(s,table))){rs.next();return rs.getLong(1);}}
    static void contract(JdbcSink.Settings settings)throws Exception{
        TableSchema table=schema(false);SourceTransaction first=transaction(1,insert(table,1,"same"),insert(table,2,"same"));
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);emptySnapshot(sink,1,table);
            JdbcSink.Settings alias=Codec.decode(Codec.encode(settings),JdbcSink.Settings.class);alias.pipelineId="alias-pipeline";alias.id="alias-target";alias.fingerprint=Codec.sha256("alias-config");
            try(JdbcSink accidentalAlias=new JdbcSink(alias)){accidentalAlias.activate(1);assertEquals("DESTINATION_ALREADY_OWNED",assertThrows(IllegalStateException.class,()->accidentalAlias.prepare(1,table)).getMessage());}
            sink.apply(1,1,first);sink.apply(1,1,first);assertEquals(2,count(settings,"items"));
            SourceTransaction broken=transaction(2,insert(table,3,"third"),insert(table,2,"duplicate-primary-key"));assertThrows(SQLException.class,()->sink.apply(1,2,broken));
            assertEquals(2,count(settings,"items"));assertEquals(1,sink.checkpoint().sequence);
            Change update=new Change();update.eventId="update";update.tableId=table.id;update.operation=Change.Operation.UPDATE;update.before=row(1,"same");update.after=row(10,"changed");sink.apply(1,2,transaction(2,update));assertEquals(2,count(settings,"items"));
            sink.activate(2);assertThrows(IllegalStateException.class,()->sink.apply(1,3,transaction(3,insert(table,3,"stale"))));assertThrows(IllegalStateException.class,()->sink.activate(1));
        }
        try(JdbcSink sink=new JdbcSink(settings)){
            SinkAdapter.Checkpoint cp=sink.activate(3);assertEquals(2,cp.sequence);sink.prepare(3,table);
            Change delete=new Change();delete.eventId="delete";delete.tableId=table.id;delete.operation=Change.Operation.DELETE;delete.before=row(10,"changed");sink.apply(3,3,transaction(3,delete));assertEquals(1,count(settings,"items"));
            assertThrows(IllegalStateException.class,()->sink.apply(3,3,transaction(3,insert(table,999,"changed-content"))));
            TableSchema values=new TableSchema();values.database="sales";values.name="type_values";values.id=TableSchema.identity("sales","",values.name);values.primaryKey.add("id");
            TableSchema.Type[] types={TableSchema.Type.INT64,TableSchema.Type.STRING,TableSchema.Type.UINT64,TableSchema.Type.DATE,TableSchema.Type.DATETIME,TableSchema.Type.BYTES,TableSchema.Type.JSON};
            String[] names={"id","empty_string","unsigned_value","event_date","event_time","empty_bytes","null_json"};
            for(int i=0;i<types.length;i++){TableSchema.Column col=new TableSchema.Column();col.name=names[i];col.type=types[i];col.length=100;col.nullable=i!=0;values.columns.add(col);}
            emptySnapshot(sink,3,values);Change value=new Change();value.tableId=values.id;value.eventId="wide-values";value.operation=Change.Operation.INSERT;value.after=Arrays.asList(Cell.value("1"),Cell.value(""),Cell.value("18446744073709551615"),Cell.value("2024-02-29"),Cell.value("2024-02-29 12:34:56.123456"),Cell.value(""),Cell.value(null));sink.apply(3,4,transaction(4,value));
            try(Connection c=JdbcConnections.open(settings.driver,settings.url,settings.username,settings.password);Statement st=c.createStatement();ResultSet rs=st.executeQuery("SELECT * FROM "+q(settings,settings.databases.get("sales"))+"."+q(settings,values.name))){rs.next();assertEquals("",rs.getString(2));assertEquals("18446744073709551615",rs.getBigDecimal(3).toPlainString());assertEquals("2024-02-29",rs.getDate(4).toString());assertEquals("2024-02-29T12:34:56.123456",rs.getTimestamp(5).toLocalDateTime().toString());assertEquals(0,rs.getBytes(6).length);assertNull(rs.getString(7));}
            JdbcSink.Settings lost=Codec.decode(Codec.encode(settings),JdbcSink.Settings.class);lost.generation="lost-journal";
            assertThrows(IllegalStateException.class,()->new JdbcSink(lost));
        }
    }
    @Test void atomicBusinessCheckpointRecoveryAndEpochFencing()throws Exception{contract(settings());}
    @Test void parkedFenceRechecksTheExactSnapshotAndReceiptAndReplaysOnlyTheSameCut()throws Exception{
        JdbcSink.Settings config=settings();
        TableSchema table=schema(false);
        Journal.SnapshotState snapshot=snapshot(table);
        try(JdbcSink sink=new JdbcSink(config)){
            sink.activate(3);sink.prepare(3,table);
            sink.beginSnapshot(3,table,snapshot);sink.finishSnapshot(3,table,snapshot);
            sink.apply(3,1,transaction(1,insert(table,1,"original")));
            Map<String,Journal.SnapshotState> snapshots=Map.of(table.id,snapshot);
            var cut=sink.verifyWriterCut(3,1,List.of(table),snapshots);
            var changed=Codec.decode(Codec.encode(cut),SinkAdapter.Checkpoint.class);
            changed.transactionHash=Codec.sha256("different event");
            assertEquals("HANDOFF_TARGET_CUT_CHANGED",assertThrows(IllegalStateException.class,
                    ()->sink.fenceWriterCut(4,changed,List.of(table),snapshots,()->{})).getMessage());
            assertThrows(IllegalStateException.class,()->sink.fenceWriterCut(4,cut,List.of(table),
                    Map.of(table.id,snapshot(table)),()->{}));
            assertThrows(SecurityException.class,()->sink.fenceWriterCut(4,cut,List.of(table),snapshots,
                    ()->{throw new SecurityException("FORBIDDEN");}));
            assertEquals(Codec.encode(cut),Codec.encode(sink.checkpoint()));
            var fenced=sink.fenceWriterCut(4,cut,List.of(table),snapshots,()->{});
            assertEquals(Codec.encode(fenced),Codec.encode(sink.fenceWriterCut(4,cut,List.of(table),snapshots,()->{})));
            assertEquals(4,fenced.epoch);
            assertEquals(cut.sequence,fenced.sequence);
            assertEquals(cut.transactionHash,fenced.transactionHash);
            assertEquals("SINK_FENCED",assertThrows(IllegalStateException.class,
                    ()->sink.apply(3,2,transaction(2,insert(table,2,"stale")))).getMessage());
            assertEquals(1,count(config,table.name));
        }
    }
    @Test void keylessSnapshotPreservesMultiplicityAndRetriesWithoutDuplicates()throws Exception{
        JdbcSink.Settings settings=settings();TableSchema t=schema(true);Journal.SnapshotState snapshot=snapshot(t);
        Journal.SnapshotPart part=new Journal.SnapshotPart();part.rows=Arrays.asList(row(1,"duplicate"),row(1,"duplicate"));snapshot.nextPart=1;snapshot.rows=2;
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);sink.prepare(1,t);sink.beginSnapshot(1,t,snapshot);sink.snapshotPart(1,t,snapshot.snapshotId,part);sink.snapshotPart(1,t,snapshot.snapshotId,part);assertEquals(2,count(settings,t.name));
        }
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(2);sink.prepare(2,t);assertEquals(1,sink.beginSnapshot(2,t,snapshot).nextPart);sink.finishSnapshot(2,t,snapshot);
            Change deletion=new Change();deletion.tableId=t.id;deletion.eventId="one-occurrence";deletion.operation=Change.Operation.DELETE;deletion.before=row(1,"duplicate");deletion.rowId=Codec.sha256(snapshot.snapshotId+":0:0");sink.apply(2,1,transaction(1,deletion));assertEquals(1,count(settings,t.name));
            Change insertion=insert(t,2,"new");insertion.rowId=Codec.sha256(insertion.eventId);sink.apply(2,2,transaction(2,insertion));assertEquals(2,count(settings,t.name));
        }
    }
    @Test void incompleteImagesAndSnapshotsNeverAdvanceCheckpoints()throws Exception{
        JdbcSink.Settings settings=settings();TableSchema t=schema(false);
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);sink.prepare(1,t);assertThrows(IllegalStateException.class,()->sink.apply(1,1,transaction(1,insert(t,1,"early"))));
            Journal.SnapshotState snapshot=snapshot(t);sink.beginSnapshot(1,t,snapshot);sink.finishSnapshot(1,t,snapshot);
            Change bad=insert(t,1,"missing");bad.after.set(3,Cell.missing());assertThrows(IllegalArgumentException.class,()->sink.apply(1,1,transaction(1,bad)));assertEquals(0,sink.checkpoint().sequence);assertEquals(0,count(settings,t.name));
        }
    }
    @Test void indexPolicyNeverDowngradesIdentityAndRequiresPlansForUnportableDefinitions(){
        TableSchema t=schema(false);TableSchema.Index gsi=new TableSchema.Index();gsi.name="global_email";gsi.method="GSI";gsi.unique=true;gsi.columns.add("email");t.indexes.add(gsi);
        List<IndexPolicy.Decision> plan=IndexPolicy.plan(t,TargetProduct.GAUSS,JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS);assertEquals("DOWNGRADE_UNIQUE",plan.get(0).action);assertEquals("REPLACE_GSI",plan.get(1).action);assertFalse(plan.get(1).unique);
        t.identityKey.add("email");assertTrue(IndexPolicy.plan(t,TargetProduct.GAUSS,JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS).get(0).unique);
        assertEquals("REQUIRES_PLAN",IndexPolicy.plan(t,TargetProduct.GAUSS,JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS).get(1).action);
        gsi.prefixLengths.add(20);assertEquals("REQUIRES_PLAN",IndexPolicy.plan(t,TargetProduct.GAUSS,JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS).get(1).action);
    }
    @Test void globalIndexCoveringPrimaryOrCandidateIdentityStopsBeforeBusinessDdl()throws Exception{
        JdbcSink.Settings settings=settings();
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);
            for(boolean primary:List.of(true,false)){
                TableSchema table=schema(false);
                table.primaryKey.clear();
                List<String> key=primary?table.primaryKey:table.identityKey;
                key.addAll(List.of("id","email"));
                TableSchema.Index index=table.indexes.get(0);
                index.columns=new ArrayList<>(List.of("email","id"));
                assertTrue(IndexPolicy.plan(table,TargetProduct.GAUSS,settings.uniquePolicy).get(0).unique);
                index.method="GSI";
                IndexPolicy.Decision decision=IndexPolicy.plan(table,TargetProduct.GAUSS,settings.uniquePolicy).get(0);
                assertEquals("REQUIRES_PLAN",decision.action);
                assertEquals("GLOBAL_IDENTITY_REQUIRES_DISTRIBUTION_PLAN",decision.reason);
                assertTrue(decision.unique);
                assertEquals("INDEX_CONVERSION_REQUIRES_PLAN",assertThrows(IllegalStateException.class,()->sink.prepare(1,table)).getMessage());
                index.columns.add("amount");
                assertEquals("REPLACE_GSI",IndexPolicy.plan(table,TargetProduct.GAUSS,settings.uniquePolicy).get(0).action);
            }
            TableSchema table=schema(false);
            TableSchema.Index index=table.indexes.get(0);
            index.name="PRIMARY";
            index.columns=new ArrayList<>(table.primaryKey);
            index.primary=true;
            assertTrue(IndexPolicy.plan(table,TargetProduct.GAUSS,settings.uniquePolicy).isEmpty());
            index.method="GLOBAL_BTREE";
            IndexPolicy.Decision decision=IndexPolicy.plan(table,TargetProduct.GAUSS,settings.uniquePolicy).get(0);
            assertEquals("REQUIRES_PLAN",decision.action);
            assertEquals("GLOBAL_IDENTITY_REQUIRES_DISTRIBUTION_PLAN",decision.reason);
            assertEquals("INDEX_CONVERSION_REQUIRES_PLAN",assertThrows(IllegalStateException.class,()->sink.prepare(1,table)).getMessage());
        }
        try(Connection c=JdbcConnections.open(settings.driver,settings.url,settings.username,settings.password);
            ResultSet schemas=c.getMetaData().getSchemas()){
            while(schemas.next())assertNotEquals(settings.databases.get("sales"),schemas.getString("TABLE_SCHEM"));
        }
    }
    @Test void anEmptyTableCannotHaveTwoPublishersAndLostTablesRequireARebuildPlan()throws Exception{
        JdbcSink.Settings settings=settings();TableSchema table=schema(false);
        try(JdbcSink first=new JdbcSink(settings)){
            first.activate(1);emptySnapshot(first,1,table);
            JdbcSink.Settings other=Codec.decode(Codec.encode(settings),JdbcSink.Settings.class);other.pipelineId="another-pipeline";other.id="other-target";other.fingerprint=Codec.sha256("other");
            try(JdbcSink second=new JdbcSink(other)){second.activate(1);assertEquals("DESTINATION_ALREADY_OWNED",assertThrows(IllegalStateException.class,()->second.prepare(1,table)).getMessage());}
            try(Connection c=JdbcConnections.open(settings.driver,settings.url,settings.username,settings.password);Statement st=c.createStatement()){st.execute("DROP TABLE \"sales_replica\".\"items\"");}
            assertEquals("TARGET_TABLE_MISSING_REBUILD_REQUIRED",assertThrows(IllegalStateException.class,()->first.prepare(1,table)).getMessage());assertEquals(0,first.checkpoint().sequence);
        }
    }
}
