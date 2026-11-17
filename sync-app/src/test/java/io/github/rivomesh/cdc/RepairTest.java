package io.github.rivomesh.cdc;

import io.github.rivomesh.*;
import io.github.rivomesh.repair.*;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RepairTest {
    @TempDir Path root;
    final class Fixture implements AutoCloseable {
        final JdbcSink.Settings settings;
        final JdbcJournal journal;
        final JdbcSink sink;
        final TableSchema table;
        final PipelineValidationBarrier.Certificate cut=new PipelineValidationBarrier.Certificate();
        final ValidationStore store;
        final RepairPlan plan;
        Fixture(boolean keyless)throws Exception{
            JdbcSinkTest helper=new JdbcSinkTest();helper.directory=root;settings=helper.settings();settings.url="jdbc:h2:file:"+root.resolve("sink")+";WRITE_DELAY=0;LOCK_TIMEOUT=5000";
            JdbcJournal.Settings j=new JdbcJournal.Settings();j.driver="org.h2.Driver";j.url="jdbc:h2:file:"+root.resolve("journal")+";WRITE_DELAY=0;LOCK_TIMEOUT=5000";j.pipelineId=settings.pipelineId;journal=new JdbcJournal(j);journal.initialize(settings.fingerprint,settings.generation,Collections.singletonList(settings.id));Journal.PipelineState paused=journal.pausePipeline();
            table=JdbcSinkTest.schema(keyless);table.id=TableSchema.identity(table.database,"",table.name);
            sink=new JdbcSink(settings);sink.activate(paused.epoch);sink.prepare(paused.epoch,table);
            Journal.SnapshotState snapshot=JdbcSinkTest.snapshot(table);snapshot.nextPart=1;snapshot.rows=2;
            Journal.SnapshotPart part=new Journal.SnapshotPart();part.rows=keyless?Arrays.asList(JdbcSinkTest.row(1,"same"),JdbcSinkTest.row(1,"same")):Arrays.asList(JdbcSinkTest.row(1,"changed"),JdbcSinkTest.row(3,"extra"));
            sink.beginSnapshot(paused.epoch,table,snapshot);sink.snapshotPart(paused.epoch,table,snapshot.snapshotId,part);sink.finishSnapshot(paused.epoch,table,snapshot);
            if(keyless)sql("UPDATE \"sales_replica\".\"duplicates\" SET \"_rivomesh_row_id\"='"+Codec.sha256("wrong")+"' WHERE \"_rivomesh_row_id\"='"+Codec.sha256(snapshot.snapshotId+":0:1")+"'");
            cut.id=UUID.randomUUID().toString();cut.generation=settings.generation;cut.fingerprint=settings.fingerprint;cut.epoch=paused.epoch;cut.errorGeneration=paused.errorGeneration;cut.selectedTargets.add(settings.id);cut.schemas.put(table.id,Codec.sha256(Codec.encode(table)));cut.targetCheckpoints.put(settings.id,sink.checkpoint());
            store=ValidationStore.create(root.resolve("reports"),cut.id,cut,Collections.singletonList(table),Collections.singletonMap(settings.id,Collections.singleton(table.id)),new ValidationStore.Limits());
            if(keyless)for(int i=0;i<2;i++){JdbcJournal.IdentityRow identity=new JdbcJournal.IdentityRow();identity.rowId=Codec.sha256(snapshot.snapshotId+":0:"+i);identity.payload=RowValues.canonical(table,JdbcSinkTest.row(1,"same"));identity.hash=Codec.sha256(identity.payload);store.identity(table.id,identity);}
            if(keyless){store.source(table.id,JdbcSinkTest.row(1,"same"));store.source(table.id,JdbcSinkTest.row(1,"same"));}else{store.source(table.id,JdbcSinkTest.row(1,"expected"));store.source(table.id,JdbcSinkTest.row(2,"missing"));}
            store.finishSourceTable(table.id);store.completeSource();try(TargetRead reader=sink.validationRead(table,sink.checkpoint(),30000,65536)){while(true){List<TargetRead.Row> rows=reader.next(100);if(rows.isEmpty())break;for(TargetRead.Row row:rows)store.target(settings.id,table.id,row.rowId,row.values);}}
            store.finishTarget(settings.id,table.id);store.complete();plan=RepairPlans.create(store,Collections.singleton(settings.id+"/"+table.id),"admin");sink.prepareRepair();
        }
        void sql(String sql)throws Exception{try(Connection c=JdbcConnections.open(settings.driver,settings.url,"","");Statement s=c.createStatement()){s.execute(sql);}}
        JdbcSink.RepairReceipt repair()throws Exception{try(PausedRepairFence fence=new PausedRepairFence(journal,cut,30000)){return sink.repair(plan,plan.tables.values().iterator().next(),store,root.resolve("scratch"),"admin",Codec.sha256("request"),fence);}}
        String liveRoot()throws Exception{try(TargetRead reader=sink.validationRead(table,sink.checkpoint(),30000,65536);RepairProbe probe=new RepairProbe(root.resolve("probe"),table,1048576,100,65536)){while(true){List<TargetRead.Row> rows=reader.next(10);if(rows.isEmpty())break;for(TargetRead.Row row:rows)probe.append(row);}return probe.finish().root;}}
        public void close()throws Exception{store.close();sink.close();journal.close();}
    }
    @Test void keyedRepairCommitsReceiptWithUnchangedCheckpointAndSurvivesReplay()throws Exception{
        try(Fixture f=new Fixture(false)){
            String checkpoint=Codec.encode(f.sink.checkpoint());RepairPlan.Table table=f.plan.tables.values().iterator().next();assertEquals(table.beforeRoot,f.liveRoot());
            JdbcSink.RepairReceipt receipt=f.repair();assertFalse(receipt.alreadyApplied);assertEquals(2,receipt.deleted);assertEquals(2,receipt.inserted);assertEquals(table.afterRoot,f.liveRoot());assertEquals(checkpoint,Codec.encode(f.sink.checkpoint()));
            assertTrue(f.repair().alreadyApplied);assertEquals(table.afterRoot,f.liveRoot());assertEquals("PAUSED",f.journal.status().get("run_state"));
            f.journal.resumePipeline(f.cut.errorGeneration);assertTrue(f.sink.repairReceipt(f.plan,table).alreadyApplied);
        }
    }
    @Test void keylessRepairRestoresExactOccurrenceIdsWithoutCollapsingDuplicates()throws Exception{
        try(Fixture f=new Fixture(true)){assertNotEquals(f.plan.tables.values().iterator().next().afterRoot,f.liveRoot());JdbcSink.RepairReceipt receipt=f.repair();assertEquals(1,receipt.deleted);assertEquals(1,receipt.inserted);assertEquals(f.plan.tables.values().iterator().next().afterRoot,f.liveRoot());assertEquals(2,JdbcSinkTest.count(f.settings,f.table.name));}
    }
    @Test void changedLiveTargetIsRefusedWithoutBusinessWritesOrReceipt()throws Exception{
        try(Fixture f=new Fixture(false)){f.sql("UPDATE \"sales_replica\".\"items\" SET \"email\"='later' WHERE \"id\"=1");String before=f.liveRoot();assertEquals("REPAIR_TARGET_DRIFTED",assertThrows(IllegalStateException.class,f::repair).getMessage());assertEquals(before,f.liveRoot());assertNull(f.sink.repairReceipt(f.plan,f.plan.tables.values().iterator().next()));}
    }
    @Test void sqlFailureAfterDeletesRollsBackWholeTableAndLeavesNoReceipt()throws Exception{
        try(Fixture f=new Fixture(false)){f.sql("ALTER TABLE \"sales_replica\".\"items\" ADD CONSTRAINT reject_expected CHECK (\"email\" <> 'expected')");String before=f.liveRoot(),checkpoint=Codec.encode(f.sink.checkpoint());assertThrows(SQLException.class,f::repair);assertEquals(before,f.liveRoot());assertEquals(checkpoint,Codec.encode(f.sink.checkpoint()));assertNull(f.sink.repairReceipt(f.plan,f.plan.tables.values().iterator().next()));f.sql("ALTER TABLE \"sales_replica\".\"items\" DROP CONSTRAINT reject_expected");assertFalse(f.repair().alreadyApplied);}
    }
    @Test void persistedPlanHashScopeAndReportContentsCannotBeChangedSilently()throws Exception{
        try(Fixture f=new Fixture(false)){RepairPlans.save(root.resolve("reports"),f.plan);RepairPlan loaded=RepairPlans.load(root.resolve("reports"),f.cut.id,f.plan.id);assertEquals(f.plan.hash,loaded.hash);RepairPlans.verifyReport(loaded,f.store);loaded.tables.values().iterator().next().beforeRows++;assertThrows(IllegalArgumentException.class,loaded::validate);}
    }
    @Test void changedReceiptCannotAuthorizeReplay()throws Exception{
        try(Fixture f=new Fixture(false)){f.repair();f.sql("UPDATE \"rivomesh_v3\".\"rm3_sink_repair\" SET after_root='"+Codec.sha256("wrong")+"'");assertEquals("REPAIR_RECEIPT_CONFLICT",assertThrows(IllegalStateException.class,()->f.sink.repairReceipt(f.plan,f.plan.tables.values().iterator().next())).getMessage());}
    }
    @Test void receiptSchemaWithoutItsIdentityKeyIsRefused()throws Exception{
        try(Fixture f=new Fixture(false)){f.sql("ALTER TABLE \"rivomesh_v3\".\"rm3_sink_repair\" DROP PRIMARY KEY");assertEquals("REPAIR_RECEIPT_PRIMARY_KEY_INVALID",assertThrows(IllegalStateException.class,f.sink::prepareRepair).getMessage());}
    }
    @Test void changedPlanResourceBudgetRequiresANewApprovalHash()throws Exception{
        try(Fixture f=new Fixture(false)){f.plan.maximumAffectedRows++;assertEquals("REPAIR_PLAN_HASH_OR_LIMIT_INVALID",assertThrows(IllegalArgumentException.class,f.plan::validate).getMessage());}
    }
    public static final class NoOpTrigger implements org.h2.api.Trigger {
        public void fire(Connection c,Object[] oldRow,Object[] newRow){} public void init(Connection c,String schema,String trigger,String table,boolean before,int type){}public void close(){}public void remove(){}
    }
    @Test void incomingCascadingForeignKeyIsRefusedBeforeAnyBusinessWrite()throws Exception{
        try(Fixture f=new Fixture(false)){String before=f.liveRoot();f.sql("CREATE TABLE \"sales_replica\".\"child\" (id BIGINT PRIMARY KEY,parent BIGINT REFERENCES \"sales_replica\".\"items\"(\"id\") ON DELETE CASCADE)");f.sql("INSERT INTO \"sales_replica\".\"child\" VALUES(1,1)");assertEquals("REPAIR_DEPENDENCY_PLAN_REQUIRED",assertThrows(IllegalStateException.class,f::repair).getMessage());assertEquals(before,f.liveRoot());assertEquals(1,JdbcSinkTest.count(f.settings,"child"));}
    }
    @Test void outgoingForeignKeyUsesRepairSpecificRefusalBeforeTheGeneralShapeGuard()throws Exception{
        try(Fixture f=new Fixture(false)){String before=f.liveRoot();f.sql("CREATE TABLE \"sales_replica\".\"parent\"(id BIGINT PRIMARY KEY)");f.sql("INSERT INTO \"sales_replica\".\"parent\" VALUES(1),(3)");f.sql("ALTER TABLE \"sales_replica\".\"items\" ADD CONSTRAINT repair_parent FOREIGN KEY(\"id\") REFERENCES \"sales_replica\".\"parent\"(id)");assertEquals("REPAIR_DEPENDENCY_PLAN_REQUIRED",assertThrows(IllegalStateException.class,f::repair).getMessage());assertNull(f.sink.repairReceipt(f.plan,f.plan.tables.values().iterator().next()));f.sql("ALTER TABLE \"sales_replica\".\"items\" DROP CONSTRAINT repair_parent");assertEquals(before,f.liveRoot());}
    }
    @Test void targetTriggerIsRefusedEvenWhenItsBodyHasNoVisibleSideEffect()throws Exception{
        try(Fixture f=new Fixture(false)){String before=f.liveRoot();f.sql("CREATE TRIGGER repair_trigger BEFORE DELETE ON \"sales_replica\".\"items\" FOR EACH ROW CALL '"+NoOpTrigger.class.getName()+"'");assertEquals("REPAIR_DEPENDENCY_PLAN_REQUIRED",assertThrows(IllegalStateException.class,f::repair).getMessage());assertEquals(before,f.liveRoot());}
    }
    @Test void pausedFenceBlocksResumeUntilReleasedAndRejectsAnOldCut()throws Exception{
        try(Fixture f=new Fixture(false)){
            ExecutorService pool=Executors.newSingleThreadExecutor();Future<?> resume;
            try(PausedRepairFence fence=new PausedRepairFence(f.journal,f.cut,30000)){resume=pool.submit(()->{try{f.journal.resumePipeline(f.cut.errorGeneration);}catch(Exception e){throw new RuntimeException(e);}});assertThrows(TimeoutException.class,()->resume.get(200,TimeUnit.MILLISECONDS));fence.verify();}
            try{resume.get(5,TimeUnit.SECONDS);assertEquals("RUNNING",f.journal.status().get("run_state"));assertThrows(IllegalStateException.class,()->new PausedRepairFence(f.journal,f.cut,30000));}finally{pool.shutdownNow();}
        }
    }
}
