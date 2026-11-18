package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.*;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Journal contracts use a synthetic native receipt; native SKIP execution is tested separately. */
class SkipJournalTest {
    @TempDir Path directory;
    CommitAcknowledgementTest setup;
    FailureCheckpointInspectionTest fixture;
    SkipPlan plan;
    @BeforeEach void prepare()throws Exception{
        setup=new CommitAcknowledgementTest();setup.directory=directory;setup.prepare();fixture=setup.fixture;setup.pauseAndRetain(true);
        plan=SkipPlan.create(fixture.retained,"skip_request_12345678","admin","Reviewed complete transaction;accept the explicit gap",fixture.normalized);
    }
    @AfterEach void close()throws Exception{if(setup!=null)setup.close();}
    void prepareIntent()throws Exception{
        JdbcSkipJournal.ensureMetadata(fixture.journal,fixture.retained.value);
        try(Connection c=fixture.journal.connect()){c.setAutoCommit(false);JdbcSkipJournal.prepare(fixture.journal,c,plan,fixture.retained.value);c.commit();}
    }
    SkipReceipt receipt(){SkipReceipt r=new SkipReceipt();r.operationId=plan.id;r.planHash=plan.hash();r.pipelineId=plan.pipelineId;r.subscriptionId=plan.subscriptionId;r.generation=plan.generation;r.fingerprint=plan.fingerprint;r.transactionId=plan.transactionId;r.projectedHash=plan.projectedHash;r.epoch=plan.epoch;r.sequence=plan.sequence;r.committedAt=Instant.now().toString();return r;}
    JdbcSkipJournal.Stored complete(SkipReceipt receipt)throws Exception{
        try(Connection c=fixture.journal.connect()){c.setAutoCommit(false);JdbcSkipJournal.Stored result=JdbcSkipJournal.complete(fixture.journal,c,plan,receipt,fixture.retained.value,SkipResolution.create(plan.id,"resolution_12345678","SKIP_COMMITTED","admin",plan.reason));c.commit();return result;}
    }
    static void code(String expected,org.junit.jupiter.api.function.Executable action){FailureCheckpointInspectionTest.code(expected,action);}
    long gaps()throws Exception{try(Connection c=fixture.journal.connect();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+fixture.journal.table("gap"))){assertTrue(r.next());return r.getLong(1);}}
    @Test void intentAndFormatUpgradeCommitTogetherAndBlockResumeWithoutAcknowledging()throws Exception{
        prepareIntent();JdbcJournal j=fixture.journal;assertEquals(4,j.status().get("dataFormat"));assertEquals(false,j.status().get("hasGaps"));assertEquals(0,j.subscriptions().get(0).acknowledged);
        code("SKIP_RECOVERY_PENDING",()->j.resumePipeline(j.pausePipeline().errorGeneration));code("SKIP_RECOVERY_PENDING",()->j.resume("target",1));
        try(Connection c=j.connect()){JdbcSkipJournal.Stored saved=JdbcSkipJournal.load(j,c,plan.id);assertEquals(plan.hash(),saved.hash);assertEquals("PREPARED",saved.state);}
    }
    @Test void rolledBackPrepareDoesNotUpgradeFormatOrLeaveAnIntent()throws Exception{
        JdbcSkipJournal.ensureMetadata(fixture.journal,fixture.retained.value);
        try(Connection c=fixture.journal.connect()){c.setAutoCommit(false);JdbcSkipJournal.prepare(fixture.journal,c,plan,fixture.retained.value);c.rollback();}
        assertEquals(3,fixture.journal.status().get("dataFormat"));try(Connection c=fixture.journal.connect()){assertNull(JdbcSkipJournal.load(fixture.journal,c,plan.id));}
    }
    private static List<String> indexColumns(JdbcJournal journal,String table,String index)throws SQLException{
        boolean mysql=journal.settings.driver.startsWith("com.mysql");
        TreeMap<Integer,String> columns=new TreeMap<>();
        try(Connection c=journal.connect();ResultSet rows=c.getMetaData().getIndexInfo(
                mysql?journal.settings.namespace:c.getCatalog(),mysql?null:journal.settings.namespace,"rm3_"+table,false,false)){
            while(rows.next()){
                String name=rows.getString("INDEX_NAME");
                if(name!=null&&name.toLowerCase(Locale.ROOT).startsWith(index))
                    columns.put(rows.getInt("ORDINAL_POSITION"),rows.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
            }
        }
        return List.copyOf(columns.values());
    }
    @Test void peerMetadataPreparationBuildsCollectorIndexesOnceAndKeepsThemAfterReopen()throws Exception{
        JdbcJournal original=fixture.journal;
        ExecutorService callers=Executors.newFixedThreadPool(2);
        try(JdbcJournal peer=new JdbcJournal(original.settings)){
            CountDownLatch start=new CountDownLatch(1);
            Future<?> first=callers.submit(()->{start.await();JdbcSkipJournal.ensureMetadata(original,fixture.retained.value);return null;});
            Future<?> second=callers.submit(()->{start.await();JdbcSkipJournal.ensureMetadata(peer,fixture.retained.value);return null;});
            start.countDown();first.get(30,TimeUnit.SECONDS);second.get(30,TimeUnit.SECONDS);
        }finally{callers.shutdownNow();callers.awaitTermination(5,TimeUnit.SECONDS);}
        prepareIntent();
        original.close();fixture.journal=new JdbcJournal(original.settings);
        assertEquals(List.of("pipeline_id","stream_seq","subscription_id","operation_id"),indexColumns(fixture.journal,"gap","rm3_gap_sequence"));
        assertEquals(List.of("pipeline_id","run_state","stream_seq","operation_id"),indexColumns(fixture.journal,"skip_plan","rm3_skip_plan_state_sequence"));
    }
    @Test void gapAckResolutionAndNewErrorGenerationCommitTogetherAndRemainPaused()throws Exception{
        prepareIntent();SkipReceipt receipt=receipt();JdbcSkipJournal.Stored stored=complete(receipt);assertEquals("COMPLETED",stored.state);assertEquals("SKIPPED",stored.receipt.outcome);assertEquals("admin",stored.resolution.actor);assertEquals(1,gaps());assertEquals(true,fixture.journal.status().get("hasGaps"));
        Journal.Subscription sub=fixture.journal.subscriptions().get(0);assertEquals(1,sub.acknowledged);assertEquals(2,sub.errorGeneration);assertEquals("PAUSED",sub.state);assertEquals("TRANSACTION_SKIPPED_DATA_GAP",sub.errorCode);assertEquals("PAUSED",fixture.journal.status().get("run_state"));
        assertEquals(receipt.hash(),complete(receipt).receipt.hash());assertEquals(1,gaps());
    }
    @Test void failedAckRollsBackGapAndResolutionAndKeepsThePendingPlan()throws Exception{
        prepareIntent();JdbcJournal j=fixture.journal;FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+" ADD CONSTRAINT reject_gap_ack CHECK (acknowledged=0)");
        assertThrows(SQLException.class,()->complete(receipt()));assertEquals(0,gaps());assertEquals(0,j.subscriptions().get(0).acknowledged);assertEquals(1,j.subscriptions().get(0).errorGeneration);
        try(Connection c=j.connect()){assertEquals("PREPARED",JdbcSkipJournal.load(j,c,plan.id).state);}code("SKIP_RECOVERY_PENDING",()->j.resumePipeline(j.pausePipeline().errorGeneration));
    }
    static String payload(JdbcJournal journal,String table,long sequence)throws Exception{
        try(Connection c=journal.connect();PreparedStatement query=c.prepareStatement("SELECT payload FROM "
                +journal.table(table)+" WHERE pipeline_id=? AND stream_seq=?")){
            query.setString(1,journal.settings.pipelineId);query.setLong(2,sequence);
            try(ResultSet rows=query.executeQuery()){return rows.next()?rows.getString(1):null;}
        }
    }
    private static boolean commitTimeRetained(JdbcJournal journal,long sequence)throws SQLException{
        try(Connection c=journal.connect();PreparedStatement query=c.prepareStatement("SELECT 1 FROM "
                +journal.table("source_commit_time")+" WHERE pipeline_id=? AND stream_seq=?")){
            query.setString(1,journal.settings.pipelineId);query.setLong(2,sequence);
            try(ResultSet rows=query.executeQuery()){return rows.next();}
        }
    }
    @Test void openGapPinsExactRawAndNormalizedBytesAcrossReopenWhileSiblingAndLaterEventsProgress()throws Exception{
        prepareIntent();
        JdbcJournal original=fixture.journal;
        FailureReviewTest.sql(original,"INSERT INTO "+original.table("subscription")
                +" (pipeline_id,subscription_id,acknowledged,run_state,error_generation) VALUES (?,'sibling',0,'RUNNING',0)",plan.pipelineId);
        SkipReceipt receipt=receipt();complete(receipt);
        long retainedBytes=(long)original.status().get("pendingBytes");
        JdbcJournal.Settings settings=original.settings;
        settings.deliveryTiming=true;
        try(Connection c=original.connect()){
            JdbcDeliveryTiming.initialize(original,c,settings.driver.startsWith("com.mysql"));
            JdbcDeliveryTiming.capture(original,c,1,Instant.parse("2026-11-17T08:06:14Z"));
        }
        original.resumePipeline(original.pausePipeline().errorGeneration);
        original.close();
        JdbcJournal j=new JdbcJournal(settings);fixture.journal=j;
        Journal.Lease owner=j.claim("new-owner",30000);j.resume("target",2);
        assertEquals(1,j.subscriptions().stream().filter(sub->"target".equals(sub.id)).findFirst().orElseThrow().acknowledged);
        assertEquals(0,j.subscriptions().stream().filter(sub->"sibling".equals(sub.id)).findFirst().orElseThrow().acknowledged);
        assertEquals(0,j.collect(owner,100));
        assertEquals(Codec.encode(fixture.retained.value.review.raw),Codec.encode(j.read("sibling",1).getFirst().transaction));
        j.acknowledge(owner,"sibling",1);

        SourceTransaction next=JdbcSinkTest.transaction(2,JdbcSinkTest.insert(fixture.table,2,"later-value"));
        next.sourceCommittedAt=Instant.parse("2026-11-17T08:06:15Z");
        assertEquals(2,j.append(owner,next));
        Journal.Record record=j.read("sibling",1).getFirst();
        j.normalize(owner,record,Map.of(fixture.table.id,fixture.table),Map.of(fixture.table.id,next.position),(position,id)->false);
        j.acknowledge(owner,"target",2);
        assertEquals(0,j.collect(owner,1));
        j.acknowledge(owner,"sibling",2);
        assertEquals(1,j.collect(owner,1));
        assertNull(payload(j,"log",2));assertNull(payload(j,"normalized",2));
        assertTrue(commitTimeRetained(j,1));assertFalse(commitTimeRetained(j,2));
        assertEquals(Codec.encode(fixture.retained.value.review.raw),payload(j,"log",1));
        assertEquals(Codec.encode(fixture.normalized),payload(j,"normalized",1));
        assertEquals(retainedBytes,j.status().get("pendingBytes"));
        assertEquals(receipt.hash(),complete(receipt).receipt.hash());assertEquals(1,gaps());

        settings.maxPendingBytes=retainedBytes;
        SourceTransaction blocked=JdbcSinkTest.transaction(3,JdbcSinkTest.insert(fixture.table,3,"quota-value"));
        assertThrows(JdbcJournal.Capacity.class,()->j.append(owner,blocked));
        assertEquals(next.position,j.captured());
    }
    @Test void cancelledIntentKeepsOriginalFailureButInvalidatesOldErrorGeneration()throws Exception{
        prepareIntent();JdbcJournal j=fixture.journal;try(Connection c=j.connect()){c.setAutoCommit(false);JdbcSkipJournal.Stored cancelled=JdbcSkipJournal.cancel(j,c,plan,fixture.retained.value,SkipResolution.create(plan.id,"cancel_request_1234","CANCEL","another-admin","Native absence was verified before cancellation"));c.commit();assertEquals("CANCELLED",cancelled.state);}
        assertEquals(0,gaps());assertEquals(0,j.subscriptions().get(0).acknowledged);assertEquals(2,j.subscriptions().get(0).errorGeneration);assertEquals(SubscriberFailure.Phase.TARGET_APPLY,j.inspectFailure("target",2,100000).failure.phase);
        code("ERROR_GENERATION_CHANGED",()->j.resume("target",1));j.resumePipeline(j.pausePipeline().errorGeneration);j.resume("target",2);assertNotNull(j.claim("new-owner",30000));
    }
    @Test void missingFormatFourHistoryIsNeverRecreatedByInitialization()throws Exception{
        prepareIntent();JdbcJournal j=fixture.journal;FailureReviewTest.sql(j,"DROP TABLE "+j.table("gap"));code("SKIP_METADATA_LOST",()->j.verifyIdentity(plan.fingerprint,java.util.Collections.singletonList("target")));code("SKIP_METADATA_LOST",()->j.initialize(plan.fingerprint,plan.generation,java.util.Collections.singletonList("target")));
        try(Connection c=j.connect();Statement s=c.createStatement()){assertThrows(SQLException.class,()->s.executeQuery("SELECT COUNT(*) FROM "+j.table("gap")));}
    }
    @Test void ordinaryCheckpointJsonDoesNotAcquireNewNullFields()throws Exception{
        SinkAdapter.Checkpoint cp=new SinkAdapter.Checkpoint();cp.epoch=2;cp.sequence=1;cp.transactionId="tx";cp.transactionHash=Codec.sha256("tx");String encoded=Codec.encode(cp);assertFalse(encoded.contains("skipOperation"));assertFalse(encoded.contains("skipHash"));cp.skipOperation=plan.id;cp.skipHash=receipt().hash();assertTrue(Codec.encode(cp).contains("skipOperation"));
    }
}
