package io.github.rivomesh.cdc;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class FailureReviewTest {
    @TempDir Path directory;
    private JdbcJournal.Settings external;
    JdbcJournal.Settings settings(){
        if(external!=null)return external;
        JdbcJournal.Settings s=new JdbcJournal.Settings();s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0;LOCK_TIMEOUT=5000";s.pipelineId="pipeline";
        if(System.getenv("RIVOMESH_FAILURE_DB_URL")!=null){
            s.driver=System.getenv("RIVOMESH_FAILURE_DB_DRIVER");s.url=System.getenv("RIVOMESH_FAILURE_DB_URL");s.username=System.getenv("RIVOMESH_FAILURE_DB_USER");s.password=System.getenv("RIVOMESH_FAILURE_DB_PASSWORD");
            s.namespace="failure_"+UUID.randomUUID().toString().replace("-","");external=s;
        }return s;
    }
    @AfterEach void cleanupExternalNamespace()throws Exception{
        if(external==null)return;
        try(Connection c=io.github.rivomesh.JdbcConnections.open(external.driver,external.url,external.username,external.password);Statement statement=c.createStatement()){
            statement.execute(external.driver.startsWith("com.mysql")?"DROP DATABASE IF EXISTS `"+external.namespace+"`":"DROP SCHEMA IF EXISTS \""+external.namespace+"\" CASCADE");
        }
    }
    static SourceTransaction tx(int ordinal){return new JournalTest().transaction(ordinal);}
    static Journal.Lease prepare(JdbcJournal j)throws Exception{new JournalTest().initialize(j);Journal.Lease lease=j.claim("owner",30000);j.append(lease,tx(1));return lease;}
    static SubscriberFailure failure(){SubscriberFailure f=new SubscriberFailure();f.phase=SubscriberFailure.Phase.TARGET_APPLY;f.sequence=1;f.transactionId=tx(1).id;f.projectedHash=Codec.sha256("projected transaction");return f;}
    static void code(String expected,org.junit.jupiter.api.function.Executable action){assertEquals(expected,assertThrows(IllegalStateException.class,action).getMessage());}
    static void sql(JdbcJournal j,String statement,Object...args)throws Exception{try(Connection c=j.connect()){JdbcJournal.execute(c,statement,args);}}
    static void normalize(JdbcJournal j,Journal.Lease lease)throws Exception{
        TableSchema schema=new JournalTest().schema();schema.columns.clear();
        for(int i=0;i<5;i++){TableSchema.Column column=new TableSchema.Column();column.name="c"+i;column.type=TableSchema.Type.STRING;schema.columns.add(column);}schema.primaryKey.add("c0");
        // Included in the snapshot baseline: normalized transaction is deliberately empty.
        j.normalize(lease,j.read("slow",1).get(0),Collections.singletonMap(schema.id,schema),Collections.singletonMap(schema.id,tx(1).position),(position,id)->true);
    }
    @Test void restartRetainsAtomicFailureAndReadsWithoutAdvancingOrNormalizing()throws Exception{
        JdbcJournal.Settings settings=settings();long failureEpoch;
        try(JdbcJournal j=new JdbcJournal(settings)){
            Journal.Lease lease=prepare(j);failureEpoch=lease.epoch;normalize(j,lease);
            j.pauseFailure(lease,"slow","SQL_23514",failure());j.pause(lease,"slow","OTHER_ERROR");
            j.acknowledge(lease,"fast",1);assertEquals(0,j.collect(lease,100));j.release(lease);
        }
        try(JdbcJournal j=new JdbcJournal(settings)){
            j.verifyIdentity(Codec.sha256("config"),Arrays.asList("fast","slow"));
            String status=Codec.encode(j.status());Journal.FailureReview review=j.inspectFailure("slow",1,100000);
            assertEquals(Codec.sha256("config"),review.fingerprint);assertEquals("generation",review.generation);
            assertEquals("pipeline",review.pipelineId);assertEquals(1,review.sequence);assertEquals(0,review.subscription.acknowledged);
            assertEquals("SQL_23514",review.subscription.errorCode);assertEquals(failureEpoch,review.failureEpoch);
            assertEquals(SubscriberFailure.Phase.TARGET_APPLY,review.failure.phase);assertEquals(failure().projectedHash,review.failure.projectedHash);
            assertEquals(Codec.encode(tx(1)),Codec.encode(review.raw));assertEquals(Codec.sha256(Codec.encode(tx(1))),review.rawHash);
            assertNotNull(review.normalizedHash);assertTrue(review.normalized.changes.isEmpty());
            assertEquals(status,Codec.encode(j.status()));assertTrue(j.read("slow",1).isEmpty());
            review.raw.changes.clear();assertFalse(j.inspectFailure("slow",1,100000).raw.changes.isEmpty(),"Caller cannot mutate retained journal content");
        }
    }
    @Test void stateGenerationMissingHeadAndLegacyDiagnosticsNeverGrantPermission()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);code("FAILURE_REVIEW_REQUIRES_PAUSED",()->j.inspectFailure("slow",0,100000));
            j.pause(lease,"slow","SQL_ERROR");assertEquals(SubscriberFailure.Phase.UNKNOWN,j.inspectFailure("slow",1,100000).failure.phase);
            code("ERROR_GENERATION_CHANGED",()->j.inspectFailure("slow",0,100000));
            j.resume("slow",1);j.acknowledge(lease,"slow",1);j.pause(lease,"slow","ACTIVATION_ERROR");
            code("FAILURE_TRANSACTION_UNAVAILABLE",()->j.inspectFailure("slow",2,100000));
            assertEquals(1,j.subscriptions().stream().filter(s->s.id.equals("slow")).findFirst().get().acknowledged);
        }
    }
    @Test void snapshotOrPreparationFaultDoesNotIdentifyTheNextIncrementalTransaction()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);SubscriberFailure f=new SubscriberFailure();f.phase=SubscriberFailure.Phase.TARGET_PREPARE;
            j.pauseFailure(lease,"slow","SQL_ERROR",f);code("FAILURE_REVIEW_REQUIRES_INCREMENTAL_FAILURE",()->j.inspectFailure("slow",1,100000));
            j.resume("slow",1);f.phase=SubscriberFailure.Phase.TARGET_SNAPSHOT;j.pauseFailure(lease,"slow","SQL_ERROR",f);
            code("FAILURE_REVIEW_REQUIRES_INCREMENTAL_FAILURE",()->j.inspectFailure("slow",2,100000));assertEquals(0,j.subscriptions().get(1).acknowledged);
        }
    }
    @Test void rawNormalizedAndDiagnosticCorruptionAreDistinctRefusals()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);normalize(j,lease);j.pauseFailure(lease,"slow","SQL_ERROR",failure());
            String raw=Codec.encode(tx(1)),hash=Codec.sha256(raw);
            sql(j,"UPDATE "+j.table("log")+" SET payload_hash=?",Codec.sha256("tampered"));
            code("JOURNAL_CHECKSUM_MISMATCH",()->j.inspectFailure("slow",1,100000));sql(j,"UPDATE "+j.table("log")+" SET payload_hash=?",hash);
            sql(j,"UPDATE "+j.table("normalized")+" SET raw_hash=?",Codec.sha256("another raw transaction"));
            code("NORMALIZED_TRANSACTION_CHECKSUM_MISMATCH",()->j.inspectFailure("slow",1,100000));sql(j,"UPDATE "+j.table("normalized")+" SET raw_hash=?",hash);
            sql(j,"UPDATE "+j.table("failure")+" SET payload_hash=?",Codec.sha256("tampered"));
            code("FAILURE_DIAGNOSTIC_CHECKSUM_MISMATCH",()->j.inspectFailure("slow",1,100000));
            assertEquals(0,j.subscriptions().get(1).acknowledged);assertEquals(1,j.subscriptions().get(1).errorGeneration);
        }
    }
    @Test void statusDiagnosticsContainNoTransactionValuesAndCorruptionRemainsVisible()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);j.pauseFailure(lease,"slow","SQL_23514",failure());
            List<Map<String,Object>> summaries=j.subscriptionDiagnostics();Map<String,Object> slow=summaries.get(1);
            assertEquals("TARGET_APPLY",slow.get("failurePhase"));assertEquals(1L,slow.get("failedSequence"));assertEquals(lease.epoch,slow.get("failureEpoch"));
            String safe=Codec.encode(summaries);assertFalse(safe.contains(tx(1).id));assertFalse(safe.contains("18446744073709551615"));assertFalse(safe.contains("projectedHash"));assertFalse(safe.contains("before"));assertFalse(safe.contains("after"));
            sql(j,"UPDATE "+j.table("failure")+" SET payload_hash=?",Codec.sha256("tampered"));
            slow=j.subscriptionDiagnostics().get(1);assertEquals("UNKNOWN",slow.get("failurePhase"));assertEquals("FAILURE_DIAGNOSTIC_UNAVAILABLE",slow.get("failureDiagnosticProblem"));assertFalse(slow.containsKey("failedSequence"));
            j.resume("slow",1);assertFalse(j.subscriptionDiagnostics().get(1).containsKey("failurePhase"));
        }
    }
    @Test void forgedLengthsAndSharedRawNormalizedBudgetCannotBypassBoundedRead()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);normalize(j,lease);j.pauseFailure(lease,"slow","SQL_ERROR",failure());
            long rawBytes=Codec.encode(tx(1)).getBytes(StandardCharsets.UTF_8).length;
            code("FAILURE_REVIEW_TOO_LARGE",()->j.inspectFailure("slow",1,rawBytes));
            sql(j,"UPDATE "+j.table("log")+" SET payload_bytes=0");sql(j,"UPDATE "+j.table("normalized")+" SET payload_bytes=0");
            code("FAILURE_REVIEW_TOO_LARGE",()->j.inspectFailure("slow",1,10));
            code("FAILURE_REVIEW_TOO_LARGE",()->j.inspectFailure("slow",1,rawBytes));
            assertThrows(IllegalArgumentException.class,()->j.inspectFailure("slow",1,JdbcFailureReview.MAXIMUM_BYTES+1));
            assertThrows(IllegalArgumentException.class,()->j.inspectFailure("slow",1,0));
            assertEquals(0,j.subscriptions().get(1).acknowledged);
        }
    }
    @Test void missingHistoryAndMislocatedDiagnosticRefuseWithoutAcknowledge()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);SubscriberFailure f=failure();f.sequence=2;j.pauseFailure(lease,"slow","SQL_ERROR",f);
            code("FAILURE_LOCATION_CHANGED",()->j.inspectFailure("slow",1,100000));
            sql(j,"DELETE FROM "+j.table("log"));code("JOURNAL_SEQUENCE_GAP",()->j.inspectFailure("slow",1,100000));
            assertEquals(0,j.subscriptions().get(1).acknowledged);
        }
    }
    @Test void failedDiagnosticPersistenceRollsBackPauseAndErrorGeneration()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);
            sql(j,"ALTER TABLE "+j.table("failure")+" ADD CONSTRAINT reject_diagnostic CHECK (payload_hash='invalid')");
            assertThrows(SQLException.class,()->j.pauseFailure(lease,"slow","SQL_ERROR",failure()));
            Journal.Subscription sub=j.subscriptions().get(1);assertEquals("RUNNING",sub.state);assertEquals(0,sub.errorGeneration);assertNull(sub.errorCode);
        }
    }
    @Test void missingDiagnosticTableAndOlderWriterNewGenerationRemainUnknown()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);j.pauseFailure(lease,"slow","SQL_ERROR",failure());
            sql(j,"UPDATE "+j.table("subscription")+" SET error_generation=2 WHERE subscription_id='slow'");
            assertEquals(SubscriberFailure.Phase.UNKNOWN,j.inspectFailure("slow",2,100000).failure.phase);
            sql(j,"DROP TABLE "+j.table("failure"));assertEquals(SubscriberFailure.Phase.UNKNOWN,j.inspectFailure("slow",2,100000).failure.phase);
        }
    }
    @Test void concurrentResumeAcknowledgeAndCollectionCannotMixInspectionStates()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){
            Journal.Lease lease=prepare(j);j.pauseFailure(lease,"slow","SQL_ERROR",failure());j.acknowledge(lease,"fast",1);
            ExecutorService pool=Executors.newFixedThreadPool(2);CountDownLatch start=new CountDownLatch(1);
            try{
                Future<?> read=pool.submit(()->{try{start.await();Journal.FailureReview r=j.inspectFailure("slow",1,100000);assertEquals("PAUSED",r.subscription.state);assertEquals(0,r.subscription.acknowledged);assertEquals(tx(1).id,r.raw.id);}catch(IllegalStateException expected){assertEquals("FAILURE_REVIEW_REQUIRES_PAUSED",expected.getMessage());}catch(Exception failure){throw new RuntimeException(failure);}});
                Future<?> mutate=pool.submit(()->{try{start.await();j.resume("slow",1);j.acknowledge(lease,"slow",1);j.collect(lease,10);}catch(Exception failure){throw new RuntimeException(failure);}});
                start.countDown();read.get(10,TimeUnit.SECONDS);mutate.get(10,TimeUnit.SECONDS);assertEquals(1,j.subscriptions().get(1).acknowledged);
            }finally{pool.shutdownNow();}
        }
    }
}
