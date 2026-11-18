package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class FanoutDispatcherTest {
    @TempDir Path directory;
    static final class FaultSink implements SinkAdapter {
        final SinkAdapter sink;boolean sqlFailure,uncertainCommit,activationFailure;long failSequence;
        FaultSink(SinkAdapter sink){this.sink=sink;}
        public String id(){return sink.id();}public boolean includes(TableSchema t){return sink.includes(t);}public Consistency consistency(){return sink.consistency();}
        public void prepare(long e,TableSchema t)throws Exception{sink.prepare(e,t);}public Checkpoint activate(long e)throws Exception{if(activationFailure)throw new SQLException("test activation fault","23514");return sink.activate(e);}public Checkpoint checkpoint()throws Exception{return sink.checkpoint();}
        public void apply(long e,long s,SourceTransaction tx)throws Exception{if(sqlFailure||s==failSequence)throw new SQLException("test fault","23514");sink.apply(e,s,tx);if(uncertainCommit){uncertainCommit=false;throw new java.io.IOException("reply lost after commit");}}
        public SnapshotCheckpoint beginSnapshot(long e,TableSchema t,Journal.SnapshotState s)throws Exception{return sink.beginSnapshot(e,t,s);}public void snapshotPart(long e,TableSchema t,String id,Journal.SnapshotPart p)throws Exception{sink.snapshotPart(e,t,id,p);}public void finishSnapshot(long e,TableSchema t,Journal.SnapshotState s)throws Exception{sink.finishSnapshot(e,t,s);}public Map<String,Object> diagnostics()throws Exception{return sink.diagnostics();}public void close()throws Exception{sink.close();}
    }
    JdbcSink.Settings target(String id){JdbcSink.Settings s=new JdbcSink.Settings();s.driver="org.h2.Driver";s.product=TargetProduct.H2;s.pipelineId="pipeline";s.generation="generation";s.fingerprint=Codec.sha256("config");s.databases.put("sales","sales_replica");s.url="jdbc:h2:file:"+directory.resolve(id)+";WRITE_DELAY=0";s.id=id;return s;}
    JdbcJournal.Settings journal(){JdbcJournal.Settings s=new JdbcJournal.Settings();s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0";s.pipelineId="pipeline";return s;}
    @Test void sqlFaultIsIsolatedAndAnUncertainCommitIsReconciledBeforeReplay()throws Exception{
        JdbcSink.Settings a=target("fast"),b=target("slow");TableSchema table=JdbcSinkTest.schema(false);Map<String,TableSchema> schemas=Collections.singletonMap(table.id,table);
        try(JdbcJournal journal=new JdbcJournal(journal());JdbcSink fast=new JdbcSink(a);FaultSink slow=new FaultSink(new JdbcSink(b))){
            new JournalTest().initialize(journal);Journal.Lease lease=journal.claim("owner",30000);fast.activate(lease.epoch);slow.activate(lease.epoch);JdbcSinkTest.emptySnapshot(fast,lease.epoch,table);JdbcSinkTest.emptySnapshot((JdbcSink)slow.sink,lease.epoch,table);
            journal.append(lease,JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"first")));journal.append(lease,JdbcSinkTest.transaction(2,JdbcSinkTest.insert(table,2,"second")));
            slow.sqlFailure=true;FanoutDispatcher dispatcher=new FanoutDispatcher(journal,lease,Arrays.asList(slow,fast),schemas,r->r.transaction);assertEquals(2,dispatcher.drain(10));assertEquals("PAUSED",dispatcher.states().get("slow"));assertEquals(2,JdbcSinkTest.count(a,"items"));assertEquals(0,JdbcSinkTest.count(b,"items"));assertEquals(0,journal.collect(lease,100));
            slow.sqlFailure=false;slow.uncertainCommit=true;journal.resume("slow",1);dispatcher.drain(10);assertEquals(1,slow.checkpoint().sequence);assertEquals(1,JdbcSinkTest.count(b,"items"));assertEquals(0,journal.subscriptions().stream().filter(s->s.id.equals("slow")).findFirst().get().acknowledged);
            Journal.Subscription retrying=journal.subscriptions().stream().filter(s->s.id.equals("slow")).findFirst().get();assertEquals("RUNNING",retrying.state);assertEquals("TARGET_CONNECTION_RETRY",retrying.errorCode);assertEquals(1,retrying.errorGeneration);
            FanoutDispatcher restarted=new FanoutDispatcher(journal,lease,Arrays.asList(fast,slow),schemas,r->r.transaction);assertEquals(2,restarted.drain(10));assertEquals(2,JdbcSinkTest.count(b,"items"));assertEquals(2,journal.collect(lease,100));assertNull(journal.subscriptions().stream().filter(s->s.id.equals("slow")).findFirst().get().errorCode);
        }
    }
    @Test void sourceNormalizationFaultStopsAllSubscribersWithoutMislabelingTargetErrors()throws Exception{
        JdbcSink.Settings a=target("fast");TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal journal=new JdbcJournal(journal());JdbcSink fast=new JdbcSink(a)){
            journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("fast"));Journal.Lease lease=journal.claim("owner",30000);journal.append(lease,JdbcSinkTest.transaction(1));
            FanoutDispatcher dispatcher=new FanoutDispatcher(journal,lease,Collections.singletonList(fast),Collections.singletonMap(table.id,table),r->{throw new IllegalStateException("BEFORE_IMAGE_OCCURRENCE_MISSING");});
            FanoutDispatcher.NormalizationFailure failure=assertThrows(FanoutDispatcher.NormalizationFailure.class,()->dispatcher.drain(10));
            assertEquals(PipelineFailure.Phase.NORMALIZE,failure.phase);
            assertEquals("RUNNING",journal.subscriptions().get(0).state);assertEquals(0,fast.checkpoint().sequence);
        }
    }
    @Test void failedJournalAcknowledgmentIsDistinctFromTheConfirmedTargetWrite()throws Exception{
        JdbcSink.Settings settings=target("fast");
        JdbcJournal.Settings store=journal();
        store.deliveryTiming=true;
        TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal journal=new JdbcJournal(store);JdbcSink sink=new JdbcSink(settings)){
            journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("fast"));
            Journal.Lease lease=journal.claim("owner",30000);
            sink.activate(lease.epoch);
            JdbcSinkTest.emptySnapshot(sink,lease.epoch,table);
            SourceTransaction transaction=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"native commit"));
            transaction.position=new SourcePosition(1,"POSTGRESQL","source",Map.of("lsn","0/7"));
            transaction.sourceCommittedAt=Instant.parse("2024-01-02T03:04:05.123456Z");
            journal.append(lease,transaction);
            Journal failing=(Journal)Proxy.newProxyInstance(Journal.class.getClassLoader(),new Class<?>[]{Journal.class},
                    (proxy,method,args)->{
                        if("acknowledgeApplied".equals(method.getName()))throw new SQLException("fixture storage failure","58030");
                        try{return method.invoke(journal,args);}
                        catch(InvocationTargetException failure){throw failure.getCause();}
                    });
            FanoutDispatcher dispatcher=new FanoutDispatcher(failing,lease,Collections.singletonList(sink),
                    Collections.singletonMap(table.id,table),record->record.transaction);
            FanoutDispatcher.JournalFailure failure=assertThrows(FanoutDispatcher.JournalFailure.class,()->dispatcher.drain(1));
            assertEquals(PipelineFailure.Phase.JOURNAL_ACK,failure.phase);
            assertEquals(1,sink.checkpoint().sequence);
            assertEquals(0,journal.subscriptions().get(0).acknowledged);
            assertNull(journal.subscriptions().get(0).errorCode);
            FanoutDispatcher reconciler=new FanoutDispatcher(journal,lease,Collections.singletonList(sink),
                    Collections.singletonMap(table.id,table),record->record.transaction);
            assertEquals(1,reconciler.drain(1));
            assertEquals(1,journal.subscriptions().getFirst().acknowledged);
            Map<?,?> progress=(Map<?,?>)journal.deliveryProgress().get("subscriptions");
            assertEquals(Map.of("state","UNKNOWN"),((Map<?,?>)progress.get("fast")).get("lastDeliveryTiming"));
        }
    }
    @Test void activationAndSecondTransactionApplyFaultsHaveDistinctDurableLocations()throws Exception{
        JdbcSink.Settings settings=target("fast");TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal journal=new JdbcJournal(journal());FaultSink sink=new FaultSink(new JdbcSink(settings))){
            journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("fast"));Journal.Lease lease=journal.claim("owner",30000);
            sink.activate(lease.epoch);JdbcSinkTest.emptySnapshot((JdbcSink)sink.sink,lease.epoch,table);
            journal.append(lease,JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"first")));journal.append(lease,JdbcSinkTest.transaction(2,JdbcSinkTest.insert(table,2,"second")));
            FanoutDispatcher dispatcher=new FanoutDispatcher(journal,lease,Collections.singletonList(sink),Collections.singletonMap(table.id,table),r->r.transaction);
            sink.activationFailure=true;dispatcher.drain(10);Journal.FailureReview activation=journal.inspectFailure("fast",1,100000);
            assertEquals(SubscriberFailure.Phase.TARGET_ACTIVATE,activation.failure.phase);assertEquals(0,activation.failure.sequence);assertNull(activation.failure.transactionId);
            journal.resume("fast",1);sink.activationFailure=false;sink.failSequence=2;assertEquals(1,dispatcher.drain(10));
            Journal.FailureReview apply=journal.inspectFailure("fast",2,100000);assertEquals(2,apply.sequence);assertEquals(1,apply.subscription.acknowledged);
            assertEquals(SubscriberFailure.Phase.TARGET_APPLY,apply.failure.phase);assertEquals(apply.raw.id,apply.failure.transactionId);assertEquals(Codec.sha256(Codec.encode(apply.raw)),apply.failure.projectedHash);
            assertEquals(1,JdbcSinkTest.count(settings,"items"));assertEquals(1,sink.checkpoint().sequence);
        }
    }
}
