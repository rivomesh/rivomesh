package io.github.rivomesh.cdc;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class KafkaHistoryOriginTest {
    @TempDir Path directory;
    private static final String TASK="pipeline",TARGET="fast",PEER="slow",FINGERPRINT=Codec.sha256("fixture");
    private static final String KEY=Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
    private static final List<String> SUBSCRIBERS=List.of(TARGET,PEER);

    @Test void aLegacyJobCannotAcquireAnOriginEvenWithTheSameGeneration()throws Exception{
        JdbcJournal.Settings settings=settings();
        KafkaPhysicalReceipt.Grant grant=newGrant();
        try(JdbcJournal legacy=new JdbcJournal(settings)){
            legacy.initialize(FINGERPRINT,grant.historyOrigin().generation(),SUBSCRIBERS);
        }
        settings.kafkaPhysicalGrant=grant;
        try(JdbcJournal enrolled=new JdbcJournal(settings)){
            assertEquals("KAFKA_HISTORY_ORIGIN_MISSING_OR_CHANGED",assertThrows(IllegalStateException.class,
                    ()->enrolled.prepareKafkaHistoryOrigin(FINGERPRINT,SUBSCRIBERS,true)).getMessage());
            assertEquals(0,rows(enrolled,"kafka_history_origin"));
            assertEquals(0,rows(enrolled,"kafka_history_first_send"));
            assertEquals(0,enrolled.subscriptions().getFirst().acknowledged);
        }
    }

    @Test void noPartialAnchorOrJobWhenAnyInitialTargetEvidenceCannotBeWritten()throws Exception{
        JdbcJournal.Settings settings=settings();
        settings.kafkaPhysicalGrant=newGrant();
        try(JdbcJournal journal=new JdbcJournal(settings)){
            try(Connection c=journal.connect()){journal.createStorage(c);}
            FailureReviewTest.sql(journal,"ALTER TABLE "+journal.table("kafka_history_first_send")+" ADD CONSTRAINT "
                    +"refuse_fixture CHECK (subscription_id <> 'fast')");
            assertThrows(java.sql.SQLException.class,()->journal.prepareKafkaHistoryOrigin(FINGERPRINT,SUBSCRIBERS,true));
            assertEquals(0,rows(journal,"job"));assertEquals(0,rows(journal,"subscription"));
            assertEquals(0,rows(journal,"kafka_history_origin"));
            FailureReviewTest.sql(journal,"ALTER TABLE "+journal.table("kafka_history_first_send")+" DROP CONSTRAINT refuse_fixture");
            journal.prepareKafkaHistoryOrigin(FINGERPRINT,SUBSCRIBERS,true);
            assertEquals(1,rows(journal,"kafka_history_origin"));
            assertEquals(1,rows(journal,"kafka_history_first_send"));
            assertEquals(2,rows(journal,"subscription"));
        }
    }

    @Test void firstWireIntentCommitsBeforeSendSurvivesLostReplyAndRemainsUnknownAfterReplay()throws Exception{
        JdbcJournal.Settings settings=settings();
        settings.kafkaPhysicalGrant=newGrant();
        var table=JdbcSinkTest.schema(false);
        var broker=new CheckingBroker();
        String first;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            journal.prepareKafkaHistoryOrigin(FINGERPRINT,SUBSCRIBERS,true);
            broker.journal=journal;
            var lease=journal.claim("first",30000);
            try(MessageSink sink=sink(journal,lease,broker)){
                sink.activate(lease.epoch);
                broker.loseFirst=true;
                assertThrows(IOException.class,()->sink.prepare(lease.epoch,table));
                assertEquals(0,sink.checkpoint().sequence);
                assertFalse(new JdbcPublicationStore(journal,TARGET,lease).schemaPrepared(table));
                first=payload(journal);
                assertFalse(first.contains(table.name));
                var intent=Codec.JSON.readTree(first);
                assertEquals("UNKNOWN",intent.path("result").asText());
                assertEquals("SCHEMA",intent.path("kinds").get(0).asText());
                assertEquals(settings.kafkaPhysicalGrant.scope().configurationVersion(),intent.path("scope").path("configurationVersion").asText());
                assertEquals(lease.epoch,intent.path("epoch").asLong());
                assertTrue(intent.path("wireSha256").asText().matches("[a-f0-9]{64}"));
            }
            journal.release(lease);
        }

        var origin=settings.kafkaPhysicalGrant.historyOrigin();
        settings.kafkaPhysicalGrant=grant(origin,uuid(),2);
        try(JdbcJournal reopened=new JdbcJournal(settings)){
            reopened.initialize(FINGERPRINT,null,SUBSCRIBERS);
            broker.journal=reopened;
            var lease=reopened.claim("restart",30000);
            try(MessageSink sink=sink(reopened,lease,broker)){
                sink.activate(lease.epoch);
                sink.prepare(lease.epoch,table);
                var transaction=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,2,"incremental-secret"));
                transaction.streamGeneration=origin.generation();
                var snapshot=reopened.beginSnapshot(lease,"source",table,transaction.position);
                reopened.appendSnapshot(lease,snapshot,0,List.of(JdbcSinkTest.row(1,"snapshot-secret")));
                reopened.finishSnapshot(lease,snapshot);
                sink.beginSnapshot(lease.epoch,table,snapshot);
                sink.snapshotPart(lease.epoch,table,snapshot.snapshotId,reopened.snapshotParts("source",table.id,snapshot.snapshotId,0,1).getFirst());
                sink.finishSnapshot(lease.epoch,table,snapshot);
                sink.apply(lease.epoch,1,transaction);
                assertEquals(1,sink.checkpoint().sequence);
            }
            assertEquals(List.of("SCHEMA","SCHEMA","SNAPSHOT_BEGIN","SNAPSHOT_ROW","SNAPSHOT_END","BEGIN","ROW","COMMIT"),broker.kinds);
            assertEquals(first,payload(reopened));
            assertEquals(1,rows(reopened,"kafka_history_first_send"));
            assertTrue(reopened.subscriptions().stream().allMatch(s->s.acknowledged==0));
        }
    }

    @Test void missingOrModifiedIntentStopsTheActualSendAndLeavesAllProgressUnchanged()throws Exception{
        JdbcJournal.Settings settings=settings();
        settings.kafkaPhysicalGrant=newGrant();
        try(JdbcJournal journal=new JdbcJournal(settings)){
            journal.prepareKafkaHistoryOrigin(FINGERPRINT,SUBSCRIBERS,true);
            var lease=journal.claim("owner",30000);
            var broker=new CheckingBroker();
            broker.journal=journal;
            try(MessageSink sink=sink(journal,lease,broker)){
                sink.activate(lease.epoch);
                var table=JdbcSinkTest.schema(false);
                FailureReviewTest.sql(journal,"UPDATE "+journal.table("kafka_history_first_send")+" SET payload='unverified'");
                assertEquals("KAFKA_HISTORY_ORIGIN_MISSING_OR_CHANGED",assertThrows(IllegalStateException.class,
                        ()->sink.prepare(lease.epoch,table)).getMessage());
                FailureReviewTest.sql(journal,"DELETE FROM "+journal.table("kafka_history_first_send"));
                assertEquals("KAFKA_HISTORY_ORIGIN_MISSING_OR_CHANGED",assertThrows(IllegalStateException.class,
                        ()->sink.prepare(lease.epoch,table)).getMessage());
                assertEquals(0,rows(journal,"kafka_history_first_send"));
                assertTrue(broker.kinds.isEmpty());
                assertEquals(0,sink.checkpoint().sequence);
                assertTrue(journal.subscriptions().stream().allMatch(s->s.acknowledged==0));
            }
        }
    }

    @Test void revocationDuringTheSendWindowKeepsUnknownAndPausingOneKafkaTargetLeavesItsPeerIndependent()throws Exception{
        JdbcJournal.Settings settings=settings();
        settings.kafkaPhysicalGrant=newGrant();
        settings.kafkaPublicationTargets=Set.of(TARGET,PEER);
        var table=JdbcSinkTest.schema(false);
        try(JdbcJournal journal=new JdbcJournal(settings)){
            journal.prepareKafkaHistoryOrigin(FINGERPRINT,SUBSCRIBERS,true);
            var original=journal.claim("revocable",30000);
            var targetBroker=new CheckingBroker();
            targetBroker.journal=journal;
            targetBroker.afterJournalCommit=()->{journal.release(original);return null;};
            try(MessageSink target=sink(journal,original,targetBroker)){
                target.activate(original.epoch);
                assertThrows(JdbcJournal.Fenced.class,()->target.prepare(original.epoch,table));
                // A broker may already accept a send when the lease is withdrawn.
                assertEquals(List.of("SCHEMA"),targetBroker.kinds);
                assertEquals("UNKNOWN",Codec.JSON.readTree(payload(journal)).path("result").asText());
                assertFalse(new JdbcPublicationStore(journal,TARGET,original).schemaPrepared(table));
                assertThrows(JdbcJournal.Fenced.class,()->target.prepare(original.epoch,table));
                assertEquals(1,targetBroker.kinds.size());
            }
            String first=payload(journal);
            var successor=journal.claim("successor",30000);
            journal.pause(successor,TARGET,"KAFKA_RETRY");
            var peerBroker=new CheckingBroker(PEER);
            peerBroker.journal=journal;
            try(MessageSink paused=sink(journal,successor,targetBroker);MessageSink peer=sink(journal,successor,peerBroker)){
                assertThrows(JdbcJournal.Paused.class,()->paused.prepare(successor.epoch,table));
                assertEquals(1,targetBroker.kinds.size());
                assertTrue(payload(journal,PEER).isEmpty());
                peer.activate(successor.epoch);
                peer.prepare(successor.epoch,table);
                assertEquals(List.of("SCHEMA"),peerBroker.kinds);
                assertEquals(PEER,Codec.JSON.readTree(payload(journal,PEER)).path("target").asText());
            }
            journal.resume(TARGET,1);
            try(MessageSink resumed=sink(journal,successor,targetBroker)){
                resumed.activate(successor.epoch);
                resumed.prepare(successor.epoch,table);
                assertEquals(List.of("SCHEMA","SCHEMA"),targetBroker.kinds);
            }
            assertEquals(first,payload(journal));
            assertEquals(2,rows(journal,"kafka_history_first_send"));
            assertTrue(journal.subscriptions().stream().allMatch(s->s.acknowledged==0));
        }
    }

    private JdbcJournal.Settings settings(){
        JdbcJournal.Settings s=new JdbcJournal.Settings();
        s.pipelineId=TASK;s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0";
        s.kafkaPublicationTargets=Set.of(TARGET);return s;
    }
    private static KafkaPhysicalReceipt.Grant newGrant(){
        return grant(new KafkaHistoryOrigin(uuid(),TASK,uuid(),uuid(),uuid(),KEY),uuid(),1);
    }
    private static KafkaPhysicalReceipt.Grant grant(KafkaHistoryOrigin origin,String revision,long epoch){
        var scope=new KafkaPhysicalReceipt.Scope(origin.authority(),TASK,origin.directory(),revision,Codec.sha256("worker"));
        var member=new KafkaPhysicalReceipt.Member(scope,"node",uuid(),epoch,100,200);
        return new KafkaPhysicalReceipt.Grant(scope,KEY,new KafkaPhysicalReceipt.MemberWitness(member,KEY),origin,KEY);
    }
    private static String uuid(){return UUID.randomUUID().toString();}
    private static MessageSink sink(JdbcJournal journal,Journal.Lease lease,CheckingBroker broker)throws Exception{
        return new MessageSink(broker.target,Map.of("sales","replica"),new JdbcPublicationStore(journal,broker.target,lease),()->broker,
                65536,journal.settings.kafkaPhysicalGrant);
    }
    private static String payload(JdbcJournal journal)throws Exception{
        return payload(journal,TARGET);
    }
    private static String payload(JdbcJournal journal,String target)throws Exception{
        try(var c=journal.connect();var p=c.prepareStatement("SELECT payload FROM "+journal.table("kafka_history_first_send")
                +" WHERE subscription_id=?")){
            p.setString(1,target);
            try(var r=p.executeQuery()){
                assertTrue(r.next());var value=r.getString(1);assertFalse(r.next());return value;
            }
        }
    }
    private static int rows(JdbcJournal journal,String table)throws Exception{
        try(var c=journal.connect();var s=c.createStatement();var r=s.executeQuery("SELECT COUNT(*) FROM "+journal.table(table))){
            assertTrue(r.next());return r.getInt(1);
        }
    }
    private static final class CheckingBroker implements KafkaPhysicalPublisher {
        final List<String> kinds=new ArrayList<>();
        final String target;
        JdbcJournal journal;
        boolean loseFirst;
        java.util.concurrent.Callable<Void> afterJournalCommit;
        CheckingBroker(){this(TARGET);}
        CheckingBroker(String target){this.target=target;}
        public void prepare(Set<String> destinations){}
        public void require(Set<String> destinations){}
        public void beginPhysicalObservation(Set<String> namespaces){}
        public KafkaPhysicalReceipt.Observation finishPhysicalObservation(){return null;}
        public void publish(List<Message> messages)throws Exception{
            assertFalse(payload(journal,target).isEmpty(),"A separate connection must see the committed intent before any publish");
            var hook=afterJournalCommit;
            afterJournalCommit=null;
            if(hook!=null)hook.call();
            for(Message message:messages)kinds.add(message.headers.get("rivomesh_kind"));
            if(loseFirst){loseFirst=false;throw new IOException("fixture reply lost after acceptance");}
        }
        public void close(){}
    }
}
