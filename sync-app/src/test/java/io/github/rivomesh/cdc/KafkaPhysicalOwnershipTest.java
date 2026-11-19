package io.github.rivomesh.cdc;

import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.cdc.KafkaPhysicalReceipt.Grant;
import io.github.rivomesh.cdc.KafkaPhysicalReceipt.Observation;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KafkaPhysicalOwnershipTest {
    private static final String TASK="ownership-fixture",TARGET="kafka",GENERATION="generation";
    private static final byte[] PROJECTION="task projection".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final String SCOPED_KEY=Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
    private static final long CONTROL_OBSERVED=4102444800000L;

    @Test void grantBindsActualWorkerBytesAndConfigurationVersion()throws Exception{
        var issued=grant();
        var opened=Grant.open(issued.encode(),TASK,PROJECTION);
        assertEquals(issued.scope(),opened.scope());
        assertEquals(issued.member(),opened.member());
        var proof=issued.sign(TARGET,GENERATION,1,2,Codec.sha256("checked transaction"),new Broker().observation());
        assertTrue(opened.verifies(proof));
        assertFalse(grant().verifies(proof));
        assertThrows(IllegalArgumentException.class,()->Grant.open(issued.encode(),"peer",PROJECTION));
        assertThrows(IllegalArgumentException.class,()->Grant.open(issued.encode(),TASK,new byte[]{1}));
    }

    @Test void historicalProofAndSubscriptionAckAreAtomicAcrossRestart()throws Exception{
        var settings=settings();
        var version=grant();var broker=new Broker();
        settings.kafkaPhysicalGrant=version;
        KafkaPhysicalReceipt.Signed first;
        try(JdbcJournal j=new JdbcJournal(settings)){
            var lease=initialize(j);
            j.append(lease,tx(1));j.append(lease,tx(2));
            try(MessageSink old=sink(j,lease,broker,null)){
                assertEquals(1,dispatch(j,lease,old).drain(1));
            }
            var store=new JdbcPublicationStore(j,TARGET,lease);
            assertNull(store.kafkaPhysicalReceipt(GENERATION,version));
            try(MessageSink observed=sink(j,lease,broker,version)){
                observed.activate(lease.epoch);
                observed.apply(lease.epoch,2,tx(2));
            }
            assertEquals(2,store.checkpoint().sequence);
            assertNull(store.kafkaPhysicalReceipt(GENERATION,version));
            assertEquals(1,j.subscriptions().getFirst().acknowledged);
            FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+" ADD CONSTRAINT fixture_ack CHECK (acknowledged <= 1)");
            assertThrows(SQLException.class,()->j.acknowledge(lease,TARGET,2));
            assertNull(store.kafkaPhysicalReceipt(GENERATION,version));
            assertEquals(1,j.subscriptions().getFirst().acknowledged);
            FailureReviewTest.sql(j,"ALTER TABLE "+j.table("subscription")+" DROP CONSTRAINT fixture_ack");
            j.release(lease);
        }
        try(JdbcJournal reopened=new JdbcJournal(settings)){
            var lease=reopened.claim("restart",30000);
            var successor=memberGrant(version.scope(),2);
            settings.kafkaPhysicalGrant=successor;
            var sameStore=new JdbcPublicationStore(reopened,TARGET,lease);
            int emitted=broker.writes.size();
            try(MessageSink resumed=sink(reopened,lease,broker,successor)){
                assertEquals(1,dispatch(reopened,lease,resumed).drain(1));
                assertEquals(emitted,broker.writes.size());
                assertEquals(2,reopened.subscriptions().getFirst().acknowledged);
                first=sameStore.kafkaPhysicalReceipt(GENERATION,version);
                assertEquals(2,first.receipt().sequence());
                assertTrue(version.verifies(first));
                assertTrue(successor.verifies(first));
                assertEquals(version.member(),first.receipt().member());
                assertNotEquals(successor.member(),first.receipt().member());
                var time=first.receipt().journalTime();
                assertTrue(time.publishedUtcMillis()<=time.acknowledgedUtcMillis());
                assertEquals(lease.epoch,time.acknowledgementEpoch());
                assertTrue(time.acknowledgedUtcMillis()<version.member().member().observedUtcMillis(),
                        "Different control and journal clocks must not suppress a valid journal observation");
                reopened.append(lease,tx(3));
                assertEquals(1,dispatch(reopened,lease,resumed).drain(1));
                assertEquals(first,sameStore.kafkaPhysicalReceipt(GENERATION,version));
                assertEquals(1,broker.observations);
            }
            var updated=grant();
            settings.kafkaPhysicalGrant=updated;
            assertNull(sameStore.kafkaPhysicalReceipt(GENERATION,updated));
            reopened.append(lease,tx(4));
            try(MessageSink newWorker=sink(reopened,lease,broker,updated)){
                assertEquals(1,dispatch(reopened,lease,newWorker).drain(1));
                var next=sameStore.kafkaPhysicalReceipt(GENERATION,updated);
                assertEquals(4,next.receipt().sequence());
                assertTrue(updated.verifies(next));
                assertFalse(version.verifies(next));
                assertEquals(first,sameStore.kafkaPhysicalReceipt(GENERATION,version));
            }
        }
    }

    @Test void unavailableNativeReadDoesNotRepublishOrCreateEvidence()throws Exception{
        var version=grant();var broker=new Broker();broker.unavailable=true;
        var settings=settings();settings.kafkaPhysicalGrant=version;
        try(JdbcJournal j=new JdbcJournal(settings)){
            var lease=initialize(j);
            j.append(lease,tx(1));j.append(lease,tx(2));
            try(MessageSink worker=sink(j,lease,broker,version)){
                assertEquals(2,dispatch(j,lease,worker).drain(2));
                assertEquals(2,j.subscriptions().getFirst().acknowledged);
                assertEquals(List.of("BEGIN","COMMIT","BEGIN","COMMIT"),broker.writes);
                assertEquals(1,broker.observations);
                assertNull(new JdbcPublicationStore(j,TARGET,lease).kafkaPhysicalReceipt(GENERATION,version));
            }
        }
    }

    @Test void restartWithDifferentConfigurationDoesNotAttestAnOldPendingPublication()throws Exception{
        var settings=settings();
        var older=grant();var newer=grant();var broker=new Broker();
        settings.kafkaPhysicalGrant=older;
        try(JdbcJournal j=new JdbcJournal(settings)){
            var lease=initialize(j);
            j.append(lease,tx(1));
            try(MessageSink old=sink(j,lease,broker,older)){
                old.activate(lease.epoch);old.apply(lease.epoch,1,tx(1));
            }
            j.release(lease);
        }
        settings.kafkaPhysicalGrant=newer;
        try(JdbcJournal reopened=new JdbcJournal(settings)){
            var lease=reopened.claim("new-version",30000);
            try(MessageSink replacement=sink(reopened,lease,broker,newer)){
                assertEquals(1,dispatch(reopened,lease,replacement).drain(1));
            }
            assertEquals(1,reopened.subscriptions().getFirst().acknowledged);
            assertEquals(List.of("BEGIN","COMMIT"),broker.writes);
            var store=new JdbcPublicationStore(reopened,TARGET,lease);
            assertNull(store.kafkaPhysicalReceipt(GENERATION,older));
            assertNull(store.kafkaPhysicalReceipt(GENERATION,newer));
        }
    }

    @Test void existingDefaultJournalNeedsNoPhysicalTablesAndOptInChecksBeforePublication()throws Exception{
        var settings=settings();var broker=new Broker();var grant=grant();
        try(JdbcJournal j=new JdbcJournal(settings)){
            var lease=initialize(j);
            j.append(lease,tx(1));
            try(MessageSink off=sink(j,lease,broker,null)){
                assertEquals(1,dispatch(j,lease,off).drain(1));
            }
            try(var c=j.connect();var statement=c.createStatement()){
                assertThrows(SQLException.class,()->statement.executeQuery("SELECT 1 FROM "+j.table("kafka_physical_pending")));
                assertThrows(SQLException.class,()->statement.executeQuery("SELECT 1 FROM "+j.table("kafka_physical_history")));
            }
            j.release(lease);
            FailureReviewTest.sql(j,"CREATE TABLE "+j.table("kafka_physical_pending")+" (invalid_shape INTEGER)");
        }
        settings.kafkaPhysicalGrant=grant;
        try(JdbcJournal j=new JdbcJournal(settings)){
            var failure=assertThrows(SyncFailure.class,()->initialize(j));
            assertEquals("KAFKA_PHYSICAL_JOURNAL_NOT_READY",failure.code);
            assertEquals(List.of("BEGIN","COMMIT"),broker.writes);
            FailureReviewTest.sql(j,"DROP TABLE "+j.table("kafka_physical_pending"));
            var lease=initialize(j);
            j.append(lease,tx(2));
            try(MessageSink on=sink(j,lease,broker,grant)){
                assertEquals(1,dispatch(j,lease,on).drain(1));
            }
            assertEquals(2,new JdbcPublicationStore(j,TARGET,lease).kafkaPhysicalReceipt(GENERATION,grant).receipt().sequence());
        }
    }

    @Test void disablingThenEnablingEvidenceDoesNotBackfillAnAckWhileOff()throws Exception{
        var settings=settings();var version=grant();var broker=new Broker();settings.kafkaPhysicalGrant=version;
        try(JdbcJournal j=new JdbcJournal(settings)){
            var lease=initialize(j);
            j.append(lease,tx(1));j.append(lease,tx(2));
            try(MessageSink observed=sink(j,lease,broker,version)){
                observed.activate(lease.epoch);observed.apply(lease.epoch,1,tx(1));
            }
            j.release(lease);
            settings.kafkaPhysicalGrant=null;
            lease=j.claim("disabled",30000);
            try(MessageSink off=sink(j,lease,broker,null)){
                assertEquals(1,dispatch(j,lease,off).drain(1));
            }
            assertEquals(1,j.subscriptions().getFirst().acknowledged);
            assertNull(new JdbcPublicationStore(j,TARGET,lease).kafkaPhysicalReceipt(GENERATION,version));
            assertEquals(List.of("BEGIN","COMMIT"),broker.writes);
            j.release(lease);
            settings.kafkaPhysicalGrant=version;
            lease=initialize(j);
            try(MessageSink observed=sink(j,lease,broker,version)){
                assertEquals(1,dispatch(j,lease,observed).drain(1));
            }
            assertEquals(2,new JdbcPublicationStore(j,TARGET,lease).kafkaPhysicalReceipt(GENERATION,version).receipt().sequence());
            assertEquals(List.of("BEGIN","COMMIT","BEGIN","COMMIT"),broker.writes);
        }
    }

    @Test void readOnlyJournalCredentialsCannotStartOwnershipEvidence()throws Exception{
        var adminSettings=settings();var grant=grant();adminSettings.kafkaPhysicalGrant=grant;
        try(var admin=new JdbcJournal(adminSettings)){
            var lease=initialize(admin);
            FailureReviewTest.sql(admin,"CREATE USER reader PASSWORD 'fixture-journal-role'");
            FailureReviewTest.sql(admin,"GRANT SELECT ON "+admin.table("kafka_physical_pending")+", "
                    +admin.table("kafka_physical_history")+", "+admin.table("kafka_physical_attested")+" TO reader");
            var restricted=settings();
            restricted.url=adminSettings.url.substring(0,adminSettings.url.indexOf(';'));
            restricted.username="reader";restricted.password="fixture-journal-role";restricted.kafkaPhysicalGrant=grant;
            try(var worker=new JdbcJournal(restricted);var c=worker.connect()){
                var denied=assertThrows(SyncFailure.class,()->JdbcKafkaPhysicalHistory.prepare(worker,c,false));
                assertEquals("KAFKA_PHYSICAL_JOURNAL_NOT_READY",denied.code);
                var startup=assertThrows(SyncFailure.class,()->initialize(worker));
                assertEquals("KAFKA_PHYSICAL_JOURNAL_NOT_READY",startup.code);
            }
            assertEquals(0,new JdbcPublicationStore(admin,TARGET,lease).checkpoint().sequence);
            assertEquals(0,admin.subscriptions().getFirst().acknowledged);
        }
    }

    @Test void priorPhysicalReceiptIsPreservedWithoutFillingInMemberOrTime()throws Exception{
        var settings=settings();var grant=grant();var legacy=new Grant(grant.scope(),SCOPED_KEY);var broker=new Broker();
        settings.kafkaPhysicalGrant=legacy;
        try(var j=new JdbcJournal(settings)){
            var lease=initialize(j);
            j.append(lease,tx(1));
            try(var sink=sink(j,lease,broker,legacy)){assertEquals(1,dispatch(j,lease,sink).drain(1));}
            var store=new JdbcPublicationStore(j,TARGET,lease);
            var original=store.kafkaPhysicalReceipt(GENERATION,legacy);
            assertNull(original.receipt().member());
            assertNull(original.receipt().journalTime());
            j.release(lease);

            settings.kafkaPhysicalGrant=grant;
            lease=initialize(j);
            assertNull(store.kafkaPhysicalReceipt(GENERATION,grant));
            j.append(lease,tx(2));
            try(var sink=sink(j,lease,broker,grant)){assertEquals(1,dispatch(j,lease,sink).drain(1));}
            assertEquals(original,store.kafkaPhysicalReceipt(GENERATION,legacy));
            var witnessed=store.kafkaPhysicalReceipt(GENERATION,grant);
            assertEquals(2,witnessed.receipt().sequence());
            assertEquals(grant.member(),witnessed.receipt().member());
            assertNotNull(witnessed.receipt().journalTime().acknowledgedUtcMillis());
        }
    }

    @Test void backwardJournalTimeKeepsBusinessAckAndLeavesTheCommitUnverified()throws Exception{
        var settings=settings();var grant=grant();var broker=new Broker();
        settings.kafkaPhysicalGrant=grant;
        try(var j=new JdbcJournal(settings)){
            var lease=initialize(j);
            j.append(lease,tx(1));
            var store=new JdbcPublicationStore(j,TARGET,lease);
            try(var sink=sink(j,lease,broker,grant)){
                sink.activate(lease.epoch);sink.apply(lease.epoch,1,tx(1));
            }
            try(var c=j.connect();var row=c.prepareStatement("SELECT proof FROM "+j.table("kafka_physical_pending"));
                var staged=row.executeQuery()){
                assertTrue(staged.next());
                var original=Codec.decode(staged.getString(1),KafkaPhysicalReceipt.Signed.class);
                var receipt=original.receipt();
                assertTrue(grant.verifies(original));
                assertTrue(receipt.journalTime().publishedUtcMillis()<=j.now(c));
                var untimed=grant.sign(TARGET,GENERATION,lease.epoch,1,receipt.transactionSha256(),receipt.observation());
                var future=grant.published(untimed,Math.addExact(j.now(c),60000));
                JdbcJournal.execute(c,"UPDATE "+j.table("kafka_physical_pending")+" SET proof=? WHERE pipeline_id=?",
                        Codec.encode(future),TASK);
            }
            j.acknowledge(lease,TARGET,1);
            assertEquals(1,j.subscriptions().getFirst().acknowledged);
            assertNull(store.kafkaPhysicalReceipt(GENERATION,grant));
            assertEquals(List.of("BEGIN","COMMIT"),broker.writes);
            j.append(lease,tx(2));
            try(var resumed=sink(j,lease,broker,grant)){assertEquals(1,dispatch(j,lease,resumed).drain(1));}
            assertEquals(2,store.kafkaPhysicalReceipt(GENERATION,grant).receipt().sequence());
        }
    }

    @Test void journalOwnershipStorageRejectsViewsAsDurableReceiptTables()throws Exception{
        var settings=settings();settings.kafkaPhysicalGrant=grant();
        try(var journal=new JdbcJournal(settings);var connection=journal.connect()){
            initialize(journal);
            String table="kafka_physical_history";
            journal.requirePhysicalEvidenceTable(connection,table);
            FailureReviewTest.sql(journal,"CREATE VIEW "+journal.table("fixture_view")+" AS SELECT * FROM "+journal.table(table));
            assertThrows(SQLException.class,()->journal.requirePhysicalEvidenceTable(connection,"fixture_view"));
        }
    }

    private static Grant grant(){
        return memberGrant(new KafkaPhysicalReceipt.Scope(UUID.randomUUID().toString(),TASK,UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),Codec.sha256(PROJECTION)),1);
    }

    private static Grant memberGrant(KafkaPhysicalReceipt.Scope scope,long epoch){
        var member=new KafkaPhysicalReceipt.Member(scope,"fixture-owner",UUID.randomUUID().toString(),epoch,
                CONTROL_OBSERVED,CONTROL_OBSERVED+5000);
        return new Grant(scope,SCOPED_KEY,new KafkaPhysicalReceipt.MemberWitness(member,SCOPED_KEY));
    }

    private static JdbcJournal.Settings settings(){
        var settings=new JdbcJournal.Settings();settings.pipelineId=TASK;settings.driver="org.h2.Driver";
        settings.url="jdbc:h2:mem:ownership_"+UUID.randomUUID().toString().replace("-","")+";DB_CLOSE_DELAY=-1";
        settings.username="sa";settings.kafkaPublicationTargets=Set.of(TARGET);
        return settings;
    }

    private static Journal.Lease initialize(JdbcJournal journal)throws Exception{
        journal.initialize(Codec.sha256("ownership fixture"),GENERATION,List.of(TARGET));
        return journal.claim("fixture",30000);
    }

    private static SourceTransaction tx(int sequence){
        SourceTransaction tx=FailureReviewTest.tx(sequence);
        tx.streamGeneration=GENERATION;tx.changes.clear();
        return tx;
    }

    private static MessageSink sink(JdbcJournal journal,Journal.Lease lease,Broker broker,Grant grant)throws Exception{
        return new MessageSink(TARGET,Map.of("sales","orders"),new JdbcPublicationStore(journal,TARGET,lease),()->broker,65536,grant);
    }

    private static FanoutDispatcher dispatch(JdbcJournal journal,Journal.Lease lease,MessageSink sink){
        return new FanoutDispatcher(journal,lease,List.of(sink),Map.of(),record->record.transaction);
    }

    private static final class Broker implements KafkaPhysicalPublisher {
        final List<String> writes=new ArrayList<>();
        final List<KafkaPhysicalReceipt.Wire> receipts=new ArrayList<>();
        boolean unavailable,watching;
        int observations;
        @Override public void prepare(Set<String> destinations){}
        @Override public void require(Set<String> destinations){}
        @Override public void beginPhysicalObservation(Set<String> namespaces){watching=true;observations++;receipts.clear();}
        @Override public void publish(List<Message> messages){
            for(Message message:messages){
                String kind=message.headers.get("rivomesh_kind");
                writes.add(kind);
                if(watching&&"COMMIT".equals(kind))receipts.add(new KafkaPhysicalReceipt.Wire("orders",writes.size()-1,Codec.sha256(message.body)));
            }
        }
        @Override public Observation finishPhysicalObservation(){watching=false;return unavailable?null:new Observation(topic(),receipts);}
        @Override public void close(){}
        Observation observation(){return new Observation(topic(),List.of(new KafkaPhysicalReceipt.Wire("orders",22,Codec.sha256("checked commit"))));}
        private static KafkaPhysicalReceipt.Topic topic(){return new KafkaPhysicalReceipt.Topic(Codec.sha256("fixture broker"),"cKtKfC5KQGq-qM1RDCb_3w");}
    }
}
