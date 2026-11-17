package io.github.rivomesh.cdc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class MessageSinkTest {
    @TempDir Path directory;
    static final class Broker implements ConfirmedPublisher {
        final List<Message> accepted=new ArrayList<>();final Set<String> channels=new HashSet<>();boolean loseReply;String clusterId;
        public void prepare(Set<String> destinations){channels.addAll(destinations);}
        public void require(Set<String> destinations){if(!channels.containsAll(destinations))throw new IllegalStateException("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");}
        public void publish(List<Message> messages)throws Exception{
            for(Message message:messages)accepted.add(Codec.decode(Codec.encode(message),Message.class));
            if(loseReply){loseReply=false;throw new IOException("confirmation lost after acceptance");}
        }
        public void close(){}
        public String identity(){return clusterId;}
        MessageEvent event(int i){return Codec.decode(new String(accepted.get(i).body,StandardCharsets.UTF_8),MessageEvent.class);}
    }
    JdbcJournal.Settings settings(){JdbcJournal.Settings s=new JdbcJournal.Settings();s.pipelineId="pipeline";s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0";return s;}
    MessageSink sink(JdbcJournal j,Journal.Lease lease,Broker broker)throws Exception{return new MessageSink("fast",Collections.singletonMap("sales","replica"),new JdbcPublicationStore(j,"fast",lease),()->broker,65536);}
    Journal.SnapshotState empty(JdbcJournal journal,Journal.Lease lease,MessageSink sink,TableSchema table)throws Exception{
        Journal.SnapshotState snapshot=journal.beginSnapshot(lease,"source",table,new JournalTest().transaction(1).position);journal.finishSnapshot(lease,snapshot);sink.prepare(lease.epoch,table);sink.beginSnapshot(lease.epoch,table,snapshot);sink.finishSnapshot(lease.epoch,table,snapshot);return snapshot;
    }
    @Test void brokerConfirmationLossReplaysStableIdsAndCheckpointNeverLeadsConfirmation()throws Exception{
        Broker broker=new Broker();TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal j=new JdbcJournal(settings())){
            new JournalTest().initialize(j);Journal.Lease old=j.claim("old",30000);MessageSink sink=sink(j,old,broker);sink.activate(old.epoch);empty(j,old,sink,table);broker.accepted.clear();
            SourceTransaction tx=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"inserted"));broker.loseReply=true;
            assertThrows(IOException.class,()->sink.apply(old.epoch,1,tx));assertEquals(0,sink.checkpoint().sequence);assertEquals("BEGIN",broker.event(0).kind);sink.close();
            j.release(old);Journal.Lease next=j.claim("new",30000);
            try(MessageSink restarted=sink(j,next,broker)){
                restarted.activate(next.epoch);restarted.prepare(next.epoch,table);restarted.apply(next.epoch,1,tx);
                assertEquals(1,restarted.checkpoint().sequence);assertEquals(broker.accepted.get(0).id,broker.accepted.get(1).id);assertArrayEquals(broker.accepted.get(0).body,broker.accepted.get(1).body);assertNotEquals(broker.accepted.get(0).headers.get("rivomesh_epoch"),broker.accepted.get(1).headers.get("rivomesh_epoch"));
                assertEquals("ROW",broker.event(2).kind);assertEquals("COMMIT",broker.event(3).kind);assertEquals(1,broker.event(3).expectedChanges.get("replica"));int count=broker.accepted.size();restarted.apply(next.epoch,1,tx);assertEquals(count,broker.accepted.size());
                SourceTransaction changed=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"altered"));assertThrows(IllegalStateException.class,()->restarted.apply(next.epoch,1,changed));
            }
            int count=broker.accepted.size();assertThrows(JdbcJournal.Fenced.class,()->sink.apply(old.epoch,1,tx));assertEquals(count,broker.accepted.size());
        }
    }
    @Test void nativeProgressReceiptsAreGenerationScopedCompareAndSetAndLeaseFenced()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){new JournalTest().initialize(j);Journal.Lease old=j.claim("old",30000);JdbcPublicationStore store=new JdbcPublicationStore(j,"fast",old);store.activate(old.epoch);assertNull(store.progress());store.progress(old.epoch,null,"native-position-1");assertEquals("native-position-1",store.progress());assertThrows(IllegalStateException.class,()->store.progress(old.epoch,null,"incorrect-overwrite"));assertEquals(0,store.checkpoint().sequence);j.release(old);Journal.Lease next=j.claim("new",30000);JdbcPublicationStore resumed=new JdbcPublicationStore(j,"fast",next);resumed.activate(next.epoch);assertEquals("native-position-1",resumed.progress());assertThrows(JdbcJournal.Fenced.class,()->store.progress(old.epoch,"native-position-1","stale-writer"));resumed.progress(next.epoch,"native-position-1","native-position-2");assertEquals("native-position-2",resumed.progress());}
    }
    @Test void anEstablishedPublicationCannotInventAMissingRequiredNativeProgressBaseline()throws Exception{
        Broker broker=new Broker();TableSchema table=JdbcSinkTest.schema(false);try(JdbcJournal j=new JdbcJournal(settings())){new JournalTest().initialize(j);Journal.Lease lease=j.claim("owner",30000);try(MessageSink original=sink(j,lease,broker)){original.activate(lease.epoch);empty(j,lease,original,table);}int count=broker.accepted.size();ConfirmedPublisher required=new ConfirmedPublisher(){public boolean requiresProgress(){return true;}public void prepare(Set<String> channels){fail("Must not provision before migration approval");}public void require(Set<String> channels){fail("Must not adopt without a native receipt");}public void publish(List<Message> messages){fail("Must not publish without a native receipt");}public void close(){}};try(MessageSink migrated=new MessageSink("fast",Collections.singletonMap("sales","replica"),new JdbcPublicationStore(j,"fast",lease),()->required,65536)){migrated.activate(lease.epoch);assertEquals("PUBLICATION_TRANSPORT_UPGRADE_REQUIRES_PLAN",assertThrows(IllegalStateException.class,()->migrated.prepare(lease.epoch,table)).getMessage());assertEquals(count,broker.accepted.size());assertNull(new JdbcPublicationStore(j,"fast",lease).progress());}}
    }
    @Test void snapshotOccurrencesAndPrimaryKeyChangesHaveUnambiguousConsumerKeys()throws Exception{
        Broker broker=new Broker();TableSchema table=JdbcSinkTest.schema(true);
        try(JdbcJournal j=new JdbcJournal(settings())){
            new JournalTest().initialize(j);Journal.Lease lease=j.claim("owner",30000);
            try(MessageSink sink=sink(j,lease,broker)){
                sink.activate(lease.epoch);sink.prepare(lease.epoch,table);Journal.SnapshotState state=j.beginSnapshot(lease,"source",table,new JournalTest().transaction(1).position);
                j.appendSnapshot(lease,state,0,Arrays.asList(JdbcSinkTest.row(1,"duplicate"),JdbcSinkTest.row(1,"duplicate")));j.finishSnapshot(lease,state);sink.beginSnapshot(lease.epoch,table,state);
                Journal.SnapshotPart part=j.snapshotParts("source",table.id,state.snapshotId,0,1).get(0);broker.accepted.clear();broker.loseReply=true;assertThrows(IOException.class,()->sink.snapshotPart(lease.epoch,table,state.snapshotId,part));assertEquals(0,new JdbcPublicationStore(j,"fast",lease).snapshot(table.id).nextPart);
                sink.snapshotPart(lease.epoch,table,state.snapshotId,part);assertEquals(broker.event(0).id,broker.event(2).id);assertNotEquals(broker.event(0).afterKey,broker.event(1).afterKey);sink.finishSnapshot(lease.epoch,table,state);int count=broker.accepted.size();sink.snapshotPart(lease.epoch,table,state.snapshotId,part);sink.finishSnapshot(lease.epoch,table,state);assertEquals(count,broker.accepted.size());
                part.rows.get(0).set(1,Cell.value("changed"));assertThrows(IllegalStateException.class,()->sink.snapshotPart(lease.epoch,table,state.snapshotId,part));
                TableSchema keyed=JdbcSinkTest.schema(false);keyed.id="keyed";keyed.name="keyed";empty(j,lease,sink,keyed);broker.accepted.clear();
                Change update=NormalizationTest.change(keyed,"pk-update",Change.Operation.UPDATE,JdbcSinkTest.row(1,"old"),JdbcSinkTest.row(2,"new"));sink.apply(lease.epoch,1,JdbcSinkTest.transaction(1,update));MessageEvent row=broker.event(1);assertNotEquals(row.beforeKey,row.afterKey);assertEquals(row.afterKey,broker.accepted.get(1).key);
            }
        }
    }
    @Test void oversizedRowsAndMissingIdentityAreRejectedBeforeTransactionBegin()throws Exception{
        Broker broker=new Broker();TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal j=new JdbcJournal(settings())){
            new JournalTest().initialize(j);Journal.Lease lease=j.claim("owner",30000);
            try(MessageSink sink=sink(j,lease,broker)){
                sink.activate(lease.epoch);empty(j,lease,sink,table);broker.accepted.clear();
                SourceTransaction large=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,String.join("",Collections.nCopies(65536,"x"))));assertThrows(IllegalStateException.class,()->sink.apply(lease.epoch,1,large));assertTrue(broker.accepted.isEmpty());
                Change invalid=JdbcSinkTest.insert(table,1,"missing");invalid.after.set(0,Cell.missing());assertThrows(IllegalArgumentException.class,()->sink.apply(lease.epoch,1,JdbcSinkTest.transaction(1,invalid)));assertTrue(broker.accepted.isEmpty());assertEquals(0,sink.checkpoint().sequence);
            }
        }
    }
    @Test void losingInitializedBrokerDestinationsCannotRecreateEmptyChannelsAtAnOldCheckpoint()throws Exception{
        Broker broker=new Broker();TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal j=new JdbcJournal(settings())){
            new JournalTest().initialize(j);Journal.Lease lease=j.claim("owner",30000);
            try(MessageSink sink=sink(j,lease,broker)){sink.activate(lease.epoch);empty(j,lease,sink,table);sink.apply(lease.epoch,1,JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"published")));}
            broker.channels.clear();int count=broker.accepted.size();
            try(MessageSink restarted=sink(j,lease,broker)){restarted.activate(lease.epoch);assertThrows(IllegalStateException.class,()->restarted.prepare(lease.epoch,table));assertTrue(broker.channels.isEmpty());assertEquals(count,broker.accepted.size());assertEquals(1,restarted.checkpoint().sequence);}
        }
    }
    @Test void changingTheBrokerClusterCannotSilentlyReuseExistingPublicationCheckpoints()throws Exception{
        Broker broker=new Broker();broker.clusterId="cluster-one";TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcJournal j=new JdbcJournal(settings())){
            new JournalTest().initialize(j);Journal.Lease lease=j.claim("owner",30000);
            try(MessageSink sink=sink(j,lease,broker)){sink.activate(lease.epoch);empty(j,lease,sink,table);}
            int count=broker.accepted.size();broker.clusterId="independent-cluster";
            try(MessageSink wrong=sink(j,lease,broker)){wrong.activate(lease.epoch);assertEquals("BROKER_IDENTITY_CHANGED",assertThrows(IllegalStateException.class,()->wrong.prepare(lease.epoch,table)).getMessage());assertEquals(count,broker.accepted.size());}
            broker.clusterId="cluster-one";try(MessageSink original=sink(j,lease,broker)){original.activate(lease.epoch);original.prepare(lease.epoch,table);assertEquals(count,broker.accepted.size());}
        }
    }
    @Test void crossDatabaseManifestsDescribeEveryNamespaceAndReconstructTheCommittedContentHash()throws Exception{
        Broker broker=new Broker();TableSchema sales=JdbcSinkTest.schema(false),inventory=JdbcSinkTest.schema(false);inventory.id="inventory-table";inventory.database="inventory";Map<String,String> mappings=new TreeMap<>();mappings.put("sales","replica");mappings.put("inventory","warehouse");
        try(JdbcJournal j=new JdbcJournal(settings())){
            new JournalTest().initialize(j);Journal.Lease lease=j.claim("owner",30000);
            try(MessageSink sink=new MessageSink("fast",mappings,new JdbcPublicationStore(j,"fast",lease),()->broker,65536)){
                sink.activate(lease.epoch);empty(j,lease,sink,sales);empty(j,lease,sink,inventory);broker.accepted.clear();SourceTransaction tx=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(sales,1,"sale"),JdbcSinkTest.insert(inventory,2,"stock"));tx.attributes.put("origin","stable");Change barrier=new Change();barrier.eventId="cutover-barrier";barrier.operation=Change.Operation.BARRIER;barrier.attributes.put("marker","durable");tx.changes.add(1,barrier);sink.apply(lease.epoch,1,tx);
                MessageEvent begin=broker.event(0),commit=broker.event(4);assertEquals(2,begin.totalChanges);assertEquals(1,begin.expectedChanges.get("replica"));assertEquals(1,begin.expectedChanges.get("warehouse"));
                SourceTransaction reconstructed=new SourceTransaction();reconstructed.id=begin.transactionId;reconstructed.streamGeneration=begin.generation;reconstructed.kind=SourceTransaction.Kind.valueOf(begin.transactionKind);reconstructed.attributes=begin.attributes;reconstructed.position=begin.position;reconstructed.changes.add(broker.event(2).change);reconstructed.changes.add(begin.controls.get(1));reconstructed.changes.add(broker.event(3).change);assertEquals(2,broker.event(3).ordinal);assertEquals(Codec.sha256(Codec.encode(reconstructed)),commit.contentHash);
            }
        }
    }
}
