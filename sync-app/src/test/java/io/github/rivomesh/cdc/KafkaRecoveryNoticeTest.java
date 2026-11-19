package io.github.rivomesh.cdc;

import io.github.rivomesh.cdc.KafkaRecoveryNotice.Attempt;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Authority;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Plan;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Receipt;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Topic;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class KafkaRecoveryNoticeTest {
    private static final Topic CONTROL=new Topic("dedicated-control",Codec.sha256("KAFKA:cluster"),Uuid.randomUuid().toString());
    private static final Authority AUTHORIZED=()->{};
    @TempDir Path directory;
    private JdbcJournal.Settings settings;
    private Plan plan;

    @BeforeEach void seed()throws Exception {
        settings=new JdbcJournal.Settings();
        settings.pipelineId="pipeline";
        settings.driver="org.h2.Driver";
        settings.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0;LOCK_TIMEOUT=5000";
        try(JdbcJournal journal=new JdbcJournal(settings)){
            Journal.Lease owner=FailureReviewTest.prepare(journal);
            FailureReviewTest.normalize(journal,owner);
            journal.pauseFailure(owner,"slow","KAFKA_WRITE_REFUSED",FailureReviewTest.failure());
            journal.acknowledge(owner,"fast",1);
            journal.pausePipeline();
            JdbcKafkaNoticeLedger ledger=new JdbcKafkaNoticeLedger(journal);
            ledger.provision();
            plan=ledger.prepare("slow",1,CONTROL,Codec.sha256("verified preceding checkpoint"),AUTHORIZED);
        }
    }

    @Test void lostCommitResponseRecoversExactImmutableReceiptAfterJournalRestart()throws Exception {
        Native broker=new Native();
        broker.commitReplyLost=true;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            KafkaRecoveryNotice protocol=protocol(journal,broker);
            assertThrows(IOException.class,()->protocol.publish(plan,plan.hash(),AUTHORIZED));
            assertEquals(1,broker.commitCalls);
            assertTrue(new JdbcKafkaNoticeLedger(journal).existing(plan).hasCommitIntent());
            assertFalse(new JdbcKafkaNoticeLedger(journal).existing(plan).verified());
        }
        try(JdbcJournal reopened=new JdbcJournal(settings)){
            KafkaRecoveryNotice protocol=protocol(reopened,broker);
            Receipt recovered=protocol.publish(plan,plan.hash(),AUTHORIZED);
            assertEquals(broker.receipt,recovered);
            assertEquals(recovered,protocol.publish(plan,plan.hash(),AUTHORIZED));
            assertEquals(List.of(plan.transactionalId()),broker.producers);
            assertEquals(1,broker.commitCalls);
            assertEquals(List.of(broker.receipt.offset(),broker.receipt.offset()),broker.requestedOffsets);
            assertEquals(0,reopened.inspectFailure("slow",1,100000).subscription.acknowledged);
            assertEquals(1,reopened.subscriptions().stream().filter(sub->sub.id.equals("fast")).findFirst().orElseThrow().acknowledged);
        }
    }

    @Test void neverResendsAnAmbiguousCommitWithoutNativeReceipt()throws Exception {
        Native broker=new Native();
        broker.commitReplyLost=true;
        broker.commitInvisible=true;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            assertThrows(IOException.class,()->protocol(journal,broker).publish(plan,plan.hash(),AUTHORIZED));
        }
        try(JdbcJournal reopened=new JdbcJournal(settings)){
            KafkaRecoveryNotice protocol=protocol(reopened,broker);
            assertNull(protocol.publish(plan,plan.hash(),AUTHORIZED));
            assertNull(protocol.publish(plan,plan.hash(),AUTHORIZED));
            assertEquals(1,broker.producers.size());
            assertEquals(1,broker.commitCalls);
            assertFalse(new JdbcKafkaNoticeLedger(reopened).existing(plan).verified());
        }
    }

    @Test void mismatchedNativeBytesCannotVerifyOrTriggerASecondSend()throws Exception {
        Native broker=new Native();
        broker.commitReplyLost=true;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            KafkaRecoveryNotice protocol=protocol(journal,broker);
            assertThrows(IOException.class,()->protocol.publish(plan,plan.hash(),AUTHORIZED));
            Receipt original=broker.receipt;
            broker.receipt=new Receipt(original.noticeId(),original.topic(),original.offset(),Codec.sha256("changed bytes"));
            assertEquals("KAFKA_NOTICE_NATIVE_RECEIPT_CONFLICT",assertThrows(IllegalStateException.class,
                    ()->protocol.publish(plan,plan.hash(),AUTHORIZED)).getMessage());
            assertFalse(new JdbcKafkaNoticeLedger(journal).existing(plan).verified());
            assertEquals(1,broker.commitCalls);
        }
    }

    @Test void aVerifiedLedgerRowCannotSupplyAnOffsetOrRecoverAVanishedNativeReceipt()throws Exception {
        Native broker=new Native();
        try(JdbcJournal journal=new JdbcJournal(settings)){
            var protocol=protocol(journal,broker);
            Receipt original=protocol.publish(plan,plan.hash(),AUTHORIZED);
            assertNotNull(original);
            var ledger=new JdbcKafkaNoticeLedger(journal);
            long replacement=original.offset()+20,number=ledger.existing(plan).number();
            String forgedChecksum=Codec.sha256(Codec.encode(List.of(plan.id(),plan.hash(),"VERIFIED",number,Long.toString(replacement))));
            FailureReviewTest.sql(journal,"UPDATE "+journal.table("kafka_recovery_notice")+" SET native_offset=?,state_hash=? "
                    +"WHERE pipeline_id=? AND notice_id=?",replacement,forgedChecksum,plan.failure().pipeline(),plan.id());
            assertEquals(replacement,ledger.existing(plan).offset());
            assertEquals("KAFKA_NOTICE_NATIVE_RECEIPT_CONFLICT",assertThrows(IllegalStateException.class,
                    ()->protocol.publish(plan,plan.hash(),AUTHORIZED)).getMessage());
            broker.receipt=null;
            assertNull(protocol.publish(plan,plan.hash(),AUTHORIZED));
            assertEquals(List.of(original.offset(),replacement,replacement),broker.requestedOffsets);
            assertEquals(1,broker.commitCalls);
            assertEquals(1,broker.producers.size());
            assertEquals(0,journal.inspectFailure("slow",1,100000).subscription.acknowledged);
            assertEquals(1,journal.subscriptions().stream().filter(sub->sub.id.equals("fast")).findFirst().orElseThrow().acknowledged);
        }
    }

    @Test void revokedAfterNativeReadCannotReplayAPreviouslyVerifiedReceipt()throws Exception {
        Native broker=new Native();
        AtomicBoolean allowed=new AtomicBoolean(true);
        Authority authority=()->{if(!allowed.get())throw new SecurityException("AUTHORIZATION_CHANGED");};
        try(JdbcJournal journal=new JdbcJournal(settings)){
            var protocol=protocol(journal,broker);
            assertNotNull(protocol.publish(plan,plan.hash(),authority));
            broker.afterRead=()->allowed.set(false);
            assertThrows(SecurityException.class,()->protocol.publish(plan,plan.hash(),authority));
            assertEquals(1,broker.commitCalls);
            assertEquals(1,broker.producers.size());
            assertEquals(0,journal.inspectFailure("slow",1,100000).subscription.acknowledged);
        }
    }

    @Test void failureBeforeCommitIntentRetriesWithStableProducerIdentity()throws Exception {
        Native broker=new Native();
        broker.sendReplyLost=true;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            assertThrows(IOException.class,()->protocol(journal,broker).publish(plan,plan.hash(),AUTHORIZED));
            assertFalse(new JdbcKafkaNoticeLedger(journal).existing(plan).hasCommitIntent());
        }
        broker.sendReplyLost=false;
        try(JdbcJournal reopened=new JdbcJournal(settings)){
            assertNotNull(protocol(reopened,broker).publish(plan,plan.hash(),AUTHORIZED));
            assertEquals(List.of(plan.transactionalId(),plan.transactionalId()),broker.producers);
            assertEquals(1,broker.commitCalls);
            assertEquals(2,new JdbcKafkaNoticeLedger(reopened).existing(plan).number());
            assertFalse(broker.openUncommitted);
        }
    }

    @Test void staleAttemptAndChangedApprovalCannotCommit()throws Exception {
        try(JdbcJournal journal=new JdbcJournal(settings)){
            JdbcKafkaNoticeLedger ledger=new JdbcKafkaNoticeLedger(journal);
            JdbcKafkaNoticeLedger.Entry old=ledger.start(plan,AUTHORIZED);
            JdbcKafkaNoticeLedger.Entry current=ledger.start(plan,AUTHORIZED);
            assertEquals("KAFKA_NOTICE_ATTEMPT_FENCED",assertThrows(IllegalStateException.class,
                    ()->ledger.commitIntent(plan,old.number(),25,AUTHORIZED)).getMessage());
            ledger.commitIntent(plan,current.number(),26,AUTHORIZED);
            Plan changed=new Plan(plan.failure(),plan.topic(),Codec.sha256("different preceding receipt"));
            assertEquals(plan.id(),changed.id());
            Native broker=new Native();
            assertEquals("KAFKA_NOTICE_APPROVAL_CHANGED",assertThrows(IllegalStateException.class,
                    ()->protocol(journal,broker).publish(changed,changed.hash(),AUTHORIZED)).getMessage());
            assertTrue(broker.producers.isEmpty());
        }
    }

    @Test void restoredLedgerUsesRetainedReceiptAndNeverStoresBusinessRows()throws Exception {
        Native broker=new Native();
        try(JdbcJournal journal=new JdbcJournal(settings)){
            KafkaRecoveryNotice protocol=protocol(journal,broker);
            Receipt receipt=protocol.publish(plan,plan.hash(),AUTHORIZED);
            assertNotNull(receipt);
            FailureReviewTest.sql(journal,"DELETE FROM "+journal.table("kafka_recovery_notice"));
            assertEquals(receipt,protocol.publish(plan,plan.hash(),AUTHORIZED));
            assertEquals(1,broker.commitCalls);
            String published=new String(broker.nativeBytes,StandardCharsets.UTF_8);
            assertTrue(published.contains(KafkaRecoveryNotice.KIND));
            assertFalse(published.contains(FailureReviewTest.tx(1).id));
            assertFalse(published.contains("before"));
            assertFalse(published.contains("after"));
            assertFalse(published.contains(plan.failure().contentSha256()));
            List<String> fields=new ArrayList<>();
            Codec.JSON.readTree(published).fieldNames().forEachRemaining(fields::add);
            assertEquals(List.of("approvalScopeSha256","failedSequence","kind","noticeId"),fields.stream().sorted().toList());
        }
    }

    @Test void resumedTaskAndRevokedAuthorityFenceInitialSend()throws Exception {
        Native broker=new Native();
        try(JdbcJournal journal=new JdbcJournal(settings)){
            assertThrows(SecurityException.class,()->protocol(journal,broker).publish(plan,plan.hash(),()->{
                throw new SecurityException("AUTHORIZATION_CHANGED");
            }));
            assertTrue(broker.producers.isEmpty());
            journal.resumePipeline(1);
            assertEquals("KAFKA_NOTICE_REQUIRES_PIPELINE_PAUSED",assertThrows(IllegalStateException.class,
                    ()->protocol(journal,broker).publish(plan,plan.hash(),AUTHORIZED)).getMessage());
            assertTrue(broker.producers.isEmpty());
        }
    }

    @Test void authorityLossAfterNativeSendRefusesCommit()throws Exception {
        Native broker=new Native();
        AtomicBoolean allowed=new AtomicBoolean(true);
        broker.afterSend=()->allowed.set(false);
        Authority authority=()->{if(!allowed.get())throw new SecurityException("AUTHORIZATION_CHANGED");};
        try(JdbcJournal journal=new JdbcJournal(settings)){
            assertThrows(SecurityException.class,()->protocol(journal,broker).publish(plan,plan.hash(),authority));
            assertEquals(0,broker.commitCalls);
            assertFalse(new JdbcKafkaNoticeLedger(journal).existing(plan).hasCommitIntent());
        }
    }

    private static KafkaRecoveryNotice protocol(JdbcJournal journal,Native broker){
        return new KafkaRecoveryNotice(new JdbcKafkaNoticeLedger(journal),broker);
    }

    private static final class Native implements KafkaRecoveryNotice.Broker {
        boolean commitReplyLost,commitInvisible,sendReplyLost,openUncommitted;
        byte[] nativeBytes;
        Runnable afterSend=()->{},afterRead=()->{};
        int commitCalls;
        Receipt receipt;
        final List<Long> requestedOffsets=new ArrayList<>();
        final List<String> producers=new ArrayList<>();

        @Override public Receipt read(Plan plan,Long offset){
            if(offset==null&&openUncommitted)throw new IllegalStateException("KAFKA_NOTICE_HISTORY_UNAVAILABLE");
            if(offset!=null){requestedOffsets.add(offset);afterRead.run();}
            return receipt;
        }

        @Override public Attempt begin(Plan plan){
            producers.add(plan.transactionalId());
            openUncommitted=false;
            long offset=40L+producers.size();
            return new Attempt(){
                byte[] bytes;
                @Override public long send(byte[] payload)throws IOException {
                    bytes=payload.clone();
                    openUncommitted=true;
                    afterSend.run();
                    if(sendReplyLost)throw new IOException("send reply lost");
                    return offset;
                }
                @Override public void commit()throws IOException{
                    commitCalls++;
                    openUncommitted=false;
                    if(!commitInvisible){
                        nativeBytes=bytes;
                        receipt=new Receipt(plan.id(),plan.topic(),offset,Codec.sha256(bytes));
                    }
                    if(commitReplyLost)throw new IOException("commit reply lost");
                }
                @Override public void close(){}
            };
        }
    }
}
