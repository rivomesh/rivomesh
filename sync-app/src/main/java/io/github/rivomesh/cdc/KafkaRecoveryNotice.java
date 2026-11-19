package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.FailureEvidence;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.Uuid;

/** Internal notice protocol. A notice describes a reviewed failure while delivery remains unresolved. */
final class KafkaRecoveryNotice {
    static final String KIND="RIVOMESH_KAFKA_FAILURE_OBSERVED_V1";

    record Topic(String name,String clusterSha256,String topicId) {
        Topic {
            if(name==null||!name.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,248}")||!FailureEvidence.hash(clusterSha256)
                    ||topicId==null||!topicId.matches("[A-Za-z0-9_-]{22}")||Uuid.ZERO_UUID.equals(Uuid.fromString(topicId)))
                throw new IllegalArgumentException("KAFKA_NOTICE_TOPIC_INVALID");
        }
    }

    record Failure(String pipeline,String target,String generation,long epoch,long errorGeneration,long sequence,String contentSha256) {
        Failure {
            if(!validId(pipeline)||!validId(target)||generation==null||generation.isBlank()||generation.length()>128
                    ||epoch<0||errorGeneration<1||sequence<1||!FailureEvidence.hash(contentSha256))
                throw new IllegalArgumentException("KAFKA_NOTICE_FAILURE_INVALID");
        }
    }

    record Plan(Failure failure,Topic topic,String precedingReceiptSha256) {
        Plan {
            Objects.requireNonNull(failure);
            Objects.requireNonNull(topic);
            if(!FailureEvidence.hash(precedingReceiptSha256))throw new IllegalArgumentException("KAFKA_NOTICE_PRIOR_RECEIPT_REQUIRED");
        }

        String id(){
            return Codec.sha256(Codec.encode(List.of(KIND,failure.pipeline(),failure.generation(),failure.target(),failure.sequence())));
        }
        String hash(){return Codec.sha256(Codec.encode(this));}
        String transactionalId(){return "rivomesh-kafka-notice-"+id();}
        byte[] payload(){
            return Codec.encode(Map.of("kind",KIND,"noticeId",id(),"approvalScopeSha256",hash(),
                    "failedSequence",Long.toString(failure.sequence()))).getBytes(StandardCharsets.UTF_8);
        }
        String payloadSha256(){return Codec.sha256(payload());}
    }

    record Receipt(String noticeId,Topic topic,long offset,String payloadSha256) {}

    @FunctionalInterface
    interface Authority {
        void verify()throws Exception;
        default void commit(KafkaNoticePlanning.CommitAction action)throws Exception{
            verify();
            action.run();
        }
    }

    interface Broker {
        /** Reads a pre-owned topic without a producer. A null offset searches its bounded retained history. */
        Receipt read(Plan plan,Long offset)throws Exception;
        Attempt begin(Plan plan)throws Exception;
        default boolean confirmedAborted(Plan plan,long offset)throws Exception{return false;}
    }

    interface Attempt extends AutoCloseable {
        long send(byte[] payload)throws Exception;
        void commit()throws Exception;
        @Override void close();
    }

    private final JdbcKafkaNoticeLedger ledger;
    private final Broker broker;

    KafkaRecoveryNotice(JdbcKafkaNoticeLedger ledger,Broker broker){
        this.ledger=Objects.requireNonNull(ledger);
        this.broker=Objects.requireNonNull(broker);
    }

    /** Returns null when recovery of a persisted commit intent has no verifiable native receipt. */
    Receipt publish(Plan plan,String approvedHash,Authority authority)throws Exception {
        if(!plan.hash().equals(approvedHash))throw new IllegalStateException("KAFKA_NOTICE_APPROVAL_CHANGED");
        authority.verify();
        JdbcKafkaNoticeLedger.Entry existing=ledger.existing(plan);
        if(existing!=null&&existing.hasCommitIntent())return recover(plan,existing,authority);

        // A restored ledger can lose the attempt but retain its exact Kafka record.
        if(existing==null){
            Receipt retained=retained(plan,authority);
            if(retained!=null)return retained;
        }
        JdbcKafkaNoticeLedger.Entry attempt=ledger.start(plan,authority);
        if(attempt.hasCommitIntent())return recover(plan,attempt,authority);

        try(Attempt nativeAttempt=broker.begin(plan)){
            // Initialization has fenced any earlier uncommitted send. Examine the settled
            // history before retrying, including when a restored ledger lost a commit intent.
            if(attempt.number()>1){
                Receipt retained=retained(plan,authority);
                if(retained!=null)return retained;
            }
            long offset=nativeAttempt.send(plan.payload());
            ledger.commitIntent(plan,attempt.number(),offset,authority);
            // The durable intent precedes the only call that can make this record visible.
            ledger.commitUnderFence(plan,attempt.number(),offset,authority,nativeAttempt);
        }
        return recover(plan,ledger.existing(plan),authority);
    }

    private Receipt retained(Plan plan,Authority authority)throws Exception{
        Receipt receipt=broker.read(plan,null);
        if(receipt==null)return null;
        requireReceipt(plan,null,receipt);
        return ledger.recordRetained(plan,receipt,authority);
    }

    private Receipt recover(Plan plan,JdbcKafkaNoticeLedger.Entry entry,Authority authority)throws Exception {
        authority.verify();
        // A JDBC row alone cannot establish a native receipt, even after a previous verified read.
        Receipt nativeReceipt=broker.read(plan,entry.offset());
        if(nativeReceipt==null){
            if(!entry.verified()&&broker.confirmedAborted(plan,entry.offset()))
                ledger.retireAborted(plan,entry,authority);
            return null;
        }
        requireReceipt(plan,entry.offset(),nativeReceipt);
        return ledger.recordVerified(plan,nativeReceipt,authority);
    }

    private static void requireReceipt(Plan plan,Long expected,Receipt observed){
        if(!plan.id().equals(observed.noticeId())||!plan.topic().equals(observed.topic())
                ||observed.offset()<0||expected!=null&&expected.longValue()!=observed.offset()
                ||!plan.payloadSha256().equals(observed.payloadSha256()))
            throw new IllegalStateException("KAFKA_NOTICE_NATIVE_RECEIPT_CONFLICT");
    }

    private static boolean validId(String value){return value!=null&&value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");}
}
