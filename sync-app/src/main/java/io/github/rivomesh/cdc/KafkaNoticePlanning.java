package io.github.rivomesh.cdc;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.KafkaRecoveryReview.Observation;
import io.github.rivomesh.cdc.KafkaRecoveryReview.OriginalFailure;
import io.github.rivomesh.recovery.FailureEvidence;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Metadata-only notice planning. Inputs carrying authority come from the authenticated control store. */
public final class KafkaNoticePlanning {
    public record TaskScope(String taskId,String ownerNodeId,String ownerDirectoryId,String configurationVersion,
                            String revision) {
        public TaskScope {
            if(!id(taskId)||!id(ownerNodeId)||!uuid(ownerDirectoryId)||!uuid(configurationVersion)||!uuid(revision))
                throw rejected("KAFKA_NOTICE_TASK_SCOPE_INVALID");
        }
    }

    public record Administrator(String actorId,String authorizationRevision) {
        public Administrator {
            if(!id(actorId)||!uuid(authorizationRevision))throw rejected("KAFKA_NOTICE_ADMINISTRATOR_INVALID");
        }
    }

    /** The completed, authenticated command record retained by shared management. */
    public record Predecessor(String requestId,String taskId,String ownerNodeId,String ownerDirectoryId,
                              String configurationVersion,String actorId,Observation observation) {
        public Predecessor {
            if(requestId==null||!requestId.matches("[A-Za-z0-9_-]{8,128}")||!id(taskId)||!id(ownerNodeId)
                    ||!uuid(ownerDirectoryId)||!uuid(configurationVersion)||!id(actorId)||observation==null)
                throw rejected("KAFKA_NOTICE_PREDECESSOR_INVALID");
        }

        String digest(){return Codec.sha256(Codec.encode(this));}
    }

    /** A dedicated topic already reserved for this task and destination by management. */
    public record TopicReservation(String reservationId,String taskId,String targetId,String topicName,
                                   String kafkaClusterSha256,String topicId) {
        public TopicReservation {
            if(!uuid(reservationId)||!id(taskId)||!id(targetId))throw rejected("KAFKA_NOTICE_CONTROL_TOPIC_REQUIRED");
            new KafkaRecoveryNotice.Topic(topicName,kafkaClusterSha256,topicId);
        }
    }

    public record FailedTransaction(String targetId,String journalGeneration,String epoch,String errorGeneration,
                                    String acknowledgedSequence,String failedSequence,String contentSha256) {}

    /** Both physical identities were observed by the routed current owner at the same reviewed failure. */
    public record CatalogIdentity(String businessTopicSha256,String controlTopicSha256) {
        public CatalogIdentity {
            if(!FailureEvidence.hash(businessTopicSha256)||!FailureEvidence.hash(controlTopicSha256))
                throw rejected("KAFKA_NOTICE_CATALOG_IDENTITY_INVALID");
        }
    }

    public record Prepared(TaskScope task,Administrator administrator,String predecessorRequestId,String predecessorActorId,
                           String predecessorReceiptSha256,TopicReservation controlTopic,FailedTransaction failure,
                           String noticeId,String payloadSha256,@JsonInclude(JsonInclude.Include.NON_NULL) CatalogIdentity catalogIdentity) {
        public String approvalSha256(){return Codec.sha256(Codec.encode(this));}
    }

    @FunctionalInterface
    public interface CommitAction { void run()throws Exception; }

    public interface DispatchFence {
        void verify()throws Exception;
        void commit(CommitAction action)throws Exception;
    }

    public record DispatchReceipt(String noticeId,String targetId,String topicId,long nativeOffset,String payloadSha256){
        public DispatchReceipt {
            if(!FailureEvidence.hash(noticeId)||!id(targetId)||topicId==null||!topicId.matches("[A-Za-z0-9_-]{22}")
                    ||nativeOffset<0||!FailureEvidence.hash(payloadSha256))
                throw rejected("KAFKA_NOTICE_RECEIPT_INVALID");
        }
    }

    /** Runs only for an authenticated durable request, with the exact approval and a live authority fence. */
    public static DispatchReceipt dispatch(PipelineConfig config,Prepared prepared,List<String> endpoints,String username,String password,
                                           DispatchFence fence)throws Exception{
        if(prepared==null||fence==null||!config.pipelineId.equals(prepared.task().taskId())||!config.kafkaHistoryFromFirstSend)
            throw rejected("KAFKA_NOTICE_APPROVAL_CHANGED");
        var failure=prepared.failure();
        var control=prepared.controlTopic();
        var target=config.targets.stream().filter(t->t.product==TargetProduct.KAFKA&&failure.targetId().equals(t.id))
                .findFirst().orElseThrow(()->rejected("KAFKA_NOTICE_TARGET_UNSUPPORTED"));
        var fixed=target.messaging.physicalTopic;
        if(fixed==null||!fixed.clusterSha256().equals(control.kafkaClusterSha256())||fixed.topicId().equals(control.topicId()))
            throw rejected("KAFKA_NOTICE_PHYSICAL_HISTORY_CHANGED");
        var topic=new KafkaRecoveryNotice.Topic(control.topicName(),control.kafkaClusterSha256(),control.topicId());
        KafkaRecoveryNotice.Authority authority=new KafkaRecoveryNotice.Authority(){
            @Override public void verify()throws Exception{fence.verify();}
            @Override public void commit(CommitAction action)throws Exception{fence.commit(action);}
        };
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            var ledger=new JdbcKafkaNoticeLedger(journal);
            var plan=ledger.prepare(target.id,Long.parseLong(failure.errorGeneration()),topic,prepared.predecessorReceiptSha256(),authority);
            if(!prepared.noticeId().equals(plan.id())||!prepared.payloadSha256().equals(plan.payloadSha256())
                    ||!failure.journalGeneration().equals(plan.failure().generation())
                    ||!failure.contentSha256().equals(plan.failure().contentSha256())
                    ||!failure.epoch().equals(Long.toString(plan.failure().epoch())))
                throw rejected("KAFKA_NOTICE_FAILURE_CHANGED");
            ledger.provision();
            var broker=KafkaNoticeBroker.physicallyAddressed(target,endpoints,username,password);
            var receipt=new KafkaRecoveryNotice(ledger,broker).publish(plan,plan.hash(),authority);
            return receipt==null?null:new DispatchReceipt(receipt.noticeId(),target.id,topic.topicId(),receipt.offset(),receipt.payloadSha256());
        }
    }

    interface NativeReads {
        Observation inspectPredecessor(PipelineConfig config,String target,long errorGeneration)throws Exception;
        String inspectControlTopic(PipelineConfig.Target target,KafkaRecoveryNotice.Plan plan)throws Exception;
    }

    private KafkaNoticePlanning(){}

    public static Prepared inspect(PipelineConfig config,TaskScope task,Administrator administrator,Predecessor predecessor,
                                   TopicReservation reservation)throws Exception{
        return inspect(config,task,administrator,predecessor,reservation,new NativeReads(){
            @Override public Observation inspectPredecessor(PipelineConfig cfg,String target,long generation)throws Exception{
                return KafkaRecoveryReview.inspect(cfg,target,generation);
            }
            @Override public String inspectControlTopic(PipelineConfig.Target target,KafkaRecoveryNotice.Plan plan)throws Exception{
                return new KafkaNoticeBroker(target).inspectControlTopic(plan);
            }
        });
    }

    static Prepared inspect(PipelineConfig config,TaskScope task,Administrator administrator,Predecessor predecessor,
                            TopicReservation reservation,NativeReads reads)throws Exception{
        Objects.requireNonNull(administrator);
        requireScope(config,task,predecessor,reservation);
        Observation observation=predecessor.observation();
        OriginalFailure original=observation.originalFailure();
        if(original==null)throw rejected("KAFKA_NOTICE_PREDECESSOR_REFRESH_REQUIRED");
        if(!original.kafkaClusterSha256().equals(reservation.kafkaClusterSha256()))
            throw rejected("KAFKA_NOTICE_CONTROL_CLUSTER_CHANGED");
        PipelineConfig.Target target=config.targets.stream().filter(item->item.id.equals(observation.target())
                        &&item.product==TargetProduct.KAFKA).findFirst()
                .orElseThrow(()->rejected("KAFKA_NOTICE_TARGET_UNSUPPORTED"));
        if(config.targets.stream().anyMatch(item->item.product==TargetProduct.KAFKA&&item.messaging!=null
                &&reservation.topicName().equals(item.messaging.topic)))
            throw rejected("KAFKA_NOTICE_CONTROL_TOPIC_REQUIRED");
        Set<String> namespaces=new HashSet<>();
        for(KafkaRecoveryReview.Receipt receipt:observation.nativeReceipts())namespaces.add(receipt.namespace());
        if(!namespaces.equals(new HashSet<>(target.mappings.values())))throw rejected("KAFKA_NOTICE_PREDECESSOR_CHANGED");

        long errorGeneration=Long.parseLong(observation.errorGeneration());
        KafkaRecoveryNotice.Topic topic=new KafkaRecoveryNotice.Topic(reservation.topicName(),reservation.kafkaClusterSha256(),reservation.topicId());
        String predecessorDigest=predecessor.digest();
        KafkaRecoveryNotice.Plan plan;
        CatalogIdentity identity;
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            JdbcKafkaNoticeLedger ledger=new JdbcKafkaNoticeLedger(journal);
            plan=ledger.prepare(target.id,errorGeneration,topic,predecessorDigest,()->{});
            requireFailure(plan.failure(),observation);
            Observation fresh=reads.inspectPredecessor(config,target.id,errorGeneration);
            if(!samePredecessor(observation,fresh))throw rejected("KAFKA_NOTICE_PREDECESSOR_CHANGED");
            String controlIdentity=reads.inspectControlTopic(target,plan);
            identity=new CatalogIdentity(fresh.observedTopicIdentitySha256(),controlIdentity);
            if(!plan.equals(ledger.prepare(target.id,errorGeneration,topic,predecessorDigest,()->{})))
                throw rejected("KAFKA_NOTICE_FAILURE_CHANGED");
        }
        FailedTransaction failure=new FailedTransaction(target.id,original.journalGeneration(),observation.epoch(),
                observation.errorGeneration(),observation.acknowledgedSequence(),observation.failedSequence(),
                original.contentSha256());
        return new Prepared(task,administrator,predecessor.requestId(),predecessor.actorId(),predecessorDigest,
                reservation,failure,plan.id(),plan.payloadSha256(),identity);
    }

    /** Both prepared snapshots must come from management, including a fresh owner-side inspection. */
    public static void validateApproval(Prepared reviewed,String approvedSha256,Prepared current){
        if(reviewed==null||current==null||!FailureEvidence.hash(approvedSha256)
                ||!reviewed.approvalSha256().equals(approvedSha256))
            throw rejected("KAFKA_NOTICE_APPROVAL_CHANGED");
        if(!reviewed.administrator().equals(current.administrator()))throw rejected("KAFKA_NOTICE_ADMINISTRATOR_CHANGED");
        if(!reviewed.equals(current))throw rejected("KAFKA_NOTICE_PLAN_CHANGED");
    }

    private static void requireScope(PipelineConfig config,TaskScope task,Predecessor predecessor,TopicReservation reservation){
        if(!task.taskId().equals(config.pipelineId)||!task.taskId().equals(predecessor.taskId())
                ||!task.ownerNodeId().equals(predecessor.ownerNodeId())||!task.ownerDirectoryId().equals(predecessor.ownerDirectoryId())
                ||!task.configurationVersion().equals(predecessor.configurationVersion()))
            throw rejected("KAFKA_NOTICE_PREDECESSOR_SCOPE_CHANGED");
        if(!task.taskId().equals(reservation.taskId())||!predecessor.observation().target().equals(reservation.targetId()))
            throw rejected("KAFKA_NOTICE_CONTROL_TOPIC_REQUIRED");
    }

    private static void requireFailure(KafkaRecoveryNotice.Failure current,Observation observed){
        if(!current.generation().equals(observed.originalFailure().journalGeneration())
                ||!current.contentSha256().equals(observed.originalFailure().contentSha256())
                ||!Long.toString(current.epoch()).equals(observed.epoch())
                ||!Long.toString(current.sequence()).equals(observed.failedSequence())
                ||!Long.toString(current.sequence()-1).equals(observed.acknowledgedSequence()))
            throw rejected("KAFKA_NOTICE_FAILURE_CHANGED");
    }

    private static boolean samePredecessor(Observation before,Observation fresh){
        return fresh!=null&&before.target().equals(fresh.target())&&before.epoch().equals(fresh.epoch())
                &&before.errorGeneration().equals(fresh.errorGeneration())
                &&before.acknowledgedSequence().equals(fresh.acknowledgedSequence())
                &&before.failedSequence().equals(fresh.failedSequence())
                &&before.originalFailure().equals(fresh.originalFailure())
                &&before.observedTopicIdentitySha256().equals(fresh.observedTopicIdentitySha256())
                &&before.nativeReceipts().equals(fresh.nativeReceipts());
    }

    private static boolean id(String value){return value!=null&&value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");}
    private static boolean uuid(String value){return value!=null&&value.matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}");}
    private static IllegalStateException rejected(String code){return new IllegalStateException(code);}
}
