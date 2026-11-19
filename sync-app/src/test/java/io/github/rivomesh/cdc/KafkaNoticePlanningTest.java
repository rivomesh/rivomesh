package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.KafkaNoticePlanning.Administrator;
import io.github.rivomesh.cdc.KafkaNoticePlanning.Predecessor;
import io.github.rivomesh.cdc.KafkaNoticePlanning.Prepared;
import io.github.rivomesh.cdc.KafkaNoticePlanning.TaskScope;
import io.github.rivomesh.cdc.KafkaNoticePlanning.TopicReservation;
import io.github.rivomesh.cdc.KafkaRecoveryReview.Observation;
import io.github.rivomesh.cdc.KafkaRecoveryReview.OriginalFailure;
import io.github.rivomesh.cdc.KafkaRecoveryReview.Receipt;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class KafkaNoticePlanningTest {
    static final String TARGET="slow",TASK="pipeline";
    static final String CLUSTER=Codec.sha256("pre-provisioned cluster"),BUSINESS=Codec.sha256("native business topic");
    @TempDir Path directory;
    private PipelineConfig config;
    private TaskScope task;
    private Administrator admin;
    private Predecessor predecessor;
    private TopicReservation reservation;
    private Replay reads;

    @BeforeEach void seed()throws Exception{
        config=new PipelineConfig();
        config.pipelineId=TASK;
        config.source.username="fixture";
        config.databases=List.of("db");
        config.journal.path=directory.resolve("journal").toString();
        config.targets=List.of(target(TARGET,"business-topic"),target("fast","peer-topic"));
        config.validate();
        config.prepareLocalDirectory();
        task=new TaskScope(TASK,"node-a",id(),id(),id());
        admin=new Administrator("planning-admin",id());
        reservation=new TopicReservation(id(),TASK,TARGET,"dedicated-control",CLUSTER,Uuid.randomUuid().toString());
        try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
            journal.initialize(config.fingerprint(),"generation",config.subscribers());
            Journal.Lease writer=journal.claim("fixture",30000);
            journal.append(writer,FailureReviewTest.tx(1));
            FailureReviewTest.normalize(journal,writer);
            journal.acknowledge(writer,TARGET,1);
            journal.acknowledge(writer,"fast",1);
            journal.append(writer,FailureReviewTest.tx(2));
            FailureReviewTest.normalize(journal,writer);
            SubscriberFailure failure=FailureReviewTest.failure();
            failure.sequence=2;failure.transactionId=FailureReviewTest.tx(2).id;
            journal.pauseFailure(writer,TARGET,"KAFKA_FIXTURE_REFUSED",failure);
            journal.acknowledge(writer,"fast",2);
            journal.pausePipeline();
            Journal.FailureReview original=journal.inspectFailure(TARGET,1,100000);
            OriginalFailure binding=new OriginalFailure(original.generation,KafkaRecoveryReview.failureContentSha256(original),CLUSTER);
            predecessor=recorded(observation(Long.toString(original.epoch),binding,Codec.sha256("acknowledged Kafka wire bytes")));
        }
        reads=new Replay(predecessor.observation());
    }

    @Test void preparesStableApprovalWithoutWritingJournalOrOpeningProducer()throws Exception{
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            Journal.FailureReview before=journal.inspectFailure(TARGET,1,100000);
            Prepared reviewed=inspect();
            assertEquals(reviewed,inspect());
            assertEquals(reservation.topicId(),reviewed.controlTopic().topicId());
            assertEquals("generation",reviewed.failure().journalGeneration());
            assertEquals("1",reviewed.failure().acknowledgedSequence());
            assertEquals("2",reviewed.failure().failedSequence());
            assertEquals(predecessor.requestId(),reviewed.predecessorRequestId());
            assertEquals(predecessor.actorId(),reviewed.predecessorActorId());
            assertEquals(predecessor.digest(),reviewed.predecessorReceiptSha256());
            assertEquals(reads.plan.id(),reviewed.noticeId());
            assertEquals(reads.plan.payloadSha256(),reviewed.payloadSha256());
            assertEquals(reads.fresh.observedTopicIdentitySha256(),reviewed.catalogIdentity().businessTopicSha256());
            assertEquals(Codec.sha256("control topic identity"),reviewed.catalogIdentity().controlTopicSha256());
            assertDoesNotThrow(()->KafkaNoticePlanning.validateApproval(reviewed,reviewed.approvalSha256(),inspect()));
            assertFalse(Codec.encode(reviewed).contains(FailureReviewTest.tx(2).id));
            Journal.FailureReview after=journal.inspectFailure(TARGET,1,100000);
            assertEquals(before.rawHash,after.rawHash);
            assertEquals(before.normalizedHash,after.normalizedHash);
            assertEquals(1,after.subscription.acknowledged);
            assertEquals(2,journal.subscriptions().stream().filter(item->item.id.equals("fast")).findFirst().orElseThrow().acknowledged);
            assertThrows(SQLException.class,()->FailureReviewTest.sql(journal,"SELECT 1 FROM "+journal.table("kafka_recovery_notice")));
        }
    }

    @Test void aPreviouslyStoredLegacyReviewRemainsReadableAndCannotApproveANotice()throws Exception{
        Observation old=observation(predecessor.observation().epoch(),null,predecessor.observation().nativeReceipts().get(0).payloadSha256());
        String encoded=Codec.encode(old);
        assertFalse(Codec.JSON.readTree(encoded).has("originalFailure"));
        assertEquals(old,Codec.decode(encoded,Observation.class));
        predecessor=recorded(old);
        code("KAFKA_NOTICE_PREDECESSOR_REFRESH_REQUIRED",this::inspect);
        assertEquals(0,reads.predecessors);
        assertEquals(0,reads.controlTopics);
    }

    @Test void declinesWrongAuthorityScopeOrAnotherBusinessTopicBeforeTouchingKafka()throws Exception{
        TaskScope moved=new TaskScope(TASK,"node-b",task.ownerDirectoryId(),task.configurationVersion(),task.revision());
        code("KAFKA_NOTICE_PREDECESSOR_SCOPE_CHANGED",()->KafkaNoticePlanning.inspect(config,moved,admin,predecessor,reservation,reads));
        TopicReservation collision=new TopicReservation(id(),TASK,TARGET,"peer-topic",CLUSTER,Uuid.randomUuid().toString());
        code("KAFKA_NOTICE_CONTROL_TOPIC_REQUIRED",()->KafkaNoticePlanning.inspect(config,task,admin,predecessor,collision,reads));
        TopicReservation clusterChanged=new TopicReservation(id(),TASK,TARGET,"dedicated-control",Codec.sha256("another cluster"),reservation.topicId());
        code("KAFKA_NOTICE_CONTROL_CLUSTER_CHANGED",()->KafkaNoticePlanning.inspect(config,task,admin,predecessor,clusterChanged,reads));
        assertEquals(0,reads.predecessors);
        assertEquals(0,reads.controlTopics);
    }

    @Test void blocksDifferentReceiptBytesAndOriginalFailureGeneration()throws Exception{
        Observation original=predecessor.observation();
        reads.fresh=observation(original.epoch(),original.originalFailure(),Codec.sha256("different native body"));
        code("KAFKA_NOTICE_PREDECESSOR_CHANGED",this::inspect);
        assertEquals(1,reads.predecessors);
        assertEquals(0,reads.controlTopics);
        OriginalFailure replaced=new OriginalFailure("another-generation",original.originalFailure().contentSha256(),CLUSTER);
        predecessor=recorded(observation(original.epoch(),replaced,original.nativeReceipts().get(0).payloadSha256()));
        code("KAFKA_NOTICE_FAILURE_CHANGED",this::inspect);
        assertEquals(1,reads.predecessors);
    }

    @Test void approvalRejectsChangedAdministratorTaskRevisionOrControlTopicUuid()throws Exception{
        Prepared original=inspect();
        Prepared changedActor=KafkaNoticePlanning.inspect(config,task,new Administrator(admin.actorId(),id()),predecessor,reservation,reads);
        code("KAFKA_NOTICE_ADMINISTRATOR_CHANGED",()->KafkaNoticePlanning.validateApproval(original,original.approvalSha256(),changedActor));
        TaskScope next=new TaskScope(TASK,task.ownerNodeId(),task.ownerDirectoryId(),task.configurationVersion(),id());
        Prepared changedRevision=KafkaNoticePlanning.inspect(config,next,admin,predecessor,reservation,reads);
        code("KAFKA_NOTICE_PLAN_CHANGED",()->KafkaNoticePlanning.validateApproval(original,original.approvalSha256(),changedRevision));
        TopicReservation replacement=new TopicReservation(reservation.reservationId(),TASK,TARGET,reservation.topicName(),CLUSTER,Uuid.randomUuid().toString());
        Prepared changedTopic=KafkaNoticePlanning.inspect(config,task,admin,predecessor,replacement,reads);
        code("KAFKA_NOTICE_PLAN_CHANGED",()->KafkaNoticePlanning.validateApproval(original,original.approvalSha256(),changedTopic));
        code("KAFKA_NOTICE_APPROVAL_CHANGED",()->KafkaNoticePlanning.validateApproval(original,Codec.sha256("wrong digest"),original));
    }

    @Test void aNativeCheckCanNeverApproveAResumedTask()throws Exception{
        reads.onControl=()->{
            try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){journal.resumePipeline(1);}
        };
        code("KAFKA_NOTICE_REQUIRES_PIPELINE_PAUSED",this::inspect);
        assertEquals(1,reads.controlTopics);
    }

    private Prepared inspect()throws Exception{return KafkaNoticePlanning.inspect(config,task,admin,predecessor,reservation,reads);}
    private Predecessor recorded(Observation observation){
        return new Predecessor("durable_prior_review",TASK,task.ownerNodeId(),task.ownerDirectoryId(),task.configurationVersion(),"review-admin",observation);
    }
    private static Observation observation(String epoch,OriginalFailure original,String hash){
        return new Observation(KafkaRecoveryReview.VERIFIED,TARGET,"2026-11-19T01:00:00Z",epoch,"1","1","2",BUSINESS,
                List.of(new Receipt("orders","31",hash)),original);
    }
    private static PipelineConfig.Target target(String id,String topic){
        PipelineConfig.Target target=new PipelineConfig.Target();
        target.product=TargetProduct.KAFKA;target.id=id;target.url="kafka://fixture:9092";
        target.messaging=new PipelineConfig.Messaging();target.messaging.topic=topic;
        target.mappings.put("db","orders");return target;
    }
    private static String id(){return UUID.randomUUID().toString();}
    private static void code(String message,org.junit.jupiter.api.function.Executable action){
        assertEquals(message,assertThrows(IllegalStateException.class,action).getMessage());
    }
    @FunctionalInterface interface Check {void run()throws Exception;}
    private static final class Replay implements KafkaNoticePlanning.NativeReads {
        Observation fresh;
        KafkaRecoveryNotice.Plan plan;
        Check onControl=()->{};
        int predecessors,controlTopics;

        Replay(Observation captured){
            fresh=new Observation(captured.status(),captured.target(),"2026-11-19T03:00:00Z",captured.epoch(),
                    captured.errorGeneration(),captured.acknowledgedSequence(),captured.failedSequence(),
                    captured.observedTopicIdentitySha256(),captured.nativeReceipts(),captured.originalFailure());
        }
        @Override public Observation inspectPredecessor(PipelineConfig config,String target,long generation){
            predecessors++;
            assertEquals(TARGET,target);
            assertEquals(1,generation);
            return fresh;
        }
        @Override public String inspectControlTopic(PipelineConfig.Target target,KafkaRecoveryNotice.Plan plan)throws Exception{
            controlTopics++;
            this.plan=plan;
            onControl.run();
            return Codec.sha256("control topic identity");
        }
    }
}
