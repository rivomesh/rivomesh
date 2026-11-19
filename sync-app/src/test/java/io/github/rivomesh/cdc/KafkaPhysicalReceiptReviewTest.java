package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.PipelineControls;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class KafkaPhysicalReceiptReviewTest {
    private static final String TASK="pipeline",TARGET="slow",GENERATION="generation",CLUSTER="native-fixture";
    private static final String CLUSTER_HASH=Codec.sha256("KAFKA:"+CLUSTER);
    private static final String WIRE=Codec.sha256("retained Kafka COMMIT bytes");
    private static final String KEY=Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
    @TempDir Path directory;

    @Test void onlyExactAuthenticatedReceiptForCurrentFailureReturnsRedactedEvidence()throws Exception{
        var fixture=seed(1);
        Map<String,Object> result=fixture.inspect((target,expected)->fixture.topic());
        assertEquals("VERIFIED",result.get("status"));
        assertEquals("1",result.get("acknowledgedSequence"));
        assertEquals(fixture.scope().configurationVersion(),result.get("configurationVersion"));
        assertTrue(result.get("receiptSha256").toString().matches("[0-9a-f]{64}"));
        assertTrue(result.containsKey("publishedAt"));
        assertTrue(result.containsKey("acknowledgedAt"));
        String redacted=Codec.encode(result);
        assertFalse(redacted.contains(KEY));
        assertFalse(redacted.contains(WIRE));
        assertFalse(redacted.contains(fixture.topic().topicId()));
        assertFalse(redacted.contains(fixture.member().member().incarnation()));
        assertFalse(result.containsKey("executionAuthorized"));
        try(var journal=new JdbcJournal(fixture.config().existingJournalSettings())){
            assertEquals(1,journal.subscriptions().getFirst().acknowledged);
            assertEquals(2,journal.inspectFailure(TARGET,1,100000).sequence);
        }
    }

    @Test void oldOrUntrustedEvidenceNeverReachesBrokerInspection()throws Exception{
        var earlier=seed(2);
        assertEquals("MISSING",earlier.inspect((t,p)->fail("The earlier COMMIT is outside this exact failure cut")).get("status"));
    }

    @Test void absentOrForgedWitnessIsMissingAndBrokerOutageIsUnreachable()throws Exception{
        var fixture=seed(1);
        var unavailable=fixture.inspect((target,expected)->{throw new IOException("internal.example:9092");});
        assertEquals("UNREACHABLE",unavailable.get("status"));
        assertFalse(Codec.encode(unavailable).contains("internal.example"));
        var replaced=new KafkaPhysicalReceipt.Topic(CLUSTER_HASH,Uuid.randomUuid().toString());
        assertEquals("KAFKA_PHYSICAL_RECEIPT_TOPIC_CHANGED",fixture.inspect((t,p)->replaced).get("code"));

        var invalid=KafkaPhysicalReceiptReview.inspect(fixture.config(),fixture.scope(),fixture.predecessor(),KEY,witness->false,
                (target,expected)->fail("The authority signature must be verified before a broker query"));
        assertEquals("MISSING",invalid.get("status"));
        try(var journal=new JdbcJournal(fixture.config().existingJournalSettings())){
            FailureReviewTest.sql(journal,"DROP TABLE "+journal.table("kafka_physical_attested"));
        }
        assertEquals("MISSING",fixture.inspect((t,p)->fail("There is no attested table")).get("status"));
    }

    @Test void resumeDuringMetadataReadInvalidatesTheBoundFailure()throws Exception{
        var fixture=seed(1);
        FailureReviewTest.code("KAFKA_PHYSICAL_RECEIPT_FAILURE_CHANGED",()->fixture.inspect((t,p)->{
            try(var journal=new JdbcJournal(fixture.config().existingJournalSettings())){journal.resumePipeline(1);}
            return fixture.topic();
        }));
    }

    private Fixture seed(int acknowledged)throws Exception{
        var config=new PipelineConfig();config.pipelineId=TASK;config.source.username="fixture";config.databases=List.of("db");
        config.journal.path=directory.resolve("journal").toString();
        var target=new PipelineConfig.Target();target.id=TARGET;target.product=TargetProduct.KAFKA;target.url="kafka://fixture:9092";
        target.messaging=new PipelineConfig.Messaging();target.messaging.topic="fixture-proof";
        target.mappings.put("db","orders");config.targets=List.of(target);config.validate();config.prepareLocalDirectory();
        var scope=new KafkaPhysicalReceipt.Scope(id(),TASK,id(),id(),Codec.sha256("active projection"));
        var member=new KafkaPhysicalReceipt.Member(scope,"owner",id(),2,4102444800000L,4102444805000L);
        var witness=new KafkaPhysicalReceipt.MemberWitness(member,KEY);
        var grant=new KafkaPhysicalReceipt.Grant(scope,KEY,witness);
        var settings=config.journalSettings();settings.kafkaPhysicalGrant=grant;
        var topic=new KafkaPhysicalReceipt.Topic(CLUSTER_HASH,Uuid.randomUuid().toString());
        KafkaNoticePlanning.Predecessor predecessor;
        try(var journal=new JdbcJournal(settings)){
            journal.initialize(config.fingerprint(),GENERATION,config.subscribers());
            var lease=journal.claim("fixture",30000);
            var publication=new JdbcPublicationStore(journal,TARGET,lease);publication.activate(lease.epoch);publication.identity(lease.epoch,"KAFKA:"+CLUSTER);
            for(int sequence=1;sequence<=acknowledged;sequence++){
                var tx=FailureReviewTest.tx(sequence);journal.append(lease,tx);FailureReviewTest.normalize(journal,lease);
                String hash=Codec.sha256("published transaction "+sequence);
                var observation=new KafkaPhysicalReceipt.Observation(topic,List.of(new KafkaPhysicalReceipt.Wire("orders",31,WIRE)));
                if(sequence==1)publication.committed(lease.epoch,sequence,tx.id,hash,grant.sign(TARGET,GENERATION,lease.epoch,sequence,hash,observation));
                else publication.committed(lease.epoch,sequence,tx.id,hash);
                journal.acknowledge(lease,TARGET,sequence);
            }
            var tx=FailureReviewTest.tx(acknowledged+1);journal.append(lease,tx);FailureReviewTest.normalize(journal,lease);
            var failed=FailureReviewTest.failure();failed.sequence=acknowledged+1;failed.transactionId=tx.id;
            journal.pauseFailure(lease,TARGET,"KAFKA_FIXTURE_REFUSED",failed);
            PipelineControls.pause(config);
            var cut=journal.inspectFailure(TARGET,1,100000);
            var original=new KafkaRecoveryReview.OriginalFailure(cut.generation,KafkaRecoveryReview.failureContentSha256(cut),CLUSTER_HASH);
            var observation=new KafkaRecoveryReview.Observation(KafkaRecoveryReview.VERIFIED,TARGET,"2026-11-19T00:00:00Z",
                    Long.toString(cut.epoch),"1",Integer.toString(acknowledged),Long.toString(cut.sequence),
                    KafkaRecoveryReview.topicIdentity(CLUSTER,Uuid.fromString(topic.topicId())),
                    List.of(new KafkaRecoveryReview.Receipt("orders","31",WIRE)),original);
            predecessor=new KafkaNoticePlanning.Predecessor("prior_kafka_review",TASK,member.node(),scope.ownerDirectory(),
                    scope.configurationVersion(),"fixture-admin",observation);
        }
        return new Fixture(config,scope,witness,topic,predecessor);
    }

    private static String id(){return UUID.randomUUID().toString();}
    private record Fixture(PipelineConfig config,KafkaPhysicalReceipt.Scope scope,KafkaPhysicalReceipt.MemberWitness member,
                           KafkaPhysicalReceipt.Topic topic,KafkaNoticePlanning.Predecessor predecessor){
        Map<String,Object> inspect(KafkaPhysicalReceiptReview.TopicRead reader)throws Exception{
            return KafkaPhysicalReceiptReview.inspect(config,scope,predecessor,KEY,member::equals,reader);
        }
    }
}
