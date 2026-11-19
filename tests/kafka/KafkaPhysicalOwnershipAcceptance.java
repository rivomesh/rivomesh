package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.api.SourcePosition;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import static org.apache.kafka.common.acl.AclOperation.*;

/** Exact readback and a real WRITE-only counterexample tested against a disposable broker. */
public final class KafkaPhysicalOwnershipAcceptance {
    private static final String TOPIC="physical.owner.readable",WRITE_ONLY="physical.owner.write-only";
    private static final String ROLE="fixture-reviewer",PIPELINE="physical-owner",GENERATION="evidence-generation";
    private static final Map<String,String> KAFKA_CONFIG=Map.of("cleanup.policy","delete","retention.ms","-1","retention.bytes","-1",
            "min.insync.replicas","1","unclean.leader.election.enable","false","max.message.bytes","69632");

    public static void main(String... args)throws Exception{
        String endpoint=args[0],secret=System.getenv("RIVOMESH_KAFKA_FIXTURE_SECRET");
        if(secret==null||secret.length()<32)throw new IllegalArgumentException("Synthetic fixture credentials required");
        var readable=target(endpoint,secret,TOPIC);var denied=target(endpoint,secret,WRITE_ONLY);
        try(Admin root=Admin.create(adminProperties(readable,secret))){
            root.createTopics(List.of(new NewTopic(TOPIC,1,(short)1).configs(KAFKA_CONFIG),
                    new NewTopic(WRITE_ONLY,1,(short)1).configs(KAFKA_CONFIG))).all().get(15,TimeUnit.SECONDS);
            root.createAcls(List.of(acl(ResourceType.TOPIC,TOPIC,WRITE),acl(ResourceType.TOPIC,TOPIC,READ),
                    acl(ResourceType.TOPIC,TOPIC,DESCRIBE_CONFIGS),acl(ResourceType.TOPIC,WRITE_ONLY,WRITE),
                    acl(ResourceType.TOPIC,WRITE_ONLY,DESCRIBE_CONFIGS),acl(ResourceType.CLUSTER,Resource.CLUSTER_NAME,DESCRIBE_CONFIGS),
                    acl(ResourceType.CLUSTER,Resource.CLUSTER_NAME,IDEMPOTENT_WRITE))).all().get(15,TimeUnit.SECONDS);
            try(Admin business=Admin.create(KafkaMessagePublisher.connectionProperties(denied))){
                long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
                for(;;){
                    var descriptions=business.describeTopics(List.of(TOPIC,WRITE_ONLY),
                            new DescribeTopicsOptions().includeAuthorizedOperations(true)).allTopicNames().get(10,TimeUnit.SECONDS);
                    var permitted=descriptions.get(TOPIC).authorizedOperations();
                    var limited=descriptions.get(WRITE_ONLY).authorizedOperations();
                    if(permitted.containsAll(Set.of(READ,WRITE))&&limited.contains(WRITE)&&!limited.contains(READ))break;
                    if(System.nanoTime()>=until)throw new AssertionError("Fixture did not establish the actual READ/WRITE separation");
                    Thread.sleep(100);
                }
            }
            var before=root.describeTopics(List.of(TOPIC,WRITE_ONLY)).allTopicNames().get(10,TimeUnit.SECONDS);
            verifyBusiness(readable,before.get(TOPIC).topicId().toString(),true);
            verifyBusiness(denied,before.get(WRITE_ONLY).topicId().toString(),false);
            var offsets=root.listOffsets(Map.of(new TopicPartition(TOPIC,0),OffsetSpec.latest(),new TopicPartition(WRITE_ONLY,0),
                    OffsetSpec.latest())).all().get(10,TimeUnit.SECONDS);
            if(offsets.size()!=2||offsets.values().stream().anyMatch(position->position.offset()!=2))
                throw new AssertionError("Expected exactly two business events per topic");
            if(!root.listConsumerGroups().all().get(10,TimeUnit.SECONDS).isEmpty())
                throw new AssertionError("Ownership evidence opened a consumer group");
            System.out.println("PASS native COMMIT offset and physical Topic; WRITE-only ACK has no evidence; no duplicate events or groups.");
        }
    }

    private static void verifyBusiness(PipelineConfig.Target target,String topicId,boolean expectEvidence)throws Exception{
        var settings=new JdbcJournal.Settings();settings.pipelineId=PIPELINE;settings.driver="org.h2.Driver";
        settings.url="jdbc:h2:mem:worker_physical_"+UUID.randomUUID()+";DB_CLOSE_DELAY=-1";settings.username="sa";
        settings.kafkaPublicationTargets=Set.of(target.id);
        var scope=new KafkaPhysicalReceipt.Scope(UUID.randomUUID().toString(),PIPELINE,UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),Codec.sha256("exact fixture projection"));
        var member=new KafkaPhysicalReceipt.Member(scope,"fixture-owner",UUID.randomUUID().toString(),1,
                4102444800000L,4102444805000L);
        String key=Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
        var grant=new KafkaPhysicalReceipt.Grant(scope,key,new KafkaPhysicalReceipt.MemberWitness(member,key));
        settings.kafkaPhysicalGrant=grant;
        try(JdbcJournal journal=new JdbcJournal(settings)){
            journal.initialize(Codec.sha256("fixture fingerprint"),GENERATION,List.of(target.id));
            var lease=journal.claim("native-proof",30000);
            var tx=new SourceTransaction();tx.id="business-commit";tx.streamGeneration=GENERATION;
            String uuid="25b043c4-e42d-41ad-960f-53a7f0958a73";
            tx.position=new SourcePosition(1,"MYSQL","fixture-source",
                    Map.of("gtidSet",uuid+":1","serverUuid",uuid,"file","binlog.000001","offset","104"));
            journal.append(lease,tx);
            var store=new JdbcPublicationStore(journal,target.id,lease);
            try(var sink=new MessageSink(target.id,target.mappings,store,()->new KafkaMessagePublisher(target),
                    target.messaging.maxMessageBytes,grant)){
                var worker=new FanoutDispatcher(journal,lease,List.of(sink),Map.of(),record->record.transaction);
                if(worker.drain(1)!=1||store.checkpoint().sequence!=1||journal.subscriptions().getFirst().acknowledged!=1)
                    throw new AssertionError("A confirmed native business transaction did not ACK: "+worker.states());
            }
            var proof=store.kafkaPhysicalReceipt(GENERATION,grant);
            if(!expectEvidence){
                if(proof!=null)throw new AssertionError("Issued proof to a Worker with no Kafka READ privilege");
            }else if(proof==null||!topicId.equals(proof.receipt().observation().topic().topicId())
                    ||proof.receipt().observation().commits().getFirst().offset()!=1
                    ||proof.receipt().sequence()!=1||!grant.verifies(proof)||!grant.member().equals(proof.receipt().member())
                    ||proof.receipt().journalTime().acknowledgedUtcMillis()==null
                    ||proof.receipt().journalTime().acknowledgedUtcMillis()<proof.receipt().journalTime().publishedUtcMillis()){
                throw new AssertionError("Missing or incorrect exact broker evidence");
            }
        }
    }

    private static java.util.Properties adminProperties(PipelineConfig.Target target,String secret)throws Exception{
        var props=KafkaMessagePublisher.connectionProperties(target);
        KafkaMessagePublisher.configureSasl(props,"SCRAM-SHA-512","fixture-admin",secret+"-admin");
        return props;
    }

    private static PipelineConfig.Target target(String endpoint,String secret,String name){
        var target=new PipelineConfig.Target();target.id="kafka";target.product=TargetProduct.KAFKA;
        target.url="kafka://"+endpoint;target.username=ROLE;target.password=secret+"-reviewer";
        target.messaging=new PipelineConfig.Messaging();target.messaging.securityProtocol="SASL_SSL";
        target.messaging.saslMechanism="SCRAM-SHA-512";target.messaging.topic=name;target.messaging.maxMessageBytes=65536;
        target.messaging.replicas=1;target.messaging.autoCreate=false;target.mappings.put("sales","orders");return target;
    }

    private static AclBinding acl(ResourceType type,String name,AclOperation op){
        return new AclBinding(new ResourcePattern(type,name,PatternType.LITERAL),
                new AccessControlEntry("User:"+ROLE,"*",op,AclPermissionType.ALLOW));
    }
}
