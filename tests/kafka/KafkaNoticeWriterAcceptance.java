package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import static org.apache.kafka.common.acl.AclOperation.*;
import static org.apache.kafka.common.acl.AclPermissionType.*;

/** Single-broker native permission contract. Existing replicated preview fixtures cover the topic profile. */
public final class KafkaNoticeWriterAcceptance {
    private static final String BUSINESS="writer-review.business",CONTROL="writer-review.notice";
    private static final String ROOT="fixture-admin",REVIEWER="fixture-reviewer",WRITER="fixture-writer";

    public static void main(String[] args)throws Exception{
        String endpoint=args[0],secret=System.getenv("RIVOMESH_KAFKA_FIXTURE_SECRET");
        if(secret==null||secret.length()<32)throw new IllegalArgumentException("Synthetic fixture secret required");
        var target=new PipelineConfig.Target();
        target.id="destination";target.product=TargetProduct.KAFKA;target.url="kafka://"+endpoint;
        target.username=REVIEWER;target.password=secret+"-reviewer";target.messaging=new PipelineConfig.Messaging();
        target.messaging.topic=BUSINESS;target.messaging.securityProtocol="SASL_SSL";target.messaging.saslMechanism="SCRAM-SHA-512";
        var settings=KafkaMessagePublisher.connectionProperties(target);
        KafkaMessagePublisher.configureSasl(settings,"SCRAM-SHA-512",ROOT,secret+"-admin");
        Admin admin=Admin.create(settings);
        try{
            admin.createTopics(List.of(new NewTopic(BUSINESS,1,(short)1),new NewTopic(CONTROL,1,(short)1))).all().get(15,TimeUnit.SECONDS);
            var topics=admin.describeTopics(List.of(BUSINESS,CONTROL)).allTopicNames().get(15,TimeUnit.SECONDS);
            var business=topics.get(BUSINESS).topicId();
            var control=topics.get(CONTROL).topicId();
            String cluster=admin.describeCluster().clusterId().get(15,TimeUnit.SECONDS);
            String clusterHash=Codec.sha256("KAFKA:"+cluster);
            var plan=new KafkaNoticePlanning.Prepared(
                    new KafkaNoticePlanning.TaskScope("task","node",id(),id(),id()),new KafkaNoticePlanning.Administrator("admin",id()),
                    "prior_test","admin",Codec.sha256("predecessor"),
                    new KafkaNoticePlanning.TopicReservation(id(),"task","destination",CONTROL,clusterHash,control.toString()),
                    null,Codec.sha256("notice"),Codec.sha256("payload"),new KafkaNoticePlanning.CatalogIdentity(
                            KafkaRecoveryReview.topicIdentity(cluster,business),KafkaRecoveryReview.topicIdentity(cluster,control)));
            var writerBusiness=acl(WRITER,BUSINESS,WRITE,ALLOW);
            var reviewerControl=acl(REVIEWER,CONTROL,WRITE,ALLOW);
            var denyWriterBusinessDescribe=acl(WRITER,BUSINESS,DESCRIBE,DENY);
            add(admin,acl(REVIEWER,BUSINESS,WRITE,ALLOW),acl(REVIEWER,CONTROL,READ,ALLOW),
                    acl(WRITER,CONTROL,WRITE,ALLOW),acl(WRITER,BUSINESS,DESCRIBE,ALLOW));
            verify(target,plan,endpoint,secret+"-writer","KAFKA_NOTICE_WRITER_EFFECTIVE_PERMISSIONS_VERIFIED");
            add(admin,writerBusiness);
            verify(target,plan,endpoint,secret+"-writer","KAFKA_NOTICE_WRITER_CAN_MUTATE_BUSINESS");
            add(admin,denyWriterBusinessDescribe);
            verify(target,plan,endpoint,secret+"-writer","KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE");
            remove(admin,denyWriterBusinessDescribe);remove(admin,writerBusiness);
            add(admin,reviewerControl);
            verify(target,plan,endpoint,secret+"-writer","KAFKA_NOTICE_WRITER_REVIEWER_CAN_MUTATE_CONTROL");
            remove(admin,reviewerControl);
            verify(target,plan,endpoint,secret+"-bad","KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE");
            verify(target,plan,endpoint,secret+"-writer","KAFKA_NOTICE_WRITER_EFFECTIVE_PERMISSIONS_VERIFIED");
            var offsets=admin.listOffsets(Map.of(new TopicPartition(BUSINESS,0),OffsetSpec.latest(),
                    new TopicPartition(CONTROL,0),OffsetSpec.latest())).all().get(15,TimeUnit.SECONDS);
            if(offsets.size()!=2||offsets.values().stream().anyMatch(offset->offset.offset()!=0))
                throw new AssertionError("A read-only check changed Kafka offsets");
            if(!admin.listConsumerGroups().all().get(15,TimeUnit.SECONDS).isEmpty())
                throw new AssertionError("A read-only check created a consumer group");
            System.out.println("PASS 6 native authorization observations. Business and control end offsets 0, no consumer groups.");
        }finally{admin.close(Duration.ofSeconds(1));}
    }

    private static void verify(PipelineConfig.Target target,KafkaNoticePlanning.Prepared plan,String endpoint,String password,
                               String expected)throws Exception{
        // The KRaft metadata image can lag the completed ACL administration request.
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
        KafkaNoticeWriterReview.Result result;
        do{
            result=KafkaNoticeWriterReview.inspect(target,plan,plan.controlTopic().kafkaClusterSha256(),
                    plan.controlTopic().topicId(),List.of(endpoint),WRITER,password);
            if(expected.equals(result.code()))return;
            Thread.sleep(100);
        }while(System.nanoTime()<deadline);
        throw new AssertionError("Expected "+expected+", observed "+result.code());
    }

    private static String id(){return UUID.randomUUID().toString();}
    private static AclBinding acl(String user,String topic,AclOperation operation,AclPermissionType permission){
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC,topic,PatternType.LITERAL),
                new AccessControlEntry("User:"+user,"*",operation,permission));
    }
    private static void add(Admin admin,AclBinding... permissions)throws Exception{
        admin.createAcls(List.of(permissions)).all().get(15,TimeUnit.SECONDS);
    }
    private static void remove(Admin admin,AclBinding permission)throws Exception{
        admin.deleteAcls(List.of(permission.toFilter())).all().get(15,TimeUnit.SECONDS);
    }
}
