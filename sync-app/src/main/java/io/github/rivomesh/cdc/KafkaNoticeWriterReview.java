package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.recovery.FailureEvidence;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;

/** Native effective permissions for the two reviewed principals and two exact physical topics. */
public final class KafkaNoticeWriterReview {
    private static final Duration BUDGET=Duration.ofSeconds(6),CLOSE=Duration.ofMillis(250);
    private static final Set<AclOperation> READ_ONLY=Set.of(AclOperation.DESCRIBE,AclOperation.DESCRIBE_CONFIGS,AclOperation.READ);
    private static final Set<AclOperation> NOTICE_WRITER=Set.of(AclOperation.DESCRIBE,AclOperation.WRITE);

    public record Result(String status,String code) {
        public static Result notConfigured(){return new Result("NOT_CONFIGURED","KAFKA_NOTICE_WRITER_NOT_CONFIGURED");}
    }

    record Permissions(Set<AclOperation> reviewerControl,Set<AclOperation> writerControl,Set<AclOperation> writerBusiness) {}
    private static final class Blocked extends RuntimeException {
        Blocked(String code){super(code);}
    }

    private KafkaNoticeWriterReview(){}

    public static boolean validConfiguration(String clusterSha256,String topicId,List<String> endpoints,String username,String password){
        if(!FailureEvidence.hash(clusterSha256)||topicId==null||!topicId.matches("[A-Za-z0-9_-]{22}")
                ||!username(username)||password==null||password.isBlank()||password.length()>1024
                ||password.contains("${")||password.chars().anyMatch(Character::isISOControl)
                ||endpoints==null||endpoints.isEmpty()||endpoints.size()>8)return false;
        try {
            if(Uuid.ZERO_UUID.equals(Uuid.fromString(topicId))||new HashSet<>(endpoints).size()!=endpoints.size())return false;
            for(String endpoint:endpoints){
                if(endpoint==null||endpoint.length()>256)return false;
                URI uri=URI.create("kafka://"+endpoint);
                if(uri.getHost()==null||uri.getPort()<1||uri.getPort()>65535||uri.getRawUserInfo()!=null
                        ||!"".equals(uri.getRawPath())||uri.getRawQuery()!=null||uri.getRawFragment()!=null)return false;
            }
        }catch(IllegalArgumentException invalid){return false;}
        return true;
    }

    public static Result inspect(PipelineConfig.Target target,KafkaNoticePlanning.Prepared plan,String clusterSha256,String topicId,
                                 List<String> endpoints,String username,String password)throws InterruptedException {
        if(plan==null||plan.catalogIdentity()==null||target==null||target.product!=TargetProduct.KAFKA||target.messaging==null
                ||!target.id.equals(plan.controlTopic().targetId())
                ||!plan.controlTopic().kafkaClusterSha256().equals(clusterSha256)||!plan.controlTopic().topicId().equals(topicId)
                ||!validConfiguration(clusterSha256,topicId,endpoints,username,password))
            return blocked("KAFKA_NOTICE_WRITER_SCOPE_UNVERIFIED");
        if(!"SASL_SSL".equals(target.messaging.securityProtocol)
                ||!Set.of("SCRAM-SHA-256","SCRAM-SHA-512").contains(target.messaging.saslMechanism))
            return blocked("KAFKA_NOTICE_WRITER_SCRAM_TLS_REQUIRED");
        if(!username(target.username)||username.equals(target.username)||password.equals(target.password))
            return blocked("KAFKA_NOTICE_WRITER_CREDENTIAL_REUSED");
        if(plan.catalogIdentity().businessTopicSha256().equals(plan.catalogIdentity().controlTopicSha256()))
            return blocked("KAFKA_NOTICE_WRITER_SCOPE_UNVERIFIED");
        Admin reviewer=null,writer=null;
        try{
            long deadline=System.nanoTime()+BUDGET.toNanos();
            var settings=KafkaMessagePublisher.connectionProperties(target);
            settings.setProperty("ssl.endpoint.identification.algorithm","https");
            reviewer=Admin.create(settings);
            var separate=KafkaMessagePublisher.connectionProperties(target);
            separate.setProperty("ssl.endpoint.identification.algorithm","https");
            separate.setProperty("bootstrap.servers",String.join(",",endpoints));
            separate.setProperty("client.id","rivomesh-kafka-notice-writer-check");
            KafkaMessagePublisher.configureSasl(separate,target.messaging.saslMechanism,username,password);
            writer=Admin.create(separate);
            Permissions before=observe(reviewer,writer,target.messaging.topic,plan,deadline);
            Permissions after=observe(reviewer,writer,target.messaging.topic,plan,deadline);
            return evaluate(before,after);
        }catch(Blocked failure){
            return blocked(failure.getMessage());
        }catch(InterruptedException interrupted){
            Thread.currentThread().interrupt();
            throw interrupted;
        }catch(Exception unavailable){
            return blocked("KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE");
        }finally{
            try{if(writer!=null)writer.close(CLOSE);}
            finally{if(reviewer!=null)reviewer.close(CLOSE);}
        }
    }

    private static Permissions observe(Admin reviewer,Admin writer,String businessName,KafkaNoticePlanning.Prepared plan,
                                       long deadline)throws Exception {
        String expectedCluster=plan.controlTopic().kafkaClusterSha256(),controlName=plan.controlTopic().topicName();
        String reviewerCluster=cluster(reviewer,expectedCluster,deadline),writerCluster=cluster(writer,expectedCluster,deadline);
        var reviewerTopics=reviewer.describeTopics(List.of(businessName,controlName),new DescribeTopicsOptions().includeAuthorizedOperations(true)).topicNameValues();
        var writerTopics=writer.describeTopics(List.of(businessName,controlName),new DescribeTopicsOptions().includeAuthorizedOperations(true)).topicNameValues();
        var identity=plan.catalogIdentity();
        permissions(reviewerTopics.get(businessName),businessName,reviewerCluster,identity.businessTopicSha256(),deadline);
        return new Permissions(
                permissions(reviewerTopics.get(controlName),controlName,reviewerCluster,identity.controlTopicSha256(),deadline),
                permissions(writerTopics.get(controlName),controlName,writerCluster,identity.controlTopicSha256(),deadline),
                permissions(writerTopics.get(businessName),businessName,writerCluster,identity.businessTopicSha256(),deadline));
    }

    private static String cluster(Admin admin,String expected,long deadline)throws Exception {
        String value=await(admin.describeCluster().clusterId(),deadline);
        if(value==null||!expected.equals(Codec.sha256("KAFKA:"+value)))throw new Blocked("KAFKA_NOTICE_WRITER_PHYSICAL_TOPIC_CHANGED");
        return value;
    }

    private static Set<AclOperation> permissions(KafkaFuture<TopicDescription> future,String name,String cluster,
                                                 String expectedIdentity,long deadline)throws Exception {
        // A denied DESCRIBE alone does not prove the principal lacks WRITE.
        TopicDescription description=await(future,deadline);
        if(description==null||!name.equals(description.name())||description.isInternal()
                ||description.topicId()==null||Uuid.ZERO_UUID.equals(description.topicId())
                ||!expectedIdentity.equals(KafkaRecoveryReview.topicIdentity(cluster,description.topicId())))
            throw new Blocked("KAFKA_NOTICE_WRITER_PHYSICAL_TOPIC_CHANGED");
        Set<AclOperation> permissions=description.authorizedOperations();
        if(!known(permissions)||!permissions.contains(AclOperation.DESCRIBE))
            throw new Blocked("KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE");
        return Set.copyOf(permissions);
    }

    static Result evaluate(Permissions before,Permissions after){
        if(!before.equals(after))return blocked("KAFKA_NOTICE_WRITER_AUTHORIZATION_CHANGED");
        if(!known(before.reviewerControl())||!known(before.writerControl())||!known(before.writerBusiness())
                ||!before.reviewerControl().contains(AclOperation.DESCRIBE)||!before.writerControl().contains(AclOperation.DESCRIBE)
                ||!before.writerBusiness().contains(AclOperation.DESCRIBE))
            return blocked("KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE");
        if(!READ_ONLY.containsAll(before.reviewerControl()))
            return blocked("KAFKA_NOTICE_WRITER_REVIEWER_CAN_MUTATE_CONTROL");
        if(!NOTICE_WRITER.equals(before.writerControl()))
            return blocked("KAFKA_NOTICE_WRITER_CONTROL_ACL_UNSUPPORTED");
        if(!READ_ONLY.containsAll(before.writerBusiness()))
            return blocked("KAFKA_NOTICE_WRITER_CAN_MUTATE_BUSINESS");
        return new Result("VERIFIED_AT_REVIEW","KAFKA_NOTICE_WRITER_EFFECTIVE_PERMISSIONS_VERIFIED");
    }

    private static boolean known(Collection<AclOperation> operations){
        return operations!=null&&operations.stream().allMatch(operation->operation!=null
                &&operation!=AclOperation.UNKNOWN&&operation!=AclOperation.ANY&&operation!=AclOperation.ALL);
    }

    private static boolean username(String user){return user!=null&&user.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");}
    private static Result blocked(String code){return new Result("BLOCKED",code);}

    private static <T>T await(KafkaFuture<T> future,long deadline)throws Exception {
        if(future==null)throw new Blocked("KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE");
        long remaining=deadline-System.nanoTime();
        if(remaining<=0)throw new Blocked("KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE");
        return future.get(remaining,TimeUnit.NANOSECONDS);
    }
}
