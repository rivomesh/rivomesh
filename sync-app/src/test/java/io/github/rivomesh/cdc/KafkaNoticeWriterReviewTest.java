package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.KafkaNoticeWriterReview.Permissions;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.jupiter.api.Test;
import static org.apache.kafka.common.acl.AclOperation.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaNoticeWriterReviewTest {
    private static final Set<AclOperation> OBSERVER=Set.of(DESCRIBE,READ,DESCRIBE_CONFIGS);
    private static final Set<AclOperation> WRITER=Set.of(DESCRIBE,WRITE);
    private static final Permissions VERIFIED=new Permissions(OBSERVER,WRITER,Set.of(DESCRIBE));
    private static final String CLUSTER=Codec.sha256("KAFKA:reviewed");
    private static final String TOPIC=Uuid.randomUuid().toString();

    @Test void requiresAnIdenticalCompleteBrokerReadForBothIdentities(){
        assertEquals("VERIFIED_AT_REVIEW",KafkaNoticeWriterReview.evaluate(VERIFIED,VERIFIED).status());
        assertEquals("KAFKA_NOTICE_WRITER_AUTHORIZATION_CHANGED",KafkaNoticeWriterReview.evaluate(
                VERIFIED,new Permissions(OBSERVER,WRITER,Set.of(DESCRIBE,READ))).code());
        blocked("KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE",new Permissions(OBSERVER,WRITER,Set.of()));
        blocked("KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE",new Permissions(OBSERVER,null,Set.of(DESCRIBE)));
        blocked("KAFKA_NOTICE_WRITER_NATIVE_AUTHORIZATION_UNAVAILABLE",new Permissions(Set.of(DESCRIBE,UNKNOWN),WRITER,Set.of(DESCRIBE)));
    }

    @Test void refusesControlOrBusinessMutationByTheWrongPrincipal(){
        blocked("KAFKA_NOTICE_WRITER_REVIEWER_CAN_MUTATE_CONTROL",new Permissions(Set.of(DESCRIBE,WRITE),WRITER,Set.of(DESCRIBE)));
        blocked("KAFKA_NOTICE_WRITER_CAN_MUTATE_BUSINESS",new Permissions(OBSERVER,WRITER,Set.of(DESCRIBE,WRITE)));
        blocked("KAFKA_NOTICE_WRITER_CAN_MUTATE_BUSINESS",new Permissions(OBSERVER,WRITER,Set.of(DESCRIBE,ALTER_CONFIGS)));
        blocked("KAFKA_NOTICE_WRITER_CONTROL_ACL_UNSUPPORTED",new Permissions(OBSERVER,Set.of(DESCRIBE),Set.of(DESCRIBE)));
        blocked("KAFKA_NOTICE_WRITER_CONTROL_ACL_UNSUPPORTED",new Permissions(OBSERVER,Set.of(DESCRIBE,WRITE,DELETE),Set.of(DESCRIBE)));
    }

    @Test void validatesPrivateBootstrapsAndTransportBeforeConnecting()throws Exception{
        List<String> endpoints=List.of("localhost:9093","[::1]:9094");
        assertTrue(KafkaNoticeWriterReview.validConfiguration(CLUSTER,TOPIC,endpoints,"writer","distinct-password"));
        for(String invalid:List.of("host","host:0","host:65536","kafka://host:9092","user@host:9092","host:9092/api","host:9092?secret=true"))
            assertFalse(KafkaNoticeWriterReview.validConfiguration(CLUSTER,TOPIC,List.of(invalid),"writer","distinct-password"));
        assertFalse(KafkaNoticeWriterReview.validConfiguration(CLUSTER,TOPIC,List.of("host:9092","host:9092"),"writer","distinct-password"));
        assertFalse(KafkaNoticeWriterReview.validConfiguration(CLUSTER,Uuid.ZERO_UUID.toString(),endpoints,"writer","distinct-password"));
        PipelineConfig.Target target=new PipelineConfig.Target();target.product=TargetProduct.KAFKA;target.id="destination";
        target.messaging=new PipelineConfig.Messaging();
        target.messaging.topic="business";target.username="reviewer";target.password="reviewer-password";
        var task=new KafkaNoticePlanning.TaskScope("task","node",id(),id(),id());
        var reservation=new KafkaNoticePlanning.TopicReservation(id(),"task","destination","notice",CLUSTER,TOPIC);
        var identity=new KafkaNoticePlanning.CatalogIdentity(Codec.sha256("business"),Codec.sha256("notice"));
        var plan=new KafkaNoticePlanning.Prepared(task,new KafkaNoticePlanning.Administrator("admin",id()),"prior_test","admin",
                Codec.sha256("prior"),reservation,null,Codec.sha256("notice"),Codec.sha256("payload"),identity);
        assertEquals("KAFKA_NOTICE_WRITER_SCRAM_TLS_REQUIRED",KafkaNoticeWriterReview.inspect(
                target,plan,CLUSTER,TOPIC,endpoints,"writer","distinct-password").code());
        target.messaging.securityProtocol="SASL_SSL";target.messaging.saslMechanism="SCRAM-SHA-512";
        assertEquals("KAFKA_NOTICE_WRITER_CREDENTIAL_REUSED",KafkaNoticeWriterReview.inspect(
                target,plan,CLUSTER,TOPIC,endpoints,"reviewer","distinct-password").code());
        assertEquals("KAFKA_NOTICE_WRITER_CREDENTIAL_REUSED",KafkaNoticeWriterReview.inspect(
                target,plan,CLUSTER,TOPIC,endpoints,"writer","reviewer-password").code());
        assertEquals("KAFKA_NOTICE_WRITER_SCOPE_UNVERIFIED",KafkaNoticeWriterReview.inspect(
                target,plan,CLUSTER,Uuid.randomUuid().toString(),endpoints,"writer","distinct-password").code());
    }

    private static String id(){return UUID.randomUUID().toString();}

    private static void blocked(String code,Permissions permissions){
        var result=KafkaNoticeWriterReview.evaluate(permissions,permissions);
        assertEquals("BLOCKED",result.status());
        assertEquals(code,result.code());
    }
}
