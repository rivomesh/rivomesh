package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.KafkaRecoveryReview;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedKafkaRecoveryReviewTest {
    private static final String TASK="/api/tasks/job";
    private static final String ID="review_kafka_1234";
    private static final String OPERATOR="kafka-recovery-operator-at-least-32-bytes";
    @TempDir Path directory;

    static final class Backend extends ManagedCommitTest.Backend {
        volatile boolean uncertain;

        @Override public KafkaRecoveryReview.Observation reviewKafkaReceipts(TaskDefinition task,String target,long generation)throws Exception{
            assertEquals("job",task.jobId);
            assertEquals("kafka",target);
            assertEquals(17,generation);
            captures.incrementAndGet();
            entered.countDown();
            if(blocked)release.await();
            if(uncertain)throw new IllegalStateException("KAFKA_RECOVERY_PUBLICATION_UNCERTAIN");
            return new KafkaRecoveryReview.Observation(KafkaRecoveryReview.VERIFIED,target,"2026-11-18T00:00:00Z",
                    "3","17","6","7",Codec.sha256("test topic"),List.of(
                    new KafkaRecoveryReview.Receipt("orders","31",Codec.sha256("test payload"))));
        }
    }

    @Test void ownerReviewStoresTypedEvidenceAndHistoricalLookupWorksWithOwnerOffline()throws Exception{
        GlobalControlHttpTest fixture=new GlobalControlHttpTest();
        fixture.directory=directory;
        PlatformConfig controller=fixture.config("a"),owner=fixture.config("b");
        for(PlatformConfig config:List.of(controller,owner))config.cluster.recoveryReview=true;
        Backend backend=new Backend();
        backend.blocked=true;
        JsonNode verified;
        try(TaskService first=new TaskService(controller,new PlatformTest.Backend(),new PlatformTest.Workers());
            ConsoleServer one=new ConsoleServer(controller,first)){
            one.start();
            String api=fixture.base(one),receipt=TASK+"/recovery-receipts/"+ID;
            try(TaskService second=new TaskService(owner,backend,new PlatformTest.Workers());
                ConsoleServer two=new ConsoleServer(owner,second)){
                two.start();
                create(fixture,api);
                createScopedOperator(fixture,api);
                ObjectNode request=request(fixture,api);
                assertEquals(403,post(api,request,"operator_review_1234",OPERATOR).status);
                ObjectNode overflow=request.deepCopy();
                overflow.put("errorCount",new java.math.BigInteger("18446744073709551633"));
                assertEquals(400,post(api,overflow,"overflow_review_1234",PlatformTest.TOKEN).status);
                assertEquals(202,post(api,request,"before_pause_1234",PlatformTest.TOKEN).status);
                awaitCode(fixture,api,"before_pause_1234","KAFKA_RECOVERY_REQUIRES_PIPELINE_PAUSED");
                assertEquals(0,backend.captures.get());
                fixture.action(api,"job","PAUSE","pause_before_kafka_1234",PlatformTest.TOKEN);
                PlatformTest.await(()->fixture.completed(api,"job","pause_before_kafka_1234"));
                request=request(fixture,api);
                ObjectNode wrongTarget=request.deepCopy();
                wrongTarget.put("subscriptionId","pg");
                assertEquals(202,post(api,wrongTarget,"wrong_target_1234",PlatformTest.TOKEN).status);
                awaitCode(fixture,api,"wrong_target_1234","KAFKA_RECOVERY_TARGET_UNSUPPORTED");
                request=request(fixture,api);
                assertEquals(202,post(api,request,ID,PlatformTest.TOKEN).status);
                assertTrue(backend.entered.await(5,TimeUnit.SECONDS));
                assertFalse(fixture.get(api,receipt,PlatformTest.TOKEN).has("kafkaReview"));
                assertEquals(403,PlatformTest.http(api,receipt,OPERATOR,null,null).status);
                assertEquals(202,post(fixture.base(two),request,ID,PlatformTest.TOKEN).status);
                ObjectNode changed=request.deepCopy();
                changed.put("errorCount",18);
                assertEquals(409,post(api,changed,ID,PlatformTest.TOKEN).status);
                backend.release.countDown();
                awaitCode(fixture,api,ID,KafkaRecoveryReview.VERIFIED);
                verified=fixture.get(api,receipt,PlatformTest.TOKEN);
                assertEquals("SUCCEEDED",verified.path("status").asText());
                assertEquals("legacy-admin",verified.path("actorId").asText());
                assertEquals("b",verified.path("ownerNodeId").asText());
                assertEquals("kafka",verified.path("subscriptionId").asText());
                assertEquals("17",verified.path("kafkaReview").path("errorGeneration").asText());
                assertEquals(Codec.sha256("test payload"),verified.path("kafkaReview").path("nativeReceipts").get(0).path("payloadSha256").asText());
                assertFalse(fixture.get(api,TASK,OPERATOR).toString().contains("payloadSha256"));
                assertFalse(Model.JSON.valueToTree(second.view("job")).toString().contains("payloadSha256"));
                assertEquals(1,backend.captures.get());

                backend.uncertain=true;
                assertEquals(202,post(api,request(fixture,api),"unknown_kafka_1234",PlatformTest.TOKEN).status);
                awaitCode(fixture,api,"unknown_kafka_1234","KAFKA_RECOVERY_PUBLICATION_UNCERTAIN");
                JsonNode uncertain=fixture.get(api,TASK+"/recovery-receipts/unknown_kafka_1234",PlatformTest.TOKEN);
                assertEquals("FAILED",uncertain.path("status").asText());
                assertFalse(uncertain.has("kafkaReview"));
                assertEquals("PAUSED",fixture.get(api,TASK,PlatformTest.TOKEN).path("desired").asText());
            }finally{backend.release.countDown();}
            assertEquals(verified,fixture.get(api,receipt,PlatformTest.TOKEN));
            assertEquals(2,backend.captures.get());
        }
        try(TaskService reopened=new TaskService(controller,new PlatformTest.Backend(),new PlatformTest.Workers());
            ConsoleServer server=new ConsoleServer(controller,reopened)){
            server.start();
            assertEquals(verified,fixture.get(fixture.base(server),TASK+"/recovery-receipts/"+ID,PlatformTest.TOKEN));
        }
    }

    private static void create(GlobalControlHttpTest fixture,String api)throws Exception{
        ObjectNode request=Model.JSON.createObjectNode(),config=Model.JSON.valueToTree(PlatformTest.pipeline("job"));
        ((ObjectNode)config.path("journal")).remove("path");
        ObjectNode kafka=((ArrayNode)config.path("targets")).addObject();
        kafka.put("id","kafka").put("product","KAFKA").put("url","kafka://broker:9092");
        kafka.putObject("mappings").put("orders","orders");
        kafka.putObject("messaging").put("topic","test.events");
        request.set("config",config);
        request.put("ownerNodeId","b");
        PlatformTest.Response result=PlatformTest.http(api,"/api/tasks",PlatformTest.TOKEN,request.toString(),"create_kafka_1234");
        assertEquals(202,result.status,result.text);
        PlatformTest.await(()->fixture.completed(api,"job","create_kafka_1234"));
    }

    private static ObjectNode request(GlobalControlHttpTest fixture,String api)throws Exception{
        return Model.JSON.createObjectNode().put("action",TaskRequest.KAFKA_REVIEW_ACTION).put("subscriptionId","kafka")
                .put("errorCount",17).put("expectedRevision",fixture.get(api,TASK,PlatformTest.TOKEN).path("revision").asText());
    }

    private static void createScopedOperator(GlobalControlHttpTest fixture,String api)throws Exception{
        ObjectNode account=Model.JSON.createObjectNode().put("id","operator").put("role","OPERATOR").put("enabled",true)
                .put("token",OPERATOR).put("expectedRevision",fixture.get(api,"/api/accounts",PlatformTest.TOKEN).path("revision").asText());
        account.putArray("taskIds").add("job");
        account.putArray("nodeIds").add("b");
        PlatformTest.Response reply=PlatformTest.http(api,"/api/accounts",PlatformTest.TOKEN,account.toString(),"kafka_operator_1234");
        assertEquals(201,reply.status,reply.text);
    }

    private static PlatformTest.Response post(String api,ObjectNode request,String id,String token)throws Exception{
        return PlatformTest.http(api,TASK+"/actions",token,request.toString(),id);
    }

    private static void awaitCode(GlobalControlHttpTest fixture,String api,String id,String code)throws Exception{
        PlatformTest.await(()->code.equals(fixture.get(api,TASK+"/recovery-receipts/"+id,PlatformTest.TOKEN).path("code").asText()));
    }
}
