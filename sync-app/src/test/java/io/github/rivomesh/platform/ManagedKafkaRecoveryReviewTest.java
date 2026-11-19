package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.KafkaNoticePlanning;
import io.github.rivomesh.cdc.KafkaRecoveryReview;
import io.github.rivomesh.cdc.TargetProduct;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedKafkaRecoveryReviewTest {
    private static final String TASK="/api/tasks/job";
    private static final String ID="review_kafka_1234";
    private static final String NEW_REVIEW="replacement_kafka_review_1234";
    private static final String OPERATOR="kafka-recovery-operator-at-least-32-bytes";
    private static final String NOTICE_TOPIC_ID="cKtKfC5KQGq-qM1RDCb_3w";
    @TempDir Path directory;

    static final class Backend extends ManagedCommitTest.Backend {
        volatile boolean uncertain;
        volatile int planned;
        volatile String businessIdentity=Codec.sha256("test topic");
        volatile String controlIdentity=Codec.sha256("test control topic");

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
                    new KafkaRecoveryReview.Receipt("orders","31",Codec.sha256("test payload"))),
                    new KafkaRecoveryReview.OriginalFailure("retained-generation",Codec.sha256("original failed transaction"),
                            Codec.sha256("test Kafka cluster")));
        }
        @Override public KafkaNoticePlanning.Prepared planKafkaNotice(TaskDefinition task,KafkaNoticePlanning.TaskScope scope,
                KafkaNoticePlanning.Administrator administrator,KafkaNoticePlanning.Predecessor predecessor,
                KafkaNoticePlanning.TopicReservation control){
            planned++;
            assertEquals("b",scope.ownerNodeId());
            assertEquals("legacy-admin",administrator.actorId());
            assertTrue(List.of(ID,NEW_REVIEW).contains(predecessor.requestId()));
            assertEquals("control.events",control.topicName());
            assertEquals(NOTICE_TOPIC_ID,control.topicId());
            var proof=predecessor.observation();
            var failure=new KafkaNoticePlanning.FailedTransaction(proof.target(),proof.originalFailure().journalGeneration(),
                    proof.epoch(),proof.errorGeneration(),proof.acknowledgedSequence(),proof.failedSequence(),
                    proof.originalFailure().contentSha256());
            return new KafkaNoticePlanning.Prepared(scope,administrator,predecessor.requestId(),
                    predecessor.actorId(),Codec.sha256(Codec.encode(predecessor)),control,failure,
                    Codec.sha256("test notice id"),Codec.sha256("test notice payload"),
                    controlIdentity==null?null:new KafkaNoticePlanning.CatalogIdentity(businessIdentity,controlIdentity));
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
                create(fixture,api,false);
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
                String currentRevision=fixture.get(api,TASK,PlatformTest.TOKEN).path("revision").asText();
                ObjectNode unavailablePlan=Model.JSON.createObjectNode().put("expectedRevision",currentRevision).put("predecessorRequestId",ID);
                PlatformTest.Response unavailable=PlatformTest.http(api,TASK+"/kafka-notice-plan",PlatformTest.TOKEN,
                        unavailablePlan.toString(),null);
                assertEquals(409,unavailable.status,unavailable.text);
                assertEquals("KAFKA_NOTICE_CONTROL_TOPIC_REQUIRED",Model.JSON.readTree(unavailable.text).path("code").asText());
                assertEquals(0,backend.planned);
                assertEquals(1,backend.captures.get());

                backend.uncertain=true;
                assertEquals(202,post(api,request(fixture,api),"unknown_kafka_1234",PlatformTest.TOKEN).status);
                awaitCode(fixture,api,"unknown_kafka_1234","KAFKA_RECOVERY_PUBLICATION_UNCERTAIN");
                JsonNode uncertain=fixture.get(api,TASK+"/recovery-receipts/unknown_kafka_1234",PlatformTest.TOKEN);
                assertEquals("FAILED",uncertain.path("status").asText());
                assertFalse(uncertain.has("kafkaReview"));
                assertEquals("PAUSED",fixture.get(api,TASK,PlatformTest.TOKEN).path("desired").asText());
                assertPlanPredecessor(controller,first,fixture.get(api,TASK,PlatformTest.TOKEN),verified);
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

    @Test void readOnlyPlanUsesTheCurrentOwnerAndServerOwnedTopicWithoutAuthorizingPublication()throws Exception{
        GlobalControlHttpTest fixture=new GlobalControlHttpTest();
        fixture.directory=directory;
        PlatformConfig controller=fixture.config("a"),owner=fixture.config("b");
        controller.cluster.recoveryReview=true;owner.cluster.recoveryReview=true;
        var noticeWriter=new PlatformConfig.KafkaNoticeWriter();
        noticeWriter.taskId="job";noticeWriter.targetId="kafka";
        noticeWriter.clusterSha256=Codec.sha256("test Kafka cluster");noticeWriter.topicId=NOTICE_TOPIC_ID;
        noticeWriter.bootstrapServers=List.of("private-controller-bootstrap:9094");
        noticeWriter.username="private-fixture-writer";noticeWriter.password="private-fixture-password";
        owner.kafkaNoticeWriters=List.of(noticeWriter);
        Backend backend=new Backend();
        try(TaskService first=new TaskService(controller,new PlatformTest.Backend(),new PlatformTest.Workers());
            TaskService second=new TaskService(owner,backend,new PlatformTest.Workers());
            ConsoleServer one=new ConsoleServer(controller,first);ConsoleServer two=new ConsoleServer(owner,second)){
            one.start();two.start();
            String api=fixture.base(one),path=TASK+"/kafka-notice-plan";
            create(fixture,api,true);
            createScopedOperator(fixture,api);
            fixture.action(api,"job","PAUSE","pause_notice_1234",PlatformTest.TOKEN);
            PlatformTest.await(()->fixture.completed(api,"job","pause_notice_1234"));
            assertEquals(202,post(api,request(fixture,api),ID,PlatformTest.TOKEN).status);
            awaitCode(fixture,api,ID,KafkaRecoveryReview.VERIFIED);
            String revision=fixture.get(api,TASK,PlatformTest.TOKEN).path("revision").asText();
            ObjectNode plan=Model.JSON.createObjectNode().put("expectedRevision",revision).put("predecessorRequestId",ID);
            assertEquals(403,PlatformTest.http(api,path,OPERATOR,plan.toString(),null).status);
            ObjectNode suppliedTopic=plan.deepCopy().put("controlTopic","another-topic");
            assertEquals(400,PlatformTest.http(api,path,PlatformTest.TOKEN,suppliedTopic.toString(),null).status);
            ObjectNode stale=plan.deepCopy().put("expectedRevision",UUID.randomUUID().toString());
            assertEquals(409,PlatformTest.http(api,path,PlatformTest.TOKEN,stale.toString(),null).status);
            assertEquals(0,backend.planned);
            PlatformTest.Response result=PlatformTest.http(api,path,PlatformTest.TOKEN,plan.toString(),"preview_kafka_1234");
            assertEquals(200,result.status,result.text);
            JsonNode body=Model.JSON.readTree(result.text);
            assertEquals("KAFKA_NOTICE_PLAN_REVIEWED",body.path("status").asText());
            assertFalse(body.path("executionAuthorized").asBoolean(true));
            assertEquals("KAFKA_NOTICE_GLOBAL_OWNERSHIP_UNVERIFIED",body.path("publicationBlockCode").asText());
            assertEquals("BLOCKED",body.path("writerEntitlement").path("status").asText());
            assertEquals("KAFKA_NOTICE_WRITER_SCRAM_TLS_REQUIRED",body.path("writerEntitlement").path("code").asText());
            assertFalse(result.text.contains(noticeWriter.password));
            assertFalse(result.text.contains(noticeWriter.username));
            assertFalse(result.text.contains(noticeWriter.bootstrapServers.getFirst()));
            assertEquals("VERIFIED_SINGLETON",body.path("catalogCoverage").path("status").asText());
            assertEquals(Codec.sha256("test topic"),body.path("plan").path("catalogIdentity").path("businessTopicSha256").asText());
            assertEquals("legacy-admin",body.path("plan").path("administrator").path("actorId").asText());
            assertEquals(NOTICE_TOPIC_ID,body.path("plan").path("controlTopic").path("topicId").asText());
            assertEquals("7",body.path("plan").path("failure").path("failedSequence").asText());
            assertEquals(Codec.sha256("test notice payload"),body.path("plan").path("payloadSha256").asText());
            assertFalse(result.text.contains("source-secret"));
            assertFalse(result.text.contains("kafka://"));
            assertEquals(revision,fixture.get(api,TASK,PlatformTest.TOKEN).path("revision").asText());
            assertEquals(1,backend.planned);
            assertTrue(fixture.get(api,"/api/audit",PlatformTest.TOKEN).toString().contains("POST_KAFKA_NOTICE_PLAN"));
            assertEquals(body,noticePlan(fixture.base(two),path,plan));
            backend.controlIdentity=null;
            assertEquals("KAFKA_NOTICE_CATALOG_NATIVE_IDENTITY_MISSING",noticePlan(api,path,plan).path("catalogCoverage").path("code").asText());
            backend.controlIdentity=backend.businessIdentity;
            assertEquals("KAFKA_NOTICE_CATALOG_PHYSICAL_TOPIC_COLLISION",noticePlan(api,path,plan).path("catalogCoverage").path("code").asText());
            backend.controlIdentity=Codec.sha256("test control topic");
            backend.businessIdentity=Codec.sha256("another topic");
            PlatformTest.Response changed=PlatformTest.http(api,path,PlatformTest.TOKEN,plan.toString(),null);
            assertEquals(409,changed.status,changed.text);
            assertEquals("KAFKA_NOTICE_CATALOG_IDENTITY_CHANGED",Model.JSON.readTree(changed.text).path("code").asText());
            backend.businessIdentity=Codec.sha256("test topic");
            fixture.create(api,"another","b");
            assertEquals("KAFKA_NOTICE_CATALOG_MANAGED_TARGETS_MISSING",noticePlan(api,path,plan).path("catalogCoverage").path("code").asText());
            String configuration=TASK+"/configuration";
            JsonNode before=fixture.get(api,configuration,PlatformTest.TOKEN);
            ObjectNode edit=Model.JSON.createObjectNode().put("expectedRevision",revision);
            ObjectNode config=((ObjectNode)before.path("config")).deepCopy();
            ((ObjectNode)config.path("source")).put("password","replacement-fixture-source");
            edit.set("config",config);
            var update=PlatformConfigurationTest.request(api,configuration,"PUT",PlatformTest.TOKEN,edit.toString(),"edit_notice_config_1234");
            assertEquals(202,update.status,update.text);
            PlatformTest.await(()->fixture.completed(api,"job","edit_notice_config_1234"));
            assertEquals(202,post(api,request(fixture,api),NEW_REVIEW,PlatformTest.TOKEN).status);
            awaitCode(fixture,api,NEW_REVIEW,KafkaRecoveryReview.VERIFIED);
            String newRevision=fixture.get(api,TASK,PlatformTest.TOKEN).path("revision").asText();
            plan.put("expectedRevision",newRevision);
            PlatformTest.Response oldPlan=PlatformTest.http(api,path,PlatformTest.TOKEN,plan.toString(),null);
            assertEquals(409,oldPlan.status,oldPlan.text);
            assertEquals("KAFKA_NOTICE_PREDECESSOR_SCOPE_CHANGED",Model.JSON.readTree(oldPlan.text).path("code").asText());
            plan.put("predecessorRequestId",NEW_REVIEW);
            assertEquals("KAFKA_NOTICE_CATALOG_RETAINED_REVISION_MISSING",
                    noticePlan(api,path,plan).path("catalogCoverage").path("code").asText());
        }
    }

    private static JsonNode noticePlan(String api,String path,ObjectNode request)throws Exception{
        PlatformTest.Response result=PlatformTest.http(api,path,PlatformTest.TOKEN,request.toString(),null);
        assertEquals(200,result.status,result.text);
        return Model.JSON.readTree(result.text);
    }

    @Test void optionalControlTopicIsDistinctFromEveryTaskBusinessTopicAndClaimsKnownSharedBootstrapAliases()throws Exception{
        PipelineConfig planned=kafkaPipeline("planned","kafka://main:9092","business.events");
        PipelineConfig other=kafkaPipeline("other","kafka://alias:9092","control.events");
        PipelineConfig.Target kafka=planned.targets.getFirst();
        kafka.messaging.endpoints=List.of("kafka://alias:9092");
        String fingerprint=planned.fingerprint();
        PipelineConfig.KafkaNotice notice=new PipelineConfig.KafkaNotice();
        notice.reservationId=UUID.randomUUID().toString();notice.controlTopic="control.events";
        notice.clusterSha256=Codec.sha256("my Kafka cluster");notice.topicId=NOTICE_TOPIC_ID;
        kafka.messaging.recoveryNotice=notice;
        planned.validate();
        assertEquals(fingerprint,planned.fingerprint(),"Notice metadata cannot change the original CDC fingerprint");
        planned.journal.path=directory.resolve("owner/journals/planned").toString();
        other.journal.path=directory.resolve("peer/journals/other").toString();
        TaskDefinition owner=TaskDefinition.parse(Model.JSON.writeValueAsString(planned),directory,directory.resolve("owner"));
        TaskDefinition peer=TaskDefinition.parse(Model.JSON.writeValueAsString(other),directory,directory.resolve("peer"));
        assertTrue(owner.overlaps(peer));
        PipelineConfig.Target another=kafkaPipeline("additional","kafka://other:9092","control.events").targets.getFirst();
        another.id="second-kafka";
        planned.targets.add(another);
        assertEquals("KAFKA_NOTICE_TOPIC_COLLISION",assertThrows(IllegalArgumentException.class,planned::validate).getMessage());
    }

    private static PipelineConfig kafkaPipeline(String id,String url,String topic){
        PipelineConfig pipeline=PlatformTest.pipeline(id);
        PipelineConfig.Target target=pipeline.targets.getFirst();
        target.product=TargetProduct.KAFKA;target.url=url;
        target.messaging=new PipelineConfig.Messaging();target.messaging.topic=topic;
        return pipeline;
    }

    private static void create(GlobalControlHttpTest fixture,String api,boolean configureControl)throws Exception{
        ObjectNode request=Model.JSON.createObjectNode(),config=Model.JSON.valueToTree(PlatformTest.pipeline("job"));
        ((ObjectNode)config.path("journal")).remove("path");
        ObjectNode kafka=((ArrayNode)config.path("targets")).addObject();
        kafka.put("id","kafka").put("product","KAFKA").put("url","kafka://broker:9092");
        kafka.putObject("mappings").put("orders","orders");
        ObjectNode messaging=kafka.putObject("messaging").put("topic","test.events");
        if(configureControl)messaging.putObject("recoveryNotice").put("reservationId",UUID.randomUUID().toString())
                .put("controlTopic","control.events").put("clusterSha256",Codec.sha256("test Kafka cluster")).put("topicId",NOTICE_TOPIC_ID);
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

    private static void assertPlanPredecessor(PlatformConfig config,TaskService service,JsonNode task,JsonNode reviewed)throws Exception{
        SharedAccessStore access=service.sharedAccess();
        GlobalTaskStore store=new GlobalTaskStore(config.cluster,access,config.cluster.taskKey,service.globalTaskIdentity(),
                config.cluster.maximumTasks);
        DurableCommands commands=new DurableCommands(config.cluster,access,store,config.cluster.maximumCommands);
        AccessControl.Principal admin=AccountDirectory.authenticate(access.accounts(),"Bearer "+PlatformTest.TOKEN);
        AccessControl.Principal operator=AccountDirectory.authenticate(access.accounts(),"Bearer "+OPERATOR);
        String revision=task.path("revision").asText();
        DurableCommands.KafkaNoticePredecessor resolved=commands.kafkaNoticePredecessor("job",ID,revision,admin);
        assertEquals("b",resolved.task().ownerNodeId());
        assertEquals(task.path("activeConfigurationVersion").asText(),resolved.task().configurationVersion());
        assertEquals(admin.id,resolved.administrator().actorId());
        assertEquals(admin.authorizationRevision,resolved.administrator().authorizationRevision());
        assertEquals(reviewed.path("actorId").asText(),resolved.receipt().actorId());
        assertEquals(reviewed.path("kafkaReview"),Model.JSON.valueToTree(resolved.receipt().observation()));
        assertEquals("retained-generation",resolved.receipt().observation().originalFailure().journalGeneration());
        assertThrows(SecurityException.class,()->commands.kafkaNoticePredecessor("job",ID,revision,operator));
        assertEquals("REVISION_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->commands.kafkaNoticePredecessor("job",ID,java.util.UUID.randomUUID().toString(),admin)).getMessage());
        assertThrows(java.util.NoSuchElementException.class,
                ()->commands.kafkaNoticePredecessor("job","pause_before_kafka_1234",revision,admin));
        assertEquals("KAFKA_NOTICE_PREDECESSOR_NOT_VERIFIED",assertThrows(TaskService.Conflict.class,
                ()->commands.kafkaNoticePredecessor("job","unknown_kafka_1234",revision,admin)).getMessage());
        assertEquals(revision,store.get("job",admin).revision,"Looking up a planning predecessor cannot advance the task");
    }

    private static PlatformTest.Response post(String api,ObjectNode request,String id,String token)throws Exception{
        return PlatformTest.http(api,TASK+"/actions",token,request.toString(),id);
    }

    private static void awaitCode(GlobalControlHttpTest fixture,String api,String id,String code)throws Exception{
        PlatformTest.await(()->code.equals(fixture.get(api,TASK+"/recovery-receipts/"+id,PlatformTest.TOKEN).path("code").asText()));
    }
}
