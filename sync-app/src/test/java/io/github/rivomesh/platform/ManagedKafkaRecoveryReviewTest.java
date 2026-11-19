package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.KafkaNoticePlanning;
import io.github.rivomesh.cdc.KafkaPhysicalReceipt;
import io.github.rivomesh.cdc.KafkaRecoveryReview;
import io.github.rivomesh.cdc.TargetProduct;
import java.nio.file.Path;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
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

    @Test void retainedKafkaRouteWithRevokedOrProtectedWorkerSecretCannotConnect()throws Exception{
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("PATH")!=null);
        GlobalControlHttpTest fixture=new GlobalControlHttpTest();
        fixture.directory=directory;
        PlatformConfig config=fixture.config("b");
        config.cluster.recoveryReview=true;
        config.workerSecretReferences=Map.of("retained",List.of("PATH"));
        try(ServerSocket sentinel=new ServerSocket(0,1,InetAddress.getByName("127.0.0.1"));
            TaskService service=new TaskService(config,new Backend(),new PlatformTest.Workers());
            ConsoleServer owner=new ConsoleServer(config,service)){
            owner.start();
            PipelineConfig pipeline=kafkaPipeline("retained","kafka://127.0.0.1:"+sentinel.getLocalPort(),"retained.events");
            PipelineConfig.Target target=pipeline.targets.get(0);
            target.username="fixture-worker";target.password="${PATH}";
            target.messaging.securityProtocol="SASL_PLAINTEXT";
            target.messaging.saslMechanism="SCRAM-SHA-512";
            String yaml=Model.JSON.writeValueAsString(pipeline),version=UUID.randomUUID().toString();
            assertTrue(TaskWorker.Jvm.environment(System.getenv(),yaml,config.protectedEnvironment,config.workerSecretReferences).containsKey("PATH"));
            var selection=new KafkaNoticeCatalog.Selection("retained",UUID.randomUUID().toString(),"b",service.ownerDirectoryIdentity(),
                    null,List.of(new KafkaNoticeCatalog.Revision(version,yaml,List.of(target.id))));
            var actor=AccountDirectory.authenticate(service.sharedAccess().accounts(),"Bearer "+PlatformTest.TOKEN);
            config.workerSecretReferences=Map.of("retained",List.of());
            var revoked=service.probeKafkaCatalog(selection,actor);
            assertEquals(Map.of(),revoked.get("topicIdentities"));
            assertFalse(Model.JSON.writeValueAsString(revoked).contains("PATH"));
            config.workerSecretReferences=null;
            config.protectedEnvironment.add("PATH");
            assertEquals(Map.of(),service.probeKafkaCatalog(selection,actor).get("topicIdentities"));
            sentinel.setSoTimeout(300);
            assertThrows(SocketTimeoutException.class,()->{
                try(var unexpected=sentinel.accept()){}
            });
        }
    }

    static final class Backend extends ManagedCommitTest.Backend {
        volatile boolean uncertain;
        volatile int planned;
        volatile String businessIdentity=Codec.sha256("test topic");
        volatile String controlIdentity=Codec.sha256("test control topic");
        final AtomicInteger physicalReads=new AtomicInteger();
        volatile boolean physicalVerified,wrongPhysicalTarget;

        @Override public Map<String,Object> reviewKafkaPhysicalReceipt(TaskDefinition task,KafkaPhysicalReceipt.Scope scope,
                KafkaNoticePlanning.Predecessor predecessor,String key,Predicate<KafkaPhysicalReceipt.MemberWitness> verifyMember){
            physicalReads.incrementAndGet();
            assertEquals("job",task.jobId);
            assertEquals("b",predecessor.ownerNodeId());
            assertEquals(ID,predecessor.requestId());
            assertEquals(predecessor.configurationVersion(),scope.configurationVersion());
            assertEquals(predecessor.ownerDirectoryId(),scope.ownerDirectory());
            assertEquals(Codec.sha256(task.workerYaml),scope.workerProjectionSha256());
            assertTrue(key.matches("[A-Za-z0-9_-]{43}"));
            String target=wrongPhysicalTarget?"different-target":"kafka";
            if(physicalVerified)return Map.of("status","VERIFIED","code","KAFKA_PHYSICAL_RECEIPT_VERIFIED",
                    "targetId",target,"configurationVersion",scope.configurationVersion(),"acknowledgedSequence","6",
                    "receiptSha256",Codec.sha256("checked physical receipt"),"signature","private fixture material");
            return Map.of("status","MISSING","code","KAFKA_PHYSICAL_RECEIPT_UNVERIFIED","targetId",target,
                    "configurationVersion",scope.configurationVersion(),"acknowledgedSequence","6");
        }

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

    @Test void physicalReceiptIsAnAdministratorOnlyReadThroughBothEndpoints()throws Exception{
        var fixture=new GlobalControlHttpTest();fixture.directory=directory;
        var controller=fixture.config("a");var owner=fixture.config("b");
        controller.cluster.recoveryReview=true;owner.cluster.recoveryReview=true;
        Backend backend=new Backend();
        try(TaskService first=new TaskService(controller,new PlatformTest.Backend(),new PlatformTest.Workers());
            ConsoleServer one=new ConsoleServer(controller,first)){
            one.start();
            String api=fixture.base(one),route=TASK+"/kafka-physical-receipt";
            ObjectNode lookup;
            try(TaskService second=new TaskService(owner,backend,new PlatformTest.Workers());
                ConsoleServer two=new ConsoleServer(owner,second)){
                two.start();create(fixture,api,false);createScopedOperator(fixture,api);
                fixture.action(api,"job","PAUSE","physical_pause_job",PlatformTest.TOKEN);
                PlatformTest.await(()->fixture.completed(api,"job","physical_pause_job"));
                assertEquals(202,post(api,request(fixture,api),ID,PlatformTest.TOKEN).status);
                awaitCode(fixture,api,ID,KafkaRecoveryReview.VERIFIED);
                var task=fixture.get(api,TASK,PlatformTest.TOKEN);
                String revision=task.path("revision").asText();
                lookup=Model.JSON.createObjectNode().put("expectedRevision",revision).put("predecessorRequestId",ID);
                assertEquals(403,PlatformTest.http(api,route,OPERATOR,lookup.toString(),null).status);
                assertEquals(409,PlatformTest.http(api,route,PlatformTest.TOKEN,
                        lookup.deepCopy().put("expectedRevision",UUID.randomUUID().toString()).toString(),null).status);
                assertEquals(404,PlatformTest.http(api,route,PlatformTest.TOKEN,
                        lookup.deepCopy().put("predecessorRequestId","nonexistent_review").toString(),null).status);
                assertEquals(400,PlatformTest.http(api,route,PlatformTest.TOKEN,
                        lookup.deepCopy().put("configurationVersion",UUID.randomUUID().toString()).toString(),null).status);
                assertEquals(0,backend.physicalReads.get());
                for(String endpoint:List.of(api,fixture.base(two))){
                    var response=PlatformTest.http(endpoint,route,PlatformTest.TOKEN,lookup.toString(),null);
                    assertEquals(200,response.status,response.text);
                    var result=Model.JSON.readTree(response.text);
                    assertEquals("MISSING",result.path("status").asText());
                    assertEquals(task.path("activeConfigurationVersion").asText(),result.path("configurationVersion").asText());
                    assertFalse(result.has("signature"));assertFalse(result.has("executionAuthorized"));assertFalse(result.has("nativeReceipts"));
                }
                assertEquals(revision,fixture.get(api,TASK,PlatformTest.TOKEN).path("revision").asText());
                assertEquals(2,backend.physicalReads.get());
            }
            var result=PlatformTest.http(api,route,PlatformTest.TOKEN,lookup.toString(),null);
            assertEquals(200,result.status,result.text);
            assertEquals("UNREACHABLE",Model.JSON.readTree(result.text).path("status").asText());
            assertEquals(2,backend.physicalReads.get());
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
            assertFalse(result.text.contains(noticeWriter.bootstrapServers.get(0)));
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
            String approvalRoute=TASK+"/kafka-notice-approvals",approveId="approve_kafka_1234";
            String scopeHash=body.path("approvalScopeSha256").asText();
            assertTrue(scopeHash.matches("[a-f0-9]{64}"));
            ObjectNode approvalInput=plan.deepCopy().put("approvalScopeSha256",scopeHash);
            assertEquals(403,PlatformTest.http(api,approvalRoute,OPERATOR,approvalInput.toString(),approveId).status);
            assertEquals(400,PlatformTest.http(api,approvalRoute,PlatformTest.TOKEN,
                    approvalInput.deepCopy().put("controlTopic","forged").toString(),approveId).status);
            assertCode(409,"KAFKA_NOTICE_APPROVAL_CHANGED",PlatformTest.http(api,approvalRoute,PlatformTest.TOKEN,
                    approvalInput.deepCopy().put("approvalScopeSha256","0".repeat(64)).toString(),"wrong_scope_kafka_1234"));
            var recorded=PlatformTest.http(api,approvalRoute,PlatformTest.TOKEN,approvalInput.toString(),approveId);
            assertEquals(200,recorded.status,recorded.text);
            JsonNode approval=Model.JSON.readTree(recorded.text);
            assertEquals("RECORDED_BLOCKED",approval.path("status").asText());
            assertFalse(approval.path("executionAuthorized").asBoolean(true));
            assertEquals(scopeHash,approval.path("approvalScopeSha256").asText());
            assertEquals(body.path("plan"),approval.path("plan"));
            assertEquals(body.path("catalogCoverage").path("physicalRoutesSha256"),approval.path("physicalRoutesSha256"));
            assertTrue(approval.path("blockers").toString().contains("KAFKA_NOTICE_HISTORICAL_OWNERSHIP_UNVERIFIED"));
            String approvalReceipt=TASK+"/recovery-receipts/"+approveId;
            assertEquals(approval,fixture.get(api,approvalReceipt,PlatformTest.TOKEN));
            var replay=PlatformTest.http(fixture.base(two),approvalRoute,PlatformTest.TOKEN,approvalInput.toString(),approveId);
            assertEquals(200,replay.status,replay.text);
            assertEquals(approval,Model.JSON.readTree(replay.text));
            assertCode(409,"REQUEST_ID_REUSED",PlatformTest.http(api,approvalRoute,PlatformTest.TOKEN,
                    approvalInput.deepCopy().put("approvalScopeSha256","0".repeat(64)).toString(),approveId));
            assertEquals(revision,fixture.get(api,TASK,PlatformTest.TOKEN).path("revision").asText());
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
            assertEquals("KAFKA_NOTICE_CATALOG_RESTORABLE_ROUTES_VERIFIED",noticePlan(api,path,plan).path("catalogCoverage").path("code").asText());
            assertCode(409,"KAFKA_NOTICE_CATALOG_CHANGED",
                    PlatformTest.http(api,approvalRoute,PlatformTest.TOKEN,approvalInput.toString(),approveId));
            assertEquals(approval,fixture.get(api,approvalReceipt,PlatformTest.TOKEN));
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
            assertCode(409,"REVISION_CHANGED",PlatformTest.http(api,approvalRoute,PlatformTest.TOKEN,approvalInput.toString(),approveId));
            String replacement=createReplacementAdmin(fixture,api);
            assertCode(409,"AUTHORIZATION_CHANGED",
                    PlatformTest.http(fixture.base(two),approvalRoute,PlatformTest.TOKEN,approvalInput.toString(),approveId));
            assertEquals(approval,fixture.get(api,approvalReceipt,PlatformTest.TOKEN));
            String accessRevision=fixture.get(api,"/api/accounts",replacement).path("revision").asText();
            ObjectNode revoke=Model.JSON.createObjectNode().put("expectedRevision",accessRevision).put("role","VIEWER").put("enabled",true);
            var revoked=AccountManagementTest.http(api,"PUT","/api/accounts/legacy-admin",replacement,revoke.toString(),"revoke_notice_actor");
            assertEquals(200,revoked.status,revoked.json.toString());
            assertEquals(403,PlatformTest.http(fixture.base(two),approvalRoute,PlatformTest.TOKEN,approvalInput.toString(),approveId).status);
            assertEquals(approval,fixture.get(api,approvalReceipt,replacement));
            String auditKey=null;
            for(JsonNode event:fixture.get(api,"/api/audit?limit=1000",replacement).path("events")){
                if("KAFKA_NOTICE_APPROVAL_RECORDED".equals(event.path("kind").asText())&&scopeHash.equals(event.path("operationHash").asText()))
                    auditKey=event.path("id").asText();
            }
            assertNotNull(auditKey);
            JdbcControlStore control=new JdbcControlStore(controller.cluster);
            try(var connection=control.connect()){
                assertEquals(1,control.update(connection,"DELETE FROM "+control.table("audit_receipt")+" WHERE cluster_id=? AND event_key=?",
                        controller.cluster.id,auditKey));
            }
            assertCode(409,"KAFKA_NOTICE_APPROVAL_AUDIT_REQUIRED",
                    PlatformTest.http(api,approvalReceipt,replacement,null,null));
        }
    }

    @Test void senderPreflightBindsTheRecordedApprovalAndNeverClearsHistoryOrChangesProgress()throws Exception{
        var fixture=new GlobalControlHttpTest();fixture.directory=directory;
        var controller=fixture.config("a");var owner=fixture.config("b");
        controller.cluster.recoveryReview=true;owner.cluster.recoveryReview=true;
        var backend=new Backend();
        try(var first=new TaskService(controller,new PlatformTest.Backend(),new PlatformTest.Workers());
            var second=new TaskService(owner,backend,new PlatformTest.Workers());
            var one=new ConsoleServer(controller,first);var two=new ConsoleServer(owner,second)){
            one.start();two.start();String api=fixture.base(one),peer=fixture.base(two);
            create(fixture,api,true);createScopedOperator(fixture,api);
            String otherAdmin=createReplacementAdmin(fixture,api);
            fixture.action(api,"job","PAUSE","pause_before_sender_preflight",PlatformTest.TOKEN);
            PlatformTest.await(()->fixture.completed(api,"job","pause_before_sender_preflight"));
            assertEquals(202,post(api,request(fixture,api),ID,PlatformTest.TOKEN).status);
            awaitCode(fixture,api,ID,KafkaRecoveryReview.VERIFIED);
            var task=fixture.get(api,TASK,PlatformTest.TOKEN);String revision=task.path("revision").asText();
            var planRequest=Model.JSON.createObjectNode().put("expectedRevision",revision).put("predecessorRequestId",ID);
            var plan=noticePlan(api,TASK+"/kafka-notice-plan",planRequest);
            String scope=plan.path("approvalScopeSha256").asText(),approvalId="record_sender_preflight";
            var recorded=PlatformTest.http(api,TASK+"/kafka-notice-approvals",PlatformTest.TOKEN,
                    planRequest.deepCopy().put("approvalScopeSha256",scope).toString(),approvalId);
            assertEquals(200,recorded.status,recorded.text);
            var historical=Model.JSON.readTree(recorded.text);
            var input=Model.JSON.createObjectNode().put("expectedRevision",revision).put("approvalScopeSha256",scope);
            String preflight=TASK+"/kafka-notice-approvals/"+approvalId+"/preflight";
            assertEquals(403,PlatformTest.http(api,preflight,OPERATOR,input.toString(),null).status);
            assertEquals(403,PlatformTest.http(peer,preflight,otherAdmin,input.toString(),null).status);
            assertCode(404,"RECOVERY_RECEIPT_NOT_FOUND",PlatformTest.http(api,
                    TASK+"/kafka-notice-approvals/unknown_sender_approval/preflight",PlatformTest.TOKEN,input.toString(),null));
            assertCode(400,"KAFKA_NOTICE_PREFLIGHT_REQUEST_INVALID",PlatformTest.http(api,preflight,PlatformTest.TOKEN,
                    input.deepCopy().put("offset","31").toString(),null));
            assertCode(409,"REVISION_CHANGED",PlatformTest.http(api,preflight,PlatformTest.TOKEN,
                    input.deepCopy().put("expectedRevision",UUID.randomUUID().toString()).toString(),null));
            assertCode(409,"KAFKA_NOTICE_APPROVAL_CHANGED",PlatformTest.http(api,preflight,PlatformTest.TOKEN,
                    input.deepCopy().put("approvalScopeSha256","0".repeat(64)).toString(),null));
            assertEquals(0,backend.physicalReads.get());

            var missing=noticePlan(api,preflight,input);
            assertEquals("SENDER_BLOCKED",missing.path("status").asText());
            assertFalse(missing.path("executionAuthorized").asBoolean(true));
            assertEquals("MISSING",missing.path("precedingPhysicalReceipt").path("status").asText());
            assertTrue(missing.path("blockers").toString().contains("KAFKA_NOTICE_PREDECESSOR_PHYSICAL_RECEIPT_REQUIRED"));
            backend.physicalVerified=true;
            var verified=noticePlan(api,preflight,input);
            assertEquals(verified,noticePlan(peer,preflight,input));
            assertEquals("SENDER_BLOCKED",verified.path("status").asText());
            assertFalse(verified.path("executionAuthorized").asBoolean(true));
            assertEquals(scope,verified.path("approvalScopeSha256").asText());
            assertEquals(approvalId,verified.path("approvalRequestId").asText());
            assertEquals(plan.path("plan").path("task"),verified.path("task"));
            assertEquals(plan.path("plan").path("failure"),verified.path("failure"));
            assertEquals(plan.path("plan").path("controlTopic").path("topicId"),verified.path("controlTopicId"));
            assertEquals(plan.path("plan").path("payloadSha256"),verified.path("payloadSha256"));
            assertEquals("VERIFIED",verified.path("precedingPhysicalReceipt").path("status").asText());
            assertEquals(Codec.sha256("checked physical receipt"),verified.path("precedingPhysicalReceipt").path("receiptSha256").asText());
            assertFalse(verified.toString().contains("private fixture"));
            assertEquals(List.of("KAFKA_NOTICE_HISTORICAL_OWNERSHIP_UNVERIFIED","KAFKA_NOTICE_WRITER_NOT_CONFIGURED",
                    "KAFKA_NOTICE_TOPIC_ID_PRODUCE_REQUIRED"),
                    Model.JSON.convertValue(verified.path("blockers"),List.class));
            assertEquals(revision,fixture.get(api,TASK,PlatformTest.TOKEN).path("revision").asText());
            assertEquals(historical,fixture.get(api,TASK+"/recovery-receipts/"+approvalId,PlatformTest.TOKEN));
            assertEquals(1,backend.captures.get());
            backend.wrongPhysicalTarget=true;
            assertCode(409,"KAFKA_NOTICE_PREFLIGHT_RECEIPT_CHANGED",PlatformTest.http(peer,preflight,PlatformTest.TOKEN,input.toString(),null));
            backend.wrongPhysicalTarget=false;
            backend.controlIdentity=backend.businessIdentity;
            assertCode(409,"KAFKA_NOTICE_PREFLIGHT_REVIEW_CHANGED",PlatformTest.http(peer,preflight,PlatformTest.TOKEN,input.toString(),null));
            backend.controlIdentity=Codec.sha256("test control topic");
            var changedWriter=new PlatformConfig.KafkaNoticeWriter();changedWriter.taskId="job";changedWriter.targetId="kafka";
            changedWriter.clusterSha256=Codec.sha256("test Kafka cluster");changedWriter.topicId=NOTICE_TOPIC_ID;
            changedWriter.bootstrapServers=List.of("fixture-writer-only:9094");
            changedWriter.username="configured-writer";changedWriter.password="configured-fixture-secret";
            owner.kafkaNoticeWriters=List.of(changedWriter);
            assertCode(409,"KAFKA_NOTICE_PREFLIGHT_REVIEW_CHANGED",PlatformTest.http(peer,preflight,PlatformTest.TOKEN,input.toString(),null));
            owner.kafkaNoticeWriters=List.of();

            String actorRevision=fixture.get(api,"/api/accounts",otherAdmin).path("revision").asText();
            var change=Model.JSON.createObjectNode().put("expectedRevision",actorRevision).put("role","VIEWER").put("enabled",true);
            var revoked=AccountManagementTest.http(api,"PUT","/api/accounts/operator",otherAdmin,change.toString(),"revoke_sender_context");
            assertEquals(200,revoked.status,revoked.json.toString());
            assertCode(409,"AUTHORIZATION_CHANGED",PlatformTest.http(peer,preflight,PlatformTest.TOKEN,input.toString(),null));
            assertEquals(historical,fixture.get(api,TASK+"/recovery-receipts/"+approvalId,otherAdmin));
        }
    }

    private static void assertCode(int status,String code,PlatformTest.Response response)throws Exception{
        assertEquals(status,response.status,response.text);
        assertEquals(code,Model.JSON.readTree(response.text).path("code").asText());
    }

    private static String createReplacementAdmin(GlobalControlHttpTest fixture,String api)throws Exception{
        String token="approval-observer-admin-for-console-fixture";
        ObjectNode account=Model.JSON.createObjectNode().put("id","approval-observer").put("role","ADMIN").put("enabled",true)
                .put("token",token)
                .put("expectedRevision",fixture.get(api,"/api/accounts",PlatformTest.TOKEN).path("revision").asText());
        var created=PlatformTest.http(api,"/api/accounts",PlatformTest.TOKEN,account.toString(),"approval_rotate_auth");
        assertEquals(201,created.status,created.text);
        return token;
    }

    private static JsonNode noticePlan(String api,String path,ObjectNode request)throws Exception{
        PlatformTest.Response result=PlatformTest.http(api,path,PlatformTest.TOKEN,request.toString(),null);
        assertEquals(200,result.status,result.text);
        return Model.JSON.readTree(result.text);
    }

    @Test void optionalControlTopicIsDistinctFromEveryTaskBusinessTopicAndClaimsKnownSharedBootstrapAliases()throws Exception{
        PipelineConfig planned=kafkaPipeline("planned","kafka://main:9092","business.events");
        PipelineConfig other=kafkaPipeline("other","kafka://alias:9092","control.events");
        PipelineConfig.Target kafka=planned.targets.get(0);
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
        PipelineConfig.Target another=kafkaPipeline("additional","kafka://other:9092","control.events").targets.get(0);
        another.id="second-kafka";
        planned.targets.add(another);
        assertEquals("KAFKA_NOTICE_TOPIC_COLLISION",assertThrows(IllegalArgumentException.class,planned::validate).getMessage());
    }

    private static PipelineConfig kafkaPipeline(String id,String url,String topic){
        PipelineConfig pipeline=PlatformTest.pipeline(id);
        PipelineConfig.Target target=pipeline.targets.get(0);
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
