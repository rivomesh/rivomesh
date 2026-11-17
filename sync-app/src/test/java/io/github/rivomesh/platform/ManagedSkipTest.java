package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.recovery.FailureEvidence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedSkipTest {
    @TempDir Path directory;
    static class Backend extends ManagedCommitTest.Backend {
        final CountDownLatch readEntered=new CountDownLatch(1),readRelease=new CountDownLatch(1);volatile boolean blockRead;volatile String action,plan,request;
        @Override public Map<String,Object> skip(TaskDefinition task,Path root,FailureEvidence.Scope scope,String action,SkipRequest supplied,String operation,String actor,String subscription,long generation)throws Exception{
            captures.incrementAndGet();this.action=action;this.request=operation;this.plan="SKIP_TRANSACTION".equals(action)?operation:supplied.planId;entered.countDown();if(blocked)release.await();Map<String,Object> result=new LinkedHashMap<>();result.put("planId",plan);result.put("planHash",Codec.sha256("plan"));result.put("hash",Codec.sha256("result"));result.put("code","SKIP_CANCEL".equals(action)?"SKIP_CANCELLED":"SKIP_COMPLETED");return result;
        }
        @Override public Map<String,Object> skipPlan(TaskDefinition task,String plan,boolean nativeProof)throws Exception{
            readEntered.countDown();if(blockRead)readRelease.await();Map<String,Object> result=new LinkedHashMap<>();result.put("planId",plan);result.put("planHash",Codec.sha256("plan"));result.put("state","PREPARED");result.put("target","pg");if(nativeProof)result.put("nativeProof",Collections.singletonMap("state","ABSENT"));return result;
        }
        @Override public Map<String,Object> recoveryPage(TaskDefinition task,String sub,String after,int limit,boolean gaps){return Collections.singletonMap("items",Collections.singletonList(Collections.singletonMap("planId","known_plan_1234")));}
    }
    PlatformConfig config(){ManagedCommitTest helper=new ManagedCommitTest();helper.directory=directory;return helper.config();}
    void createPaused(TaskService service)throws Exception{ManagedCommitTest helper=new ManagedCommitTest();helper.directory=directory;helper.createPaused(service);}
    SkipRequest skip(){SkipRequest request=new SkipRequest();request.evidenceId="11111111-1111-1111-1111-111111111111";request.evidenceHash=Codec.sha256("evidence");request.reason="Reviewed the complete transaction and accept this explicit data gap";request.acceptDataLoss=true;return request;}
    SkipRequest resolution(String id,boolean retry){SkipRequest request=new SkipRequest();request.planId=id;request.planHash=Codec.sha256("plan");request.reason="Reviewed the existing immutable recovery plan";request.acceptDataLoss=retry;return request;}
    void command(TaskService service,String revision,String operation,String action,SkipRequest request)throws Exception{service.command("job",action,revision,operation,1,"pg",ManagedRepairTest.admin(),null,null,null,request);}
    @Test void administratorPauseAndExplicitLossApprovalAreRequired()throws Exception{
        Backend backend=new Backend();try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            ManagedRepairTest.create(service,"job");String revision=PlatformTest.revision(service,"job");assertEquals("SKIP_REQUIRES_PIPELINE_PAUSED",assertThrows(TaskService.Conflict.class,()->command(service,revision,"skip_no_pause_1234","SKIP_TRANSACTION",skip())).getMessage());
            assertThrows(SecurityException.class,()->service.command("job","SKIP_TRANSACTION",revision,"operator_skip_1234",1,"pg",new AccessControl.Principal("operator",PlatformConfig.Role.OPERATOR),null,null,null,skip()));SkipRequest invalid=skip();invalid.acceptDataLoss=false;assertThrows(IllegalArgumentException.class,()->command(service,revision,"skip_no_approval_1234","SKIP_TRANSACTION",invalid));assertEquals(0,backend.captures.get());
        }
    }
    @Test void operationAndOriginalPlanIdentityPersistBeforeNativeWorkAndRetryIsIdempotent()throws Exception{
        Backend backend=new Backend();backend.blocked=true;try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            createPaused(service);String revision=PlatformTest.revision(service,"job");command(service,revision,"skip_original_1234","SKIP_TRANSACTION",skip());assertTrue(backend.entered.await(3,TimeUnit.SECONDS));JsonNode pending=ManagedFailureReviewTest.op(service,"skip_original_1234");assertEquals("skip_original_1234",pending.path("skipPlanId").asText());assertTrue(pending.path("skip").path("acceptDataLoss").asBoolean());assertTrue(new String(Files.readAllBytes(Paths.get(config().dataDirectory,"controller.json")),java.nio.charset.StandardCharsets.UTF_8).contains(skip().evidenceHash));command(service,revision,"skip_original_1234","SKIP_TRANSACTION",skip());assertEquals(1,backend.captures.get());backend.release.countDown();PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"skip_original_1234").path("status").asText()));assertEquals("PAUSED",service.view("job").get("desired"));
        }finally{backend.release.countDown();}
    }
    @Test void resolutionOperationsReferenceTheOriginalPlanAndNeverResume()throws Exception{
        Backend backend=new Backend();try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            createPaused(service);for(String action:Arrays.asList("SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL")){
                String request=action.toLowerCase()+"_12345678";command(service,PlatformTest.revision(service,"job"),request,action,resolution("original_plan_1234","SKIP_RETRY".equals(action)));PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,request).path("status").asText()));assertEquals("original_plan_1234",backend.plan);assertEquals(action,backend.action);assertEquals("PAUSED",service.view("job").get("desired"));
            }
        }
    }
    @Test void pendingSkipReopensWithItsOriginalRequestEvidenceAndActorBinding()throws Exception{
        PlatformConfig config=config();Backend first=new Backend();try(TaskService service=new TaskService(config,first,new PlatformTest.Workers())){createPaused(service);command(service,PlatformTest.revision(service,"job"),"skip_restart_1234","SKIP_TRANSACTION",skip());PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"skip_restart_1234").path("status").asText()));}
        Path file=Paths.get(config.dataDirectory,"controller.json");JsonNode saved=Model.JSON.readTree(Files.readAllBytes(file));for(JsonNode op:saved.path("tasks").path("job").path("operations"))if("skip_restart_1234".equals(op.path("id").asText()))((ObjectNode)op).put("status","PENDING");Files.write(file,Model.JSON.writeValueAsBytes(saved));Backend restarted=new Backend();
        try(TaskService service=new TaskService(config,restarted,new PlatformTest.Workers())){PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"skip_restart_1234").path("status").asText()));assertEquals("skip_restart_1234",restarted.plan);assertEquals("skip_restart_1234",restarted.request);assertEquals(skip().evidenceHash,ManagedFailureReviewTest.op(service,"skip_restart_1234").path("skip").path("evidenceHash").asText());}
    }
    @Test void slowNativeReadDoesNotHoldTheWholeTaskServiceMonitor()throws Exception{
        Backend backend=new Backend();backend.blockRead=true;ExecutorService threads=Executors.newFixedThreadPool(2);try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            createPaused(service);ManagedRepairTest.create(service,"other");Future<?> reading=threads.submit(()->{try{return service.skipPlan("job","known_plan_1234",true,ManagedRepairTest.admin());}catch(Exception e){throw new RuntimeException(e);}});assertTrue(backend.readEntered.await(3,TimeUnit.SECONDS));Future<?> other=threads.submit(()->service.view("other"));assertNotNull(other.get(1,TimeUnit.SECONDS));backend.readRelease.countDown();assertNotNull(reading.get(3,TimeUnit.SECONDS));
        }finally{backend.readRelease.countDown();threads.shutdownNow();}
    }
    @Test void httpLedgerRoutesAreAdminOnlyBoundedAndAudited()throws Exception{
        Backend backend=new Backend();PlatformConfig config=config();config.users.add(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,"ledger-viewer-token-at-least-24-bytes"));try(TaskService service=new TaskService(config,backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,service)){
            createPaused(service);server.start();String base="http://127.0.0.1:"+server.port();assertTrue(Model.JSON.readTree(PlatformTest.http(base,"/api/session",PlatformTest.TOKEN,null,null).text).path("recoverySkip").asBoolean());String route="/api/tasks/job/recovery-plans/known_plan_1234";
            assertEquals(200,PlatformTest.http(base,route,PlatformTest.TOKEN,null,null).status);assertEquals(200,PlatformTest.http(base,route+"/inspect",PlatformTest.TOKEN,"{}","native_read_1234").status);assertEquals(403,PlatformTest.http(base,route,"ledger-viewer-token-at-least-24-bytes",null,null).status);
            assertEquals(200,PlatformTest.http(base,"/api/tasks/job/gaps",PlatformTest.TOKEN,"{\"subscriptionId\":\"pg\",\"after\":\"0\",\"limit\":20}","gap_page_1234").status);assertEquals(400,PlatformTest.http(base,"/api/tasks/job/gaps",PlatformTest.TOKEN,"{\"subscriptionId\":\"pg\",\"after\":\"-1\"}","bad_gap_page_1234").status);String audit=Model.JSON.writeValueAsString(service.auditPage(0,0,1000));assertTrue(audit.contains("SKIP_PLAN_READ"));assertTrue(audit.contains("DATA_GAPS_READ"));
        }
    }
    @Test void commitOnlyClusterRejectsSkipBeforeTaskLookupOrQueueing()throws Exception{
        GlobalControlHttpTest helper=new GlobalControlHttpTest();helper.directory=directory;PlatformConfig config=helper.config("a");config.cluster.recoveryReview=true;config.cluster.recoveryCommit=true;Backend backend=new Backend();try(TaskService service=new TaskService(config,backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,service)){
            server.start();ObjectNode body=Model.JSON.createObjectNode();body.put("action","SKIP_TRANSACTION");body.put("expectedRevision","revision");body.put("subscriptionId","pg");body.put("errorCount",1);body.set("skip",Model.JSON.valueToTree(skip()));PlatformTest.Response response=PlatformTest.http(helper.base(server),"/api/tasks/missing/actions",PlatformTest.TOKEN,body.toString(),"skip_disabled_1234");assertEquals(409,response.status,response.text);assertTrue(response.text.contains("SKIP_DISABLED"));assertEquals(0,backend.captures.get());
        }
    }
    @Test void protocolSevenRequiresNormallyStoppedMembersAndRefusesOlderModes()throws Exception{
        GlobalControlHttpTest helper=new GlobalControlHttpTest();helper.directory=directory;ClusterConfig previous=helper.config("old").cluster;previous.recoveryReview=true;previous.recoveryCommit=true;previous.separateExecution=true;ClusterConfig next=helper.config("next").cluster;next.recoveryReview=true;next.recoveryCommit=true;next.recoverySkip=true;next.separateExecution=true;
        try(JdbcControlStore old=new JdbcControlStore(previous)){ControlStore.Node member=ControlStoreTest.node("old");member.role="CONTROLLER_WORKER";ControlStore.Lease lease=old.register(member,Collections.emptyList());assertEquals("GLOBAL_TASK_QUIESCENCE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(next)).getMessage());ControlStoreTest.expire(previous,"old");assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(next));old.stopped(lease);try(JdbcControlStore upgraded=new JdbcControlStore(next)){assertEquals("CONTROL_STORE_PROTOCOL_MISMATCH",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(previous)).getMessage());}}
        next.recoveryCommit=false;assertEquals("RECOVERY_SKIP_POLICY_INVALID",assertThrows(IllegalArgumentException.class,next::validate).getMessage());
    }
}
