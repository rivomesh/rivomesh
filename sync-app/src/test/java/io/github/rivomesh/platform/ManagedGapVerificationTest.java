package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedGapVerificationTest {
    @TempDir Path directory;
    static class Backend extends ManagedCommitTest.Backend {
        volatile GapVerificationRequest last;volatile String actor,operation;volatile int planReads;
        @Override public Map<String,Object> gapVerification(TaskDefinition task,Path reports,GapVerificationRequest request,boolean execute,String operation,String actor,String createdAt,String subscription,long generation)throws Exception{
            captures.incrementAndGet();last=request;this.actor=actor;this.operation=operation;entered.countDown();if(blocked)release.await();Map<String,Object> result=new LinkedHashMap<>();result.put("id",execute?request.planId:operation);result.put("hash",Codec.sha256("gap-plan"));result.put("code",execute?"GAP_VERIFICATION_COMPLETED":"GAP_VERIFICATION_PLAN_READY");return result;
        }
        @Override public Map<String,Object> gapVerificationPlan(TaskDefinition task,String id){planReads++;Map<String,Object> result=new LinkedHashMap<>();result.put("id",id);result.put("hash",Codec.sha256("gap-plan"));result.put("state","PREPARED");return result;}
    }
    PlatformConfig config(){ManagedCommitTest helper=new ManagedCommitTest();helper.directory=directory;return helper.config();}
    void paused(TaskService service)throws Exception{ManagedCommitTest helper=new ManagedCommitTest();helper.directory=directory;helper.createPaused(service);}
    GapVerificationRequest plan(){GapVerificationRequest request=new GapVerificationRequest();request.reportId="11111111-1111-1111-1111-111111111111";request.gaps.add("original_skip_1234");request.reason="Review the common-cut scope before a fresh destination scan";return request;}
    void command(TaskService service,String revision,String id,String action,GapVerificationRequest request)throws Exception{service.command("job",action,revision,id,1,"pg",ManagedRepairTest.admin(),null,null,null,null,request);}
    @Test void administratorAndWholeTaskPauseAreRequiredBeforeAdmission()throws Exception{
        Backend backend=new Backend();try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            ManagedRepairTest.create(service,"job");String revision=PlatformTest.revision(service,"job");assertEquals("SKIP_REQUIRES_PIPELINE_PAUSED",assertThrows(TaskService.Conflict.class,()->command(service,revision,"gap_before_pause_1234","GAP_VERIFY_PLAN",plan())).getMessage());
            assertThrows(SecurityException.class,()->service.command("job","GAP_VERIFY_PLAN",revision,"operator_gap_1234",1,"pg",new AccessControl.Principal("operator",PlatformConfig.Role.OPERATOR),null,null,null,null,plan()));assertEquals(0,backend.captures.get());
        }
    }
    @Test void inputsAreDurableAndIdempotentBeforeLongRunningWorkAndCancelDoesNotClaimRollback()throws Exception{
        Backend backend=new Backend();backend.blocked=true;try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            paused(service);String revision=PlatformTest.revision(service,"job");command(service,revision,"gap_plan_durable_1234","GAP_VERIFY_PLAN",plan());assertTrue(backend.entered.await(3,TimeUnit.SECONDS));JsonNode pending=ManagedFailureReviewTest.op(service,"gap_plan_durable_1234");assertEquals("gap_plan_durable_1234",pending.path("gapPlanId").asText());assertEquals(plan().gaps.iterator().next(),pending.path("gap").path("gaps").get(0).asText());
            command(service,revision,"gap_plan_durable_1234","GAP_VERIFY_PLAN",plan());assertEquals(1,backend.captures.get());GapVerificationRequest changed=plan();changed.reason="Changed the approved scope reason on the same request";assertThrows(TaskService.Conflict.class,()->command(service,revision,"gap_plan_durable_1234","GAP_VERIFY_PLAN",changed));
            service.command("job","GAP_VERIFY_CANCEL",PlatformTest.revision(service,"job"),"gap_cancel_1234",-1,null,ManagedRepairTest.admin());PlatformTest.await(()->"GAP_VERIFICATION_CANCELLED_CHECK_RECEIPT".equals(ManagedFailureReviewTest.op(service,"gap_plan_durable_1234").path("code").asText()));assertEquals("PAUSED",service.view("job").get("desired"));
        }finally{backend.release.countDown();}
    }
    @Test void persistedPendingExecutionRetainsPlanHashRequestAndActorAfterRestart()throws Exception{
        PlatformConfig config=config();GapVerificationRequest request=new GapVerificationRequest();request.planId="prepared_gap_plan_1234";request.approvedHash=Codec.sha256("gap-plan");request.reason="Approved the reviewed scope and target-exclusive writes";
        try(TaskService service=new TaskService(config,new Backend(),new PlatformTest.Workers())){paused(service);command(service,PlatformTest.revision(service,"job"),"gap_execute_restart_1234","GAP_VERIFY_EXECUTE",request);PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"gap_execute_restart_1234").path("status").asText()));}
        Path file=Paths.get(config.dataDirectory,"controller.json");JsonNode saved=Model.JSON.readTree(Files.readAllBytes(file));for(JsonNode op:saved.path("tasks").path("job").path("operations"))if("gap_execute_restart_1234".equals(op.path("id").asText()))((ObjectNode)op).put("status","PENDING");Files.write(file,Model.JSON.writeValueAsBytes(saved));Backend restarted=new Backend();
        try(TaskService service=new TaskService(config,restarted,new PlatformTest.Workers())){PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"gap_execute_restart_1234").path("status").asText()));assertEquals(request.planId,restarted.last.planId);assertEquals(request.approvedHash,restarted.last.approvedHash);assertEquals("gap_execute_restart_1234",restarted.operation);assertEquals("legacy-admin",restarted.actor);assertEquals("PAUSED",service.view("job").get("desired"));}
    }
    @Test void httpPlanReadAndCommandsRequireAdminAndCarryTheTypedScope()throws Exception{
        PlatformConfig config=config();String viewer="gap-viewer-token-at-least-24-bytes";config.users.add(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,viewer));Backend backend=new Backend();
        try(TaskService service=new TaskService(config,backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,service)){
            paused(service);server.start();String base="http://127.0.0.1:"+server.port(),route="/api/tasks/job/gap-verifications/gap_read_1234";assertEquals(403,PlatformTest.http(base,route,viewer,null,null).status);assertEquals(200,PlatformTest.http(base,route,PlatformTest.TOKEN,null,null).status);assertEquals(1,backend.planReads);
            ObjectNode body=Model.JSON.createObjectNode();body.put("action","GAP_VERIFY_PLAN");body.put("expectedRevision",PlatformTest.revision(service,"job"));body.put("subscriptionId","pg");body.put("errorCount",1);body.set("gap",Model.JSON.valueToTree(plan()));assertEquals(403,PlatformTest.http(base,"/api/tasks/job/actions",viewer,body.toString(),"viewer_gap_plan_1234").status);assertEquals(202,PlatformTest.http(base,"/api/tasks/job/actions",PlatformTest.TOKEN,body.toString(),"http_gap_plan_1234").status);PlatformTest.await(()->backend.captures.get()==1);assertEquals(plan().gaps,backend.last.gaps);assertTrue(Model.JSON.writeValueAsString(service.auditPage(0,0,1000)).contains("GAP_VERIFICATION_READ"));
        }
    }
    @Test void executeCannotChangePlanLimitsOrSmuggleAnotherScope(){
        GapVerificationRequest request=new GapVerificationRequest();request.planId="prepared_gap_plan_1234";request.approvedHash=Codec.sha256("gap-plan");request.reason="Explicit administrator approval for the reviewed scope";request.maximumMillis=1000;assertThrows(IllegalArgumentException.class,()->request.validate(true));request.maximumMillis=300000;request.gaps.add("another_skip_1234");assertThrows(IllegalArgumentException.class,()->request.validate(true));request.gaps.clear();assertDoesNotThrow(()->request.validate(true));
    }
    @Test void twoManagementEntriesPreserveGapInputsAndUseOwnerReadsAndReservedCancellation()throws Exception{
        GlobalControlHttpTest helper=new GlobalControlHttpTest();helper.directory=directory;PlatformConfig a=helper.config("a"),b=helper.config("b");for(PlatformConfig c:Arrays.asList(a,b)){c.cluster.recoveryReview=true;c.cluster.recoveryCommit=true;c.cluster.recoverySkip=true;}Backend backend=new Backend();
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());TaskService second=new TaskService(b,backend,new PlatformTest.Workers());ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();String api=helper.base(one);helper.createPipeline(api);helper.action(api,"job","PAUSE","global_gap_pause_1234",PlatformTest.TOKEN);PlatformTest.await(()->helper.completed(api,"job","global_gap_pause_1234"));backend.blocked=true;
            ObjectNode body=Model.JSON.createObjectNode();body.put("action","GAP_VERIFY_PLAN");body.put("expectedRevision",helper.get(api,"/api/tasks/job",PlatformTest.TOKEN).path("revision").asText());body.put("subscriptionId","pg");body.put("errorCount",1);body.set("gap",Model.JSON.valueToTree(plan()));assertEquals(202,PlatformTest.http(api,"/api/tasks/job/actions",PlatformTest.TOKEN,body.toString(),"global_gap_plan_1234").status);assertTrue(backend.entered.await(3,TimeUnit.SECONDS));PlatformTest.await(()->helper.operation(api,"global_gap_plan_1234").has("gapPlanId"));JsonNode observed=helper.operation(api,"global_gap_plan_1234");assertEquals("global_gap_plan_1234",observed.path("gapPlanId").asText());assertEquals(plan().gaps.iterator().next(),observed.path("gap").path("gaps").get(0).asText());
            assertEquals(200,PlatformTest.http(api,"/api/tasks/job/gap-verifications/global_gap_plan_1234",PlatformTest.TOKEN,null,null).status);assertEquals(1,backend.planReads);
            helper.action(helper.base(two),"job","GAP_VERIFY_CANCEL","global_gap_cancel_1234",PlatformTest.TOKEN);PlatformTest.await(()->helper.completed(api,"job","global_gap_cancel_1234"));PlatformTest.await(()->"GAP_VERIFICATION_CANCELLED_CHECK_RECEIPT".equals(helper.operation(api,"global_gap_plan_1234").path("code").asText()));
        }finally{backend.release.countDown();}
    }

}
