package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import io.github.rivomesh.cdc.Codec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedRepairTest {
    @TempDir Path directory;
    static final String HASH=Codec.sha256("reviewed plan");
    static class Backend implements TaskBackend {
        final AtomicInteger calls=new AtomicInteger(),reads=new AtomicInteger(),checks=new AtomicInteger();final CountDownLatch entered=new CountDownLatch(1);volatile boolean block;volatile RepairRequest last;volatile String actor;
        public Map<String,Object> status(Config c){return Collections.singletonMap("run_state","PAUSED");}public Map<String,Object> status(TaskDefinition t){return status(t.pipeline.legacy());}public void pause(Config c){}public Map<String,Object> pause(TaskDefinition t){return Collections.emptyMap();}public void resume(Config c,long g){}public Map<String,Object> check(Config c){checks.incrementAndGet();return Collections.emptyMap();}public Map<String,Object> check(TaskDefinition c){return check(c.pipeline.legacy());}
        public Map<String,Object> repair(TaskDefinition t,Path reports,RepairRequest request,boolean execute,String actor,String operation,String createdAt)throws Exception{
            calls.incrementAndGet();last=request;this.actor=actor;entered.countDown();if(block)new CountDownLatch(1).await();Map<String,Object> result=new LinkedHashMap<>();result.put("code",execute?"REPAIR_COMPLETED":"REPAIR_PLAN_READY");result.put("id",request.planId);result.put("hash",HASH);return result;
        }
        public Map<String,Object> repairPlan(TaskDefinition t,Path r,String report,String plan){reads.incrementAndGet();Map<String,Object> result=new LinkedHashMap<>();result.put("id",plan);result.put("hash",HASH);return result;}
    }
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.pollMillis=100;c.authToken=PlatformTest.TOKEN;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;}
    static RepairRequest plan(){RepairRequest request=new RepairRequest();request.reportId=UUID.randomUUID().toString();request.scope.add("pg/"+Codec.sha256("table"));return request;}
    static void create(TaskService service,String id)throws Exception{
        PipelineConfig pipeline=PlatformTest.pipeline(id);pipeline.source.serverId=id.equals("other")?920002:920001;ObjectNode node=Model.JSON.valueToTree(pipeline);((ObjectNode)node.get("journal")).remove("path");service.create(Model.JSON.writeValueAsString(node),"create_"+id);
    }
    static AccessControl.Principal admin(){return new AccessControl.Principal("legacy-admin",PlatformConfig.Role.ADMIN);}
    @Test void planIdIsPersistedBeforeWorkAndRequestReplayCannotCreateAnotherPlan()throws Exception{
        Backend backend=new Backend();backend.block=true;try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            create(service,"job");create(service,"other");RepairRequest request=plan();String revision=PlatformTest.revision(service,"job");service.command("job","REPAIR_PLAN",revision,"plan_job",-1,null,admin(),null,request);assertTrue(backend.entered.await(3,TimeUnit.SECONDS));assertNotNull(backend.last.planId);assertNull(request.planId);String state=new String(Files.readAllBytes(Paths.get(config().dataDirectory,"controller.json")),java.nio.charset.StandardCharsets.UTF_8);assertTrue(state.contains(backend.last.planId));assertFalse(state.contains("source-secret"));
            service.command("job","REPAIR_PLAN",revision,"plan_job",-1,null,admin(),null,request);assertEquals(1,backend.calls.get());service.command("other","CHECK",PlatformTest.revision(service,"other"),"other_check",-1);PlatformTest.await(()->backend.checks.get()==1);
            assertThrows(TaskService.Conflict.class,()->service.command("job","REPAIR_PLAN",revision,"new_plan",-1,null,admin(),null,request));service.command("job","REPAIR_CANCEL",PlatformTest.revision(service,"job"),"cancel_plan",-1,null,admin());PlatformTest.await(()->PlatformTest.operations(service,"job").contains("REPAIR_CANCELLED"));
        }
    }
    @Test void executionCancellationIsDurableAndKeepsTaskPaused()throws Exception{
        Backend backend=new Backend();backend.block=true;try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            create(service,"job");service.command("job","PAUSE",PlatformTest.revision(service,"job"),"pause_before_repair",-1);PlatformTest.await(()->PlatformTest.operations(service,"job").contains("SHARED_PAUSED"));RepairRequest execute=plan();execute.scope.clear();execute.planId=UUID.randomUUID().toString();execute.approvedHash=HASH;service.command("job","REPAIR_EXECUTE",PlatformTest.revision(service,"job"),"execute_job",-1,null,admin(),null,execute);assertTrue(backend.entered.await(3,TimeUnit.SECONDS));assertEquals("legacy-admin",backend.actor);
            service.command("job","REPAIR_CANCEL",PlatformTest.revision(service,"job"),"cancel_execute",-1,null,admin());PlatformTest.await(()->PlatformTest.operations(service,"job").contains("REPAIR_CANCELLED"));assertEquals("PAUSED",service.view("job").get("desired"));String audit=Model.JSON.writeValueAsString(service.auditPage(0,0,1000));assertTrue(audit.contains(execute.planId));assertTrue(audit.contains(HASH));
        }
    }
    @Test void persistedPendingPlanReplaysSameIdentityAcrossControllerRestart()throws Exception{
        PlatformConfig config=config();Backend backend=new Backend();String planId;
        try(TaskService service=new TaskService(config,backend,new PlatformTest.Workers())){create(service,"job");service.command("job","REPAIR_PLAN",PlatformTest.revision(service,"job"),"plan_restart",-1,null,admin(),null,plan());PlatformTest.await(()->PlatformTest.operations(service,"job").contains("REPAIR_PLAN_READY"));planId=backend.last.planId;}
        Path file=Paths.get(config.dataDirectory,"controller.json");JsonNode state=Model.JSON.readTree(Files.readAllBytes(file));for(JsonNode operation:state.path("tasks").path("job").path("operations"))if(operation.path("id").asText().equals("plan_restart"))((ObjectNode)operation).put("status","PENDING");Files.write(file,Model.JSON.writeValueAsBytes(state));
        PlatformTest.Workers workers=new PlatformTest.Workers();try(TaskService service=new TaskService(config,backend,workers)){PlatformTest.await(()->backend.calls.get()==2&&PlatformTest.operations(service,"job").contains("REPAIR_PLAN_READY"));assertEquals(planId,backend.last.planId);assertEquals(0,workers.starts.get());}
    }
    @Test void httpRejectsRestrictedRolesBeforeRepairWorkOrPlanReads()throws Exception{
        PlatformConfig config=config();String op="operator-managed-repair-token-at-least-32",viewer="viewer-managed-repair-token-at-least-32";for(PlatformConfig.User user:Arrays.asList(PlatformTest.user("operator",PlatformConfig.Role.OPERATOR,op),PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,viewer))){user.taskIds=Collections.singletonList("job");config.users.add(user);}Backend backend=new Backend();
        try(TaskService service=new TaskService(config,backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,service)){
            create(service,"job");server.start();String base="http://127.0.0.1:"+server.port();RepairRequest request=plan();Map<String,Object> body=new LinkedHashMap<>();body.put("action","REPAIR_PLAN");body.put("expectedRevision",PlatformTest.revision(service,"job"));body.put("repair",request);String json=Model.JSON.writeValueAsString(body);
            for(String token:Arrays.asList(op,viewer))assertEquals(403,PlatformTest.http(base,"/api/tasks/job/actions",token,json,"deny_"+token.substring(0,4)).status);assertEquals(0,backend.calls.get());assertEquals(202,PlatformTest.http(base,"/api/tasks/job/actions",PlatformTest.TOKEN,json,"admin_plan").status);PlatformTest.await(()->backend.calls.get()==1&&PlatformTest.operations(service,"job").contains("REPAIR_PLAN_READY"));String path="/api/tasks/job/validations/"+request.reportId+"/repairs/"+backend.last.planId;
            for(String token:Arrays.asList(op,viewer))assertEquals(403,PlatformTest.http(base,path,token,null,null).status);assertEquals(0,backend.reads.get());assertEquals(200,PlatformTest.http(base,path,PlatformTest.TOKEN,null,null).status);assertEquals(1,backend.reads.get());assertTrue(Model.JSON.writeValueAsString(service.auditPage(0,0,1000)).contains("READ_REPAIR_PLAN"));
        }
    }
}
