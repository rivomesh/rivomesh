package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.recovery.FailureEvidence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedCommitTest {
    @TempDir Path directory;
    static class Backend extends ManagedFailureReviewTest.Backend {
        volatile String operation,reason;
        @Override public Map<String,Object> pause(TaskDefinition task){return Collections.emptyMap();}
        @Override public Map<String,Object> confirmCommit(TaskDefinition task,Path root,FailureEvidence.Scope scope,CommitRequest request,String operation,String actor,String subscription,long generation)throws Exception{
            captures.incrementAndGet();this.operation=operation;this.reason=request.reason;evidence=request.evidenceId;entered.countDown();if(blocked)release.await();Map<String,Object> result=new LinkedHashMap<>();result.put("hash",Codec.sha256("receipt"));result.put("code","FAILURE_ACK_CONFIRMED");return result;
        }
    }
    PlatformConfig config(){ManagedFailureReviewTest fixture=new ManagedFailureReviewTest();fixture.directory=directory;return fixture.config();}
    CommitRequest request(){CommitRequest request=new CommitRequest();request.evidenceId="11111111-1111-1111-1111-111111111111";request.evidenceHash=Codec.sha256("evidence");request.reason="Reviewed original committed transaction";return request;}
    void createPaused(TaskService tasks)throws Exception{
        ManagedRepairTest.create(tasks,"job");tasks.command("job","PAUSE",PlatformTest.revision(tasks,"job"),"pause_for_confirm",-1,null,ManagedRepairTest.admin());PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(tasks,"pause_for_confirm").path("status").asText()));
    }
    void confirm(TaskService tasks,String revision,String operation,CommitRequest request)throws Exception{tasks.command("job","CONFIRM_COMMIT",revision,operation,1,"pg",ManagedRepairTest.admin(),null,null,request);}
    @Test void adminPauseAndExplicitEvidenceAreRequiredBeforeAdmission()throws Exception{
        Backend backend=new Backend();try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            ManagedRepairTest.create(service,"job");String revision=PlatformTest.revision(service,"job");assertEquals("FAILURE_ACK_REQUIRES_PIPELINE_PAUSED",assertThrows(TaskService.Conflict.class,()->confirm(service,revision,"confirm_no_pause",request())).getMessage());
            assertThrows(SecurityException.class,()->service.command("job","CONFIRM_COMMIT",revision,"confirm_operator",1,"pg",new AccessControl.Principal("operator",PlatformConfig.Role.OPERATOR),null,null,request()));
            CommitRequest invalid=request();invalid.reason="short";assertThrows(IllegalArgumentException.class,()->confirm(service,revision,"confirm_invalid",invalid));assertEquals(0,backend.captures.get());
        }
    }
    @Test void requestAndEvidenceBindingPersistBeforeExecutionAndReplayDoesNotResume()throws Exception{
        Backend backend=new Backend();backend.blocked=true;try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            createPaused(service);String revision=PlatformTest.revision(service,"job");CommitRequest request=request();confirm(service,revision,"confirm_original",request);assertTrue(backend.entered.await(3,TimeUnit.SECONDS));
            JsonNode pending=ManagedFailureReviewTest.op(service,"confirm_original");assertEquals(request.evidenceId,pending.path("failureEvidenceId").asText());assertEquals(request.reason,pending.path("commit").path("reason").asText());assertTrue(new String(Files.readAllBytes(Paths.get(config().dataDirectory,"controller.json")),java.nio.charset.StandardCharsets.UTF_8).contains(request.evidenceHash));
            confirm(service,revision,"confirm_original",request);assertEquals(1,backend.captures.get());CommitRequest other=request();other.reason="Different approval reason";assertEquals("REQUEST_ID_REUSED",assertThrows(TaskService.Conflict.class,()->confirm(service,revision,"confirm_original",other)).getMessage());
            backend.release.countDown();PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"confirm_original").path("status").asText()));assertEquals("PAUSED",service.view("job").get("desired"));assertEquals("FAILURE_ACK_CONFIRMED",ManagedFailureReviewTest.op(service,"confirm_original").path("code").asText());
        }finally{backend.release.countDown();}
    }
    @Test void restartReplaysPendingConfirmationWithSameOperationAndEvidence()throws Exception{
        PlatformConfig config=config();Backend first=new Backend();try(TaskService service=new TaskService(config,first,new PlatformTest.Workers())){createPaused(service);confirm(service,PlatformTest.revision(service,"job"),"confirm_restart",request());PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"confirm_restart").path("status").asText()));}
        Path path=Paths.get(config.dataDirectory,"controller.json");JsonNode state=Model.JSON.readTree(Files.readAllBytes(path));for(JsonNode op:state.path("tasks").path("job").path("operations"))if("confirm_restart".equals(op.path("id").asText()))((ObjectNode)op).put("status","PENDING");Files.write(path,Model.JSON.writeValueAsBytes(state));Backend restarted=new Backend();
        try(TaskService service=new TaskService(config,restarted,new PlatformTest.Workers())){PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"confirm_restart").path("status").asText()));assertEquals("confirm_restart",restarted.operation);assertEquals(request().evidenceId,restarted.evidence);assertEquals(request().reason,restarted.reason);assertEquals("PAUSED",service.view("job").get("desired"));}
    }
    @Test void reviewOnlyClusterRejectsConfirmationBeforeTaskLookupOrQueueing()throws Exception{
        GlobalControlHttpTest fixture=new GlobalControlHttpTest();fixture.directory=directory;PlatformConfig config=fixture.config("a");config.cluster.recoveryReview=true;Backend backend=new Backend();
        try(TaskService service=new TaskService(config,backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,service)){
            server.start();ObjectNode body=Model.JSON.createObjectNode();body.put("action","CONFIRM_COMMIT");body.put("expectedRevision","revision");body.put("errorCount",1);body.put("subscriptionId","pg");body.set("commit",Model.JSON.valueToTree(request()));PlatformTest.Response response=PlatformTest.http(fixture.base(server),"/api/tasks/missing/actions",PlatformTest.TOKEN,body.toString(),"disabled_confirm");assertEquals(409,response.status,response.text);assertTrue(response.text.contains("FAILURE_ACK_DISABLED"));assertEquals(0,backend.captures.get());assertFalse(fixture.get(fixture.base(server),"/api/session",PlatformTest.TOKEN).path("recoveryCommit").asBoolean());
        }
    }
    @Test void httpActionParsesEvidenceRequestAndSessionAdvertisesStandaloneSupport()throws Exception{
        Backend backend=new Backend();try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config(),service)){
            createPaused(service);server.start();String base="http://127.0.0.1:"+server.port();assertTrue(Model.JSON.readTree(PlatformTest.http(base,"/api/session",PlatformTest.TOKEN,null,null).text).path("recoveryCommit").asBoolean());
            ObjectNode body=Model.JSON.createObjectNode();body.put("action","CONFIRM_COMMIT");body.put("expectedRevision",PlatformTest.revision(service,"job"));body.put("errorCount",1);body.put("subscriptionId","pg");body.set("commit",Model.JSON.valueToTree(request()));PlatformTest.Response response=PlatformTest.http(base,"/api/tasks/job/actions",PlatformTest.TOKEN,body.toString(),"http_confirm_commit");assertEquals(202,response.status,response.text);PlatformTest.await(()->"SUCCEEDED".equals(ManagedFailureReviewTest.op(service,"http_confirm_commit").path("status").asText()));assertEquals(request().reason,backend.reason);assertEquals("PAUSED",service.view("job").get("desired"));
        }
    }
}
