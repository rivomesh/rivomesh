package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.recovery.FailureEvidence;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedFailureReviewTest {
    @TempDir Path directory;
    static class Backend implements TaskBackend {
        final AtomicInteger captures=new AtomicInteger(),reads=new AtomicInteger();final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);volatile boolean blocked;volatile String evidence;
        public Map<String,Object> status(Config c){return Collections.singletonMap("run_state","RUNNING");}public Map<String,Object> status(TaskDefinition t){return status(t.pipeline.legacy());}public void pause(Config c){}public void resume(Config c,long g){}public Map<String,Object> check(Config c){return Collections.emptyMap();}
        public Map<String,Object> reviewFailure(TaskDefinition task,Path root,FailureEvidence.Scope scope,String id,String request,String revision,String actor,String subscription,long generation)throws Exception{
            captures.incrementAndGet();evidence=id;entered.countDown();if(blocked)release.await();Map<String,Object> summary=new LinkedHashMap<>();summary.put("id",id);summary.put("hash",Codec.sha256("evidence"));summary.put("configurationRevision",revision);summary.put("rawEvents",2);summary.put("code","FAILURE_EVIDENCE_READY");return summary;
        }
        public Map<String,Object> failureEvidence(Path root,FailureEvidence.Scope scope,String id,String view,int from,int limit){reads.incrementAndGet();Map<String,Object> result=new LinkedHashMap<>();result.put("id",id);result.put("hash",Codec.sha256("evidence"));if(view!=null)result.put("events",Collections.singletonList(Collections.singletonMap("value","authorized-raw-value")));return result;}
    }
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.pollMillis=100;c.authToken=PlatformTest.TOKEN;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;}
    static JsonNode op(TaskService service,String request){for(JsonNode item:Model.JSON.valueToTree(service.view("job")).path("operations"))if(request.equals(item.path("id").asText()))return item;throw new AssertionError("Missing operation");}
    @Test void reviewIdIsPersistedBeforeCaptureAndDuplicateRequestKeepsTheSameObservation()throws Exception{
        Backend backend=new Backend();backend.blocked=true;
        try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers())){
            ManagedRepairTest.create(service,"job");String revision=PlatformTest.revision(service,"job");service.command("job","FAILURE_REVIEW",revision,"review_original",1,"pg",ManagedRepairTest.admin());assertTrue(backend.entered.await(3,TimeUnit.SECONDS));
            JsonNode pending=op(service,"review_original");assertEquals(backend.evidence,pending.path("failureEvidenceId").asText());String state=new String(Files.readAllBytes(Paths.get(config().dataDirectory,"controller.json")),java.nio.charset.StandardCharsets.UTF_8);assertTrue(state.contains(backend.evidence));assertFalse(state.contains("authorized-raw-value"));
            service.command("job","FAILURE_REVIEW",revision,"review_original",1,"pg",ManagedRepairTest.admin());assertEquals(1,backend.captures.get());backend.release.countDown();PlatformTest.await(()->"SUCCEEDED".equals(op(service,"review_original").path("status").asText()));assertEquals(backend.evidence,op(service,"review_original").path("result").path("id").asText());assertEquals("STOPPED",service.view("job").get("desired"));
        }finally{backend.release.countDown();}
    }
    @Test void reopenedPendingReviewUsesTheOriginalIdAndConfigurationRevision()throws Exception{
        Backend backend=new Backend();PlatformConfig config=config();String id,revision;
        try(TaskService service=new TaskService(config,backend,new PlatformTest.Workers())){ManagedRepairTest.create(service,"job");revision=PlatformTest.revision(service,"job");service.command("job","FAILURE_REVIEW",revision,"review_restart",1,"pg",ManagedRepairTest.admin());PlatformTest.await(()->"SUCCEEDED".equals(op(service,"review_restart").path("status").asText()));id=backend.evidence;}
        Path file=Paths.get(config.dataDirectory,"controller.json");JsonNode state=Model.JSON.readTree(Files.readAllBytes(file));for(JsonNode item:state.path("tasks").path("job").path("operations"))if(item.path("id").asText().equals("review_restart"))((ObjectNode)item).put("status","PENDING");Files.write(file,Model.JSON.writeValueAsBytes(state));
        Backend restarted=new Backend();try(TaskService service=new TaskService(config,restarted,new PlatformTest.Workers())){PlatformTest.await(()->"SUCCEEDED".equals(op(service,"review_restart").path("status").asText()));assertEquals(id,restarted.evidence);assertEquals(revision,op(service,"review_restart").path("result").path("configurationRevision").asText());}
    }
    @Test void operatorCannotCreateOrReadEvidenceAndAdminReadsAreAudited()throws Exception{
        Backend backend=new Backend();try(TaskService service=new TaskService(config(),backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config(),service)){
            ManagedRepairTest.create(service,"job");AccessControl.Principal operator=new AccessControl.Principal("operator",PlatformConfig.Role.OPERATOR);String evidence=UUID.randomUUID().toString();
            assertThrows(SecurityException.class,()->service.command("job","FAILURE_REVIEW",PlatformTest.revision(service,"job"),"operator_review",1,"pg",operator));assertThrows(SecurityException.class,()->service.failureEvidence("job",evidence,"RAW",0,20,operator));assertEquals(0,backend.reads.get());
            server.start();String base="http://127.0.0.1:"+server.port();assertTrue(Model.JSON.readTree(PlatformTest.http(base,"/api/session",PlatformTest.TOKEN,null,null).text).path("recoveryReview").asBoolean());
            String uri="/api/tasks/job/failure-evidence/"+evidence;assertEquals(200,PlatformTest.http(base,uri,PlatformTest.TOKEN,null,null).status);PlatformTest.Response raw=PlatformTest.http(base,uri+"/events",PlatformTest.TOKEN,"{\"view\":\"RAW\",\"from\":0,\"limit\":20}","review_values");assertEquals(200,raw.status,raw.text);assertTrue(raw.text.contains("authorized-raw-value"));
            String audit=Model.JSON.writeValueAsString(service.auditPage(0,0,1000));assertTrue(audit.contains("FAILURE_EVIDENCE_EVENTS_READ"));assertFalse(audit.contains("authorized-raw-value"));
        }
    }
    @Test void missingEvidenceReadDoesNotInitializeDirectoryOrAuthority()throws Exception{
        TaskBackend backend=new TaskBackend.Database();FailureEvidence.Scope scope=new FailureEvidence.Scope();scope.taskId="job";scope.ownerNodeId="local";scope.ownerDirectoryId=UUID.randomUUID().toString();Path root=directory.resolve("not-created");String id=UUID.randomUUID().toString();
        assertEquals("FAILURE_EVIDENCE_NOT_FOUND",assertThrows(IllegalStateException.class,()->backend.failureEvidence(root,scope,id,null,0,20)).getMessage());assertFalse(Files.exists(root));
        Files.createDirectory(root);assertEquals("FAILURE_EVIDENCE_AUTHORITY_MISSING",assertThrows(IllegalStateException.class,()->backend.failureEvidence(root,scope,id,null,0,20)).getMessage());assertFalse(Files.exists(root.resolve("evidence.key")));assertFalse(Files.exists(root.resolve("authority.json")));
    }
    @Test void sharedControlWithoutReviewOptInRejectsNewCommandBeforeQueueing()throws Exception{
        GlobalControlHttpTest fixture=new GlobalControlHttpTest();fixture.directory=directory;PlatformConfig config=fixture.config("a");
        try(TaskService service=new TaskService(config,new Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,service)){
            server.start();String base=fixture.base(server);ObjectNode create=Model.JSON.createObjectNode();create.set("config",Model.JSON.valueToTree(PlatformTest.pipeline("job")));((ObjectNode)create.path("config").path("journal")).remove("path");assertEquals(202,PlatformTest.http(base,"/api/tasks",PlatformTest.TOKEN,create.toString(),"disabled_create").status);PlatformTest.await(()->fixture.completed(base,"job","disabled_create"));
            String revision=fixture.get(base,"/api/tasks/job",PlatformTest.TOKEN).path("revision").asText();String body="{\"action\":\"FAILURE_REVIEW\",\"expectedRevision\":\""+revision+"\",\"errorCount\":1,\"subscriptionId\":\"pg\"}";PlatformTest.Response response=PlatformTest.http(base,"/api/tasks/job/actions",PlatformTest.TOKEN,body,"disabled_review");assertEquals(409,response.status,response.text);assertTrue(response.text.contains("FAILURE_REVIEW_DISABLED"));assertEquals(revision,fixture.get(base,"/api/tasks/job",PlatformTest.TOKEN).path("revision").asText());
        }
    }
    @Test void bothEntriesRetainReviewIdentityAndRouteAuthorizedRawPages()throws Exception{
        GlobalControlHttpTest fixture=new GlobalControlHttpTest();fixture.directory=directory;PlatformConfig a=fixture.config("a"),b=fixture.config("b");a.cluster.recoveryReview=true;b.cluster.recoveryReview=true;Backend backend=new Backend();
        try(TaskService first=new TaskService(a,new Backend(),new PlatformTest.Workers());TaskService second=new TaskService(b,backend,new PlatformTest.Workers());ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();String entry=fixture.base(one);fixture.createPipeline(entry);String revision=fixture.get(entry,"/api/tasks/job",PlatformTest.TOKEN).path("revision").asText();String body="{\"action\":\"FAILURE_REVIEW\",\"expectedRevision\":\""+revision+"\",\"errorCount\":1,\"subscriptionId\":\"pg\"}";assertEquals(202,PlatformTest.http(entry,"/api/tasks/job/actions",PlatformTest.TOKEN,body,"remote_review").status);PlatformTest.await(()->fixture.completed(entry,"job","remote_review"));
            PlatformTest.await(()->fixture.operation(entry,"remote_review").hasNonNull("failureEvidenceId"));assertEquals(backend.evidence,fixture.operation(entry,"remote_review").path("failureEvidenceId").asText());assertEquals(1,backend.captures.get());
            for(String base:Arrays.asList(entry,fixture.base(two))){String uri="/api/tasks/job/failure-evidence/"+backend.evidence;assertEquals(200,PlatformTest.http(base,uri,PlatformTest.TOKEN,null,null).status);PlatformTest.Response raw=PlatformTest.http(base,uri+"/events",PlatformTest.TOKEN,"{\"view\":\"RAW\",\"from\":0,\"limit\":1}","remote_raw_values");assertEquals(200,raw.status,raw.text);assertTrue(raw.text.contains("authorized-raw-value"));}
        }
    }
}
