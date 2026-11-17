package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import com.fasterxml.jackson.databind.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedValidationTest {
    @TempDir Path directory;
    static final class Backend implements TaskBackend {
        final AtomicInteger scans=new AtomicInteger(),checks=new AtomicInteger(),details=new AtomicInteger();final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);volatile boolean blocked=true;String id;
        public Map<String,Object> status(Config config){return Collections.singletonMap("run_state","RUNNING");}
        public Map<String,Object> status(TaskDefinition config){return status(config.pipeline.legacy());}
        public void pause(Config config){}public void resume(Config config,long generation){}
        public Map<String,Object> check(Config config){checks.incrementAndGet();return Collections.emptyMap();}
        public Map<String,Object> check(TaskDefinition config){return check(config.pipeline.legacy());}
        public Map<String,Object> validateRows(TaskDefinition config,Path reports,ValidationRequest request,boolean retry)throws Exception{
            scans.incrementAndGet();id=request.reportId;Files.createDirectories(reports.resolve(id));entered.countDown();if(blocked)release.await();return summary(id);
        }
        public Map<String,Object> validationReport(TaskDefinition config,Path reports,String reportId){return summary(reportId);}
        public Map<String,Object> validationDifferences(TaskDefinition config,Path reports,String reportId,String target,String table,String after,int limit,String identity){details.incrementAndGet();return Collections.singletonMap("value","private-detail-value");}
        static Map<String,Object> summary(String id){Map<String,Object> result=new LinkedHashMap<>();result.put("id",id);result.put("state","COMPLETE");result.put("code","VALIDATION_MATCH");result.put("results",Collections.emptyMap());return result;}
    }
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.pollMillis=100;c.authToken=PlatformTest.TOKEN;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;}
    static AccessControl.Principal admin(){return new AccessControl.Principal("legacy-admin",PlatformConfig.Role.ADMIN);}
    static ValidationRequest request(){ValidationRequest request=new ValidationRequest();request.targets.add("pg");return request;}
    @Test void ownerReceiptUsesTheSameNormalizationForRetryWithNullTargetScope()throws Exception{
        Backend backend=new Backend();backend.blocked=false;PlatformTest.Workers workers=new PlatformTest.Workers();try(TaskService service=new TaskService(config(),backend,workers)){
            createAndStart(service,workers,"job");TaskRequest retry=TaskRequest.of(TaskRequest.Kind.ACTION,"job");retry.requestId="retry_null_target_scope";retry.action="VALIDATE_RETRY";retry.revision=PlatformTest.revision(service,"job");retry.validation=new ValidationRequest();retry.validation.reportId=UUID.randomUUID().toString();retry.validation.targets=null;
            new LocalTaskPlane(service).invoke(retry,service.authenticate("Bearer "+PlatformTest.TOKEN));assertNotNull(service.ownerReceipt(retry,"legacy-admin","ADMIN"));PlatformTest.await(()->"SUCCEEDED".equals(service.ownerReceipt(retry,"legacy-admin","ADMIN").phase));assertNull(retry.validation.targets);
        }
    }
    static void createAndStart(TaskService service,PlatformTest.Workers workers,String id)throws Exception{
        PipelineConfig pipeline=PlatformTest.pipeline(id);pipeline.source.serverId=id.equals("other")?910100:910099;
        com.fasterxml.jackson.databind.node.ObjectNode document=Model.JSON.valueToTree(pipeline);((com.fasterxml.jackson.databind.node.ObjectNode)document.get("journal")).remove("path");
        service.create(Model.JSON.writeValueAsString(document),"create_"+id);service.command(id,"START",PlatformTest.revision(service,id),"start_"+id,-1);PlatformTest.await(()->service.view(id).get("workerState").equals("RUNNING"));
    }
    @Test void persistedScopeIsIdempotentAndLongValidationDoesNotBlockOtherControls()throws Exception{
        Backend backend=new Backend();PlatformTest.Workers workers=new PlatformTest.Workers();try(TaskService service=new TaskService(config(),backend,workers)){
            createAndStart(service,workers,"job");createAndStart(service,workers,"other");String revision=PlatformTest.revision(service,"job");ValidationRequest options=request();
            service.command("job","VALIDATE",revision,"validate_job",-1,null,admin(),options);assertTrue(backend.entered.await(3,TimeUnit.SECONDS));
            service.command("job","VALIDATE",revision,"validate_job",-1,null,admin(),request());assertEquals(1,backend.scans.get());assertNull(options.reportId);
            assertThrows(TaskService.Conflict.class,()->service.command("job","VALIDATE",revision,"stale_validation",-1,null,admin(),request()));
            service.command("other","CHECK",PlatformTest.revision(service,"other"),"other_check",-1);PlatformTest.await(()->backend.checks.get()==1);
            String saved=Files.readAllLines(Paths.get(config().dataDirectory,"controller.json")).toString();assertTrue(saved.contains(backend.id));assertFalse(saved.contains("source-secret"));
            backend.release.countDown();PlatformTest.await(()->PlatformTest.operations(service,"job").contains("VALIDATION_MATCH"));assertEquals("PAUSED",service.view("job").get("desired"));
        }
    }
    @Test void cancellationIsDurableAuditedAndNeverResumesTheTask()throws Exception{
        Backend backend=new Backend();PlatformTest.Workers workers=new PlatformTest.Workers();try(TaskService service=new TaskService(config(),backend,workers)){
            createAndStart(service,workers,"job");service.command("job","VALIDATE",PlatformTest.revision(service,"job"),"start_validation",-1,null,admin(),request());assertTrue(backend.entered.await(3,TimeUnit.SECONDS));
            String revision=PlatformTest.revision(service,"job");service.command("job","VALIDATE_CANCEL",revision,"cancel_validation",-1,null,admin());
            PlatformTest.await(()->PlatformTest.operations(service,"job").contains("VALIDATION_CANCELLED"));service.command("job","VALIDATE_CANCEL",revision,"cancel_validation",-1,null,admin());
            assertEquals("PAUSED",service.view("job").get("desired"));PlatformTest.await(()->workers.active.get()==0);assertEquals(1,backend.scans.get());assertTrue(Model.JSON.writeValueAsString(service.auditPage(0,0,1000)).contains("VALIDATE_CANCEL"));
        }
    }
    @Test void restartReusesThePersistedReportIdWithoutStartingAWorker()throws Exception{
        Backend backend=new Backend();backend.blocked=false;PlatformConfig c=config();String report;
        try(TaskService service=new TaskService(c,backend,new PlatformTest.Workers())){
            createAndStart(service,new PlatformTest.Workers(),"job");service.command("job","VALIDATE",PlatformTest.revision(service,"job"),"validate_restart",-1,null,admin(),request());PlatformTest.await(()->PlatformTest.operations(service,"job").contains("VALIDATION_MATCH"));report=backend.id;
        }
        // Model a crash after the report commit and before its controller-operation completion commit.
        Path state=Paths.get(c.dataDirectory,"controller.json");JsonNode tree=Model.JSON.readTree(Files.readAllBytes(state));for(JsonNode operation:tree.path("tasks").path("job").path("operations"))if(operation.path("id").asText().equals("validate_restart"))((com.fasterxml.jackson.databind.node.ObjectNode)operation).put("status","PENDING");Files.write(state,Model.JSON.writeValueAsBytes(tree));
        PlatformTest.Workers restarted=new PlatformTest.Workers();try(TaskService service=new TaskService(c,backend,restarted)){PlatformTest.await(()->backend.scans.get()==2&&PlatformTest.operations(service,"job").contains("VALIDATION_MATCH"));assertEquals(report,backend.id);assertEquals(0,restarted.starts.get());assertEquals("PAUSED",service.view("job").get("desired"));}
    }
    @Test void httpEnforcesAdminMutationAndTaskScopedSummaryReads()throws Exception{
        PlatformConfig c=config();String operator="operator-validation-token-at-least-32-chars",viewer="viewer-validation-token-at-least-32-chars";PlatformConfig.User op=PlatformTest.user("operator",PlatformConfig.Role.OPERATOR,operator),view=PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,viewer);op.taskIds=Arrays.asList("job");view.taskIds=Arrays.asList("job");c.users.add(op);c.users.add(view);Backend backend=new Backend();backend.blocked=false;
        try(TaskService service=new TaskService(c,backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(c,service)){
            createAndStart(service,new PlatformTest.Workers(),"job");server.start();String base="http://127.0.0.1:"+server.port(),body="{\"action\":\"VALIDATE\",\"expectedRevision\":\""+PlatformTest.revision(service,"job")+"\",\"validation\":{\"targets\":[\"pg\"]}}";
            for(String token:Arrays.asList(operator,viewer))assertEquals(403,PlatformTest.http(base,"/api/tasks/job/actions",token,body,"deny_validate_"+token.substring(0,4)).status);
            assertEquals(0,backend.scans.get());assertEquals(202,PlatformTest.http(base,"/api/tasks/job/actions",PlatformTest.TOKEN,body,"http_validate").status);PlatformTest.await(()->backend.scans.get()==1&&PlatformTest.operations(service,"job").contains("VALIDATION_MATCH"));
            String path="/api/tasks/job/validations/"+backend.id;assertEquals(200,PlatformTest.http(base,path,viewer,null,null).status);assertEquals(404,PlatformTest.http(base,path.replace("/job/","/hidden/"),viewer,null,null).status);assertEquals(401,PlatformTest.http(base,path,null,null,null).status);
            assertTrue(Model.JSON.writeValueAsString(service.auditPage(0,0,1000)).contains("GET_VALIDATION_READ"));
        }
    }
    @Test void rawDetailsRequireAdminAndAuditDoesNotContainBusinessValues()throws Exception{
        PlatformConfig c=config();String viewer="scoped-viewer-detail-token-at-least-32-chars",operator="scoped-operator-detail-token-at-least-32-chars";
        for(PlatformConfig.User user:Arrays.asList(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,viewer),PlatformTest.user("operator",PlatformConfig.Role.OPERATOR,operator))){user.taskIds=Collections.singletonList("job");c.users.add(user);}
        Backend backend=new Backend();try(TaskService service=new TaskService(c,backend,new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(c,service)){
            createAndStart(service,new PlatformTest.Workers(),"job");server.start();String base="http://127.0.0.1:"+server.port(),report=UUID.randomUUID().toString(),path="/api/tasks/job/validations/"+report+"/values",body="{\"target\":\"pg\",\"tableId\":\"table\",\"identityHash\":\""+io.github.rivomesh.cdc.Codec.sha256("row")+"\"}";
            for(String token:Arrays.asList(viewer,operator)){assertEquals(403,PlatformTest.http(base,path,token,body,"denied_values_"+token.substring(0,6)).status);assertEquals(403,PlatformTest.http(base,path.replace("/values","/differences"),token,body,"denied_diffs_"+token.substring(0,6)).status);}
            assertEquals(0,backend.details.get());PlatformTest.Response response=PlatformTest.http(base,path,PlatformTest.TOKEN,body,"admin_values");assertEquals(200,response.status);assertTrue(response.text.contains("private-detail-value"));assertEquals(1,backend.details.get());
            String audit=Model.JSON.writeValueAsString(service.auditPage(0,0,1000));assertTrue(audit.contains("READ_VALIDATION_VALUES"));assertTrue(audit.contains(report));assertFalse(audit.contains("private-detail-value"));assertFalse(Files.readAllLines(Paths.get(c.dataDirectory,"controller.json")).toString().contains("private-detail-value"));
        }
    }
}
