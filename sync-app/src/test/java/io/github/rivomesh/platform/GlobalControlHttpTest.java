package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class GlobalControlHttpTest {
    @TempDir Path directory;
    PlatformConfig config(String node){
        PlatformConfig config=new PlatformConfig();config.port=0;config.authToken=PlatformTest.TOKEN;config.pollMillis=100;config.sessions.secureCookie=false;config.tasksDirectory=directory.resolve(node+"/tasks").toString();config.dataDirectory=directory.resolve(node+"/data").toString();config.cluster=ControlStoreTest.cluster("global_http_"+directory.getFileName().toString(),node);config.cluster.sharedAccess=true;config.cluster.sharedTasks=true;config.cluster.taskKey=GlobalTaskStoreTest.key(1);config.cluster.maximumTasks=100;config.cluster.maximumCommands=20;return config;
    }
    String base(ConsoleServer server){return "http://127.0.0.1:"+server.port();}
    JsonNode get(String base,String path,String token)throws Exception{PlatformTest.Response response=PlatformTest.http(base,path,token,null,null);assertEquals(200,response.status,response.text);return Model.JSON.readTree(response.text);}
    JsonNode create(String base,String task,String owner)throws Exception{ObjectNode body=Model.JSON.createObjectNode();body.set("config",Model.JSON.valueToTree(PlatformTest.task(task)));body.put("ownerNodeId",owner);PlatformTest.Response response=PlatformTest.http(base,"/api/tasks",PlatformTest.TOKEN,body.toString(),"global_create_"+task);assertEquals(202,response.status,response.text);return Model.JSON.readTree(response.text);}
    void action(String base,String id,String action,String request,String token)throws Exception{ObjectNode body=Model.JSON.createObjectNode();body.put("action",action);body.put("expectedRevision",get(base,"/api/tasks/"+id,token).path("revision").asText());PlatformTest.Response response=PlatformTest.http(base,"/api/tasks/"+id+"/actions",token,body.toString(),request);assertEquals(202,response.status,response.text);}
    boolean completed(String base,String id,String request)throws Exception{for(JsonNode op:get(base,"/api/tasks/"+id,PlatformTest.TOKEN).path("operations"))if(request.equals(op.path("id").asText()))return "SUCCEEDED".equals(op.path("status").asText());return false;}
    void createPipeline(String base)throws Exception{
        ObjectNode document=Model.JSON.valueToTree(PlatformTest.pipeline("job"));((ObjectNode)document.get("journal")).remove("path");ObjectNode second=((ObjectNode)document.path("targets").get(0)).deepCopy();second.put("id","my");second.put("product","MYSQL");second.put("url","jdbc:mysql://mysql-copy:3306/");((com.fasterxml.jackson.databind.node.ArrayNode)document.get("targets")).add(second);ObjectNode body=Model.JSON.createObjectNode();body.set("config",document);body.put("ownerNodeId","b");assertEquals(202,PlatformTest.http(base,"/api/tasks",PlatformTest.TOKEN,body.toString(),"global_create_job").status);PlatformTest.await(()->completed(base,"job","global_create_job"));
    }
    JsonNode operation(String base,String request)throws Exception{for(JsonNode op:get(base,"/api/tasks/job",PlatformTest.TOKEN).path("operations"))if(request.equals(op.path("id").asText()))return op;throw new AssertionError("Missing operation");}
    @Test void remoteValidationRetainsTheReportIdentityRequiredByTheConsole()throws Exception{
        PlatformConfig a=config("a"),b=config("b");ManagedValidationTest.Backend backend=new ManagedValidationTest.Backend();backend.blocked=false;PlatformTest.Workers workers=new PlatformTest.Workers();
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());TaskService second=new TaskService(b,backend,workers);ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();String api=base(one);createPipeline(api);action(api,"job","START","global_start_job",PlatformTest.TOKEN);PlatformTest.await(()->workers.active.get()==1&&completed(api,"job","global_start_job"));
            ObjectNode body=Model.JSON.createObjectNode();body.put("action","VALIDATE");body.put("expectedRevision",get(api,"/api/tasks/job",PlatformTest.TOKEN).path("revision").asText());ValidationRequest options=ManagedValidationTest.request();options.targets.add("my");body.set("validation",Model.JSON.valueToTree(options));assertEquals(202,PlatformTest.http(api,"/api/tasks/job/actions",PlatformTest.TOKEN,body.toString(),"global_validate_job").status);PlatformTest.await(()->completed(api,"job","global_validate_job"));
            PlatformTest.await(()->operation(api,"global_validate_job").has("result"));JsonNode op=operation(api,"global_validate_job");assertEquals(backend.id,op.path("validation").path("reportId").asText());assertEquals(2,op.path("validation").path("targets").size());assertEquals("SUCCEEDED",op.path("deliveryStatus").asText());assertTrue(op.hasNonNull("finishedAt"));assertFalse(op.toString().contains("source-secret"));
        }
    }
    @Test void remoteRepairRetainsTheReviewedPlanIdentityRequiredByTheConsole()throws Exception{
        PlatformConfig a=config("a"),b=config("b");ManagedRepairTest.Backend backend=new ManagedRepairTest.Backend();
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());TaskService second=new TaskService(b,backend,new PlatformTest.Workers());ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();String api=base(one);createPipeline(api);RepairRequest plan=ManagedRepairTest.plan();plan.scope.add("my/"+io.github.rivomesh.cdc.Codec.sha256("table"));ObjectNode body=Model.JSON.createObjectNode();body.put("action","REPAIR_PLAN");body.put("expectedRevision",get(api,"/api/tasks/job",PlatformTest.TOKEN).path("revision").asText());body.set("repair",Model.JSON.valueToTree(plan));assertEquals(202,PlatformTest.http(api,"/api/tasks/job/actions",PlatformTest.TOKEN,body.toString(),"global_repair_plan").status);PlatformTest.await(()->completed(api,"job","global_repair_plan"));PlatformTest.await(()->operation(api,"global_repair_plan").has("result"));JsonNode op=operation(api,"global_repair_plan");assertEquals(backend.last.planId,op.path("repair").path("planId").asText());assertEquals(plan.reportId,op.path("repair").path("reportId").asText());assertEquals("SUCCEEDED",op.path("deliveryStatus").asText());
        }
    }
    @Test void separateHttpEntriesCreateAndControlTheRealOwnerAndReturnGlobalState()throws Exception{
        PlatformConfig a=config("a"),b=config("b");PlatformTest.Workers wa=new PlatformTest.Workers(),wb=new PlatformTest.Workers();PlatformTest.Backend ba=new PlatformTest.Backend(),bb=new PlatformTest.Backend();
        try(TaskService first=new TaskService(a,ba,wa);TaskService second=new TaskService(b,bb,wb);ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();String api=base(one),owner=base(two);create(api,"job","b");PlatformTest.await(()->completed(api,"job","global_create_job"));assertThrows(NoSuchElementException.class,()->first.view("job"));assertEquals("job",second.view("job").get("id"));assertEquals("b",get(api,"/api/tasks/job",PlatformTest.TOKEN).path("ownerNodeId").asText());assertEquals(1,get(owner,"/api/tasks",PlatformTest.TOKEN).path("total").asInt());
            assertEquals("GLOBAL",get(api,"/api/session",PlatformTest.TOKEN).path("taskControlScope").asText());assertEquals("GLOBAL",get(owner,"/api/nodes",PlatformTest.TOKEN).path("taskControlScope").asText());bb.initialize("job");action(api,"job","START","global_start_job",PlatformTest.TOKEN);PlatformTest.await(()->wb.active.get()==1&&completed(api,"job","global_start_job"));assertEquals(0,wa.starts.get());assertEquals(1,wb.starts.get());action(owner,"job","STOP","global_stop_job",PlatformTest.TOKEN);PlatformTest.await(()->wb.active.get()==0&&completed(api,"job","global_stop_job"));
            assertEquals("job",get(api,"/api/tasks?ownerNodeId=b&taskId=job",PlatformTest.TOKEN).path("tasks").path(0).path("id").asText());
            assertEquals(0,get(owner,"/api/tasks?ownerNodeId=a&taskId=job",PlatformTest.TOKEN).path("total").asInt());
            assertEquals(400,PlatformTest.http(api,"/api/tasks?ownerNodeId=a&ownerNodeId=b",PlatformTest.TOKEN,null,null).status);
            assertFalse(get(api,"/api/tasks/job",PlatformTest.TOKEN).toString().contains("source-secret"));assertEquals(400,PlatformTest.http(api,"/api/tasks?limit=51",PlatformTest.TOKEN,null,null).status);
        }
    }
    @Test void ownerScopedOperatorCanControlBThroughAWithoutLocalNodePermission()throws Exception{
        PlatformConfig a=config("a"),b=config("b");PlatformTest.Workers workers=new PlatformTest.Workers();PlatformTest.Backend backend=new PlatformTest.Backend();
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());TaskService second=new TaskService(b,backend,workers);ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();String api=base(one);create(api,"job","b");PlatformTest.await(()->completed(api,"job","global_create_job"));ObjectNode account=Model.JSON.createObjectNode();account.put("expectedRevision",get(api,"/api/accounts",PlatformTest.TOKEN).path("revision").asText());account.put("id","operator");account.put("role","OPERATOR");account.put("enabled",true);account.put("token","global-http-operator-at-least-32-bytes");account.putArray("taskIds").add("job");account.putArray("nodeIds").add("b");assertEquals(201,PlatformTest.http(api,"/api/accounts",PlatformTest.TOKEN,account.toString(),"global_add_operator").status);
            String token="global-http-operator-at-least-32-bytes";assertEquals("b",get(api,"/api/tasks/job",token).path("ownerNodeId").asText());assertEquals(403,PlatformTest.http(api,"/api/nodes",token,null,null).status);backend.initialize("job");action(api,"job","START","operator_start_job",token);PlatformTest.await(()->workers.active.get()==1&&completed(api,"job","operator_start_job"));
            assertEquals(1,get(api,"/api/tasks?ownerNodeId=b&taskId=job",token).path("total").asInt());
            assertEquals(0,get(api,"/api/tasks?ownerNodeId=a&taskId=job",token).path("total").asInt());

        }
    }
    @Test void persistedGlobalBindingRefusesDisableChangedNodeMissingDirectoryAndWrongKey()throws Exception{
        PlatformConfig c=config("a");
        try(TaskService tasks=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(c,tasks)){server.start();assertEquals("GLOBAL",get(base(server),"/api/session",PlatformTest.TOKEN).path("taskControlScope").asText());}
        c.cluster.sharedTasks=false;c.cluster.taskKey=null;assertEquals("GLOBAL_TASK_AUTHORITY_REQUIRED",assertThrows(java.io.IOException.class,()->new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())).getMessage());c.cluster.sharedTasks=true;c.cluster.taskKey=GlobalTaskStoreTest.key(1);c.cluster.nodeId="changed";assertEquals("GLOBAL_TASK_AUTHORITY_REQUIRED",assertThrows(java.io.IOException.class,()->new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())).getMessage());c.cluster.nodeId="a";
        c.cluster.taskKey=GlobalTaskStoreTest.key(2);try(TaskService tasks=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){assertEquals("CONTROL_KEY_MISMATCH",assertThrows(TaskService.Conflict.class,()->new ConsoleServer(c,tasks)).getMessage());}c.cluster.taskKey=GlobalTaskStoreTest.key(1);Files.delete(Paths.get(c.dataDirectory).resolve("owner-directory-id"));try(TaskService tasks=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){assertEquals("OWNER_DIRECTORY_REQUIRED",assertThrows(java.io.IOException.class,()->new ConsoleServer(c,tasks)).getMessage());}
    }
    @Test void enablingGlobalTasksRefusesExistingLocalTasksBeforeUpgradingTheStore()throws Exception{
        PlatformConfig c=config("a");c.cluster.sharedTasks=false;c.cluster.taskKey=null;c.cluster.sharedAccess=false;
        try(TaskService tasks=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){tasks.create(Model.JSON.writeValueAsString(PlatformTest.task("local")),"local_before_global");}
        c.cluster.sharedAccess=true;c.cluster.sharedTasks=true;c.cluster.taskKey=GlobalTaskStoreTest.key(1);assertEquals("GLOBAL_TASK_IMPORT_REQUIRED",assertThrows(TaskService.Conflict.class,()->new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())).getMessage());c.cluster.sharedTasks=false;c.cluster.sharedAccess=false;c.cluster.taskKey=null;try(JdbcControlStore untouched=new JdbcControlStore(c.cluster)){assertEquals("LOCAL",untouched.page("",1).get("taskControlScope"));}
    }
    @Test void ownerReopenResumesOnlyAfterAdmissionAndAReplacementDirectoryCannotTakeOver()throws Exception{
        PlatformConfig a=config("a"),b=config("b");PlatformTest.Workers workers=new PlatformTest.Workers();PlatformTest.Backend backend=new PlatformTest.Backend();
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer one=new ConsoleServer(a,first)){
            one.start();String api=base(one);
            try(TaskService second=new TaskService(b,backend,workers);ConsoleServer two=new ConsoleServer(b,second)){two.start();create(api,"job","b");PlatformTest.await(()->completed(api,"job","global_create_job"));backend.initialize("job");action(api,"job","START","global_start_job",PlatformTest.TOKEN);PlatformTest.await(()->workers.active.get()==1&&completed(api,"job","global_start_job"));}
            assertEquals(0,workers.active.get());
            try(TaskService restored=new TaskService(b,backend,workers);ConsoleServer server=new ConsoleServer(b,restored)){assertEquals(0,workers.active.get());server.start();PlatformTest.await(()->workers.active.get()==1&&"HEALTHY".equals(get(api,"/api/tasks/job",PlatformTest.TOKEN).path("ownerState").asText()));assertEquals(2,workers.starts.get());}
            action(api,"job","STOP","offline_stop_before_restart",PlatformTest.TOKEN);
            try(TaskService restored=new TaskService(b,backend,workers);ConsoleServer server=new ConsoleServer(b,restored)){server.start();PlatformTest.await(()->completed(api,"job","offline_stop_before_restart"));assertEquals(2,workers.starts.get(),"An offline STOP must be considered before restoring the previous RUNNING intent");assertEquals(0,workers.active.get());}
            b.tasksDirectory=directory.resolve("replacement/tasks").toString();b.dataDirectory=directory.resolve("replacement/data").toString();
            try(TaskService replacement=new TaskService(b,backend,workers);ConsoleServer server=new ConsoleServer(b,replacement)){assertEquals("AGENT_DIRECTORY_CHANGED",assertThrows(TaskService.Conflict.class,server::start).getMessage());assertEquals(0,workers.active.get());}
        }
    }
}
