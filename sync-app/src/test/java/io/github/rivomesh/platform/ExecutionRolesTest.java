package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ExecutionRolesTest {
    @TempDir Path directory;
    GlobalControlHttpTest http;
    @BeforeEach void initialize(){http=new GlobalControlHttpTest();http.directory=directory;}
    PlatformConfig config(String node,ClusterConfig.Role role){PlatformConfig c=http.config(node);c.cluster.separateExecution=true;c.cluster.role=role;return c;}
    @Test void separatedRolesRequireTheExplicitSharedExecutionProtocol()throws Exception{
        PlatformConfig c=config("a",ClusterConfig.Role.AGENT);c.cluster.separateExecution=false;assertEquals("EXECUTION_ROLE_POLICY_INVALID",assertThrows(IllegalArgumentException.class,c::validate).getMessage());c.cluster.separateExecution=true;c.cluster.sharedTasks=false;assertEquals("EXECUTION_ROLE_POLICY_INVALID",assertThrows(IllegalArgumentException.class,c::validate).getMessage());
        assertFalse(Model.JSON.writeValueAsString(ControlStoreTest.node("legacy")).contains("role"),"Default registry metadata must remain readable by protocol3 members");
    }
    @Test void managementEntryExitDoesNotStopAnIndependentAgentAndAgentExposesOnlyProbes()throws Exception{
        PlatformConfig a=config("a",ClusterConfig.Role.CONTROLLER),b=config("b",ClusterConfig.Role.CONTROLLER),worker=config("worker",ClusterConfig.Role.AGENT);PlatformTest.Workers workers=new PlatformTest.Workers(),none=new PlatformTest.Workers();PlatformTest.Backend backend=new PlatformTest.Backend();
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),none);TaskService second=new TaskService(b,new PlatformTest.Backend(),none);TaskService agent=new TaskService(worker,backend,workers);ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second);ConsoleServer probes=new ConsoleServer(worker,agent)){
            one.start();two.start();probes.start();String api=http.base(one);assertEquals("CONTROLLER",http.get(api,"/api/session",PlatformTest.TOKEN).path("nodeRole").asText());assertEquals(200,PlatformTest.http(http.base(probes),"/readyz",null,null,null).status);
            for(String path:Arrays.asList("/","/app.js","/api/session","/api/tasks","/api/nodes"))assertEquals(404,PlatformTest.http(http.base(probes),path,PlatformTest.TOKEN,null,null).status,path);
            http.create(api,"job","worker");PlatformTest.await(()->http.completed(api,"job","global_create_job"));backend.initialize("job");http.action(api,"job","START","independent_start_job",PlatformTest.TOKEN);PlatformTest.await(()->workers.active.get()==1&&http.completed(api,"job","independent_start_job"));assertEquals(0,none.starts.get());
            JsonNode nodes=http.get(api,"/api/nodes",PlatformTest.TOKEN);for(JsonNode node:nodes.path("nodes"))if(node.path("role").asText().equals("CONTROLLER")){assertEquals(0,node.path("taskCapacity").asInt());assertEquals(0,node.path("runningCapacity").asInt());assertEquals(0,node.path("tasks").size());}
            assertEquals("OWNER_ROLE_REQUIRED",assertThrows(TaskService.Conflict.class,first::beginOwnerScheduling).getMessage());
            one.close();first.close();assertEquals(1,workers.active.get());two.close();second.close();assertEquals(1,workers.active.get());assertEquals("RUNNING",agent.view("job").get("desired"));
            try(TaskService restored=new TaskService(b,new PlatformTest.Backend(),none);ConsoleServer entry=new ConsoleServer(b,restored)){entry.start();String reopened=http.base(entry);assertEquals("worker",http.get(reopened,"/api/tasks/job",PlatformTest.TOKEN).path("ownerNodeId").asText());http.action(reopened,"job","STOP","independent_stop_job",PlatformTest.TOKEN);PlatformTest.await(()->workers.active.get()==0&&http.completed(reopened,"job","independent_stop_job"));}
        }
    }
    @Test void controllersCannotReceiveTasksEvenThroughAnOldAgentBinding()throws Exception{
        PlatformConfig a=config("a",ClusterConfig.Role.CONTROLLER),b=config("b",ClusterConfig.Role.AGENT);
        try(TaskService agent=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer probes=new ConsoleServer(b,agent)){probes.start();}
        b.cluster.role=ClusterConfig.Role.CONTROLLER;
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());TaskService second=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();ObjectNode body=Model.JSON.createObjectNode();body.set("config",Model.JSON.valueToTree(PlatformTest.task("job")));PlatformTest.Response missing=PlatformTest.http(http.base(one),"/api/tasks",PlatformTest.TOKEN,body.toString(),"missing_owner_request");assertEquals(409,missing.status);assertEquals("TASK_OWNER_REQUIRED",Model.JSON.readTree(missing.text).path("code").asText());
            body.put("ownerNodeId","b");PlatformTest.Response rejected=PlatformTest.http(http.base(one),"/api/tasks",PlatformTest.TOKEN,body.toString(),"controller_owner_request");assertEquals(409,rejected.status);assertEquals("OWNER_ROLE_REQUIRED",Model.JSON.readTree(rejected.text).path("code").asText());assertEquals(0,http.get(http.base(one),"/api/tasks",PlatformTest.TOKEN).path("total").asInt());
        }
    }
    @Test void anExecutionDirectoryCannotBeReusedAsAController()throws Exception{
        PlatformConfig c=config("b",ClusterConfig.Role.COMBINED);
        try(TaskService tasks=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(c,tasks)){server.start();http.create(http.base(server),"job","b");PlatformTest.await(()->http.completed(http.base(server),"job","global_create_job"));}
        c.cluster.separateExecution=true;c.cluster.role=ClusterConfig.Role.CONTROLLER;assertEquals("CONTROLLER_EXECUTION_DIRECTORY_NOT_EMPTY",assertThrows(TaskService.Conflict.class,()->new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())).getMessage());
        ClusterConfig previous=Model.JSON.readValue(Model.JSON.writeValueAsString(c.cluster),ClusterConfig.class);previous.role=ClusterConfig.Role.COMBINED;try(JdbcControlStore untouched=new JdbcControlStore(previous)){assertNotNull(untouched.page("",20));}
        c.cluster.role=ClusterConfig.Role.AGENT;try(TaskService restored=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer agent=new ConsoleServer(c,restored)){agent.start();assertEquals("job",restored.view("job").get("id"));}
    }
    @Test void aReplacementControllerDirectoryCannotHideTasksOwnedByThatNode()throws Exception{
        PlatformConfig c=config("b",ClusterConfig.Role.COMBINED);
        try(TaskService tasks=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(c,tasks)){server.start();http.create(http.base(server),"job","b");PlatformTest.await(()->http.completed(http.base(server),"job","global_create_job"));}
        c.cluster.role=ClusterConfig.Role.CONTROLLER;c.tasksDirectory=directory.resolve("replacement/tasks").toString();c.dataDirectory=directory.resolve("replacement/data").toString();try(TaskService replacement=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){assertEquals("CONTROLLER_OWNS_TASKS",assertThrows(TaskService.Conflict.class,()->new ConsoleServer(c,replacement)).getMessage());}
    }
    @Test void registryCannotPublishAControllerAsAnAgentOrWithWorkerCapacity()throws Exception{
        PlatformConfig platform=config("a",ClusterConfig.Role.CONTROLLER);ClusterConfig c=platform.cluster;try(TaskService tasks=new TaskService(platform,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(platform,tasks);JdbcControlStore store=new JdbcControlStore(c)){
            ControlStore.Node node=ControlStoreTest.node("a");node.role="AGENT";assertEquals("NODE_ROLE_INVALID",assertThrows(IllegalArgumentException.class,()->store.register(node,Collections.emptyList())).getMessage());node.role="CONTROLLER";assertEquals("NODE_ROLE_INVALID",assertThrows(IllegalArgumentException.class,()->store.register(node,Collections.emptyList())).getMessage());node.taskCapacity=0;node.runningCapacity=0;assertNotNull(store.register(node,Collections.emptyList()));
        }
    }
    @Test void aCachedAgentBindingCannotAdmitCreationAfterTheOwnerBecomesAController()throws Exception{
        PlatformConfig a=config("a",ClusterConfig.Role.CONTROLLER),b=config("b",ClusterConfig.Role.AGENT);String oldDirectory;
        try(TaskService agent=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer probes=new ConsoleServer(b,agent)){probes.start();oldDirectory=agent.ownerDirectoryIdentity();}
        b.cluster.role=ClusterConfig.Role.CONTROLLER;
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());TaskService second=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();GlobalTaskStore store=new GlobalTaskStore(a.cluster,first.sharedAccess(),a.cluster.taskKey,first.globalTaskIdentity(),a.cluster.maximumTasks);DurableCommands commands=new DurableCommands(a.cluster,first.sharedAccess(),store,a.cluster.maximumCommands);TaskRequest request=TaskRequest.of(TaskRequest.Kind.CREATE,null);request.requestId="stale_agent_binding_create";request.yaml=Model.JSON.writeValueAsString(PlatformTest.task("job"));
            assertEquals("OWNER_ROLE_REQUIRED",assertThrows(TaskService.Conflict.class,()->commands.create("job","b",oldDirectory,request,first.authenticate("Bearer "+PlatformTest.TOKEN))).getMessage());assertThrows(NoSuchElementException.class,()->store.get("job",first.authenticate("Bearer "+PlatformTest.TOKEN)));
        }
    }
    @Test void aTaskAdmittedAfterControllerConstructionStillPreventsControllerRegistration()throws Exception{
        PlatformConfig a=config("a",ClusterConfig.Role.CONTROLLER),b=config("b",ClusterConfig.Role.AGENT);
        try(TaskService agent=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer probes=new ConsoleServer(b,agent)){probes.start();}
        b.cluster.role=ClusterConfig.Role.CONTROLLER;
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer one=new ConsoleServer(a,first)){
            one.start();String api=http.base(one);
            try(TaskService second=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer two=new ConsoleServer(b,second)){
                http.create(api,"job","b");assertEquals("CONTROLLER_OWNS_TASKS",assertThrows(TaskService.Conflict.class,two::start).getMessage());
            }
            b.cluster.role=ClusterConfig.Role.AGENT;try(TaskService restored=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer probes=new ConsoleServer(b,restored)){probes.start();PlatformTest.await(()->http.completed(api,"job","global_create_job"));assertEquals("job",restored.view("job").get("id"));}
        }
    }
}
