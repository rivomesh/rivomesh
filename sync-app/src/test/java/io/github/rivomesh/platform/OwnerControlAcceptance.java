package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Native SQL control protocol fixture; business CDC and two-JVM HTTP acceptance remain separate. */
public final class OwnerControlAcceptance {
    static void require(boolean condition,String code){if(!condition)throw new AssertionError(code);}
    static String env(String name){String value=System.getenv(name);if(value==null)throw new IllegalArgumentException("FIXTURE_ENV_REQUIRED");return value;}
    static final class Backend implements TaskBackend {
        final AtomicInteger checks=new AtomicInteger();
        public Map<String,Object> status(Config c){return Collections.emptyMap();}
        public void pause(Config c){}
        public void resume(Config c,long generation){}
        public Map<String,Object> check(Config c){checks.incrementAndGet();return Collections.singletonMap("tables",1);}
    }
    static final class Workers implements TaskWorker.Factory {
        final AtomicInteger starts=new AtomicInteger(),active=new AtomicInteger();
        public TaskWorker start(Path config,Path log,int heap){starts.incrementAndGet();active.incrementAndGet();return new TaskWorker(){boolean live=true;public boolean alive(){return live;}public int exitCode(){return 0;}public void stop(){if(live){live=false;active.decrementAndGet();}}};}
    }
    static AccessControl.Principal admin(SharedAccessStore access){return AccountDirectory.authenticate(access.accounts(),"Bearer "+env("RIVOMESH_OWNER_TOKEN"));}
    static TaskRequest action(GlobalTaskStore store,SharedAccessStore access,String id,String action)throws Exception{
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.ACTION,"job");request.requestId=id;request.action=action;request.revision=store.get("job",admin(access)).revision;return request;
    }
    static void await(OwnerExecution execution,DurableCommands commands,SharedAccessStore access,String id,String expected)throws Exception{
        long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(45);
        while(System.nanoTime()<deadline){execution.cycle();if(expected.equals(commands.view(id,admin(access)).get("status")))return;Thread.sleep(50);}throw new AssertionError("OWNER_OUTCOME_TIMEOUT");
    }
    static PlatformConfig configuration(Path home){
        PlatformConfig config=new PlatformConfig();config.authToken=env("RIVOMESH_OWNER_TOKEN");config.pollMillis=100;config.tasksDirectory=home.resolve("tasks").toString();config.dataDirectory=home.resolve("controller").toString();config.cluster=new ClusterConfig();
        ClusterConfig cluster=config.cluster;cluster.id="native-owner";cluster.nodeId="b";cluster.sharedAccess=true;cluster.driver=env("RIVOMESH_OWNER_DRIVER");cluster.url=env("RIVOMESH_OWNER_URL");cluster.username=env("RIVOMESH_OWNER_USERNAME");cluster.password=env("RIVOMESH_OWNER_DB_PASSWORD");cluster.heartbeatMillis=250;cluster.leaseMillis=5000;
        return config;
    }
    public static void main(String[] arguments)throws Exception{
        Path home=Paths.get(arguments[0]);PlatformConfig config=configuration(home);ClusterConfig cluster=config.cluster;
        SharedAccessStore access=new SharedAccessStore(config,AccountDirectory.configured(config),null,Collections.emptyMap());GlobalTaskStore entry=new GlobalTaskStore(cluster,access,env("RIVOMESH_OWNER_KEY"),null,100),owner=new GlobalTaskStore(cluster,access,env("RIVOMESH_OWNER_KEY"),entry.identity,100);
        DurableCommands incoming=new DurableCommands(cluster,access,entry,10),queue=new DurableCommands(cluster,access,owner,10);Backend backend=new Backend();Workers workers=new Workers();String directory=UUID.randomUUID().toString();AgentDirectory agents=new AgentDirectory(cluster,access,owner);
        try(TaskService tasks=new TaskService(config,backend,workers);NodeDirectory nodes=new NodeDirectory(config,tasks)){
            nodes.start();agents.register(nodes.currentLease(),directory);OwnerExecution execution=new OwnerExecution(queue,agents,nodes::currentLease,directory,tasks);
            Config pipeline=new Config();pipeline.jobId="job";pipeline.source.host="fixture-source";pipeline.source.database="sales";pipeline.source.username="source-user";pipeline.source.password="source-private-fixture";pipeline.target.url="jdbc:postgresql://fixture-target:5432/business";pipeline.target.username="target-user";pipeline.target.password="target-private-fixture";pipeline.target.schema="sales";pipeline.target.metadataSchema="sync_metadata";
            TaskRequest create=TaskRequest.of(TaskRequest.Kind.CREATE,null);create.requestId="native_create_job";create.yaml=Model.JSON.writeValueAsString(pipeline);incoming.create("job","b",directory,create,admin(access));await(execution,incoming,access,create.requestId,"SUCCEEDED");
            TaskRequest start=action(entry,access,"native_start_job","START");incoming.enqueue(start,admin(access));await(execution,incoming,access,start.requestId,"SUCCEEDED");require(workers.starts.get()==1&&workers.active.get()==1,"OWNER_WORKER_NOT_STARTED");execution.step(start.requestId);require(workers.starts.get()==1,"DUPLICATE_OWNER_START");
            TaskRequest check=action(entry,access,"native_check_job","CHECK");incoming.enqueue(check,admin(access));await(execution,incoming,access,check.requestId,"SUCCEEDED");require(backend.checks.get()==1,"OWNER_CHECK_NOT_EXECUTED_ONCE");
            GlobalTaskViews views=new GlobalTaskViews(cluster,owner,agents,300000);views.publish(nodes.currentLease(),directory,Collections.singletonList(tasks.view("job")));OwnerQueries queries=new OwnerQueries(cluster,owner,agents,10,30000);GlobalTaskPlane global=new GlobalTaskPlane(entry,incoming,agents,views,queries,"entry-a");
            require("b".equals(global.invoke(TaskRequest.of(TaskRequest.Kind.VIEW,"job"),admin(access)).get("ownerNodeId")),"GLOBAL_OWNER_VIEW_MISSING");require(((Number)global.invoke(TaskRequest.of(TaskRequest.Kind.LIST,null),admin(access)).get("total")).longValue()==1,"GLOBAL_TASK_PAGE_MISSING");
            String query=queries.submit(TaskRequest.of(TaskRequest.Kind.VIEW,"job"),admin(access));queries.serve(query,nodes.currentLease(),directory,new LocalTaskPlane(tasks));require("job".equals(queries.await(query,admin(access)).get("id")),"OWNER_QUERY_RESPONSE_MISSING");
            TaskRequest stop=action(entry,access,"native_stop_job","STOP");incoming.enqueue(stop,admin(access));DurableCommands.Dispatch dispatch=queue.claim(stop.requestId,agents,nodes.currentLease(),directory);TaskRequest local=queue.bindLocal(dispatch,tasks.prepareOwnerRequest(dispatch.command.request),agents,nodes.currentLease(),directory);tasks.pinOwnerRequest(local.requestId);new LocalTaskPlane(tasks).invoke(local,dispatch.actor);
            // Simulate loss of the global response after the private controller commit.
            await(execution,incoming,access,stop.requestId,"SUCCEEDED");require(workers.active.get()==0,"OWNER_WORKER_NOT_STOPPED");
            TaskRequest revoked=action(entry,access,"native_revoked_check","CHECK");incoming.enqueue(revoked,admin(access));String revision=access.accounts().revision;
            access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":[],\"token\":\"native-owner-viewer-at-least-32-bytes\"}"),"native_add_viewer",admin(access));await(execution,incoming,access,revoked.requestId,"REJECTED");require(backend.checks.get()==1,"REVOKED_COMMAND_EXECUTED");
            require(!Model.JSON.writeValueAsString(incoming.view(create.requestId,admin(access))).contains("source-private-fixture"),"COMMAND_VIEW_LEAKED_SECRET");
            require("HEALTHY".equals(agents.owner("b",directory).state),"OWNER_BINDING_NOT_HEALTHY");
            System.out.println("OWNER_CONTROL_NATIVE_ACCEPTED: "+cluster.driver+" encrypted admission, agent/lease/directory binding, real TaskService create/start/check/stop, lost global reply, once-only local receipt, fresh authorization, encrypted task observations/global pages and expiring read-only query; fixture Workers only, no business CDC or two-JVM HTTP claim");
        }
    }
}
