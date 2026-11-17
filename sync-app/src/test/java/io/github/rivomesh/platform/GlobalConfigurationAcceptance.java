package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static io.github.rivomesh.platform.OwnerControlAcceptance.*;

/** Native configuration transactions/preview/active-version protocol, with fixture data workers. */
public final class GlobalConfigurationAcceptance {
    static Map<String,Object> config(GlobalTaskPlane plane,SharedAccessStore access)throws Exception{return plane.invoke(TaskRequest.of(TaskRequest.Kind.CONFIGURATION,"job"),admin(access));}
    static TaskRequest edit(Map<String,Object> state,String request){TaskRequest r=TaskRequest.of(TaskRequest.Kind.CONFIG_UPDATE,"job");r.requestId=request;r.revision=(String)state.get("revision");r.document=((JsonNode)state.get("config")).deepCopy();return r;}
    @SuppressWarnings("unchecked") static List<Map<String,Object>> history(GlobalTaskPlane plane,SharedAccessStore access)throws Exception{return(List<Map<String,Object>>)plane.invoke(TaskRequest.of(TaskRequest.Kind.CONFIG_HISTORY,"job"),admin(access)).get("versions");}
    public static void main(String[] arguments)throws Exception{
        Path home=Paths.get(arguments[0]);PlatformConfig cfg=configuration(home);cfg.cluster.leaseMillis=15000;ClusterConfig cluster=cfg.cluster;
        SharedAccessStore access=new SharedAccessStore(cfg,AccountDirectory.configured(cfg),null,Collections.emptyMap());GlobalTaskStore a=new GlobalTaskStore(cluster,access,env("RIVOMESH_OWNER_KEY"),null,100),b=new GlobalTaskStore(cluster,access,env("RIVOMESH_OWNER_KEY"),a.identity,100);
        DurableCommands commands=new DurableCommands(cluster,access,a,10);AgentDirectory agents=new AgentDirectory(cluster,access,b);GlobalTaskViews views=new GlobalTaskViews(cluster,b,agents,300000);OwnerQueries queries=new OwnerQueries(cluster,b,agents,10,30000);GlobalTaskPlane entryA=new GlobalTaskPlane(a,commands,agents,views,queries,"entry-a"),entryB=new GlobalTaskPlane(b,new DurableCommands(cluster,access,b,10),agents,views,queries,"b");
        AtomicReference<Throwable> queryFailure=new AtomicReference<>();ScheduledExecutorService reader=Executors.newSingleThreadScheduledExecutor();
        try(TaskService local=new TaskService(cfg,new Backend(),new Workers());NodeDirectory nodes=new NodeDirectory(cfg,local)){
            nodes.start();String directory=UUID.randomUUID().toString();agents.register(nodes.currentLease(),directory);OwnerExecution execution=new OwnerExecution(commands,agents,nodes::currentLease,directory,local);
            reader.scheduleWithFixedDelay(()->{try{ControlStore.Lease lease=nodes.currentLease();agents.register(lease,directory);for(String id:queries.pending(lease,directory,10))queries.serve(id,lease,directory,new LocalTaskPlane(local));}catch(Throwable failure){if(!reader.isShutdown())queryFailure.compareAndSet(null,failure);}},0,50,TimeUnit.MILLISECONDS);
            Config task=new Config();task.jobId="job";task.source.host="source-fixture";task.source.database="sales";task.source.username="source-user";task.source.password="original-private-source";task.target.url="jdbc:postgresql://target-fixture:5432/business";task.target.username="target-user";task.target.password="private-target";task.target.schema="sales";task.target.metadataSchema="sync_metadata";
            TaskRequest create=TaskRequest.of(TaskRequest.Kind.CREATE,null);create.ownerNodeId="b";create.yaml=Model.JSON.writeValueAsString(task);create.requestId="native_config_create";entryA.invoke(create,admin(access));await(execution,commands,access,create.requestId,"SUCCEEDED");
            Map<String,Object> before=config(entryA,access);Object active=before.get("activeConfigurationVersion");TaskRequest update=edit(before,"native_config_edit");((ObjectNode)update.document.path("source")).put("password","next-private-source");
            TaskRequest preview=update.snapshot();preview.kind=TaskRequest.Kind.CONFIG_PREVIEW;preview.requestId=null;Map<String,Object> comparison=entryB.invoke(preview,admin(access));require(Boolean.FALSE.equals(comparison.get("migrationRequired")),"NATIVE_PREVIEW_MIGRATION_CHANGED");require(!Model.JSON.writeValueAsString(comparison).contains("next-private-source"),"NATIVE_PREVIEW_SECRET_LEAK");
            entryB.invoke(update,admin(access));require(active.equals(config(entryA,access).get("activeConfigurationVersion")),"ACTIVE_CHANGED_BEFORE_OWNER_COMMIT");await(execution,commands,access,update.requestId,"SUCCEEDED");entryA.invoke(update,admin(access));require(history(entryB,access).size()==2,"NATIVE_CONFIG_REPLAY_DUPLICATED");
            Map<String,Object> after=config(entryA,access);require(after.get("configurationVersion").equals(after.get("activeConfigurationVersion")),"NATIVE_ACTIVE_VERSION_NOT_UPDATED");require(!Model.JSON.writeValueAsString(after).contains("next-private-source"),"NATIVE_CONFIG_SECRET_LEAK");
            TaskRequest restore=TaskRequest.of(TaskRequest.Kind.RESTORE,"job");restore.requestId="native_config_restore";restore.reference=(String)history(entryA,access).get(1).get("reference");restore.revision=(String)after.get("revision");entryB.invoke(restore,admin(access));await(execution,commands,access,restore.requestId,"SUCCEEDED");entryA.invoke(restore,admin(access));require(history(entryB,access).size()==3,"NATIVE_RESTORE_NOT_VERSIONED");require(new String(Files.readAllBytes(Paths.get(cfg.tasksDirectory).resolve("job.yml")),java.nio.charset.StandardCharsets.UTF_8).contains("original-private-source"),"NATIVE_RESTORE_OWNER_NOT_APPLIED");
            Map<String,Object> restored=config(entryA,access);TaskRequest invalid=edit(restored,"native_config_rejected");((ObjectNode)invalid.document.path("source")).put("database","different-source");entryB.invoke(invalid,admin(access));await(execution,commands,access,invalid.requestId,"REJECTED");Map<String,Object> rejected=config(entryA,access);require(restored.get("activeConfigurationVersion").equals(rejected.get("activeConfigurationVersion")),"REJECTED_PROPOSAL_REPLACED_ACTIVE");require(!rejected.get("activeConfigurationVersion").equals(rejected.get("configurationVersion")),"REJECTED_PROPOSAL_NOT_VISIBLE");require(queryFailure.get()==null,"NATIVE_QUERY_WORKER_FAILED");
            reader.shutdownNow();reader.awaitTermination(5,TimeUnit.SECONDS);
            System.out.println("GLOBAL_CONFIGURATION_NATIVE_ACCEPTED: "+cluster.driver+" cross-entry masked references, owner preview, atomic proposal/command, active-after-owner-success, replay, authenticated history/restore and rejected proposal preserving active; fixture Workers, no public runtime/HTTP/business CDC claim");
        }finally{reader.shutdownNow();reader.awaitTermination(5,TimeUnit.SECONDS);}
    }
}
