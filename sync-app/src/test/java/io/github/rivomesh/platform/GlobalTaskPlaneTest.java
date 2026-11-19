package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class GlobalTaskPlaneTest {
    @TempDir Path directory;
    OwnerExecutionTest fixture;GlobalTaskPlane a,b;GlobalTaskViews views;OwnerExecution execution;
    @BeforeEach void initialize()throws Exception{
        fixture=new OwnerExecutionTest();fixture.directory=directory;fixture.initialize();execution=fixture.executor();GlobalTaskStoreTest store=fixture.fixture;ClusterConfig config=store.config.cluster;
        views=new GlobalTaskViews(config,store.a,fixture.owner.a,300000);OwnerQueries queries=new OwnerQueries(config,store.a,fixture.owner.a,10,30000);
        a=new GlobalTaskPlane(store.a,fixture.commands,fixture.owner.a,views,queries,"a");
        b=new GlobalTaskPlane(store.b,new DurableCommands(config,store.access,store.b,10),fixture.owner.b,new GlobalTaskViews(config,store.b,fixture.owner.b,300000),new OwnerQueries(config,store.b,fixture.owner.b,10,30000),"b");
    }
    @AfterEach void close()throws Exception{if(fixture!=null)fixture.close();}
    Map<String,Object> create()throws Exception{
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.CREATE,null);request.ownerNodeId="a";request.requestId="global_create_job";request.yaml=Model.JSON.writeValueAsString(PlatformTest.task("job"));return b.invoke(request,fixture.admin());
    }
    @SuppressWarnings("unchecked") void publish()throws Exception{views.publish(fixture.owner.lease,fixture.fixture.directory,(List<Map<String,Object>>)fixture.local.list(fixture.admin()).get("tasks"));}
    Map<String,Object> view(GlobalTaskPlane entry)throws Exception{return entry.invoke(TaskRequest.of(TaskRequest.Kind.VIEW,"job"),fixture.admin());}
    @Test void taskCreatedAtAnotherEntryHasOneSharedRevisionAndRealOwnerExecution()throws Exception{
        Map<String,Object> created=create();assertEquals("a",created.get("ownerNodeId"));assertEquals("UNKNOWN",created.get("workerState"));assertEquals(created.get("revision"),view(a).get("revision"));
        execution.cycle();publish();assertEquals("SHARED",view(b).get("authority"));assertEquals("HEALTHY",view(b).get("ownerState"));assertEquals("SUCCEEDED",((Map<?,?>)((List<?>)view(b).get("operations")).get(0)).get("status"));
        fixture.backend.initialize("job");TaskRequest start=TaskRequest.of(TaskRequest.Kind.ACTION,"job");start.requestId="global_start_job";start.revision=(String)view(b).get("revision");start.action="START";b.invoke(start,fixture.admin());
        PlatformTest.await(()->{execution.cycle();publish();return fixture.workers.active.get()==1&&"SUCCEEDED".equals(fixture.status(start.requestId));});assertEquals(1,fixture.workers.starts.get());b.invoke(start,fixture.admin());execution.cycle();assertEquals(1,fixture.workers.starts.get());
    }
    @Test void aUserGrantedTheOwnerCanUseAnotherEntryWithoutGrantingThatEntry()throws Exception{
        create();execution.cycle();publish();GlobalTaskStoreTest store=fixture.fixture;
        store.access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+store.access.accounts().revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"nodeIds\":[\"a\"],\"taskIds\":[\"job\"],\"token\":\"global-plane-viewer-token-at-least-32\"}"),"global_create_viewer",fixture.admin());AccessControl.Principal viewer=AccountDirectory.authenticate(store.access.accounts(),"Bearer global-plane-viewer-token-at-least-32");
        assertFalse(viewer.node("b"));assertEquals("job",b.invoke(TaskRequest.of(TaskRequest.Kind.VIEW,"job"),viewer).get("id"));assertEquals(1L,b.invoke(TaskRequest.of(TaskRequest.Kind.LIST,null),viewer).get("total"));
        TaskRequest stop=TaskRequest.of(TaskRequest.Kind.ACTION,"job");stop.action="STOP";stop.requestId="global_forbidden_stop";stop.revision=(String)view(b).get("revision");assertThrows(SecurityException.class,()->b.invoke(stop,viewer));
        AccessControl.Principal forged=new AccessControl.Principal(viewer.id,PlatformConfig.Role.ADMIN,null,viewer.authorizationRevision);assertThrows(SecurityException.class,()->b.invoke(stop,forged));
    }
    @Test void offlineOwnerIsUnknownAndStopRemainsPendingWithoutPretendingToStopIt()throws Exception{
        create();execution.cycle();publish();fixture.owner.registry.stopped(fixture.owner.lease);assertEquals("UNKNOWN",view(b).get("workerState"));assertEquals("STOPPED",view(b).get("ownerState"));assertEquals("STOPPED",view(b).get("ownerControlState"));
        TaskRequest stop=TaskRequest.of(TaskRequest.Kind.ACTION,"job");stop.action="STOP";stop.requestId="global_offline_stop";stop.revision=(String)view(b).get("revision");Map<String,Object> accepted=b.invoke(stop,fixture.admin());assertEquals("UNKNOWN",accepted.get("workerState"));assertEquals("PENDING",fixture.status(stop.requestId));
        assertThrows(ControlStore.Fenced.class,()->publish());
    }
    @Test void bothEntriesDistinguishTheControlLeaseFromUnobservedWorkerAndCapture()throws Exception{
        create();
        assertEquals("HEALTHY",view(b).get("ownerControlState"));
        assertEquals("UNOBSERVED",view(b).get("ownerState"));
        execution.cycle();publish();
        fixture.owner.sql("UPDATE rivomesh_control.rmc_node SET lease_until=0 WHERE node_id='a'");
        for(GlobalTaskPlane entry:List.of(a,b)){
            Map<String,Object> task=view(entry);
            assertEquals("a",task.get("ownerNodeId"));
            assertEquals("STALE",task.get("ownerControlState"));
            assertEquals("UNKNOWN",task.get("workerState"));
            assertEquals("UNKNOWN",task.get("captureLeaseState"));
            assertEquals("UNKNOWN",task.get("desired"));
            assertEquals(false,task.get("statusAvailable"));
        }
        assertThrows(ControlStore.Expired.class,()->publish());

        ControlStore.Lease next=fixture.owner.registry.register(ControlStoreTest.node("a"),List.of());
        String otherDirectory=UUID.randomUUID().toString();
        assertEquals("AGENT_DIRECTORY_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->fixture.owner.a.register(next,otherDirectory)).getMessage());
        assertThrows(ControlStore.Fenced.class,()->publish());
        assertEquals("REGISTERING",fixture.owner.b.owner("a",fixture.fixture.directory).state);
        for(GlobalTaskPlane entry:List.of(a,b)){
            Map<String,Object> task=view(entry);
            assertEquals("HEALTHY",task.get("ownerControlState"));
            assertEquals("REGISTERING",task.get("ownerState"));
            assertEquals("UNKNOWN",task.get("workerState"));
            assertEquals("UNKNOWN",task.get("captureLeaseState"));
        }
    }
    @Test void observationsCannotBePublishedUnderAnotherDirectoryOrReadAsAnotherTask()throws Exception{
        create();execution.cycle();publish();assertFalse(Model.JSON.writeValueAsString(view(b)).contains("source-secret"));assertFalse(Model.JSON.writeValueAsString(view(b)).contains("url-secret"));
        assertEquals("AGENT_DIRECTORY_CHANGED",assertThrows(TaskService.Conflict.class,()->views.publish(fixture.owner.lease,UUID.randomUUID().toString(),Collections.singletonList(fixture.local.view("job")))).getMessage());
        fixture.owner.sql("UPDATE rivomesh_control.rmc_task_observation SET envelope='invalid'");assertThrows(TaskService.Conflict.class,()->view(b));
    }
}
