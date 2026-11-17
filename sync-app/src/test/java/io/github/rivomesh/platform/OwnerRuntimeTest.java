package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class OwnerRuntimeTest {
    @TempDir Path directory;
    OwnerExecutionTest fixture;TaskService local;GlobalTaskViews views;OwnerQueries queries;OwnerRuntime runtime;
    @BeforeEach void initialize()throws Exception{
        fixture=new OwnerExecutionTest();fixture.directory=directory;fixture.initialize();fixture.local.close();fixture.local=null;
        local=TaskService.deferredOwner(fixture.config,fixture.backend,fixture.workers);fixture.local=local;views=new GlobalTaskViews(fixture.config.cluster,fixture.fixture.a,fixture.owner.a,300000);queries=new OwnerQueries(fixture.config.cluster,fixture.fixture.a,fixture.owner.a,10,30000);
    }
    @AfterEach void close()throws Exception{if(runtime!=null)runtime.close();if(fixture!=null)fixture.close();}
    OwnerRuntime runtime(){return new OwnerRuntime(fixture.config,local,fixture.fixture.a,fixture.commands,fixture.owner.a,views,queries,()->fixture.owner.lease,fixture.fixture.directory);}
    @Test void runtimeExecutesCommandsPublishesViewsAndServesQueriesInTheBackground()throws Exception{
        fixture.create();runtime=runtime();runtime.start();PlatformTest.await(()->"SUCCEEDED".equals(fixture.status("request_create_job")));fixture.backend.initialize("job");fixture.action("request_start_job","START");PlatformTest.await(()->fixture.workers.active.get()==1&&"SUCCEEDED".equals(fixture.status("request_start_job")));
        String query=queries.submit(TaskRequest.of(TaskRequest.Kind.VIEW,"job"),fixture.admin());assertEquals("job",queries.await(query,fixture.admin()).get("id"));PlatformTest.await(()->"HEALTHY".equals(views.get("job",fixture.admin()).get("ownerState")));assertTrue(runtime.ready());fixture.action("request_stop_job","STOP");PlatformTest.await(()->fixture.workers.active.get()==0&&"SUCCEEDED".equals(fixture.status("request_stop_job")));
    }
    @Test void lostCreateReplyIsReconciledBeforeLocalConfigurationAdmission()throws Exception{
        fixture.create();DurableCommands.Dispatch dispatch=fixture.commands.claim("request_create_job",fixture.owner.a,fixture.owner.lease,fixture.fixture.directory);TaskRequest request=fixture.commands.bindLocal(dispatch,local.prepareOwnerRequest(dispatch.command.request),fixture.owner.a,fixture.owner.lease,fixture.fixture.directory);local.pinOwnerRequest(request.requestId);new LocalTaskPlane(local).invoke(request,dispatch.actor);local.close();fixture.local=null;
        local=TaskService.deferredOwner(fixture.config,fixture.backend,fixture.workers);fixture.local=local;assertEquals(0,fixture.workers.starts.get());runtime=runtime();runtime.start();assertEquals("SUCCEEDED",fixture.status(request.requestId));assertTrue(runtime.ready());assertEquals(1,((List<?>)local.list(fixture.admin()).get("tasks")).size());
    }
    @Test void oldLocalConfigurationCannotStartAgainstANewerActiveAuthority()throws Exception{
        fixture.create();fixture.executor().step("request_create_job");GlobalTaskStore store=fixture.fixture.a;GlobalTaskStore.Record current=store.get("job",fixture.admin());io.github.rivomesh.Config changed=PlatformTest.task("job");changed.source.password="new-authoritative-password";GlobalTaskStore.Record next=store.update("job",current.revision,Model.JSON.writeValueAsString(changed),"request_new_authority",fixture.admin());store.service((c,accounts)->{store.applied(c,store.record(c,"job"),next.configurationVersion);return null;});
        runtime=runtime();assertEquals("OWNER_CONFIGURATION_PROJECTION_CHANGED",assertThrows(TaskService.Conflict.class,()->runtime.start()).getMessage());assertEquals(0,fixture.workers.starts.get());assertFalse(runtime.ready());
    }
    @Test void anUnregisteredLocalTaskRefusesAdmissionBeforeScheduling()throws Exception{
        new LocalTaskPlane(local).invoke(localCreate(),fixture.admin());runtime=runtime();assertEquals("OWNER_TASK_NOT_REGISTERED",assertThrows(TaskService.Conflict.class,()->runtime.start()).getMessage());assertEquals(0,fixture.workers.starts.get());
    }
    @Test void offlineStopWaitsForAnOlderLocalOperationWithoutRestartingTheWorker()throws Exception{
        fixture.create();fixture.executor().step("request_create_job");fixture.backend.initialize("job");
        TaskRequest start=TaskRequest.of(TaskRequest.Kind.ACTION,"job");start.requestId="request_local_start";start.action="START";start.revision=PlatformTest.revision(local,"job");new LocalTaskPlane(local).invoke(start,fixture.admin());local.beginOwnerScheduling();PlatformTest.await(()->fixture.workers.active.get()==1);
        local.close();fixture.local=null;local=TaskService.deferredOwner(fixture.config,fixture.backend,fixture.workers);fixture.local=local;
        new LocalTaskPlane(local).invoke(fixture.otherCheck("request_local_older_check"),fixture.admin());fixture.action("request_offline_stop","STOP");
        runtime=runtime();runtime.start();PlatformTest.await(()->"SUCCEEDED".equals(fixture.status("request_offline_stop")));assertEquals(1,fixture.workers.starts.get());assertEquals(0,fixture.workers.active.get());assertEquals("STOPPED",local.view("job").get("desired"));
    }
    @Test void aRejectedOfflineStopDoesNotPermanentlySuppressTheOldRunningIntent()throws Exception{
        fixture.create();fixture.executor().step("request_create_job");fixture.backend.initialize("job");
        TaskRequest start=TaskRequest.of(TaskRequest.Kind.ACTION,"job");start.requestId="request_local_start";start.action="START";start.revision=PlatformTest.revision(local,"job");new LocalTaskPlane(local).invoke(start,fixture.admin());local.beginOwnerScheduling();PlatformTest.await(()->fixture.workers.active.get()==1);
        local.close();fixture.local=null;local=TaskService.deferredOwner(fixture.config,fixture.backend,fixture.workers);fixture.local=local;fixture.action("request_revoked_stop","STOP");
        fixture.fixture.access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+fixture.fixture.access.accounts().revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":[],\"token\":\"viewer-test-token-at-least-32-bytes\"}"),"request_change_authorization",fixture.admin());
        runtime=runtime();runtime.start();PlatformTest.await(()->"REJECTED".equals(fixture.status("request_revoked_stop"))&&fixture.workers.active.get()==1);assertEquals(2,fixture.workers.starts.get());
    }
    TaskRequest localCreate()throws Exception{TaskRequest request=TaskRequest.of(TaskRequest.Kind.CREATE,null);request.requestId="request_local_unregistered";request.yaml=Model.JSON.writeValueAsString(PlatformTest.task("unregistered"));return request;}
}
