package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class OwnerExecutionTest {
    @TempDir Path directory;
    AgentDirectoryTest owner;GlobalTaskStoreTest fixture;DurableCommands commands;PlatformConfig config;TaskService local;
    final PlatformTest.Backend backend=new PlatformTest.Backend();final PlatformTest.Workers workers=new PlatformTest.Workers();
    @BeforeEach void initialize()throws Exception{
        owner=new AgentDirectoryTest();owner.initialize();fixture=owner.fixture;owner.sql("UPDATE rivomesh_control.rmc_node SET lease_until=9223372036854775807");owner.a.register(owner.lease,fixture.directory);
        commands=new DurableCommands(fixture.config.cluster,fixture.access,fixture.a,10);config=fixture.config;config.pollMillis=100;config.tasksDirectory=directory.resolve("tasks").toString();config.dataDirectory=directory.resolve("data").toString();local=new TaskService(config,backend,workers);
    }
    @AfterEach void close()throws Exception{if(local!=null)local.close();}
    AccessControl.Principal admin(){return AccountDirectory.authenticate(fixture.access.accounts(),"Bearer "+GlobalTaskStoreTest.TOKEN);}
    OwnerExecution executor(){return new OwnerExecution(commands,owner.a,()->owner.lease,fixture.directory,local);}
    TaskRequest create()throws Exception{TaskRequest request=TaskRequest.of(TaskRequest.Kind.CREATE,null);request.requestId="request_create_job";request.yaml=Model.JSON.writeValueAsString(PlatformTest.task("job"));commands.create("job","a",fixture.directory,request,admin());return request;}
    TaskRequest action(String id,String action)throws Exception{TaskRequest request=TaskRequest.of(TaskRequest.Kind.ACTION,"job");request.requestId=id;request.action=action;request.revision=fixture.a.get("job",admin()).revision;commands.enqueue(request,admin());return request;}
    String status(String id)throws Exception{return(String)commands.view(id,admin()).get("status");}
    void awaitOutcome(OwnerExecution execution,String id,String expected)throws Exception{PlatformTest.await(()->{execution.step(id);return expected.equals(status(id));});}
    @Test void acceptedRemoteCreationStartAndStopActuallyOperateTheOwnerWorker()throws Exception{
        create();OwnerExecution execution=executor();execution.cycle();assertEquals("SUCCEEDED",status("request_create_job"));assertEquals("job",local.view("job").get("id"));backend.initialize("job");
        action("request_start_job","START");awaitOutcome(execution,"request_start_job","SUCCEEDED");PlatformTest.await(()->workers.active.get()==1);assertEquals(1,workers.starts.get());execution.step("request_start_job");assertEquals(1,workers.starts.get());
        action("request_stop_job","STOP");awaitOutcome(execution,"request_stop_job","SUCCEEDED");PlatformTest.await(()->workers.active.get()==0);assertEquals("STOPPED",local.view("job").get("desired"));
    }
    @Test void handoffReceiptPlanIncludesOnlyAuthenticatedClosedLocalOutcomes()throws Exception{
        create();OwnerExecution execution=executor();execution.cycle();
        action("closed_stop_request","STOP");awaitOutcome(execution,"closed_stop_request","SUCCEEDED");
        var plan=fixture.a.service((c,accounts)->commands.closedHandoffReceipts(c,fixture.a.record(c,"job")));
        assertNull(plan.refusal());
        assertEquals(List.of("request_create_job","closed_stop_request"),
                plan.receipts().stream().map(SharedHandoffEvidence.ClosedReceipt::request).sorted(Comparator.reverseOrder()).toList());
        assertTrue(plan.receipts().stream().allMatch(receipt->receipt.phase().equals("SUCCEEDED")&&receipt.signature().length()==64));
        action("unresolved_check_request","CHECK");
        var refused=fixture.a.service((c,accounts)->commands.closedHandoffReceipts(c,fixture.a.record(c,"job")));
        assertEquals("HANDOFF_CLOSED_RECEIPT_UNPROVEN",refused.refusal());
        assertTrue(refused.receipts().isEmpty());
    }
    @Test void controllerReopenAfterLocalCommitAndLostGlobalReplyUsesTheExistingReceipt()throws Exception{
        create();DurableCommands.Dispatch dispatch=commands.claim("request_create_job",owner.a,owner.lease,fixture.directory);TaskRequest request=commands.bindLocal(dispatch,local.prepareOwnerRequest(dispatch.command.request),owner.a,owner.lease,fixture.directory);local.pinOwnerRequest(request.requestId);new LocalTaskPlane(local).invoke(request,dispatch.actor);
        assertEquals("DISPATCHED",status(request.requestId));local.close();local=null;local=new TaskService(config,backend,workers);executor().step(request.requestId);assertEquals("SUCCEEDED",status(request.requestId));assertEquals(1,((List<?>)local.list(admin()).get("tasks")).size());
    }
    @Test void aCommittedReceiptCanBeReconciledAfterItsActorAuthorizationChanged()throws Exception{
        create();DurableCommands.Dispatch dispatch=commands.claim("request_create_job",owner.a,owner.lease,fixture.directory);TaskRequest request=commands.bindLocal(dispatch,local.prepareOwnerRequest(dispatch.command.request),owner.a,owner.lease,fixture.directory);local.pinOwnerRequest(request.requestId);new LocalTaskPlane(local).invoke(request,dispatch.actor);
        fixture.access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+fixture.access.accounts().revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":[],\"token\":\"viewer-test-token-at-least-32-bytes\"}"),"request_create_viewer",admin());executor().step(request.requestId);assertEquals("SUCCEEDED",status(request.requestId));
    }
    @Test void aLostReceiptAndChangedRevisionRemainUnknownInsteadOfBeingReissued()throws Exception{
        create();executor().step("request_create_job");TaskRequest request=action("request_check_job","CHECK");DurableCommands.Dispatch dispatch=commands.claim(request.requestId,owner.a,owner.lease,fixture.directory);TaskRequest original=local.prepareOwnerRequest(dispatch.command.request);commands.bindLocal(dispatch,original,owner.a,owner.lease,fixture.directory);
        new LocalTaskPlane(local).invoke(otherCheck("request_unrelated_check"),admin());executor().step(request.requestId);assertEquals("UNKNOWN",status(request.requestId));assertEquals("REVISION_CHANGED",commands.view(request.requestId,admin()).get("code"));assertFalse(PlatformTest.operations(local,"job").contains(request.requestId));
    }
    TaskRequest otherCheck(String id){TaskRequest request=TaskRequest.of(TaskRequest.Kind.ACTION,"job");request.requestId=id;request.action="CHECK";request.revision=PlatformTest.revision(local,"job");return request;}
    @Test void outstandingOwnerReceiptsSurviveNormalReceiptRetention()throws Exception{
        create();DurableCommands.Dispatch dispatch=commands.claim("request_create_job",owner.a,owner.lease,fixture.directory);TaskRequest request=commands.bindLocal(dispatch,local.prepareOwnerRequest(dispatch.command.request),owner.a,owner.lease,fixture.directory);local.pinOwnerRequest(request.requestId);new LocalTaskPlane(local).invoke(request,dispatch.actor);
        // Configuration commits are synchronous, so this exercises retention without queued controls.
        for(int n=0;n<505;n++){
            TaskRequest update=TaskRequest.of(TaskRequest.Kind.CONFIG_UPDATE,"job");update.requestId="request_edit_"+n;update.revision=PlatformTest.revision(local,"job");update.document=Model.JSON.valueToTree(PlatformTest.task("job"));
            PlatformTest.await(()->{try{new LocalTaskPlane(local).invoke(update,admin());return true;}catch(TaskService.Conflict busy){if("TASK_BUSY".equals(busy.getMessage()))return false;throw busy;}});
        }
        assertNotNull(local.ownerReceipt(request,dispatch.command.actor,dispatch.command.actorRole));executor().step(request.requestId);assertEquals("SUCCEEDED",status(request.requestId));
    }
}
