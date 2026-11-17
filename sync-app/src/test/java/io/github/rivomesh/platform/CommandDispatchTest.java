package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class CommandDispatchTest {
    AgentDirectoryTest owner;GlobalTaskStoreTest fixture;DurableCommands a,b;
    @BeforeEach void initialize()throws Exception{
        owner=new AgentDirectoryTest();owner.initialize();fixture=owner.fixture;keepLeaseCurrent();owner.a.register(owner.lease,fixture.directory);
        a=new DurableCommands(fixture.config.cluster,fixture.access,fixture.a,1);b=new DurableCommands(fixture.config.cluster,fixture.access,fixture.b,1);
        fixture.a.create("job","a",fixture.directory,"password: private-source","import_task_job",fixture.admin);
    }
    // This unit fixture has no heartbeat thread; expiry/refencing is changed explicitly in each case.
    void keepLeaseCurrent()throws Exception{owner.sql("UPDATE rivomesh_control.rmc_node SET lease_until=9223372036854775807");}
    AccessControl.Principal admin(){return AccountDirectory.authenticate(fixture.access.accounts(),"Bearer "+GlobalTaskStoreTest.TOKEN);}
    TaskRequest action(String id,String action)throws Exception{
        TaskRequest r=TaskRequest.of(TaskRequest.Kind.ACTION,"job");r.requestId=id;r.action=action;r.revision=fixture.a.get("job",admin()).revision;
        if("VALIDATE".equals(action)){r.validation=new ValidationRequest();r.validation.targets.add("pg");}return r;
    }
    DurableCommands.Dispatch claim(String id)throws Exception{return b.claim(id,owner.b,owner.lease,fixture.directory);}
    void complete(DurableCommands.Dispatch dispatch,String phase,String code)throws Exception{b.complete(dispatch,phase,code,owner.b,owner.lease,fixture.directory);}
    TaskRequest bind(DurableCommands.Dispatch dispatch,String localRevision)throws Exception{
        TaskRequest r=dispatch.command.request.snapshot();r.revision=localRevision;return b.bindLocal(dispatch,r,owner.b,owner.lease,fixture.directory);
    }
    @Test void remoteEntryAdmitsAndOwnerPersistsTheExactLocalRequestBeforeAcknowledging()throws Exception{
        TaskRequest request=action("request_check_job","CHECK");a.enqueue(request,admin());assertEquals(Arrays.asList(request.requestId),b.pending(owner.b,owner.lease,fixture.directory,10));
        DurableCommands.Dispatch ticket=claim(request.requestId);assertEquals("DISPATCHED",a.view(request.requestId,admin()).get("status"));assertNotNull(ticket.actor);
        assertEquals("local-revision-before-dispatch",bind(ticket,"local-revision-before-dispatch").revision);
        complete(ticket,"ACCEPTED","LOCAL_OPERATION_ACCEPTED");assertEquals("ACCEPTED",a.view(request.requestId,admin()).get("status"));
        complete(ticket,"SUCCEEDED","CHECKED");complete(ticket,"SUCCEEDED","CHECKED");assertEquals("SUCCEEDED",a.view(request.requestId,admin()).get("status"));assertTrue(a.pending(owner.a,owner.lease,fixture.directory,10).isEmpty());
        assertEquals("COMMAND_OUTCOME_CHANGED",assertThrows(TaskService.Conflict.class,()->complete(ticket,"FAILED","FAILED_AFTER_SUCCESS")).getMessage());
        // A terminal command releases pending capacity without deleting its replay receipt.
        a.enqueue(action("request_stop_job","STOP"),admin());assertEquals("SUCCEEDED",a.enqueue(request,admin()).get("status"));
    }
    @Test void lostReplyAndOwnerReopenKeepTheOriginalLocalRevision()throws Exception{
        a.enqueue(action("request_check_job","CHECK"),admin());DurableCommands.Dispatch first=claim("request_check_job");bind(first,"local-original");
        owner.registry.stopped(owner.lease);owner.lease=owner.registry.register(ControlStoreTest.node("a"),Collections.emptyList());keepLeaseCurrent();owner.a.register(owner.lease,fixture.directory);
        DurableCommands reopened=new DurableCommands(fixture.config.cluster,fixture.access,fixture.b,1);DurableCommands.Dispatch recovered=reopened.claim("request_check_job",owner.b,owner.lease,fixture.directory);
        assertEquals("local-original",recovered.command.localRequest.revision);assertEquals("COMMAND_LOCAL_REVISION_CHANGED",assertThrows(TaskService.Conflict.class,()->bind(recovered,"a-new-local-revision")).getMessage());
        assertThrows(ControlStore.Fenced.class,()->complete(first,"SUCCEEDED","CHECKED"));complete(recovered,"SUCCEEDED","CHECKED");assertEquals("SUCCEEDED",a.view("request_check_job",admin()).get("status"));
    }
    @Test void revocationBeforeDispatchRejectsWithoutCreatingALocalRequest()throws Exception{
        a.enqueue(action("request_check_job","CHECK"),admin());changeDirectory();assertNull(claim("request_check_job"));assertEquals("REJECTED",b.view("request_check_job",admin()).get("status"));assertEquals("AUTHORIZATION_CHANGED",b.view("request_check_job",admin()).get("code"));
    }
    @Test void revocationAfterLocalAcceptanceStillAllowsTheServiceToRecordItsRealOutcome()throws Exception{
        a.enqueue(action("request_check_job","CHECK"),admin());DurableCommands.Dispatch ticket=claim("request_check_job");bind(ticket,"local-original");changeDirectory();
        DurableCommands.Dispatch recovered=claim("request_check_job");assertNull(recovered.actor);assertNotNull(recovered.command.localRequest);complete(recovered,"SUCCEEDED","CHECKED");assertEquals("SUCCEEDED",b.view("request_check_job",admin()).get("status"));
    }
    @Test void revocationBetweenClaimAndLocalBindingPreventsNewExecution()throws Exception{
        a.enqueue(action("request_check_job","CHECK"),admin());DurableCommands.Dispatch ticket=claim("request_check_job");changeDirectory();assertEquals("AUTHORIZATION_CHANGED",assertThrows(TaskService.Conflict.class,()->bind(ticket,"local-original")).getMessage());complete(ticket,"REJECTED","AUTHORIZATION_CHANGED");
    }
    void changeDirectory()throws Exception{
        fixture.access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+fixture.access.accounts().revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":[],\"token\":\"viewer-test-token-at-least-32-bytes\"}"),"request_create_viewer",admin());
    }
    @Test void cancellationOfAnUndispatchedParentNeverStartsTheParentLater()throws Exception{
        a.enqueue(action("request_validate_job","VALIDATE"),admin());a.enqueue(action("request_cancel_job","VALIDATE_CANCEL"),admin());
        assertEquals("request_cancel_job",b.pending(owner.b,owner.lease,fixture.directory,10).get(0));assertNull(claim("request_cancel_job"));assertEquals("CANCELLED",a.view("request_validate_job",admin()).get("status"));assertEquals("SUCCEEDED",a.view("request_cancel_job",admin()).get("status"));assertNull(claim("request_validate_job"));
    }
    @Test void cancellationWaitsForADispatchedParentReceiptAndThenUsesItsOwnLocalRevision()throws Exception{
        a.enqueue(action("request_validate_job","VALIDATE"),admin());DurableCommands.Dispatch parent=claim("request_validate_job");bind(parent,"local-before-validation");a.enqueue(action("request_cancel_job","VALIDATE_CANCEL"),admin());assertNull(claim("request_cancel_job"));
        assertEquals("PENDING",a.view("request_cancel_job",admin()).get("status"));complete(parent,"ACCEPTED","LOCAL_OPERATION_ACCEPTED");DurableCommands.Dispatch cancellation=claim("request_cancel_job");assertNotNull(cancellation);bind(cancellation,"local-after-validation");complete(cancellation,"SUCCEEDED","VALIDATION_CANCEL_REQUESTED");complete(parent,"FAILED","VALIDATION_CANCELLED");
    }
    @Test void changedPayloadWrongDirectoryExpiredLeaseAndFalseSuccessAreRejected()throws Exception{
        a.enqueue(action("request_check_job","CHECK"),admin());DurableCommands.Dispatch ticket=claim("request_check_job");
        assertEquals("COMMAND_LOCAL_REQUEST_REQUIRED",assertThrows(TaskService.Conflict.class,()->complete(ticket,"SUCCEEDED","CHECKED")).getMessage());TaskRequest altered=ticket.command.request.snapshot();altered.action="STOP";
        assertEquals("COMMAND_LOCAL_PAYLOAD_CHANGED",assertThrows(TaskService.Conflict.class,()->b.bindLocal(ticket,altered,owner.b,owner.lease,fixture.directory)).getMessage());
        assertThrows(TaskService.Conflict.class,()->b.claim("request_check_job",owner.b,owner.lease,UUID.randomUUID().toString()));owner.sql("UPDATE rivomesh_control.rmc_node SET lease_until=0");assertThrows(ControlStore.Expired.class,()->claim("request_check_job"));assertThrows(ControlStore.Expired.class,()->complete(ticket,"REJECTED","OWNER_LOST"));
    }
}
