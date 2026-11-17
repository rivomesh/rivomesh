package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class DurableCommandsTest {
    GlobalTaskStoreTest fixture;DurableCommands a,b;
    @BeforeEach void initialize()throws Exception{fixture=new GlobalTaskStoreTest();fixture.initialize();a=new DurableCommands(fixture.config.cluster,fixture.access,fixture.a,1);b=new DurableCommands(fixture.config.cluster,fixture.access,fixture.b,1);}
    void task(String id)throws Exception{fixture.a.create(id,"a",fixture.directory,"password: private-source","import_task_"+id,fixture.admin);}
    TaskRequest action(String task,String id,String action)throws Exception{TaskRequest request=TaskRequest.of(TaskRequest.Kind.ACTION,task);request.requestId=id;request.action=action;request.revision=fixture.a.get(task,fixture.admin).revision;return request;}
    @Test void offlineOwnerCommandsRemainPendingAndAreVisibleFromAnotherEntry()throws Exception{
        task("job");TaskRequest request=action("job","request_stop_job","STOP");Map<String,Object> accepted=a.enqueue(request,fixture.admin);assertEquals("PENDING",accepted.get("status"));assertEquals("a",accepted.get("ownerNodeId"));assertEquals(accepted,b.view(request.requestId,fixture.admin));assertEquals(accepted.get("revision"),fixture.b.get("job",fixture.admin).revision);
    }
    @Test void startupRecoveryIncludesPendingCommandsBeyondTheFirstPage()throws Exception{
        AgentDirectoryTest owner=new AgentDirectoryTest();owner.initialize();owner.sql("UPDATE rivomesh_control.rmc_node SET lease_until=9223372036854775807");owner.a.register(owner.lease,owner.fixture.directory);
        DurableCommands commands=new DurableCommands(owner.fixture.config.cluster,owner.fixture.access,owner.fixture.a,100);
        for(int n=0;n<51;n++){
            String task="job_"+n;GlobalTaskStore.Record record=owner.fixture.a.create(task,"a",owner.fixture.directory,"password: private-source","import_task_"+task,owner.fixture.admin);
            TaskRequest stop=TaskRequest.of(TaskRequest.Kind.ACTION,task);stop.requestId=String.format(Locale.ROOT,"request_stop_%03d",n);stop.action="STOP";stop.revision=record.revision;commands.enqueue(stop,owner.fixture.admin);
        }
        List<DurableCommands.Stored> first=commands.outstanding(owner.a,owner.lease,owner.fixture.directory,"",true);assertEquals(50,first.size());List<DurableCommands.Stored> second=commands.outstanding(owner.a,owner.lease,owner.fixture.directory,first.get(49).id,true);assertEquals(1,second.size());assertEquals("request_stop_050",second.get(0).id);assertTrue(commands.outstanding(owner.a,owner.lease,owner.fixture.directory,second.get(0).id,true).isEmpty());assertTrue(commands.recoverable(owner.a,owner.lease,owner.fixture.directory,"").isEmpty());
    }
    @Test void duplicatesReplayAcrossEntriesAndDifferentBodiesCannotReuseTheId()throws Exception{
        task("job");TaskRequest request=action("job","request_check_job","CHECK");Map<String,Object> first=a.enqueue(request,fixture.admin);assertEquals(first,b.enqueue(request,fixture.admin));request.action="STOP";assertEquals("REQUEST_ID_REUSED",assertThrows(TaskService.Conflict.class,()->b.enqueue(request,fixture.admin)).getMessage());assertEquals(first.get("revision"),fixture.a.get("job",fixture.admin).revision);
    }
    @Test void revisionsAndCapacityRejectWithoutAdvancingOtherTasks()throws Exception{
        task("one");task("two");TaskRequest invalid=action("one","request_stale_one","CHECK");invalid.revision=UUID.randomUUID().toString();assertEquals("REVISION_CHANGED",assertThrows(TaskService.Conflict.class,()->a.enqueue(invalid,fixture.admin)).getMessage());
        a.enqueue(action("one","request_check_one","CHECK"),fixture.admin);TaskRequest other=action("two","request_check_two","CHECK");assertEquals("COMMAND_CAPACITY",assertThrows(TaskService.Conflict.class,()->b.enqueue(other,fixture.admin)).getMessage());assertEquals(other.revision,fixture.a.get("two",fixture.admin).revision);
    }
    @Test void cancellationHasAParentAndReservedAdmissionWhenNormalCapacityIsFull()throws Exception{
        task("job");TaskRequest original=action("job","request_validate_job","VALIDATE");original.validation=new ValidationRequest();original.validation.targets.add("pg");a.enqueue(original,fixture.admin);
        assertEquals("OPERATION_PENDING",assertThrows(TaskService.Conflict.class,()->a.enqueue(action("job","request_other_job","CHECK"),fixture.admin)).getMessage());
        TaskRequest cancel=action("job","request_cancel_job","VALIDATE_CANCEL");Map<String,Object> accepted=b.enqueue(cancel,fixture.admin);assertEquals(original.requestId,accepted.get("parentRequestId"));assertEquals("PENDING",accepted.get("status"));assertEquals("CANCEL_ALREADY_REQUESTED",assertThrows(TaskService.Conflict.class,()->a.enqueue(action("job","request_cancel_twice","VALIDATE_CANCEL"),fixture.admin)).getMessage());
    }
    @Test void createdTaskConfigurationAndCommandAreAtomicAndStoredEncrypted()throws Exception{
        TaskRequest create=TaskRequest.of(TaskRequest.Kind.CREATE,null);create.requestId="request_create_job";create.yaml=Model.JSON.writeValueAsString(PlatformTest.task("job"));GlobalTaskStore.Record record=a.create("job","a",fixture.directory,create,fixture.admin);assertEquals(record.revision,b.view(create.requestId,fixture.admin).get("revision"));
        try(Connection c=fixture.connection();Statement s=c.createStatement();ResultSet rows=s.executeQuery("SELECT envelope FROM rivomesh_control.rmc_command")){assertTrue(rows.next());assertFalse(rows.getString(1).contains("source-secret"));}
        assertFalse(Model.JSON.writeValueAsString(a.view(create.requestId,fixture.admin)).contains("source-secret"));
        TaskRequest second=TaskRequest.of(TaskRequest.Kind.CREATE,null);second.requestId="request_create_second";second.yaml=Model.JSON.writeValueAsString(PlatformTest.task("second"));assertEquals("COMMAND_CAPACITY",assertThrows(TaskService.Conflict.class,()->b.create("second","a",fixture.directory,second,fixture.admin)).getMessage());assertThrows(NoSuchElementException.class,()->fixture.a.get("second",fixture.admin));
    }
    @Test void aConfigurationCannotCreateAnUnreservedTaskId()throws Exception{
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.CREATE,null);request.requestId="request_create_mismatch";request.yaml=Model.JSON.writeValueAsString(PlatformTest.task("other"));assertEquals("COMMAND_TASK_ID_CHANGED",assertThrows(TaskService.Conflict.class,()->a.create("job","a",fixture.directory,request,fixture.admin)).getMessage());assertThrows(NoSuchElementException.class,()->fixture.a.get("job",fixture.admin));
    }
    @Test void lostRootAndLostCommandsCannotBeSilentlyReseeded()throws Exception{
        task("job");a.enqueue(action("job","request_check_job","CHECK"),fixture.admin);
        try(Connection c=fixture.connection();Statement s=c.createStatement()){s.executeUpdate("DELETE FROM rivomesh_control.rmc_command");}
        assertEquals("COMMAND_CATALOG_INCOMPLETE",assertThrows(TaskService.Conflict.class,()->new DurableCommands(fixture.config.cluster,fixture.access,fixture.a,1)).getMessage());
        try(Connection c=fixture.connection();Statement s=c.createStatement()){s.executeUpdate("DELETE FROM rivomesh_control.rmc_command_root");}
        assertEquals("COMMAND_STATE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new DurableCommands(fixture.config.cluster,fixture.access,fixture.a,1)).getMessage());
    }
    @Test void alteredIndexMetadataIsRejectedByAuthenticatedEnvelope()throws Exception{
        task("job");TaskRequest request=action("job","request_check_job","CHECK");a.enqueue(request,fixture.admin);
        try(Connection c=fixture.connection();Statement s=c.createStatement()){s.executeUpdate("UPDATE rivomesh_control.rmc_command SET owner_id='b'");}
        assertEquals("COMMAND_RECORD_INVALID",assertThrows(TaskService.Conflict.class,()->b.view(request.requestId,fixture.admin)).getMessage());
    }
    @Test void admissionUsesTheRealRoleAndFreshDirectoryRevision()throws Exception{
        task("job");String revision=fixture.access.accounts().revision;fixture.access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+revision+"\",\"id\":\"operator\",\"role\":\"OPERATOR\",\"enabled\":true,\"taskIds\":[\"job\"],\"nodeIds\":[\"a\"],\"token\":\"operator-test-token-at-least-32-bytes\"}"),"request_add_operator",fixture.admin);
        TaskRequest check=TaskRequest.of(TaskRequest.Kind.ACTION,"job");check.requestId="request_check_job";check.action="CHECK";
        assertEquals("AUTHORIZATION_CHANGED",assertThrows(TaskService.Conflict.class,()->a.enqueue(check,fixture.admin)).getMessage());AccessControl.Principal operator=AccountDirectory.authenticate(fixture.access.accounts(),"Bearer operator-test-token-at-least-32-bytes"),forged=new AccessControl.Principal(operator.id,PlatformConfig.Role.ADMIN,null,operator.authorizationRevision);
        check.action="VALIDATE";assertThrows(SecurityException.class,()->a.enqueue(check,forged));
    }
    @Test void requestIdsCannotBeReusedAcrossTaskConfigurationAndActionLedgers()throws Exception{
        task("job");TaskRequest imported=action("job","import_task_job","CHECK");
        assertEquals("REQUEST_ID_REUSED",assertThrows(TaskService.Conflict.class,()->a.enqueue(imported,fixture.admin)).getMessage());assertEquals(imported.revision,fixture.a.get("job",fixture.admin).revision);
        TaskRequest action=action("job","request_mixed_kind","CHECK");a.enqueue(action,fixture.admin);
        TaskRequest create=TaskRequest.of(TaskRequest.Kind.CREATE,null);create.requestId=action.requestId;create.yaml=Model.JSON.writeValueAsString(PlatformTest.task("other"));
        assertEquals("REQUEST_ID_REUSED",assertThrows(TaskService.Conflict.class,()->b.create("other","a",fixture.directory,create,fixture.admin)).getMessage());assertThrows(NoSuchElementException.class,()->fixture.b.get("other",fixture.admin));
    }
}
