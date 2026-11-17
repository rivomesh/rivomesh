package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class OwnerQueriesTest {
    AgentDirectoryTest owner;GlobalTaskStoreTest fixture;OwnerQueries a,b;
    @BeforeEach void initialize()throws Exception{
        owner=new AgentDirectoryTest();owner.initialize();fixture=owner.fixture;owner.sql("UPDATE rivomesh_control.rmc_node SET lease_until=9223372036854775807");owner.a.register(owner.lease,fixture.directory);fixture.a.create("job","a",fixture.directory,"private-yaml","request_import_job",fixture.admin);
        a=new OwnerQueries(fixture.config.cluster,fixture.a,owner.a,2,30000);b=new OwnerQueries(fixture.config.cluster,fixture.b,owner.b,2,30000);
    }
    AccessControl.Principal admin(){return AccountDirectory.authenticate(fixture.access.accounts(),"Bearer "+GlobalTaskStoreTest.TOKEN);}
    TaskRequest view(){return TaskRequest.of(TaskRequest.Kind.VIEW,"job");}
    @Test void independentEntriesReadTheOwnerResultThroughAnEncryptedBoundedMailbox()throws Exception{
        String id=a.submit(view(),admin());assertEquals(Arrays.asList(id),b.pending(owner.lease,fixture.directory,5));
        b.serve(id,owner.lease,fixture.directory,(request,actor)->{assertEquals("job",request.taskId);return Collections.singletonMap("message","private-business-value");});
        assertEquals("SUCCEEDED",a.poll(id,admin()).get("status"));assertEquals("private-business-value",a.await(id,admin()).get("message"));
        try(Connection c=fixture.connection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT envelope FROM rivomesh_control.rmc_owner_query")){r.next();assertFalse(r.getString(1).contains("private-business-value"));}
    }
    @Test void mutationsCannotUseTheQueryChannelAndResultSizesAreEnforced()throws Exception{
        TaskRequest action=TaskRequest.of(TaskRequest.Kind.ACTION,"job");action.requestId="request_stop_job";action.action="STOP";assertEquals("OWNER_QUERY_READ_REQUIRED",assertThrows(IllegalArgumentException.class,()->a.submit(action,admin())).getMessage());
        String id=a.submit(view(),admin());char[] oversized=new char[1024*1024+1];Arrays.fill(oversized,'x');b.serve(id,owner.lease,fixture.directory,(r,actor)->Collections.singletonMap("raw",new String(oversized)));
        assertEquals("REJECTED",a.poll(id,admin()).get("status"));assertEquals("OWNER_QUERY_RESULT_LIMIT",a.poll(id,admin()).get("code"));assertFalse(a.poll(id,admin()).containsKey("result"));
    }
    @Test void revocationBeforeExecutionNeverCallsTheLocalReader()throws Exception{
        String id=a.submit(view(),admin());changeDirectory();b.serve(id,owner.lease,fixture.directory,(r,actor)->{fail("Revoked query executed");return null;});
        assertEquals("AUTHORIZATION_CHANGED",assertThrows(TaskService.Conflict.class,()->a.poll(id,admin())).getMessage());
    }
    @Test void revocationDuringTheReadPreventsPublishingTheBusinessResult()throws Exception{
        String id=a.submit(view(),admin());b.serve(id,owner.lease,fixture.directory,(r,actor)->{changeDirectory();return Collections.singletonMap("raw","sensitive-result");});
        try(Connection c=fixture.connection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT phase,envelope FROM rivomesh_control.rmc_owner_query")){r.next();assertEquals("REJECTED",r.getString(1));String json=fixture.a.commandCipher().open("owner-query/"+id,r.getString(2));assertFalse(json.contains("sensitive-result"));}
    }
    void changeDirectory()throws Exception{fixture.access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+fixture.access.accounts().revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":[],\"token\":\"viewer-owner-query-test-at-least-32-bytes\"}"),"request_add_viewer",admin());}
    @Test void replacedOwnerCannotServeOrDeliverAnOldEpochResponse()throws Exception{
        String id=a.submit(view(),admin());ControlStore.Lease old=owner.lease;owner.registry.stopped(old);owner.lease=owner.registry.register(ControlStoreTest.node("a"),Collections.emptyList());owner.sql("UPDATE rivomesh_control.rmc_node SET lease_until=9223372036854775807");owner.a.register(owner.lease,fixture.directory);
        assertThrows(ControlStore.Fenced.class,()->b.serve(id,old,fixture.directory,(r,actor)->Collections.emptyMap()));assertThrows(ControlStore.Fenced.class,()->b.serve(id,owner.lease,fixture.directory,(r,actor)->Collections.emptyMap()));assertEquals("OWNER_QUERY_UNAVAILABLE",assertThrows(TaskService.Conflict.class,()->a.poll(id,admin())).getMessage());
    }
    @Test void expirationReclaimsCapacityAndDoesNotReturnExpiredResults()throws Exception{
        OwnerQueries shortLived=new OwnerQueries(fixture.config.cluster,fixture.a,owner.a,1,1000);String id=shortLived.submit(view(),admin());assertEquals("OWNER_QUERY_CAPACITY",assertThrows(TaskService.Conflict.class,()->shortLived.submit(view(),admin())).getMessage());Thread.sleep(1100);
        assertEquals("OWNER_QUERY_EXPIRED",assertThrows(TaskService.Conflict.class,()->shortLived.poll(id,admin())).getMessage());assertNotNull(shortLived.submit(view(),admin()));assertThrows(NoSuchElementException.class,()->shortLived.poll(id,admin()));
    }
    @Test void alteredMetadataAndOfflineOwnerAreRefused()throws Exception{
        String id=a.submit(view(),admin());owner.sql("UPDATE rivomesh_control.rmc_owner_query SET owner_id='other'");assertEquals("OWNER_QUERY_RECORD_INVALID",assertThrows(TaskService.Conflict.class,()->a.poll(id,admin())).getMessage());owner.registry.stopped(owner.lease);assertEquals("OWNER_QUERY_UNAVAILABLE",assertThrows(TaskService.Conflict.class,()->a.submit(view(),admin())).getMessage());
    }
    @Test void readerExceptionsAreSanitizedAndDoNotReturnRawValues()throws Exception{
        String id=a.submit(view(),admin());b.serve(id,owner.lease,fixture.directory,(r,actor)->{throw new java.io.IOException("password=private-value");});Map<String,Object> result=a.poll(id,admin());assertEquals("OWNER_QUERY_FAILED",result.get("code"));assertFalse(Model.JSON.writeValueAsString(result).contains("private-value"));
    }
    @Test void anOldPreviewCannotBeDeliveredAfterTheGlobalRevisionChanges()throws Exception{
        TaskRequest preview=TaskRequest.of(TaskRequest.Kind.CONFIG_PREVIEW,"job");preview.revision=fixture.a.get("job",admin()).revision;preview.document=Model.JSON.createObjectNode();String id=a.submit(preview,admin());b.serve(id,owner.lease,fixture.directory,(r,actor)->Collections.singletonMap("migrationRequired",false));
        fixture.a.transaction("job",admin(),(c,record,actor)->{fixture.a.advance(c,record);return null;});assertEquals("REVISION_CHANGED",assertThrows(TaskService.Conflict.class,()->a.poll(id,admin())).getMessage());
    }
}
