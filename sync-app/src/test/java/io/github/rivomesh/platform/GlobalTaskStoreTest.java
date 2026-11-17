package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class GlobalTaskStoreTest {
    static final String TOKEN="global-control-test-token-at-least-32-bytes";
    static String key(int n){byte[] bytes=new byte[32];Arrays.fill(bytes,(byte)n);return Base64.getEncoder().encodeToString(bytes);}
    PlatformConfig config;SharedAccessStore access;GlobalTaskStore a,b;AccessControl.Principal admin;
    final String directory=UUID.randomUUID().toString();
    @BeforeEach void initialize()throws Exception{
        config=new PlatformConfig();config.authToken=TOKEN;config.cluster=new ClusterConfig();config.cluster.id="test";config.cluster.nodeId="a";config.cluster.sharedAccess=true;config.cluster.driver="org.h2.Driver";config.cluster.url="jdbc:h2:mem:global_"+UUID.randomUUID().toString().replace("-","")+";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";config.cluster.username="sa";config.cluster.password="";
        access=new SharedAccessStore(config,AccountDirectory.configured(config),null,Collections.emptyMap());admin=AccountDirectory.authenticate(access.accounts(),"Bearer "+TOKEN);a=new GlobalTaskStore(config.cluster,access,key(1),null,100);b=new GlobalTaskStore(config.cluster,access,key(1),a.identity,100);
    }
    Connection connection()throws Exception{return DriverManager.getConnection(config.cluster.url,"sa","");}
    @Test void bothEntrypointsShareConfigurationRevisionAndNeverStorePlainCredentials()throws Exception{
        GlobalTaskStore.Record created=a.create("task","a",directory,"password: private-database-token","request_create_01",admin),remote=b.get("task",admin);assertEquals(created.revision,remote.revision);assertEquals(created.yaml,remote.yaml);
        GlobalTaskStore.Record changed=b.update("task",remote.revision,"password: next-private-token","request_update_01",admin);assertEquals(changed.revision,a.get("task",admin).revision);assertEquals(changed.yaml,a.get("task",admin).yaml);
        try(Connection c=connection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT sealed_yaml FROM rivomesh_control.rmc_task_version")){int count=0;while(r.next()){count++;assertFalse(r.getString(1).contains("private"));}assertEquals(2,count);}
        assertEquals("REVISION_CHANGED",assertThrows(TaskService.Conflict.class,()->a.update("task",created.revision,"stale","request_stale_01",admin)).getMessage());
    }
    @Test void concurrentCreateReservesOneGlobalIdAndOneInitialVersion()throws Exception{
        ExecutorService executor=Executors.newFixedThreadPool(2);CountDownLatch start=new CountDownLatch(1);List<Future<String>> results=new ArrayList<>();
        try{for(int i=0;i<2;i++){final int n=i;results.add(executor.submit(()->{start.await();try{(n==0?a:b).create("same","a",directory,"yaml","request_concurrent_"+n,admin);return "OK";}catch(TaskService.Conflict conflict){return conflict.getMessage();}}));}start.countDown();List<String> states=Arrays.asList(results.get(0).get(30,TimeUnit.SECONDS),results.get(1).get(30,TimeUnit.SECONDS));assertTrue(states.contains("OK"));assertTrue(states.contains("TASK_ID_EXISTS"));}finally{executor.shutdownNow();}
        try(Connection c=connection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM rivomesh_control.rmc_task_version")){r.next();assertEquals(1,r.getInt(1));}
    }
    @Test void sameRequestIsIdempotentAcrossEndpointsButOtherContentIsRejected()throws Exception{
        GlobalTaskStore.Record first=a.create("task","a",directory,"yaml","request_create_01",admin);assertEquals(first.revision,b.create("task","a",directory,"yaml","request_create_01",admin).revision);
        assertEquals("REQUEST_ID_REUSED",assertThrows(TaskService.Conflict.class,()->b.create("task","a",directory,"changed","request_create_01",admin)).getMessage());
        GlobalTaskStore.Record updated=a.update("task",first.revision,"next","request_update_01",admin);assertEquals(updated.revision,b.update("task",first.revision,"next","request_update_01",admin).revision);
    }
    @Test void wrongKeyStoreAndPolicyNeverSeedAReplacementAuthority()throws Exception{
        assertEquals("CONTROL_KEY_MISMATCH",assertThrows(TaskService.Conflict.class,()->new GlobalTaskStore(config.cluster,access,key(2),a.identity,100)).getMessage());assertThrows(TaskService.Conflict.class,()->new GlobalTaskStore(config.cluster,access,key(1),UUID.randomUUID().toString(),100));assertThrows(TaskService.Conflict.class,()->new GlobalTaskStore(config.cluster,access,key(1),a.identity,101));assertEquals(a.identity,new GlobalTaskStore(config.cluster,access,key(1),a.identity,100).identity);
    }
    @Test void missingAuthorityCannotBeReseededFromTaskRowsOrAnOldBinding()throws Exception{
        a.create("task","a",directory,"private","request_create_01",admin);
        try(Connection c=connection();Statement s=c.createStatement()){s.executeUpdate("DELETE FROM rivomesh_control.rmc_task_root");}
        assertEquals("GLOBAL_TASK_STATE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new GlobalTaskStore(config.cluster,access,key(1),a.identity,100)).getMessage());
        assertEquals("GLOBAL_TASK_STATE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new GlobalTaskStore(config.cluster,access,key(1),null,100)).getMessage());
        assertThrows(TaskService.Conflict.class,()->a.get("task",admin));
    }
    @Test void missingCatalogRowsOrChangedAccessAuthorityRefuseStartup()throws Exception{
        a.create("task","a",directory,"private","request_create_01",admin);
        try(Connection c=connection();Statement s=c.createStatement()){s.executeUpdate("DELETE FROM rivomesh_control.rmc_task_authority");}
        assertEquals("GLOBAL_TASK_CATALOG_INCOMPLETE",assertThrows(TaskService.Conflict.class,()->new GlobalTaskStore(config.cluster,access,key(1),a.identity,100)).getMessage());
        try(Connection c=connection();Statement s=c.createStatement()){s.executeUpdate("UPDATE rivomesh_control.rmc_task_root SET access_identity='"+UUID.randomUUID()+"'");}
        assertEquals("GLOBAL_TASK_ACCESS_IDENTITY_CHANGED",assertThrows(TaskService.Conflict.class,()->new GlobalTaskStore(config.cluster,access,key(1),a.identity,100)).getMessage());
    }
    @Test void databaseRecordOrPayloadSubstitutionIsRefused()throws Exception{
        a.create("task","a",directory,"password: value","request_create_01",admin);
        try(Connection c=connection();Statement s=c.createStatement()){s.executeUpdate("UPDATE rivomesh_control.rmc_task_authority SET owner_id='b'");}
        assertEquals("GLOBAL_TASK_RECORD_INVALID",assertThrows(TaskService.Conflict.class,()->b.get("task",admin)).getMessage());
    }
    @Test void forgedRolesCannotElevateThePersistedAccount()throws Exception{
        a.create("task","a",directory,"yaml","request_create_01",admin);String revision=access.accounts().revision;
        access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":[\"task\"],\"nodeIds\":[\"a\"],\"token\":\"viewer-token-test-at-least-32-bytes\"}"),"request_account_01",admin);
        AccountDirectory.State state=access.accounts();AccessControl.Principal forged=new AccessControl.Principal("viewer",PlatformConfig.Role.ADMIN,null,state.revision,null);
        assertNull(b.get("task",forged).yaml);assertThrows(SecurityException.class,()->b.update("task",b.get("task",forged).revision,"stolen","request_forged_01",forged));
        assertEquals("AUTHORIZATION_CHANGED",assertThrows(TaskService.Conflict.class,()->a.get("task",admin)).getMessage());
    }
    @Test void pagesAreBoundedStableAndFilterBothNodeAndTaskGrantsInSql()throws Exception{
        for(int n=0;n<6;n++)a.create("task-"+n,n%2==0?"a":"b",directory,"password: private", "request_create_0"+n,admin);
        Map<String,Object> first=a.page("",2,admin),second=b.page((String)first.get("next"),2,admin);assertEquals(6L,first.get("total"));assertEquals(2,((List<?>)first.get("tasks")).size());assertNotEquals(first.get("tasks"),second.get("tasks"));assertFalse(Model.JSON.writeValueAsString(first).contains("private"));
        access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+access.accounts().revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":[\"task-0\",\"task-1\",\"task-4\"],\"nodeIds\":[\"a\"],\"token\":\"viewer-token-test-at-least-32-bytes\"}"),"request_account_01",admin);
        AccessControl.Principal viewer=AccountDirectory.authenticate(access.accounts(),"Bearer viewer-token-test-at-least-32-bytes");Map<String,Object> scoped=b.page("",1,viewer);assertEquals(2L,scoped.get("total"));assertEquals("task-0",((Map<?,?>)((List<?>)scoped.get("tasks")).get(0)).get("id"));assertEquals("task-4",((Map<?,?>)((List<?>)a.page((String)scoped.get("next"),1,viewer).get("tasks")).get(0)).get("id"));assertThrows(SecurityException.class,()->a.get("task-1",viewer));
    }
    @Test void rootCounterTamperingCannotResetCapacityOrOrdering()throws Exception{
        a.create("task","a",directory,"yaml","request_create_01",admin);
        try(Connection c=connection();Statement statement=c.createStatement()){statement.executeUpdate("UPDATE rivomesh_control.rmc_task_root SET next_sequence=0");}
        assertEquals("GLOBAL_TASK_ROOT_INVALID",assertThrows(TaskService.Conflict.class,()->a.get("task",admin)).getMessage());assertThrows(TaskService.Conflict.class,()->b.page("",10,admin));
    }
    @Test void zeroNodeGrantNeverBecomesGlobalAndRevocationInvalidatesPaging()throws Exception{
        a.create("task","a",directory,"yaml","request_create_01",admin);String revision=access.accounts().revision;
        access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+revision+"\",\"id\":\"viewer\",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":null,\"nodeIds\":[],\"token\":\"viewer-token-test-at-least-32-bytes\"}"),"request_account_01",admin);
        AccessControl.Principal viewer=AccountDirectory.authenticate(access.accounts(),"Bearer viewer-token-test-at-least-32-bytes");assertEquals(0L,b.page("",50,viewer).get("total"));assertThrows(SecurityException.class,()->b.get("task",viewer));assertEquals("AUTHORIZATION_CHANGED",assertThrows(TaskService.Conflict.class,()->a.page("",10,admin)).getMessage());
    }
    @Test void commandAdmissionHookRollsBackTheTaskVersionAndReceiptOnFailure()throws Exception{
        java.util.concurrent.atomic.AtomicBoolean entered=new java.util.concurrent.atomic.AtomicBoolean();
        assertThrows(java.io.IOException.class,()->a.create("task","a",directory,"private","request_create_01",admin,(connection,record,actor)->{entered.set(true);throw new java.io.IOException("COMMAND_ADMISSION_FAILED");}));assertTrue(entered.get());
        try(Connection c=connection();Statement statement=c.createStatement()){for(String table:Arrays.asList("task_authority","task_version","task_request"))try(ResultSet r=statement.executeQuery("SELECT COUNT(*) FROM rivomesh_control.rmc_"+table)){r.next();assertEquals(0,r.getInt(1));}}
        assertEquals("task",b.create("task","a",directory,"private","request_create_01",admin).id);
    }
    @Test void auditFailureRollsBackVersionRequestAndTaskAtomically()throws Exception{
        try(Connection c=connection();Statement s=c.createStatement()){s.execute("DROP TABLE rivomesh_control.rmc_audit_event");}
        assertThrows(ControlStore.Unavailable.class,()->a.create("task","a",directory,"private","request_create_01",admin));
        try(Connection c=connection();Statement s=c.createStatement()){for(String name:Arrays.asList("task_authority","task_version","task_request"))try(ResultSet r=s.executeQuery("SELECT COUNT(*) FROM rivomesh_control.rmc_"+name)){r.next();assertEquals(0,r.getInt(1));}}
    }
    @Test void aRunningEntryCannotReplaceALostCatalogRowWithANewTask()throws Exception{
        a.create("task","a",directory,"private","request_create_01",admin);
        try(Connection c=connection();Statement s=c.createStatement()){s.executeUpdate("DELETE FROM rivomesh_control.rmc_task_authority");}
        assertEquals("GLOBAL_TASK_CATALOG_INCOMPLETE",assertThrows(TaskService.Conflict.class,()->b.create("replacement","a",directory,"other","request_create_02",admin)).getMessage());
    }
}
