package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SharedAccessTest {
    @TempDir Path directory;
    @Test void auditDigestRemainsCompatibleWithPublishedSha256Vectors(){assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",AuditStore.hash(""));assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",AuditStore.hash("abc"));}
    static final String TOKEN=PlatformTest.TOKEN,VIEWER="shared-viewer-test-token-at-least-32",NEW_TOKEN="shared-rotated-test-token-at-least-32";
    PlatformConfig config(String node){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=TOKEN;c.pollMillis=100;c.sessions.secureCookie=false;c.tasksDirectory=directory.resolve(node+"/tasks").toString();c.dataDirectory=directory.resolve(node+"/data").toString();c.cluster=ControlStoreTest.cluster(directory.getFileName().toString(),node);c.cluster.sharedAccess=true;c.users.add(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,VIEWER));return c;}
    TaskService service(PlatformConfig config)throws Exception{return new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers());}
    static void change(TaskService service,String action,String id,String fields)throws Exception{
        AccessControl.Principal actor=service.authenticate("Bearer "+TOKEN);String revision=(String)service.accountView(actor).get("revision");service.mutateAccounts(action,id,Model.JSON.readTree("{\"expectedRevision\":\""+revision+"\""+fields+"}"),"shared_"+UUID.randomUUID().toString().replace("-",""),actor);
    }
    static BrowserSessions sessions(PlatformConfig c,TaskService tasks,Clock clock){return new BrowserSessions(c.sessions,tasks.sessionStore(),clock);}
    @Test void missingAuditEventsMakesReadinessFailAndRestoringThemPreservesSessions()throws Exception{
        PlatformConfig c=config("a");
        try(TaskService tasks=service(c);ConsoleServer server=new ConsoleServer(c,tasks)){
            server.start();BrowserSessionsTest.Reply login=BrowserSessionsTest.login(server,TOKEN);assertEquals(200,login.status);
            try(Connection sql=DriverManager.getConnection(c.cluster.url,"sa","");Statement statement=sql.createStatement()){statement.execute("ALTER TABLE \"rivomesh_control\".\"rmc_audit_event\" RENAME TO \"rmc_audit_event_offline\"");}
            try{
                assertEquals(503,BrowserSessionsTest.request(server,"GET","/api/session","Cookie",login.cookie()).status);
                PlatformTest.await(()->BrowserSessionsTest.request(server,"GET","/readyz").status==503);
            }finally{
                try(Connection sql=DriverManager.getConnection(c.cluster.url,"sa","");Statement statement=sql.createStatement()){statement.execute("ALTER TABLE \"rivomesh_control\".\"rmc_audit_event_offline\" RENAME TO \"rmc_audit_event\"");}
            }
            PlatformTest.await(()->BrowserSessionsTest.request(server,"GET","/readyz").status==200);
            assertEquals(200,BrowserSessionsTest.request(server,"GET","/api/session","Cookie",login.cookie()).status);
        }
    }
    @Test void cookieRestoresAtAnotherControllerAndSurvivesControllerShutdownAndRestart()throws Exception{
        PlatformConfig a=config("a"),b=config("b");
        try(TaskService first=service(a);TaskService second=service(b);ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();BrowserSessionsTest.Reply login=BrowserSessionsTest.login(one,TOKEN);assertEquals(200,login.status);
            assertEquals(200,BrowserSessionsTest.request(two,"GET","/api/session","Cookie",login.cookie()).status);
            one.close();first.close();assertEquals(200,BrowserSessionsTest.request(two,"GET","/api/session","Cookie",login.cookie()).status);
            try(TaskService reopened=service(a);ConsoleServer three=new ConsoleServer(a,reopened)){three.start();assertEquals(200,BrowserSessionsTest.request(three,"GET","/api/session","Cookie",login.cookie()).status);assertEquals(200,BrowserSessionsTest.request(two,"POST","/api/session/logout","Cookie",login.cookie(),"Origin","http://127.0.0.1:"+two.port(),"X-CSRF-Token",login.csrf()).status);assertEquals(401,BrowserSessionsTest.request(three,"GET","/api/session","Cookie",login.cookie()).status);}
        }
    }
    @Test void rotationAndGrantsInvalidateEveryNodesSessionAndStaleMutationActor()throws Exception{
        PlatformConfig a=config("a"),b=config("b");
        try(TaskService first=service(a);TaskService second=service(b);ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();BrowserSessionsTest.Reply admin=BrowserSessionsTest.login(one,TOKEN),viewer=BrowserSessionsTest.login(one,VIEWER);AccessControl.Principal stale=first.authenticate("Bearer "+TOKEN);
            change(second,"TOKEN","viewer",",\"token\":\""+NEW_TOKEN+"\"");assertEquals(401,BrowserSessionsTest.request(one,"GET","/api/session","Cookie",viewer.cookie()).status);assertEquals(401,BrowserSessionsTest.request(two,"GET","/api/session","Cookie",admin.cookie()).status);assertNull(first.authenticate("Bearer "+VIEWER));assertNotNull(first.authenticate("Bearer "+NEW_TOKEN));
            assertEquals("AUTHORIZATION_CHANGED",assertThrows(TaskService.Conflict.class,()->first.create(Model.JSON.writeValueAsString(PlatformTest.task("stale")),"shared_stale_create",stale)).getMessage());
            BrowserSessionsTest.Reply fresh=BrowserSessionsTest.login(two,NEW_TOKEN);assertEquals(200,BrowserSessionsTest.request(one,"GET","/api/session","Cookie",fresh.cookie()).status);
            change(first,"UPDATE","viewer",",\"role\":\"OPERATOR\",\"enabled\":true,\"taskIds\":[\"allowed\"]");assertEquals(401,BrowserSessionsTest.request(two,"GET","/api/session","Cookie",fresh.cookie()).status);
            AccessControl.Principal actor=second.authenticate("Bearer "+NEW_TOKEN);assertTrue(actor.operates());assertTrue(actor.task("allowed"));assertFalse(actor.task("other"));assertEquals("SHARED",second.accountView(second.authenticate("Bearer "+TOKEN)).get("authority"));
        }
    }
    @Test void databaseClockExpiryAndGlobalCapacityIgnoreCallerClockSkew()throws Exception{
        PlatformConfig a=config("a"),b=config("b");a.sessions.maximum=b.sessions.maximum=1;
        try(TaskService first=service(a);TaskService second=service(b);BrowserSessions oldClock=sessions(a,first,Clock.fixed(Instant.EPOCH,ZoneOffset.UTC));BrowserSessions futureClock=sessions(b,second,Clock.fixed(Instant.parse("2099-01-01T00:00:00Z"),ZoneOffset.UTC))){
            BrowserSessions.Issued issued=oldClock.create(first.authenticate("Bearer "+TOKEN));assertTrue(issued.session.expiresAt>System.currentTimeMillis());assertNotNull(futureClock.find(issued.id));assertThrows(TaskService.Conflict.class,()->futureClock.create(second.authenticate("Bearer "+TOKEN)));
            try(Connection c=DriverManager.getConnection(a.cluster.url,"sa","");PreparedStatement p=c.prepareStatement("UPDATE \"rivomesh_control\".\"rmc_session\" SET expires_at=0")){p.executeUpdate();}assertNull(futureClock.find(issued.id));
            BrowserSessions.Issued next=futureClock.create(second.authenticate("Bearer "+TOKEN));try(Connection c=DriverManager.getConnection(a.cluster.url,"sa","");Statement s=c.createStatement()){s.executeUpdate("UPDATE \"rivomesh_control\".\"rmc_session\" SET last_seen=0");}assertNull(oldClock.find(next.id));
        }
    }
    @Test void managedLocalDirectoryImportsWithAnchorAndCannotFallBackToLocalState()throws Exception{
        PlatformConfig config=config("imported");ClusterConfig cluster=config.cluster;config.cluster=null;String localStore;
        try(TaskService local=service(config)){change(local,"ACTIVATE",null,"");localStore=(String)local.auditPage(0,0,1).get("storeId");}
        config.cluster=cluster;
        try(TaskService shared=service(config)){assertNotNull(shared.authenticate("Bearer "+VIEWER));String json=Model.JSON.writeValueAsString(shared.auditPage(0,0,1000));assertTrue(json.contains("ACCOUNT_IMPORT_LOCAL"));assertTrue(json.contains(localStore));assertEquals(3,Model.JSON.readTree(Files.readAllBytes(Paths.get(config.dataDirectory).resolve("controller.json"))).path("version").asInt());}
        config.cluster.sharedAccess=false;assertEquals("SHARED_ACCESS_REQUIRED",assertThrows(java.io.IOException.class,()->service(config)).getMessage());
    }
    @Test void missingAuthorityRefusesOldTokenAndWillNotRebootstrapFromConfig()throws Exception{
        PlatformConfig config=config("missing");try(TaskService service=service(config)){
            try(Connection c=DriverManager.getConnection(config.cluster.url,"sa","");Statement s=c.createStatement()){s.executeUpdate("DELETE FROM \"rivomesh_control\".\"rmc_access_root\"");}
            assertThrows(ControlStore.Unavailable.class,()->service.authenticate("Bearer "+TOKEN));assertFalse(service.accessReady());
        }
        assertEquals("SHARED_ACCESS_STATE_REQUIRED",assertThrows(java.io.IOException.class,()->service(config)).getMessage());
    }
    @Test void concurrentSharedAuditIsOneChainAndRetriedOutboxEventsAppearOnlyOnce()throws Exception{
        PlatformConfig a=config("a"),b=config("b");try(TaskService first=service(a);TaskService second=service(b)){
            ExecutorService pool=Executors.newFixedThreadPool(2);try{List<Future<?>> results=new ArrayList<>();for(TaskService service:Arrays.asList(first,second))results.add(pool.submit(()->{for(int i=0;i<15;i++)try{AuditStore.Event event=service.auditBegin("GET_TEST",null,service.authenticate("Bearer "+TOKEN),"127.0.0.1",null);service.auditComplete(event,200,null);}catch(Exception e){throw new RuntimeException(e);}}));for(Future<?> result:results)result.get(15,TimeUnit.SECONDS);}finally{pool.shutdownNow();}
            Map<String,Object> left=first.auditPage(0,0,1000),right=second.auditPage(0,0,1000);assertEquals(left.get("storeId"),right.get("storeId"));assertEquals(left.get("lastHash"),right.get("lastHash"));assertEquals(left.get("events"),right.get("events"));assertFalse(Model.JSON.writeValueAsString(left).contains(TOKEN));
        }
        Path state=Paths.get(a.dataDirectory).resolve("controller.json");TaskService.Registry registry=Model.JSON.readValue(Files.readAllBytes(state),TaskService.Registry.class);AuditStore.Event pending=new AuditStore.Event();pending.kind="OPERATION_COMMITTED";pending.action="TEST_RECOVERY";pending.status="SUCCEEDED";registry.auditOutbox.add(pending);PrivateFiles.write(state,Model.JSON.writeValueAsBytes(registry));
        try(TaskService recovered=service(a)){List<?> events=(List<?>)recovered.auditPage(0,0,1000).get("events");assertEquals(1,events.stream().filter(e->pending.id.equals(((Map<?,?>)e).get("id"))).count());assertEquals(0,Model.JSON.readTree(Files.readAllBytes(state)).path("auditOutbox").size());}
        registry=Model.JSON.readValue(Files.readAllBytes(state),TaskService.Registry.class);registry.auditOutbox.add(pending);PrivateFiles.write(state,Model.JSON.writeValueAsBytes(registry));
        try(TaskService repeated=service(a)){List<?> events=(List<?>)repeated.auditPage(0,0,1000).get("events");assertEquals(1,events.stream().filter(e->pending.id.equals(((Map<?,?>)e).get("id"))).count());}
    }
    @Test void auditFailureRollsBackTokenChangeAndSessionRevocationTogether()throws Exception{
        PlatformConfig config=config("atomic");try(TaskService service=service(config);BrowserSessions browser=sessions(config,service,Clock.systemUTC())){
            BrowserSessions.Issued issued=browser.create(service.authenticate("Bearer "+VIEWER));String before=(String)service.accountView(service.authenticate("Bearer "+TOKEN)).get("revision");
            try(Connection c=DriverManager.getConnection(config.cluster.url,"sa","");Statement sql=c.createStatement()){sql.execute("ALTER TABLE \"rivomesh_control\".\"rmc_audit_event\" ADD CONSTRAINT fail_token_audit CHECK (payload NOT LIKE '%ACCOUNT_TOKEN%')");}
            assertThrows(ControlStore.Unavailable.class,()->change(service,"TOKEN","viewer",",\"token\":\""+NEW_TOKEN+"\""));assertNotNull(service.authenticate("Bearer "+VIEWER));assertNull(service.authenticate("Bearer "+NEW_TOKEN));assertNotNull(browser.find(issued.id));assertEquals(before,service.accountView(service.authenticate("Bearer "+TOKEN)).get("revision"));
        }
    }
    @Test void incompatiblePolicyIsRejectedAndRecoveryRevokesSessionsEverywhere()throws Exception{
        PlatformConfig a=config("a"),b=config("b");try(TaskService first=service(a);BrowserSessions browser=sessions(a,first,Clock.systemUTC())){
            BrowserSessions.Issued issued=browser.create(first.authenticate("Bearer "+TOKEN));b.sessions.maximum=2;assertEquals("SHARED_ACCESS_POLICY_MISMATCH",assertThrows(TaskService.Conflict.class,()->service(b)).getMessage());
            PlatformConfig recovery=config("recovery");recovery.authToken=NEW_TOKEN;TaskService.recoverAccess(recovery,"legacy-admin");assertNull(browser.find(issued.id));assertNull(first.authenticate("Bearer "+TOKEN));assertNotNull(first.authenticate("Bearer "+NEW_TOKEN));
        }
    }
    @Test void failedRecoveryCannotMutateTheVerifiedDirectorySnapshot()throws Exception{
        PlatformConfig config=config("recovery-rollback");try(TaskService service=service(config)){
            SharedAccessStore store=new SharedAccessStore(config,AccountDirectory.configured(config),null,Collections.emptyMap());
            PlatformConfig replacement=config("unused");replacement.authToken=NEW_TOKEN;AccountDirectory.User seed=AccountDirectory.configured(replacement).users.get("legacy-admin");
            try(Connection c=DriverManager.getConnection(config.cluster.url,"sa","");Statement sql=c.createStatement()){sql.execute("ALTER TABLE \"rivomesh_control\".\"rmc_audit_event\" ADD CONSTRAINT fail_recovery_audit CHECK (payload NOT LIKE '%ACCOUNT_RECOVER%')");}
            assertThrows(ControlStore.Unavailable.class,()->store.recover("legacy-admin",seed));assertNotNull(AccountDirectory.authenticate(store.accounts(),"Bearer "+TOKEN));assertNull(AccountDirectory.authenticate(store.accounts(),"Bearer "+NEW_TOKEN));
        }
    }
    @Test void largeLegalDirectorySupportsConcurrentSessionReadsWithoutCopyingScopesIntoSqlSessions()throws Exception{
        PlatformConfig config=config("large");List<String> grants=new ArrayList<>();for(int i=0;i<10000;i++)grants.add(String.format("task-%05d-",i)+String.join("",Collections.nCopies(105,"x")));
        for(int i=0;i<3;i++){PlatformConfig.User user=PlatformTest.user("large-"+i,PlatformConfig.Role.VIEWER,"large-viewer-token-at-least-32-characters-"+i);user.taskIds=grants;config.users.add(user);}
        try(TaskService service=service(config);BrowserSessions browser=sessions(config,service,Clock.systemUTC())){
            BrowserSessions.Issued issued=browser.create(service.authenticate("Bearer large-viewer-token-at-least-32-characters-0"));ExecutorService pool=Executors.newFixedThreadPool(8);
            try{List<Future<?>> work=new ArrayList<>();for(int i=0;i<16;i++)work.add(pool.submit(()->{BrowserSessions.Session session=browser.find(issued.id);assertNotNull(session);assertEquals(10000,session.principal.taskIds.size());}));for(Future<?> result:work)result.get(20,TimeUnit.SECONDS);}finally{pool.shutdownNow();}
            try(Connection c=DriverManager.getConnection(config.cluster.url,"sa","");PreparedStatement p=c.prepareStatement("SELECT state_payload FROM \"rivomesh_control\".\"rmc_access_root\"");ResultSet r=p.executeQuery()){assertTrue(r.next());assertTrue(r.getString(1).length()>3*1024*1024);}
        }
    }

    @Test void sharedRealmRefusesRegistryOnlyHttpMembers()throws Exception{
        PlatformConfig a=config("shared"),b=config("registry-only");b.cluster.sharedAccess=false;
        try(TaskService shared=service(a);TaskService local=service(b)){
            assertEquals("CONTROL_STORE_PROTOCOL_MISMATCH",assertThrows(TaskService.Conflict.class,()->new ConsoleServer(b,local)).getMessage());
        }
    }
    @Test void authorityUpgradeRequiresExplicitStoppedMembersNotExpiredHeartbeats()throws Exception{
        ClusterConfig old=config("legacy").cluster;old.sharedAccess=false;PlatformConfig upgraded=config("upgraded");
        try(ControlStore registry=new JdbcControlStore(old)){
            ControlStore.Lease lease=registry.register(ControlStoreTest.node("legacy"),Collections.emptyList());
            assertEquals("SHARED_ACCESS_QUIESCENCE_REQUIRED",assertThrows(TaskService.Conflict.class,()->service(upgraded)).getMessage());
            ControlStoreTest.expire(old,"legacy");assertEquals("SHARED_ACCESS_QUIESCENCE_REQUIRED",assertThrows(TaskService.Conflict.class,()->service(upgraded)).getMessage());
            registry.stopped(lease);try(TaskService shared=service(upgraded)){assertNotNull(shared.authenticate("Bearer "+TOKEN));}
            assertEquals("CONTROL_STORE_PROTOCOL_MISMATCH",assertThrows(TaskService.Conflict.class,()->registry.register(ControlStoreTest.node("legacy"),Collections.emptyList())).getMessage());
        }
    }

    @Test void importedTaskAccessStaysOnItsOriginalNodeUntilAnAdministratorExpandsIt()throws Exception{
        PlatformConfig a=config("a"),b=config("b");try(TaskService first=service(a);TaskService second=service(b);ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            first.create(Model.JSON.writeValueAsString(PlatformTest.task("same-id")),"node_scope_create_a");second.create(Model.JSON.writeValueAsString(PlatformTest.task("same-id")),"node_scope_create_b");one.start();two.start();
            BrowserSessionsTest.Reply login=BrowserSessionsTest.login(one,VIEWER);assertEquals(200,login.status);assertEquals(200,BrowserSessionsTest.request(two,"GET","/api/session","Cookie",login.cookie()).status);
            assertEquals(200,BrowserSessionsTest.request(one,"GET","/api/tasks/same-id","Cookie",login.cookie()).status);assertEquals(404,BrowserSessionsTest.request(two,"GET","/api/tasks/same-id","Cookie",login.cookie()).status);
            assertEquals(0,Model.JSON.readTree(BrowserSessionsTest.request(two,"GET","/api/tasks","Cookie",login.cookie()).text).path("tasks").size());
            change(first,"UPDATE","viewer",",\"role\":\"VIEWER\",\"enabled\":true,\"taskIds\":null,\"nodeIds\":[\"a\",\"b\"]");assertEquals(401,BrowserSessionsTest.request(one,"GET","/api/session","Cookie",login.cookie()).status);
            BrowserSessionsTest.Reply expanded=BrowserSessionsTest.login(one,VIEWER);assertEquals(200,BrowserSessionsTest.request(two,"GET","/api/tasks/same-id","Cookie",expanded.cookie()).status);
        }
    }
    @Test void invalidNodeScopesAreRejectedAndOmittedUpdatesPreserveExistingScope()throws Exception{
        PlatformConfig config=config("a");try(TaskService service=service(config)){
            assertTrue(service.authenticate("Bearer "+VIEWER).node("a"));assertFalse(service.authenticate("Bearer "+VIEWER).node("b"));
            assertThrows(IllegalArgumentException.class,()->change(service,"UPDATE","viewer",",\"role\":\"VIEWER\",\"enabled\":true,\"nodeIds\":[\"a\",\"a\"]"));
            assertThrows(IllegalArgumentException.class,()->change(service,"UPDATE","legacy-admin",",\"role\":\"ADMIN\",\"enabled\":true,\"nodeIds\":[\"a\"]"));
            change(service,"UPDATE","viewer",",\"role\":\"OPERATOR\",\"enabled\":true,\"taskIds\":null");assertFalse(service.authenticate("Bearer "+VIEWER).node("b"));
        }
    }

}
