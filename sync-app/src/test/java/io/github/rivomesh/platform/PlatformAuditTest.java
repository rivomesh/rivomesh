package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class PlatformAuditTest {
    @TempDir Path directory;
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=PlatformTest.TOKEN;c.pollMillis=100;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;}
    @Test void authenticationDenialsAndConfigReadsAreAuditedWithoutSecrets()throws Exception{
        PlatformConfig c=config();String viewer="audit-viewer-private-token-at-least24";c.users.add(PlatformTest.user("reader",PlatformConfig.Role.VIEWER,viewer));
        try(TaskService service=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(c,service)){
            service.create(Model.JSON.writeValueAsString(PlatformTest.task("audit_task")),"create_audit_task");server.start();String base="http://127.0.0.1:"+server.port();
            assertEquals(401,PlatformTest.http(base,"/api/session","invalid-private-token",null,null).status);
            assertEquals(403,PlatformTest.http(base,"/api/tasks",viewer,"not-json-secret-body","private-request-header").status);
            assertEquals(403,PlatformTest.http(base,"/api/audit",viewer,null,null).status);
            assertEquals(403,PlatformTest.http(base,"/api/tasks/audit_task/configuration",viewer,null,null).status);
            assertEquals(200,PlatformTest.http(base,"/api/tasks/audit_task/configuration",PlatformTest.TOKEN,null,null).status);
            assertEquals(404,PlatformTest.http(base,"/api/unknown-private-path?secret=unknown-private-query",PlatformTest.TOKEN,null,null).status);
            assertEquals(400,PlatformTest.http(base,"/api/audit?after=1&after=2",PlatformTest.TOKEN,null,null).status);
            PlatformTest.Response exported=PlatformTest.http(base,"/api/audit/export?limit=100",PlatformTest.TOKEN,null,null);assertEquals(200,exported.status);
            String text=Model.JSON.writeValueAsString(service.auditPage(0,0,1000));
            for(String secret:Arrays.asList(viewer,PlatformTest.TOKEN,"invalid-private-token","not-json-secret-body","private-request-header","source-secret","target-secret","url-secret","unknown-private-path","unknown-private-query"))assertFalse(text.contains(secret),secret);
            assertTrue(text.contains("UNAUTHORIZED"));assertTrue(text.contains("FORBIDDEN"));assertTrue(text.contains("reader"));assertTrue(text.contains("GET_CONFIGURATION"));assertTrue(text.contains("GET_AUDIT_EXPORT"));assertTrue(text.contains(AuditStore.hash("private-request-header")));
        }
    }
    @Test void asyncResultsAndActorsSurviveControllerRestart()throws Exception{
        PlatformConfig c=config();AccessControl.Principal actor=new AccessControl.Principal("audited_operator",PlatformConfig.Role.OPERATOR);
        try(TaskService service=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){
            service.create(Model.JSON.writeValueAsString(PlatformTest.task("audit_async")),"create_audit_async");service.command("audit_async","CHECK",PlatformTest.revision(service,"audit_async"),"check_audit_async",-1,null,actor);
            PlatformTest.await(()->PlatformTest.operations(service,"audit_async").contains("SUCCEEDED"));
        }
        try(TaskService service=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){
            List<?> events=(List<?>)service.auditPage(0,0,1000).get("events");List<Map<?,?>> matching=new ArrayList<>();for(Object item:events){Map<?,?> event=(Map<?,?>)item;if(AuditStore.hash("check_audit_async").equals(event.get("operationHash")))matching.add(event);}
            assertEquals(2,matching.size());assertEquals("PENDING",matching.get(0).get("status"));assertEquals("SUCCEEDED",matching.get(1).get("status"));assertEquals("audited_operator",matching.get(1).get("actorId"));
        }
    }
    @Test void auditFailureBlocksHttpMutationAndDurableOutboxReplaysOnRestart()throws Exception{
        PlatformConfig c=config();TaskService service=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(c,service);
        service.create(Model.JSON.writeValueAsString(PlatformTest.task("audit_failure")),"create_audit_failure");server.start();String base="http://127.0.0.1:"+server.port();
        java.lang.reflect.Field field=TaskService.class.getDeclaredField("audit");field.setAccessible(true);((AuditStore)field.get(service)).close();
        String revision=PlatformTest.revision(service,"audit_failure");
        String body="{\"action\":\"START\",\"expectedRevision\":\""+revision+"\"}";
        PlatformTest.Response rejected=PlatformTest.http(base,"/api/tasks/audit_failure/actions",PlatformTest.TOKEN,body,"audit_blocked_start");assertEquals(503,rejected.status);assertTrue(rejected.text.contains("AUDIT_UNAVAILABLE"));assertEquals(revision,PlatformTest.revision(service,"audit_failure"));
        // Model an already-accepted asynchronous result reaching the registry while the audit volume is unavailable.
        service.command("audit_failure","CHECK",revision,"outbox_check_request",-1);
        PlatformTest.await(()->PlatformTest.operations(service,"audit_failure").contains("SUCCEEDED"));server.close();assertThrows(java.io.IOException.class,service::close);
        TaskService.Registry saved=Model.JSON.readValue(Files.readAllBytes(Paths.get(c.dataDirectory,"controller.json")),TaskService.Registry.class);assertEquals(2,saved.auditOutbox.size());
        try(TaskService reopened=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){
            String text=Model.JSON.writeValueAsString(reopened.auditPage(0,0,1000));assertTrue(text.contains(AuditStore.hash("outbox_check_request")));
            TaskService.Registry drained=Model.JSON.readValue(Files.readAllBytes(Paths.get(c.dataDirectory,"controller.json")),TaskService.Registry.class);assertTrue(drained.auditOutbox.isEmpty());
        }
    }
    @Test void deletingAuditDatabaseCannotSilentlyStartANewHistory()throws Exception{
        PlatformConfig c=config();try(TaskService service=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){}
        Files.delete(Paths.get(c.dataDirectory,"audit/events.mv.db"));
        java.io.IOException failure=assertThrows(java.io.IOException.class,()->new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers()));assertEquals("AUDIT_STORE_IDENTITY_CHANGED",failure.getMessage());
    }
    @Test void crashAfterAuditCommitBeforeOutboxRemovalDoesNotDuplicateEvent()throws Exception{
        PlatformConfig c=config();try(TaskService service=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){}
        AuditStore.Event pending=AuditStoreTest.event();pending.kind="OPERATION_COMMITTED";pending.action="CHECK";
        try(AuditStore store=new AuditStore(Paths.get(c.dataDirectory),c.auditMaxEvents)){store.append(pending);}
        Path registry=Paths.get(c.dataDirectory,"controller.json");TaskService.Registry saved=Model.JSON.readValue(Files.readAllBytes(registry),TaskService.Registry.class);saved.auditOutbox.add(pending);PrivateFiles.write(registry,Model.JSON.writeValueAsBytes(saved));
        try(TaskService service=new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers())){long count=((List<?>)service.auditPage(0,0,1000).get("events")).stream().filter(item->pending.id.equals(((Map<?,?>)item).get("id"))).count();assertEquals(1,count);}
    }
}
