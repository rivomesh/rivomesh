package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class TaskPlaneTest {
    @TempDir Path directory;
    static final String VIEWER="task-plane-viewer-token-at-least-32-bytes";
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=PlatformTest.TOKEN;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();c.pollMillis=100;c.sessions.secureCookie=false;c.users.add(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,VIEWER));return c;}
    TaskService service(PlatformConfig c)throws Exception{return new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers());}
    @Test void fixedOperationsPreserveLocalCreationAndDuplicateReceipts()throws Exception{
        try(TaskService tasks=service(config())){
            TaskControlPlane plane=new LocalTaskPlane(tasks);AccessControl.Principal admin=tasks.authenticate("Bearer "+PlatformTest.TOKEN);
            TaskRequest create=TaskRequest.of(TaskRequest.Kind.CREATE,null);create.yaml=Model.JSON.writeValueAsString(PlatformTest.task("job"));create.requestId="request_create_job";
            assertEquals("job",plane.invoke(create,admin).get("id"));assertEquals(plane.invoke(create,admin).get("revision"),tasks.view("job").get("revision"));assertEquals(1,((List<?>)plane.invoke(TaskRequest.of(TaskRequest.Kind.LIST,null),admin).get("tasks")).size());
        }
    }
    @Test void workerDispatchCannotElevateAViewerByForgingItsRole()throws Exception{
        try(TaskService tasks=service(config())){
            tasks.create(Model.JSON.writeValueAsString(PlatformTest.task("job")),"request_create_job");LocalTaskPlane plane=new LocalTaskPlane(tasks);AccessControl.Principal viewer=tasks.authenticate("Bearer "+VIEWER),forged=new AccessControl.Principal(viewer.id,PlatformConfig.Role.ADMIN,null,viewer.authorizationRevision);
            TaskRequest action=TaskRequest.of(TaskRequest.Kind.ACTION,"job");action.action="START";action.revision=PlatformTest.revision(tasks,"job");action.requestId="request_start_job";
            assertThrows(SecurityException.class,()->plane.invoke(action,viewer));assertThrows(SecurityException.class,()->plane.invoke(action,forged));assertThrows(SecurityException.class,()->plane.invoke(TaskRequest.of(TaskRequest.Kind.CONFIGURATION,"job"),forged));assertEquals("job",plane.invoke(TaskRequest.of(TaskRequest.Kind.VIEW,"job"),viewer).get("id"));
        }
    }
    @Test void staleAccountsAndUnsignedIdentitiesCannotInvokeBusinessOperations()throws Exception{
        try(TaskService tasks=service(config())){
            LocalTaskPlane plane=new LocalTaskPlane(tasks);AccessControl.Principal old=tasks.authenticate("Bearer "+PlatformTest.TOKEN);String revision=(String)tasks.accountView(old).get("revision");tasks.mutateAccounts("ACTIVATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+revision+"\"}"),"activate_access_test",old);
            assertEquals("AUTHORIZATION_CHANGED",assertThrows(TaskService.Conflict.class,()->plane.invoke(TaskRequest.of(TaskRequest.Kind.LIST,null),old)).getMessage());assertThrows(SecurityException.class,()->plane.invoke(TaskRequest.of(TaskRequest.Kind.LIST,null),new AccessControl.Principal("legacy-admin",PlatformConfig.Role.ADMIN)));
        }
    }
    @Test void requestSnapshotsRejectUnsupportedVersionsActionsAndMutableDocuments()throws Exception{
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.CONFIG_PREVIEW,"job");request.document=Model.JSON.readTree("{\"jobId\":\"job\"}");TaskRequest copy=request.snapshot();((com.fasterxml.jackson.databind.node.ObjectNode)request.document).put("jobId","changed");assertEquals("job",copy.document.path("jobId").asText());
        request.version=2;assertThrows(IllegalArgumentException.class,request::snapshot);TaskRequest action=TaskRequest.of(TaskRequest.Kind.ACTION,"job");action.requestId="request_invalid_op";action.action="java.lang.Runtime.exec";assertThrows(IllegalArgumentException.class,action::snapshot);
    }
    @Test void httpReadsReachTheControlPlaneAndKeepEndpointAuthorization()throws Exception{
        PlatformConfig c=config();List<TaskRequest.Kind> calls=new ArrayList<>();TaskControlPlane plane=(request,actor)->{calls.add(request.kind);return Collections.singletonMap("routed",request.kind.name());};
        try(TaskService tasks=service(c);ConsoleServer server=new ConsoleServer(c,tasks,plane)){
            server.start();assertEquals(200,BrowserSessionsTest.request(server,"GET","/api/tasks","Authorization","Bearer "+PlatformTest.TOKEN).status);assertEquals(200,BrowserSessionsTest.request(server,"GET","/api/tasks/job","Authorization","Bearer "+PlatformTest.TOKEN).status);assertEquals(200,BrowserSessionsTest.request(server,"GET","/api/tasks/job/configuration","Authorization","Bearer "+PlatformTest.TOKEN).status);
            assertEquals(Arrays.asList(TaskRequest.Kind.LIST,TaskRequest.Kind.VIEW,TaskRequest.Kind.CONFIGURATION),calls);assertEquals(403,BrowserSessionsTest.request(server,"GET","/api/tasks/job/configuration","Authorization","Bearer "+VIEWER).status);assertEquals(3,calls.size());
        }
    }
}
