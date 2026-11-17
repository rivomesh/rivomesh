package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class TaskAuthorizationTest {
    @TempDir Path directory;
    static final String OPERATOR="scoped-operator-private-token-at-least24",VIEWER="scoped-viewer-private-token-at-least24",EMPTY="empty-grants-private-token-at-least24";
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=PlatformTest.TOKEN;c.pollMillis=100;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;}
    static PlatformConfig.User scoped(String id,PlatformConfig.Role role,String token,List<String> tasks){PlatformConfig.User user=PlatformTest.user(id,role,token);user.taskIds=tasks;return user;}
    static Config task(String id,long serverId){Config c=PlatformTest.task(id);c.target.metadataSchema="meta_"+id;c.source.serverId=serverId;return c;}
    @Test void listsAndAllTaskRoutesEnforceExactGrantsBeforeReadingBodies()throws Exception{
        PlatformConfig c=config();c.users.add(scoped("operator",PlatformConfig.Role.OPERATOR,OPERATOR,Arrays.asList("allowed")));c.users.add(scoped("viewer",PlatformConfig.Role.VIEWER,VIEWER,Arrays.asList("allowed")));c.users.add(scoped("empty",PlatformConfig.Role.OPERATOR,EMPTY,Collections.emptyList()));Files.createDirectories(Paths.get(c.tasksDirectory));Files.write(Paths.get(c.tasksDirectory,"private-invalid.yml"),"invalid: [".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        PlatformTest.Workers workers=new PlatformTest.Workers();
        try(TaskService service=new TaskService(c,new PlatformTest.Backend(),workers);ConsoleServer server=new ConsoleServer(c,service)){
            service.create(Model.JSON.writeValueAsString(task("allowed",910001)),"create_scope_allowed");service.create(Model.JSON.writeValueAsString(task("hidden",910002)),"create_scope_hidden");server.start();String base="http://127.0.0.1:"+server.port();
            for(String token:Arrays.asList(OPERATOR,VIEWER)){
                PlatformTest.Response response=PlatformTest.http(base,"/api/tasks",token,null,null);assertEquals(200,response.status);JsonNode list=Model.JSON.readTree(response.text);assertEquals(1,list.path("tasks").size());assertEquals("allowed",list.path("tasks").get(0).path("id").asText());assertTrue(list.path("invalidConfigs").isEmpty());assertFalse(response.text.contains("hidden"));assertFalse(response.text.contains("private-invalid"));
                assertEquals(200,PlatformTest.http(base,"/api/tasks/allowed",token,null,null).status);assertEquals(404,PlatformTest.http(base,"/api/tasks/hidden",token,null,null).status);
            }
            assertEquals(0,Model.JSON.readTree(PlatformTest.http(base,"/api/tasks",EMPTY,null,null).text).path("tasks").size());
            String hiddenRevision=PlatformTest.revision(service,"hidden");assertEquals(404,PlatformTest.http(base,"/api/tasks/hidden/actions",OPERATOR,"not-json","hidden_action_denied").status);assertEquals(hiddenRevision,PlatformTest.revision(service,"hidden"));assertEquals(0,workers.starts.get());
            assertEquals(403,PlatformTest.http(base,"/api/tasks/allowed/actions",VIEWER,"not-json","viewer_action_denied").status);assertEquals(403,PlatformTest.http(base,"/api/tasks/allowed/configuration",OPERATOR,null,null).status);
            String action="{\"action\":\"CHECK\",\"expectedRevision\":\""+PlatformTest.revision(service,"allowed")+"\"}";assertEquals(202,PlatformTest.http(base,"/api/tasks/allowed/actions",OPERATOR,action,"allowed_operator_check").status);PlatformTest.await(()->PlatformTest.operations(service,"allowed").contains("SUCCEEDED"));
            JsonNode admin=Model.JSON.readTree(PlatformTest.http(base,"/api/tasks",PlatformTest.TOKEN,null,null).text);assertEquals(2,admin.path("tasks").size());assertEquals(1,admin.path("invalidConfigs").size());
            String audit=Model.JSON.writeValueAsString(service.auditPage(0,0,1000));assertTrue(audit.contains("TASK_NOT_FOUND"));assertTrue(audit.contains("operator"));
        }
    }
    @Test void grantsAreAnImmutableCredentialSnapshotAndReloadCanRevokeAccess()throws Exception{
        PlatformConfig c=config();List<String> grants=new ArrayList<>(Arrays.asList("allowed","future_task"));c.users.add(scoped("operator",PlatformConfig.Role.OPERATOR,OPERATOR,grants));
        AccessControl access=new AccessControl(c);grants.clear();AccessControl.Principal principal=access.authenticate("Bearer "+OPERATOR);assertTrue(principal.task("allowed"));assertTrue(principal.task("future_task"));assertFalse(principal.task("allowed_suffix"));assertFalse(principal.task("other"));assertThrows(UnsupportedOperationException.class,()->principal.taskIds.add("other"));assertEquals(2,((List<?>)principal.view().get("taskScope")).size());assertFalse(new AccessControl(c).authenticate("Bearer "+OPERATOR).task("allowed"));
        assertTrue(access.authenticate("Bearer "+PlatformTest.TOKEN).task("any_task"));
    }
    @Test void invalidScopesAndScopedAdministratorsAreRejected()throws Exception{
        PlatformConfig c=config();PlatformConfig.User user=scoped("operator",PlatformConfig.Role.OPERATOR,OPERATOR,Arrays.asList("good","good"));c.users.add(user);assertThrows(IllegalArgumentException.class,c::validateAccess);
        for(String invalid:Arrays.asList("*","../other","a/b","")){user.taskIds=Arrays.asList(invalid);assertThrows(IllegalArgumentException.class,c::validateAccess);}
        user.taskIds=Arrays.asList("future_task");c.validateAccess();user.role=PlatformConfig.Role.ADMIN;assertEquals("PLATFORM_TASK_SCOPE_INVALID",assertThrows(IllegalArgumentException.class,c::validateAccess).getMessage());user.taskIds=null;c.validateAccess();
    }
}
