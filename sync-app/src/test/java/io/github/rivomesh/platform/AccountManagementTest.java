package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.Model;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class AccountManagementTest {
    @TempDir Path directory;
    static final String ROOT=PlatformTest.TOKEN,OP="managed-operator-token-at-least32-bytes",NEW="managed-rotated-token-at-least32-bytes",ADMIN="managed-second-admin-at-least32-bytes";
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=ROOT;c.pollMillis=100;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;}
    TaskService service(PlatformConfig c)throws Exception{return new TaskService(c,new PlatformTest.Backend(),new PlatformTest.Workers());}
    static ObjectNode request(String revision){return Model.JSON.createObjectNode().put("expectedRevision",revision);}
    static JsonNode read(TaskService s,String token){return Model.JSON.valueToTree(s.accountView(s.authenticate("Bearer "+token)));}
    static JsonNode change(TaskService s,String token,String action,String id,ObjectNode body,String request)throws Exception{return Model.JSON.valueToTree(s.mutateAccounts(action,id,body,request,s.authenticate("Bearer "+token)));}
    static String revision(TaskService s){return read(s,ROOT).path("revision").asText();}
    static void activate(TaskService s)throws Exception{change(s,ROOT,"ACTIVATE",null,request(revision(s)),"activate_accounts");}
    static ObjectNode user(TaskService s,String id,String role,String token){ObjectNode body=request(revision(s)).put("id",id).put("role",role).put("enabled",true).put("token",token);body.putNull("taskIds");return body;}
    static final class Response {int status;JsonNode json;}
    static Response http(String base,String method,String path,String token,String body,String request)throws Exception{
        HttpURLConnection c=(HttpURLConnection)new URL(base+path).openConnection();c.setConnectTimeout(3000);c.setReadTimeout(8000);c.setRequestMethod(method);c.setRequestProperty("Authorization","Bearer "+token);if(request!=null)c.setRequestProperty("X-Request-ID",request);
        if(body!=null){c.setDoOutput(true);c.setRequestProperty("Content-Type","application/json");try(OutputStream out=c.getOutputStream()){out.write(body.getBytes(StandardCharsets.UTF_8));}}
        Response result=new Response();result.status=c.getResponseCode();try(InputStream in=result.status<400?c.getInputStream():c.getErrorStream();ByteArrayOutputStream out=new ByteArrayOutputStream()){byte[] b=new byte[2048];int n;while((n=in.read(b))!=-1)out.write(b,0,n);result.json=Model.JSON.readTree(out.toByteArray());}finally{c.disconnect();}return result;
    }
    @Test void configurationModeNeedsExplicitActivationAndLastAdministratorSurvives()throws Exception{
        PlatformConfig c=config();try(TaskService s=service(c)){
            assertEquals("CONFIG",read(s,ROOT).path("mode").asText());ObjectNode original=request(revision(s));JsonNode first=change(s,ROOT,"ACTIVATE",null,original,"activate_accounts");assertEquals("MANAGED",first.path("mode").asText());assertTrue(change(s,ROOT,"ACTIVATE",null,original,"activate_accounts").path("replayed").asBoolean());
            TaskService.Conflict failure=assertThrows(TaskService.Conflict.class,()->change(s,ROOT,"DELETE","legacy-admin",request(revision(s)),"delete_last_admin"));assertEquals("ACCOUNT_LAST_ADMIN",failure.getMessage());assertNotNull(s.authenticate("Bearer "+ROOT));
            ObjectNode admin=user(s,"second-admin","ADMIN",ADMIN);change(s,ROOT,"CREATE",null,admin,"create_second_admin");change(s,ROOT,"DELETE","legacy-admin",request(revision(s)),"delete_old_admin");assertNull(s.authenticate("Bearer "+ROOT));assertNotNull(s.authenticate("Bearer "+ADMIN));
        }
        c.accessMode=PlatformConfig.AccessMode.MANAGED;c.authToken=null;try(TaskService restarted=service(c)){assertNull(restarted.authenticate("Bearer "+ROOT));assertNotNull(restarted.authenticate("Bearer "+ADMIN));}
    }
    @Test void scopesRotationRevocationAndReplayPersistWithoutPlaintextTokens()throws Exception{
        PlatformConfig c=config();try(TaskService s=service(c)){
            activate(s);ObjectNode body=user(s,"operator","OPERATOR",OP);body.putArray("taskIds").add("allowed");change(s,ROOT,"CREATE",null,body,"create_operator");assertTrue(change(s,ROOT,"CREATE",null,body,"create_operator").path("replayed").asBoolean());AccessControl.Principal old=s.authenticate("Bearer "+OP);assertTrue(old.task("allowed"));assertFalse(old.task("hidden"));
            ObjectNode empty=request(revision(s)).put("role","OPERATOR").put("enabled",true);empty.putArray("taskIds");change(s,ROOT,"UPDATE","operator",empty,"revoke_operator_scope");assertFalse(s.authenticate("Bearer "+OP).task("allowed"));assertEquals("AUTHORIZATION_CHANGED",assertThrows(TaskService.Conflict.class,()->s.command("allowed","CHECK","revision","stale_actor_check",-1,null,old)).getMessage());
            change(s,ROOT,"TOKEN","operator",request(revision(s)).put("token",NEW),"rotate_operator_token");assertNull(s.authenticate("Bearer "+OP));assertNotNull(s.authenticate("Bearer "+NEW));ObjectNode disabled=request(revision(s)).put("role","VIEWER").put("enabled",false);disabled.putNull("taskIds");change(s,ROOT,"UPDATE","operator",disabled,"disable_operator");assertNull(s.authenticate("Bearer "+NEW));
            String state=new String(Files.readAllBytes(Paths.get(c.dataDirectory,"controller.json")),StandardCharsets.UTF_8),audit=Model.JSON.writeValueAsString(s.auditPage(0,0,1000));for(String token:Arrays.asList(ROOT,OP,NEW)){assertFalse(state.contains(token));assertFalse(audit.contains(token));assertFalse(read(s,ROOT).toString().contains(AccountDirectory.hash(token)));}assertTrue(audit.contains("ACCOUNT_TOKEN"));assertTrue(audit.contains("operator"));
        }
        try(TaskService restarted=service(c)){assertNull(restarted.authenticate("Bearer "+OP));assertNull(restarted.authenticate("Bearer "+NEW));assertEquals("MANAGED",read(restarted,ROOT).path("mode").asText());}
    }
    @Test void staleVersionsCollisionsAndDeletedIdentityReuseAreRejected()throws Exception{
        try(TaskService s=service(config())){
            activate(s);ObjectNode operator=user(s,"operator","OPERATOR",OP);change(s,ROOT,"CREATE",null,operator,"create_operator");assertEquals("ACCOUNTS_REVISION_CHANGED",assertThrows(TaskService.Conflict.class,()->change(s,ROOT,"CREATE",null,operator.deepCopy().put("id","another"),"stale_new_account")).getMessage());
            assertEquals("ACCOUNT_TOKEN_COLLISION",assertThrows(TaskService.Conflict.class,()->change(s,ROOT,"CREATE",null,user(s,"another","VIEWER",OP),"duplicate_bearer")).getMessage());
            change(s,ROOT,"DELETE","operator",request(revision(s)),"delete_operator");assertNull(s.authenticate("Bearer "+OP));assertEquals("ACCOUNT_EXISTS",assertThrows(TaskService.Conflict.class,()->change(s,ROOT,"CREATE",null,user(s,"operator","VIEWER",NEW),"reuse_deleted_identity")).getMessage());
        }
    }
    @Test void realHttpRequiresAdminAndChangedGrantsTakeEffectOnTheNextRequest()throws Exception{
        PlatformConfig c=config();c.users.add(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,"configuration-viewer-token-at-least24"));
        try(TaskService s=service(c);ConsoleServer server=new ConsoleServer(c,s)){
            server.start();String base="http://127.0.0.1:"+server.port();assertEquals(403,http(base,"GET","/api/accounts","configuration-viewer-token-at-least24",null,null).status);assertEquals(403,http(base,"POST","/api/accounts/activate","configuration-viewer-token-at-least24","not-json","viewer_denied_early").status);
            assertEquals(200,http(base,"POST","/api/accounts/activate",ROOT,request(revision(s)).toString(),"http_activate_accounts").status);ObjectNode body=user(s,"operator","OPERATOR",OP);body.putArray("taskIds").add("allowed");assertEquals(201,http(base,"POST","/api/accounts",ROOT,body.toString(),"http_create_operator").status);
            assertEquals("allowed",http(base,"GET","/api/session",OP,null,null).json.path("taskScope").get(0).asText());assertEquals(403,http(base,"GET","/api/accounts",OP,null,null).status);
            ObjectNode empty=request(revision(s)).put("role","VIEWER").put("enabled",true);empty.putArray("taskIds");assertEquals(200,http(base,"PUT","/api/accounts/operator",ROOT,empty.toString(),"http_update_operator").status);JsonNode session=http(base,"GET","/api/session",OP,null,null).json;assertEquals("VIEWER",session.path("role").asText());assertEquals(0,session.path("taskScope").size());
            assertEquals(200,http(base,"POST","/api/accounts/operator/token",ROOT,request(revision(s)).put("token",NEW).toString(),"http_rotate_operator").status);assertEquals(401,http(base,"GET","/api/session",OP,null,null).status);assertEquals(200,http(base,"GET","/api/session",NEW,null,null).status);
        }
    }
    @Test void missingOrOlderDirectoryCannotSilentlyRestoreBootstrapCredentials()throws Exception{
        PlatformConfig c=config();byte[] old;
        try(TaskService s=service(c)){activate(s);old=Files.readAllBytes(Paths.get(c.dataDirectory,"controller.json"));change(s,ROOT,"CREATE",null,user(s,"operator","OPERATOR",OP),"create_operator");}
        Path state=Paths.get(c.dataDirectory,"controller.json");byte[] current=Files.readAllBytes(state);Files.write(state,old);assertEquals("ACCOUNT_AUDIT_ANCHOR_MISMATCH",assertThrows(Exception.class,()->service(c)).getMessage());Files.write(state,current);try(TaskService s=service(c)){assertNotNull(s.authenticate("Bearer "+OP));}
        Files.delete(state);assertEquals("ACCOUNT_STATE_REQUIRED",assertThrows(Exception.class,()->service(c)).getMessage());assertFalse(Files.exists(state));
    }
    @Test void auditAnchorSurvivesEventRetention()throws Exception{
        AccountDirectory.State state=new AccountDirectory.State();state.identity=UUID.randomUUID().toString();state.revision=UUID.randomUUID().toString();state.version=1;
        try(AuditStore audit=new AuditStore(directory,3)){audit.append(AccountDirectory.event(state,"ACTIVATE",null,null));for(int i=0;i<8;i++){AuditStore.Event event=new AuditStore.Event();event.kind="HTTP_BEGIN";audit.append(event);}assertEquals(1L,audit.accessAnchor().get("version"));assertEquals(state.revision,audit.accessAnchor().get("revision"));assertEquals(3,((List<?>)audit.page(0,0,100).get("events")).size());}
    }
    @Test void legacyAuditPayloadRemainsByteVerifiableAfterSchemaExtension()throws Exception{
        AuditStore.Event event=new AuditStore.Event();event.kind="HTTP_BEGIN";try(AuditStore audit=new AuditStore(directory,100)){audit.append(event);}
        String url="jdbc:h2:file:"+directory.resolve("audit/events").toAbsolutePath()+";DB_CLOSE_ON_EXIT=FALSE;WRITE_DELAY=0";
        try(java.sql.Connection c=java.sql.DriverManager.getConnection(url,"sa","");java.sql.Statement q=c.createStatement();java.sql.ResultSet rows=q.executeQuery("SELECT payload FROM rm_audit_event")){
            assertTrue(rows.next());ObjectNode payload=(ObjectNode)Model.JSON.readTree(rows.getString(1));payload.remove(Arrays.asList("subjectId","accessIdentity","accessRevision","accessVersion"));String raw=Model.JSON.writeValueAsString(payload),hash=AuditStore.hash("1\n"+AuditStore.ZERO+"\n"+raw);
            try(java.sql.PreparedStatement update=c.prepareStatement("UPDATE rm_audit_event SET payload=?,event_hash=?")){update.setString(1,raw);update.setString(2,hash);update.executeUpdate();}try(java.sql.PreparedStatement update=c.prepareStatement("UPDATE rm_audit_state SET last_hash=?")){update.setString(1,hash);update.executeUpdate();}
        }
        try(AuditStore audit=new AuditStore(directory,100)){Map<?,?> row=(Map<?,?>)((List<?>)audit.page(0,0,100).get("events")).get(0);assertFalse(row.containsKey("accessVersion"));assertFalse(row.containsKey("subjectId"));Map<String,Object> payload=new LinkedHashMap<>();for(Map.Entry<?,?> item:row.entrySet())if(!Arrays.asList("hash","previousHash").contains(item.getKey()))payload.put((String)item.getKey(),item.getValue());assertEquals(row.get("hash"),AuditStore.hash("1\n"+row.get("previousHash")+"\n"+Model.JSON.writeValueAsString(payload)));}
    }
    @Test void offlineRecoveryRequiresControllerLockAndDoesNotReviveOtherCredentials()throws Exception{
        PlatformConfig c=config();try(TaskService s=service(c)){activate(s);change(s,ROOT,"TOKEN","legacy-admin",request(revision(s)).put("token",ADMIN),"rotate_root_token");assertNull(s.authenticate("Bearer "+ROOT));assertThrows(TaskService.Conflict.class,()->TaskService.recoverAccess(c,"legacy-admin"));}
        TaskService.recoverAccess(c,"legacy-admin");try(TaskService s=service(c)){assertNotNull(s.authenticate("Bearer "+ROOT));assertNull(s.authenticate("Bearer "+ADMIN));assertTrue(Model.JSON.writeValueAsString(s.auditPage(0,0,1000)).contains("offline-recovery"));assertEquals(2,Model.JSON.readTree(Files.readAllBytes(Paths.get(c.dataDirectory,"controller.json"))).path("version").asInt());}
    }
}
