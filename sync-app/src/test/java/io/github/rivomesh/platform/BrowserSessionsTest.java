package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class BrowserSessionsTest {
    @TempDir Path directory;
    static final String TOKEN=PlatformTest.TOKEN, VIEWER="viewer-test-token-at-least-32-characters", NEW_TOKEN="rotated-test-token-at-least-32-characters";
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=TOKEN;c.sessions.secureCookie=false;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;}
    static final class Reply {
        int status; String text; Map<String,String> headers=new HashMap<>();
        String cookie(){String value=headers.get("set-cookie");return value==null?null:value.split(";",2)[0];}
        String csrf()throws Exception{return Model.JSON.readTree(text).path("csrfToken").asText();}
    }
    // Use raw HTTP: HttpURLConnection deliberately suppresses Origin and some cookie headers.
    static Reply request(ConsoleServer server,String method,String path,String... fields)throws Exception {
        try(Socket socket=new Socket("127.0.0.1",server.port())){
            socket.setSoTimeout(5000);StringBuilder wire=new StringBuilder(method+" "+path+" HTTP/1.1\r\nHost: 127.0.0.1:"+server.port()+"\r\nConnection: close\r\n");
            for(int i=0;i<fields.length;i+=2)if(fields[i+1]!=null)wire.append(fields[i]).append(": ").append(fields[i+1]).append("\r\n");
            wire.append("Content-Length: 0\r\n\r\n");socket.getOutputStream().write(wire.toString().getBytes(StandardCharsets.US_ASCII));
            BufferedReader reader=new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8));Reply reply=new Reply();reply.status=Integer.parseInt(reader.readLine().split(" ")[1]);
            String line;while(!(line=reader.readLine()).isEmpty()){int split=line.indexOf(':');reply.headers.put(line.substring(0,split).toLowerCase(Locale.ROOT),line.substring(split+1).trim());}
            StringBuilder body=new StringBuilder();char[] bytes=new char[4096];int n;while((n=reader.read(bytes))!=-1)body.append(bytes,0,n);reply.text=body.toString();return reply;
        }
    }
    static Reply login(ConsoleServer server,String token)throws Exception{return request(server,"POST","/api/session","Authorization","Bearer "+token,"Origin","http://127.0.0.1:"+server.port());}
    @Test void reloadUsesCookieAndLogoutRevokesCredentialWithoutLeakingSecrets()throws Exception{
        PlatformConfig config=config();String id,csrf;
        try(TaskService tasks=new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,tasks)){
            server.start();Reply login=login(server,TOKEN);assertEquals(200,login.status);id=login.cookie().split("=",2)[1];csrf=login.csrf();
            String cookie=login.headers.get("set-cookie");assertTrue(cookie.contains("HttpOnly"));assertTrue(cookie.contains("SameSite=Strict"));assertFalse(cookie.contains("Max-Age"));assertFalse(cookie.contains("Domain="));assertFalse(login.text.contains(id));assertFalse(login.text.contains(TOKEN));
            Reply restored=request(server,"GET","/api/session","Cookie",login.cookie());assertEquals(200,restored.status);assertEquals(csrf,restored.csrf());assertEquals("ADMIN",Model.JSON.readTree(restored.text).path("role").asText());
            assertEquals(200,request(server,"GET","/api/tasks","Cookie",login.cookie()).status);
            assertEquals(403,request(server,"POST","/api/session/logout","Cookie",login.cookie()).status);
            assertEquals(403,request(server,"POST","/api/session/logout","Cookie",login.cookie(),"Origin","http://127.0.0.1:"+server.port()).status);
            assertEquals(403,request(server,"POST","/api/session/logout","Cookie",login.cookie(),"Origin","http://evil.example","X-CSRF-Token",csrf).status);
            assertEquals(403,request(server,"POST","/api/session/logout","Cookie",login.cookie(),"Origin","http://127.0.0.1:"+server.port(),"X-CSRF-Token","x").status);
            Reply logout=request(server,"POST","/api/session/logout","Cookie",login.cookie(),"Origin","http://127.0.0.1:"+server.port(),"X-CSRF-Token",csrf);assertEquals(200,logout.status);assertTrue(logout.headers.get("set-cookie").contains("Max-Age=0"));
            assertEquals(401,request(server,"GET","/api/session","Cookie",login.cookie()).status);
            assertEquals(200,request(server,"GET","/api/tasks","Authorization","Bearer "+TOKEN).status);
        }
        try(java.util.stream.Stream<Path> files=Files.walk(directory)){
            for(Path path:(Iterable<Path>)files.filter(Files::isRegularFile)::iterator){String text=new String(Files.readAllBytes(path),StandardCharsets.UTF_8);assertFalse(text.contains(TOKEN));assertFalse(text.contains(id));assertFalse(text.contains(csrf));}
        }
    }
    @Test void explicitBearerNeverFallsBackAndConflictingIdentitiesAreRejected()throws Exception{
        PlatformConfig config=config();config.users.add(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,VIEWER));
        try(TaskService tasks=new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,tasks)){
            server.start();Reply login=login(server,VIEWER);assertEquals(200,login.status);
            assertEquals(401,request(server,"GET","/api/session","Cookie",login.cookie(),"Authorization","Bearer incorrect").status);
            assertEquals(200,request(server,"GET","/api/session","Cookie",login.cookie(),"Authorization","Bearer "+VIEWER).status);
            assertEquals(409,request(server,"GET","/api/session","Cookie",login.cookie(),"Authorization","Bearer "+TOKEN).status);
            assertEquals(403,request(server,"POST","/api/tasks","Cookie",login.cookie(),"Origin","http://127.0.0.1:"+server.port(),"X-CSRF-Token",login.csrf()).status);
            assertEquals(200,request(server,"POST","/api/session/logout","Cookie",login.cookie(),"Origin","http://127.0.0.1:"+server.port(),"X-CSRF-Token",login.csrf()).status);
        }
    }
    void change(TaskService tasks,String action,String id,String extra)throws Exception{
        AccessControl.Principal admin=tasks.authenticate("Bearer "+TOKEN);String revision=(String)tasks.accountView(admin).get("revision");
        tasks.mutateAccounts(action,id,Model.JSON.readTree("{\"expectedRevision\":\""+revision+"\""+extra+"}"),"session_"+UUID.randomUUID().toString().replace("-",""),admin);
    }
    @Test void rotationDisableAndGrantChangesInvalidatePreviouslyIssuedSessions()throws Exception{
        PlatformConfig config=config();config.users.add(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,VIEWER));
        try(TaskService tasks=new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,tasks)){
            change(tasks,"ACTIVATE",null,"");server.start();Reply administrator=login(server,TOKEN);Reply login=login(server,VIEWER);
            change(tasks,"TOKEN","viewer",",\"token\":\""+NEW_TOKEN+"\"");assertEquals(401,request(server,"GET","/api/session","Cookie",login.cookie()).status);assertEquals(401,login(server,VIEWER).status);assertEquals(401,request(server,"GET","/api/session","Cookie",administrator.cookie()).status);
            login=login(server,NEW_TOKEN);assertEquals(200,login.status);
            change(tasks,"UPDATE","viewer",",\"role\":\"VIEWER\",\"enabled\":false");assertEquals(401,request(server,"GET","/api/session","Cookie",login.cookie()).status);
            change(tasks,"UPDATE","viewer",",\"role\":\"VIEWER\",\"enabled\":true");login=login(server,NEW_TOKEN);assertEquals(200,login.status);
            change(tasks,"UPDATE","viewer",",\"role\":\"OPERATOR\",\"enabled\":true,\"taskIds\":[\"only-this\"]");assertEquals(401,request(server,"GET","/api/session","Cookie",login.cookie()).status);
            Reply fresh=login(server,NEW_TOKEN);assertEquals("OPERATOR",Model.JSON.readTree(fresh.text).path("role").asText());assertEquals("only-this",Model.JSON.readTree(fresh.text).path("taskScope").get(0).asText());
        }
    }
    @Test void selfRotationRequiresANewSessionAndTheSavedNewToken()throws Exception{
        PlatformConfig config=config();
        try(TaskService tasks=new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,tasks)){
            change(tasks,"ACTIVATE",null,"");server.start();Reply previous=login(server,TOKEN);assertEquals(200,previous.status);
            change(tasks,"TOKEN","legacy-admin",",\"token\":\""+NEW_TOKEN+"\"");
            assertEquals(401,request(server,"GET","/api/session","Cookie",previous.cookie()).status);assertEquals(401,login(server,TOKEN).status);
            Reply fresh=request(server,"POST","/api/session","Authorization","Bearer "+NEW_TOKEN,"Cookie",previous.cookie(),"Origin","http://127.0.0.1:"+server.port());
            assertEquals(200,fresh.status);assertNotEquals(previous.cookie(),fresh.cookie());assertNotEquals(previous.csrf(),fresh.csrf());
            assertEquals(200,request(server,"GET","/api/session","Cookie",fresh.cookie()).status);assertEquals(401,request(server,"GET","/api/session","Cookie",previous.cookie()).status);
        }
    }
    @Test void productionCookieAndProxyOriginAreExplicitAndRememberingIsOptional()throws Exception{
        PlatformConfig config=config();config.sessions.secureCookie=true;config.sessions.publicOrigin="https://console.example";config.sessions.rememberBrowser=true;
        try(TaskService tasks=new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,tasks)){
            server.start();assertEquals(403,login(server,TOKEN).status);
            Reply login=request(server,"POST","/api/session","Authorization","Bearer "+TOKEN,"Origin","https://console.example");assertEquals(200,login.status);
            String cookie=login.headers.get("set-cookie");assertTrue(cookie.startsWith("__Host-rivomesh-session="));assertTrue(cookie.contains("; Secure"));assertTrue(cookie.contains("Max-Age=28800"));
            assertEquals(200,request(server,"POST","/api/session/logout","Cookie",login.cookie(),"Origin","https://console.example","X-CSRF-Token",login.csrf()).status);
        }
    }
    @Test void disabledBrowserSessionsKeepApiCompatibleAndRestartRequiresNewLogin()throws Exception{
        PlatformConfig config=config();config.sessions.enabled=false;
        try(TaskService tasks=new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers())){
            try(ConsoleServer server=new ConsoleServer(config,tasks)){server.start();assertEquals(409,login(server,TOKEN).status);assertEquals(200,request(server,"GET","/api/session","Authorization","Bearer "+TOKEN).status);}
            config.sessions.enabled=true;String cookie;
            try(ConsoleServer server=new ConsoleServer(config,tasks)){server.start();cookie=login(server,TOKEN).cookie();}
            try(ConsoleServer server=new ConsoleServer(config,tasks)){server.start();assertEquals(401,request(server,"GET","/api/session","Cookie",cookie).status);assertEquals(200,login(server,TOKEN).status);}
        }
    }
    static final class MutableClock extends Clock {
        long now=1000000;public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return Instant.ofEpochMilli(now);}
    }
    @Test void idleAbsoluteExpiryCapacityAndReauthenticationAreBounded(){
        PlatformConfig.Sessions config=new PlatformConfig.Sessions();config.idleSeconds=30;config.absoluteSeconds=60;config.maximum=1;
        MutableClock clock=new MutableClock();AccessControl.Principal actor=new AccessControl.Principal("admin",PlatformConfig.Role.ADMIN);
        try(BrowserSessions sessions=new BrowserSessions(config,new BrowserSessions.MemoryStore(1),clock)){
            BrowserSessions.Issued first=sessions.create(actor);assertEquals(43,first.id.length());assertNotNull(sessions.find(first.id));assertThrows(TaskService.Conflict.class,()->sessions.create(actor));
            clock.now+=29000;sessions.touch(first.id);clock.now+=29000;assertNotNull(sessions.find(first.id));clock.now+=2000;assertNull(sessions.find(first.id));
            BrowserSessions.Issued second=sessions.create(actor);clock.now+=30000;assertNull(sessions.find(second.id));
            BrowserSessions.Issued third=sessions.create(actor);sessions.revoke(third.id);assertNull(sessions.find(third.id));assertNotNull(sessions.create(actor));
        }
    }
    @Test void sessionConfigurationRejectsUnsafeOriginShapesAndInvalidLimits()throws Exception{
        PlatformConfig config=config();config.sessions.publicOrigin="https://console.example/path";assertThrows(IllegalArgumentException.class,config::validate);
        config.sessions.publicOrigin="http://console.example";config.validate();config.sessions.secureCookie=true;assertThrows(IllegalArgumentException.class,config::validate);
        config.sessions.publicOrigin="https://console.example";config.validate();config.sessions.idleSeconds=0;assertThrows(IllegalArgumentException.class,config::validate);
    }
}
