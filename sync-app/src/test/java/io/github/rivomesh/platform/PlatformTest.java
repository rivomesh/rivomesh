package io.github.rivomesh.platform;

import io.github.rivomesh.cdc.TargetProduct;
import io.github.rivomesh.*;
import io.github.rivomesh.cdc.JdbcSink;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class PlatformTest {
    @TempDir Path directory;
    static final String TOKEN="platform-test-token-at-least-24-characters";
    static PlatformConfig.User user(String id,PlatformConfig.Role role,String token){PlatformConfig.User user=new PlatformConfig.User();user.id=id;user.role=role;user.token=token;return user;}
    PlatformConfig config() {
        PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=TOKEN;c.pollMillis=100;
        c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;
    }
    static Config task(String id) {
        Config c=new Config();c.jobId=id;c.source.host="mysql";c.source.database=id;c.source.username="source-user";c.source.password="source-secret";
        c.target.url="jdbc:postgresql://gaussdb:5432/test?password=url-secret";c.target.username="target-user";c.target.password="target-secret";
        c.target.schema=id;c.target.metadataSchema="sync_meta";c.source.serverId=910001;return c;
    }
    static final class Backend implements TaskBackend {
        final Map<String,Map<String,Object>> states=new ConcurrentHashMap<>();
        final AtomicInteger pauses=new AtomicInteger(),resumes=new AtomicInteger();
        synchronized void initialize(String id){Map<String,Object> state=new LinkedHashMap<>();state.put("run_state","RUNNING");state.put("error_count",0L);state.put("capture_gtid","uuid:1");state.put("required_gtid","uuid:1");states.put(id,state);}
        public synchronized Map<String,Object> status(Config c){return new LinkedHashMap<>(states.getOrDefault(c.jobId,Collections.emptyMap()));}
        public synchronized void pause(Config c){Map<String,Object> s=states.get(c.jobId);if(!"PAUSED".equals(s.get("run_state"))){s.put("run_state","PAUSED");s.put("error_count",((Number)s.get("error_count")).longValue()+1);pauses.incrementAndGet();}}
        public synchronized void resume(Config c,long generation){Map<String,Object>s=states.get(c.jobId);if(((Number)s.get("error_count")).longValue()!=generation)throw new IllegalStateException("ERROR_GENERATION_CHANGED");s.put("run_state","READY");resumes.incrementAndGet();}
        public Map<String,Object> check(Config c){return Collections.singletonMap("tables",3);}
    }
    static final class Workers implements TaskWorker.Factory {
        final AtomicInteger starts=new AtomicInteger(),active=new AtomicInteger();
        public TaskWorker start(Path config,Path log,int heap){starts.incrementAndGet();active.incrementAndGet();return new TaskWorker(){volatile boolean alive=true;public boolean alive(){return alive;}public int exitCode(){return 1;}public void stop(){if(alive){alive=false;active.decrementAndGet();}}};}
    }
    interface Condition{boolean get()throws Exception;}
    static void await(Condition condition)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(System.nanoTime()<end){if(condition.get())return;Thread.sleep(30);}fail("Timed out");}
    static String revision(TaskService service,String id){return(String)service.view(id).get("revision");}
    static String operations(TaskService service,String id){return service.view(id).get("operations").toString();}

    @Test void commandsAreDurableIdempotentAndRejectStaleRequests()throws Exception{
        Backend backend=new Backend();Workers workers=new Workers();PlatformConfig c=config();
        try(TaskService service=new TaskService(c,backend,workers)){
            String yaml=Model.JSON.writeValueAsString(task("sales"));
            service.create(yaml,"create_sales");service.create(yaml,"create_sales");assertEquals(1,((List<?>)service.list().get("tasks")).size());
            assertThrows(TaskService.Conflict.class,()->service.create(yaml+" ","create_sales"));
            backend.initialize("sales");String initial=revision(service,"sales");
            service.command("sales","START",initial,"start_sales",-1);
            await(()->workers.active.get()==1);
            service.command("sales","START",initial,"start_sales",-1);assertEquals(1,workers.starts.get());
            assertThrows(TaskService.Conflict.class,()->service.command("sales","STOP",initial,"stale_stop",-1));
            service.command("sales","PAUSE",revision(service,"sales"),"pause_sales",-1);
            await(()->backend.pauses.get()==1&&operations(service,"sales").contains("SHARED_PAUSED"));assertEquals(0,workers.active.get());
            service.command("sales","RESUME",revision(service,"sales"),"resume_sales",0);
            await(()->operations(service,"sales").contains("ERROR_GENERATION_CHANGED"));assertEquals(0,backend.resumes.get());
            service.command("sales","RESUME",revision(service,"sales"),"resume_newer",1);
            await(()->workers.active.get()==1&&backend.resumes.get()==1);
            String publicView=Model.JSON.writeValueAsString(service.list());
            assertFalse(publicView.contains("source-secret"));assertFalse(publicView.contains("target-secret"));assertFalse(publicView.contains("url-secret"));
        }
        assertEquals(0,workers.active.get());
        try(TaskService restarted=new TaskService(c,backend,workers)){await(()->workers.active.get()==1);assertEquals("RUNNING",restarted.view("sales").get("desired"));}
    }
    @Test void collisionChecksAndCorruptStateDoNotStartUnsafeTasks()throws Exception{
        PlatformConfig c=config();Backend backend=new Backend();Workers workers=new Workers();
        try(TaskService service=new TaskService(c,backend,workers)){
            Config a=task("one");service.create(Model.JSON.writeValueAsString(a),"create_one");
            Config b=task("two");b.source.serverId++ ;b.target.schema="one";
            assertEquals("DESTINATION_OVERLAP",assertThrows(TaskService.Conflict.class,()->service.create(Model.JSON.writeValueAsString(b),"create_two")).getMessage());
            b.target.schema="two";b.source.serverId=a.source.serverId;
            assertEquals("REPLICATION_SERVER_ID_COLLISION",assertThrows(TaskService.Conflict.class,()->service.create(Model.JSON.writeValueAsString(b),"create_two")).getMessage());
            assertThrows(TaskService.Conflict.class,()->new TaskService(c,backend,workers));
        }
        Files.write(directory.resolve("data/controller.json"),"invalid-json".getBytes(StandardCharsets.UTF_8));
        assertThrows(Exception.class,()->new TaskService(c,backend,workers));assertEquals(0,workers.starts.get());
    }
    @Test void workerCapacityQueuesTasksWithoutExceedingTheConfiguredBudget()throws Exception{
        PlatformConfig c=config();c.maxRunningTasks=1;Backend backend=new Backend();Workers workers=new Workers();
        try(TaskService service=new TaskService(c,backend,workers)){
            Config a=task("first"),b=task("second");b.source.serverId++;
            service.create(Model.JSON.writeValueAsString(a),"create_first");service.create(Model.JSON.writeValueAsString(b),"create_second");
            service.command("first","START",revision(service,"first"),"start_first",-1);
            await(()->workers.active.get()==1);
            service.command("second","START",revision(service,"second"),"start_second",-1);
            await(()->"QUEUED".equals(service.view("second").get("workerState")));assertEquals(1,workers.active.get());
            service.command("first","STOP",revision(service,"first"),"stop_first",-1);
            await(()->"RUNNING".equals(service.view("second").get("workerState")));assertEquals(1,workers.active.get());
        }
        assertEquals(0,workers.active.get());
    }
    @Test void fileEditsCannotChangeTheConfigurationOfAnAlreadyLoadedWorker()throws Exception{
        PlatformConfig c=config();c.autoStart=true;Backend backend=new Backend();Workers workers=new Workers();
        java.util.concurrent.atomic.AtomicReference<Config> launched=new java.util.concurrent.atomic.AtomicReference<>();
        try(TaskService service=new TaskService(c,backend,(path,log,heap)->{launched.set(Config.load(path));return workers.start(path,log,heap);})){ 
            service.create(Model.JSON.writeValueAsString(task("original")),"create_original");
            Thread.sleep(150);assertEquals(0,workers.starts.get(),"UI/API creation never inherits file autoStart");
            Files.write(Paths.get(c.tasksDirectory,"original.yml"),Model.JSON.writeValueAsBytes(task("edited")));
            service.command("original","START",revision(service,"original"),"start_original",-1);
            await(()->launched.get()!=null);
            assertEquals("original",launched.get().jobId);assertEquals("original",launched.get().target.schema);
        }
    }
    @Test void oneInvalidFileDoesNotPreventOtherIndependentConfigurations()throws Exception{
        PlatformConfig c=config();Files.createDirectories(Paths.get(c.tasksDirectory));
        Files.write(Paths.get(c.tasksDirectory,"valid.yml"),Model.JSON.writeValueAsBytes(task("valid")));
        Files.write(Paths.get(c.tasksDirectory,"invalid.yml"),"password: should-not-appear\nsource: [".getBytes(StandardCharsets.UTF_8));
        try(TaskService service=new TaskService(c,new Backend(),new Workers())){
            String view=Model.JSON.writeValueAsString(service.list());assertTrue(view.contains("valid"));assertTrue(view.contains("invalid.yml"));assertFalse(view.contains("should-not-appear"));
        }
    }
    @Test void httpRolesProtectEveryMutationBeforeItReachesTheTaskService()throws Exception{
        PlatformConfig c=config();c.authToken=null;String admin="administrator-token-private-at-least-24",viewer="read-only-token-private-at-least-24",operator="operations-token-private-at-least-24";c.users=Arrays.asList(user("admin",PlatformConfig.Role.ADMIN,admin),user("reader",PlatformConfig.Role.VIEWER,viewer),user("operator",PlatformConfig.Role.OPERATOR,operator));Backend backend=new Backend();Workers workers=new Workers();
        try(TaskService service=new TaskService(c,backend,workers);ConsoleServer server=new ConsoleServer(c,service)){
            server.start();String base="http://127.0.0.1:"+server.port();String json="{\"config\":"+Model.JSON.writeValueAsString(task("roles"))+"}";
            assertEquals(401,http(base,"/api/session","invalid-token",null,null).status);Response created=http(base,"/api/tasks",admin,json,"roles_create");assertEquals(201,created.status);assertEquals("admin",Model.JSON.readTree(created.text).path("createdBy").asText());
            Response session=http(base,"/api/session",viewer,null,null);assertEquals(200,session.status);JsonNode view=Model.JSON.readTree(session.text);assertEquals("reader",view.path("id").asText());assertEquals("VIEWER",view.path("role").asText());assertEquals(1,view.path("permissions").size());for(String secret:Arrays.asList(admin,viewer,operator))assertFalse(session.text.contains(secret));
            assertEquals(200,http(base,"/api/tasks",viewer,null,null).status);assertEquals(403,http(base,"/api/tasks",viewer,"not-json","viewer_create").status);assertEquals(403,http(base,"/api/tasks",operator,json,"operator_create").status);
            String start="{\"action\":\"START\",\"actorId\":\"admin\",\"actorRole\":\"ADMIN\",\"expectedRevision\":\""+revision(service,"roles")+"\"}";
            assertEquals(403,http(base,"/api/tasks/roles/actions",viewer,start,"viewer_start").status);assertEquals(0,workers.starts.get());assertEquals(202,http(base,"/api/tasks/roles/actions",operator,start,"operator_start").status);await(()->workers.starts.get()==1);
            JsonNode operation=Model.JSON.valueToTree(service.view("roles")).path("operations").get(0);assertEquals("operator",operation.path("actorId").asText());assertEquals("OPERATOR",operation.path("actorRole").asText());
            assertEquals(202,http(base,"/api/tasks/roles/actions",operator,start,"operator_start").status);assertEquals(409,http(base,"/api/tasks/roles/actions",admin,start,"operator_start").status);assertEquals(1,workers.starts.get());
            assertEquals(403,http(base,"/api/tasks/roles/actions",operator,"{\"action\":\"RESET\"}","operator_unknown").status);assertEquals(1,((List<?>)service.list().get("tasks")).size());
        }
        try(TaskService reloaded=new TaskService(c,backend,workers)){JsonNode state=Model.JSON.valueToTree(reloaded.view("roles"));assertEquals("admin",state.path("createdBy").asText());assertEquals("operator",state.path("operations").get(0).path("actorId").asText());}
    }
    @Test void credentialConfigurationRejectsAmbiguityAndRequiresAnAdministrator()throws Exception{
        PlatformConfig c=config();c.users.add(user("viewer",PlatformConfig.Role.VIEWER,TOKEN));assertEquals("PLATFORM_USER_TOKEN_COLLISION",assertThrows(IllegalArgumentException.class,c::validateAccess).getMessage());
        c.users.clear();c.authToken=null;c.users.add(user("reader",PlatformConfig.Role.VIEWER,TOKEN));assertEquals("PLATFORM_ADMIN_REQUIRED",assertThrows(IllegalArgumentException.class,c::validateAccess).getMessage());c.users.get(0).role=PlatformConfig.Role.ADMIN;c.validateAccess();c.users.add(user("reader",PlatformConfig.Role.OPERATOR,"different-operations-token-at-least-24"));assertEquals("PLATFORM_USERS_INVALID",assertThrows(IllegalArgumentException.class,c::validateAccess).getMessage());
    }
    @Test void httpRequiresAuthPreservesSecretsAndServesTheActualUi()throws Exception{
        PlatformConfig c=config();Backend backend=new Backend();Workers workers=new Workers();
        try(TaskService service=new TaskService(c,backend,workers);ConsoleServer server=new ConsoleServer(c,service)){
            server.start();String base="http://127.0.0.1:"+server.port();
            assertEquals(200,http(base,"/healthz",null,null,null).status);
            assertEquals(401,http(base,"/api/tasks",null,null,null).status);
            assertEquals("ADMIN",Model.JSON.readTree(http(base,"/api/session",TOKEN,null,null).text).path("role").asText());
            Response console=http(base,"/",null,null,null);
            assertEquals(200,console.status);
            assertTrue(console.text.contains("<title>RivoMesh</title>"));
            assertTrue(console.text.contains("/brand/wordmark.svg"));
            assertTrue(http(base,"/app.js",null,null,null).text.contains("expectedRevision"));
            assertTrue(http(base,"/",null,null,null).text.contains("/recovery.js"));
            Response recovery=http(base,"/recovery.js",null,null,null);assertEquals(200,recovery.status);assertTrue(recovery.text.contains("RivomeshRecovery"));
            String json="{\"config\":"+Model.JSON.writeValueAsString(task("web"))+"}";
            Response created=http(base,"/api/tasks",TOKEN,json,"web_create");assertEquals(201,created.status);assertFalse(created.text.contains("secret"));
            assertEquals(201,http(base,"/api/tasks",TOKEN,json,"web_create").status);
            assertEquals(400,http(base,"/api/tasks",TOKEN,"{\"yaml\":\"password: hidden-secret\\nsource: [\"}","bad_create").status);
            Response list=http(base,"/api/tasks",TOKEN,null,null);assertEquals(1,Model.JSON.readTree(list.text).get("tasks").size());
            // HttpURLConnection silently removes Origin, so send the wire request directly.
            try(Socket socket=new Socket("127.0.0.1",server.port())){
                socket.setSoTimeout(3000);socket.getOutputStream().write(("GET /api/tasks HTTP/1.1\r\nHost: 127.0.0.1:"+server.port()+"\r\nAuthorization: Bearer "+TOKEN+"\r\nOrigin: https://untrusted.example\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                assertTrue(new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.US_ASCII)).readLine().contains("403"));
            }
            assertEquals(404,http(base,"/../pom.xml",null,null,null).status);
        }
    }
    static PipelineConfig pipeline(String id){
        PipelineConfig p=new PipelineConfig();p.pipelineId=id;p.source.username="reader";p.source.password="source-secret";p.source.serverId=910099;p.databases=Arrays.asList("orders","inventory");
        PipelineConfig.Target t=new PipelineConfig.Target();t.id="pg";t.product=TargetProduct.POSTGRESQL;t.url="jdbc:postgresql://target/db";t.username="writer";t.password="target-secret";t.mappings.put("orders",id+"_orders");t.mappings.put("inventory",id+"_inventory");p.targets.add(t);return p;
    }
    @Test void v3ImportsKeepOneManagedJournalAcrossWorkerSnapshotsAndControllerRestart()throws Exception{
        PlatformConfig c=config();Workers workers=new Workers();PipelineConfig p=pipeline("fanout");p.source.password="${PATH}";
        com.fasterxml.jackson.databind.node.ObjectNode tree=Model.JSON.valueToTree(p);((com.fasterxml.jackson.databind.node.ObjectNode)tree.get("journal")).remove("path");String yaml=tree.toString();
        try(TaskService service=new TaskService(c,new Backend(),workers)){
            Map<String,Object> view=service.create(yaml,"create_fanout");assertEquals(3,view.get("formatVersion"));assertEquals("orders, inventory",view.get("sourceDatabase"));
            service.command("fanout","START",revision(service,"fanout"),"start_fanout",-1);await(()->workers.starts.get()==1);
            Path file=directory.resolve("data/worker-configs/fanout.yml");PipelineConfig actual=PipelineConfig.load(file);assertEquals(directory.resolve("data/journals/fanout"),actual.localDirectory());
            assertTrue(new String(Files.readAllBytes(file),StandardCharsets.UTF_8).contains("${PATH}"),"Do not expand a credential reference twice through a worker snapshot");assertFalse(Model.JSON.writeValueAsString(service.view("fanout")).contains("target-secret"));
        }
        try(TaskService service=new TaskService(c,new Backend(),workers)){await(()->workers.starts.get()==2);assertEquals(3,service.view("fanout").get("formatVersion"));assertTrue(((List<?>)service.list().get("invalidConfigs")).isEmpty());}
    }
    @Test void preparingSourceSnapshotsDoesNotDeclareUninitializedTargetsStreaming()throws Exception{
        PipelineConfig p=pipeline("snapshot-status");p.journal.path=directory.resolve("status-journal").toString();p.prepareLocalDirectory();try(io.github.rivomesh.cdc.JdbcJournal journal=new io.github.rivomesh.cdc.JdbcJournal(p.journalSettings())){
            journal.initialize(p.fingerprint(),"generation",p.subscribers());io.github.rivomesh.cdc.Journal.Lease lease=journal.claim("owner",30000);io.github.rivomesh.cdc.TableSchema table=new io.github.rivomesh.cdc.TableSchema();table.id="status-table";table.database="orders";table.name="items";io.github.rivomesh.cdc.TableSchema.Column column=new io.github.rivomesh.cdc.TableSchema.Column();column.name="value";column.type=io.github.rivomesh.cdc.TableSchema.Type.STRING;table.columns.add(column);journal.saveSchema(lease,table);io.github.rivomesh.api.SourcePosition baseline=new io.github.rivomesh.api.SourcePosition(1,"MYSQL",p.source.clusterId,Collections.singletonMap("gtidSet",""));io.github.rivomesh.cdc.Journal.SnapshotState snapshot=journal.beginSnapshot(lease,"source",table,baseline);journal.finishSnapshot(lease,snapshot);
            assertEquals("TARGET_SNAPSHOT",PipelineBackend.status(p).get("phase"));journal.acknowledgeSnapshot(lease,"pg",snapshot);assertEquals("STREAMING",PipelineBackend.status(p).get("phase"));
        }
    }
    @Test void kafkaSubscriptionsCannotShareOnePhysicalTopicBehindDifferentMappings()throws Exception{
        PipelineConfig a=pipeline("kafkaone"),b=pipeline("kafkatwo");b.source.serverId++;
        for(PipelineConfig p:Arrays.asList(a,b)){p.journal.mode="EXTERNAL";p.journal.url="jdbc:postgresql://journal/db";p.journal.username="journal";PipelineConfig.Target t=p.targets.get(0);t.product=TargetProduct.KAFKA;t.url="kafka://broker:9092";t.messaging=new PipelineConfig.Messaging();t.messaging.topic="shared.events";}
        try(TaskService service=new TaskService(config(),new Backend(),new Workers())){
            service.create(Model.JSON.writeValueAsString(a),"kafka_one");assertEquals("DESTINATION_OVERLAP",assertThrows(TaskService.Conflict.class,()->service.create(Model.JSON.writeValueAsString(b),"kafka_two")).getMessage());b.targets.get(0).messaging.topic="different.events";service.create(Model.JSON.writeValueAsString(b),"kafka_different");
        }
    }
    @Test void pulsarDefaultPortAndDifferentMappingsCannotHideASharedPhysicalTopic()throws Exception{
        PipelineConfig a=pipeline("pulsarone"),b=pipeline("pulsartwo");b.source.serverId++;
        for(PipelineConfig p:Arrays.asList(a,b)){p.journal.mode="EXTERNAL";p.journal.url="jdbc:postgresql://journal/db";p.journal.username="journal";PipelineConfig.Target t=p.targets.get(0);t.product=TargetProduct.PULSAR;t.url=p==a?"pulsar://broker:6650":"pulsar://BROKER";t.username=null;t.password=null;t.messaging=new PipelineConfig.Messaging();t.messaging.topic="persistent://public/default/shared";t.messaging.cluster="standalone";t.messaging.adminUrl="http://broker:8080";}
        try(TaskService service=new TaskService(config(),new Backend(),new Workers())){service.create(Model.JSON.writeValueAsString(a),"pulsar_one");assertEquals("DESTINATION_OVERLAP",assertThrows(TaskService.Conflict.class,()->service.create(Model.JSON.writeValueAsString(b),"pulsar_two")).getMessage());}
    }
    @Test void destinationViewsExposeConsumerSetupWithoutSecretsOrMutableConfiguration()throws Exception{
        PipelineConfig p=pipeline("consumer_setup");p.source.password="private-source-password";p.journal.mode="EXTERNAL";p.journal.url="jdbc:postgresql://journal/db";p.journal.username="journal";p.journal.password="private-journal-password";PipelineConfig.Target t=p.targets.get(0);t.product=TargetProduct.PULSAR;t.url="pulsar://private-broker:6650";t.username=null;t.password=null;t.messaging=new PipelineConfig.Messaging();t.messaging.topic="persistent://public/default/sales";t.messaging.cluster="standalone";t.messaging.adminUrl="http://private-admin:8080";t.messaging.token="private-pulsar-token";
        try(TaskService service=new TaskService(config(),new Backend(),new Workers())){
            Map<String,Object> task=service.create(Model.JSON.writeValueAsString(p),"consumer_view");String json=Model.JSON.writeValueAsString(task);assertFalse(json.contains("private-"));
            @SuppressWarnings("unchecked")Map<String,Object> destination=(Map<String,Object>)((List<?>)task.get("destinations")).get(0);assertEquals("PULSAR",destination.get("product"));assertEquals("AT_LEAST_ONCE_EVENTS",destination.get("consistency"));assertEquals(t.messaging.topic,destination.get("resource"));
            @SuppressWarnings("unchecked")Map<String,String> mappings=(Map<String,String>)destination.get("mappings");mappings.put("orders","modified-view");assertFalse(Model.JSON.writeValueAsString(service.view(p.pipelineId)).contains("modified-view"));
        }
    }
    @Test void v3LocalJournalImportsCannotSelectOtherPathsAndMappingsCannotOverlapV2()throws Exception{
        PlatformConfig c=config();try(TaskService service=new TaskService(c,new Backend(),new Workers())){
            PipelineConfig p=pipeline("unsafe");p.journal.path="../../other-application";
            String unsafe=Model.JSON.writeValueAsString(p);assertEquals("LOCAL_JOURNAL_MIGRATION_REQUIRED",assertThrows(TaskService.Conflict.class,()->service.create(unsafe,"create_unsafe")).getMessage());
            Config old=task("old");old.source.host="different-source";old.target.url="jdbc:postgresql://target/db";old.target.schema="overlap_orders";service.create(Model.JSON.writeValueAsString(old),"create_legacy");
            p=pipeline("overlap");p.journal.mode="EXTERNAL";p.journal.url="jdbc:postgresql://journal/db";p.journal.username="journal";String value=Model.JSON.writeValueAsString(p);
            assertEquals("DESTINATION_OVERLAP",assertThrows(TaskService.Conflict.class,()->service.create(value,"create_overlap")).getMessage());
        }
    }
    @Test void diagnosticBurstReturnsRetryAfterAndLeavesTaskStatusAvailable()throws Exception{
        PlatformConfig c=config();
        try(TaskService service=new TaskService(c,new Backend(),new Workers());
            ConsoleServer server=new ConsoleServer(c,service);
            HttpClient client=HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()){
            service.create(Model.JSON.writeValueAsString(task("diagnostic_limit")),"create_diag_limit");
            server.start();
            String base="http://127.0.0.1:"+server.port();
            HttpRequest request=HttpRequest.newBuilder(URI.create(base+"/api/tasks/diagnostic_limit/diagnostics"))
                    .header("Authorization","Bearer "+TOKEN).timeout(Duration.ofSeconds(15)).GET().build();
            List<CompletableFuture<HttpResponse<String>>> reads=new ArrayList<>();
            for(int i=0;i<64;i++)reads.add(client.sendAsync(request,HttpResponse.BodyHandlers.ofString()));
            CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).get(20,TimeUnit.SECONDS);
            List<HttpResponse<String>> results=reads.stream().map(CompletableFuture::join).toList();
            assertTrue(results.stream().anyMatch(response->response.statusCode()==200));
            HttpResponse<String> limited=results.stream().filter(response->response.statusCode()==429)
                    .findFirst().orElseThrow();
            assertEquals("1",limited.headers().firstValue("Retry-After").orElseThrow());
            assertEquals("DIAGNOSTICS_RATE_LIMITED",Model.JSON.readTree(limited.body()).path("code").asText());
            assertEquals(200,http(base,"/api/tasks/diagnostic_limit",TOKEN,null,null).status);
        }
    }

    static final class Response{int status;String text;Response(int status,String text){this.status=status;this.text=text;}}
    static Response http(String base,String path,String token,String body,String request)throws Exception{
        HttpURLConnection c=(HttpURLConnection)new URL(base+path).openConnection();c.setConnectTimeout(3000);c.setReadTimeout(5000);
        if(token!=null)c.setRequestProperty("Authorization","Bearer "+token);
        if(body!=null){c.setRequestMethod("POST");c.setDoOutput(true);c.setRequestProperty("Content-Type","application/json");c.setRequestProperty("X-Request-ID",request);try(OutputStream out=c.getOutputStream()){out.write(body.getBytes(StandardCharsets.UTF_8));}}
        int status=c.getResponseCode();try(InputStream in=status>=400?c.getErrorStream():c.getInputStream();ByteArrayOutputStream out=new ByteArrayOutputStream()){
            if(in!=null){byte[] b=new byte[4096];int n;while((n=in.read(b))!=-1)out.write(b,0,n);}return new Response(status,new String(out.toByteArray(),StandardCharsets.UTF_8));
        }finally{c.disconnect();}
    }
}
