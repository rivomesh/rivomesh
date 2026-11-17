package io.github.rivomesh;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.platform.*;
import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.v3.integration",matches="true")
class PlatformV3IntegrationTest {
    @TempDir Path folder;
    PlatformConfig platform(){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken="v3-integration-token-long-enough";c.pollMillis=100;c.tasksDirectory=folder.resolve("tasks").toString();c.dataDirectory=folder.resolve("data").toString();return c;}
    JsonNode http(ConsoleServer server,PlatformConfig p,String method,String path,Object body)throws Exception{
        HttpURLConnection c=(HttpURLConnection)new URL("http://127.0.0.1:"+server.port()+path).openConnection();c.setConnectTimeout(3000);c.setReadTimeout(10000);c.setRequestMethod(method);c.setRequestProperty("Authorization","Bearer "+p.authToken);
        if(body!=null){c.setRequestProperty("Content-Type","application/json");c.setRequestProperty("X-Request-ID","test_"+UUID.randomUUID().toString().replace("-",""));c.setDoOutput(true);try(OutputStream out=c.getOutputStream()){out.write(Model.JSON.writeValueAsBytes(body));}}
        try{int code=c.getResponseCode();try(InputStream in=code>=400?c.getErrorStream():c.getInputStream()){JsonNode result=Model.JSON.readTree(in);assertTrue(code>=200&&code<300,"HTTP "+code+": "+result);return result;}}finally{c.disconnect();}
    }
    void action(ConsoleServer server,PlatformConfig p,TaskService tasks,String id,String action,long generation,String subscription)throws Exception{
        Map<String,Object> body=new LinkedHashMap<>();body.put("action",action);body.put("expectedRevision",tasks.view(id).get("revision"));body.put("errorCount",generation);if(subscription!=null)body.put("subscriptionId",subscription);http(server,p,"POST","/api/tasks/"+id+"/actions",body);
    }
    JsonNode view(ConsoleServer server,PlatformConfig p,String id)throws Exception{return http(server,p,"GET","/api/tasks/"+id,null);}
    int postgresCount(PipelineConfig p,String where)throws Exception{
        PipelineConfig.Target t=p.targets.get(1);try(Connection c=JdbcConnections.open("org.postgresql.Driver",t.url,t.username,t.password);Statement s=c.createStatement();ResultSet rs=s.executeQuery("SELECT COUNT(*) FROM "+Sql.pg(t.mappings.values().iterator().next())+".items WHERE "+where)){rs.next();return rs.getInt(1);}
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void consoleControlsV3DeliveryErrorsSharedPauseAndControllerRestart(boolean local)throws Exception{
        Config legacy=ClusterHarness.config();legacy.target.type="GAUSS";legacy.target.driver="io.github.rivomesh.gaussdb.jdbc.Driver";legacy.target.url="jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10";
        PipelineConfig p=PipelineClusterIntegrationTest.pipeline(legacy);if(local)p.journal.mode="LOCAL";PlatformConfig platform=platform();
        try(ClusterHarness h=new ClusterHarness(legacy)){
            try(TaskService tasks=new TaskService(platform);ConsoleServer server=new ConsoleServer(platform,tasks)){
                server.start();ObjectNode tree=Model.JSON.valueToTree(p);if(local)((ObjectNode)tree.get("journal")).remove("path");JsonNode created=http(server,platform,"POST","/api/tasks",Collections.singletonMap("config",tree));assertEquals(3,created.path("formatVersion").asInt());assertEquals("STOPPED",created.path("desired").asText());assertFalse(created.toString().contains(ClusterHarness.PASSWORD));
                action(server,platform,tasks,p.pipelineId,"START",-1,null);h.await(()->h.count("TRUE")==40&&postgresCount(p,"TRUE")==40,90);
                try(Connection c=h.store.connect()){Sql.execute(c,"ALTER TABLE "+h.targetTable()+" ADD CONSTRAINT positive_amount CHECK(amount>=0)");}
                ClusterHarness.sql("mysql","INSERT INTO "+h.sourceTable()+" VALUES (41,-1,'bad'),(42,100,'after-error')");
                h.await(()->postgresCount(p,"id>=41")==2&&view(server,platform,p.pipelineId).path("status").path("subscriptions").findValuesAsText("state").contains("PAUSED"),40);assertEquals(0,h.count("id>=41"));
                JsonNode sub=view(server,platform,p.pipelineId).path("status").path("subscriptions").findParents("errorGeneration").stream().filter(node->node.path("id").asText().equals("gauss")).findFirst().get();long generation=sub.path("errorGeneration").asLong();
                action(server,platform,tasks,p.pipelineId,"RESUME_TARGET",generation-1,"gauss");h.await(()->tasks.view(p.pipelineId).get("operations").toString().contains("ERROR_GENERATION_CHANGED"),20);
                try(Connection c=h.store.connect()){Sql.execute(c,"ALTER TABLE "+h.targetTable()+" DROP CONSTRAINT positive_amount");}
                action(server,platform,tasks,p.pipelineId,"RESUME_TARGET",generation,"gauss");h.await(()->h.count("id>=41")==2,40);
                action(server,platform,tasks,p.pipelineId,"PAUSE",-1,null);h.await(()->"PAUSED".equals(view(server,platform,p.pipelineId).path("status").path("run_state").asText())&&tasks.view(p.pipelineId).get("operations").toString().contains("SHARED_PAUSED"),40);
                ClusterHarness.sql("mysql","INSERT INTO "+h.sourceTable()+" VALUES (43,100,'while-paused')");assertEquals(0,h.count("id=43"));assertFalse(view(server,platform,p.pipelineId).toString().contains(ClusterHarness.PASSWORD));
            }
            try(TaskService tasks=new TaskService(platform);ConsoleServer server=new ConsoleServer(platform,tasks)){
                server.start();h.await(()->"PAUSED".equals(view(server,platform,p.pipelineId).path("status").path("run_state").asText()),30);assertEquals("PAUSED",view(server,platform,p.pipelineId).path("desired").asText());assertEquals(0,h.count("id=43"));
                long generation=view(server,platform,p.pipelineId).path("status").path("error_count").asLong();action(server,platform,tasks,p.pipelineId,"RESUME",generation,null);h.await(()->h.count("id=43")==1&&postgresCount(p,"id=43")==1,45);
                h.await(()->view(server,platform,p.pipelineId).path("status").path("tables").size()==1,20);assertEquals(40,view(server,platform,p.pipelineId).path("status").path("tables").get(0).path("copiedRows").asLong());
            }
        }catch(AssertionError failure){
            StringBuilder details=new StringBuilder();Path data=folder.resolve("data");
            if(Files.exists(data.resolve("controller.json")))details.append(new String(Files.readAllBytes(data.resolve("controller.json")),StandardCharsets.UTF_8));
            if(Files.isDirectory(data.resolve("logs")))try(java.util.stream.Stream<Path> paths=Files.list(data.resolve("logs"))){for(Path file:(Iterable<Path>)paths::iterator){List<String> lines=Files.readAllLines(file);details.append('\n').append(file.getFileName()).append('\n').append(String.join("\n",lines.subList(Math.max(0,lines.size()-100),lines.size())));}}
            throw new AssertionError(details.toString(),failure);
        }finally{PipelineClusterIntegrationTest.cleanPostgres(p);}
    }
}
