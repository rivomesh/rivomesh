package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ControlStoreTest {
    @TempDir Path directory;
    static ClusterConfig cluster(String database,String node){ClusterConfig c=new ClusterConfig();c.id="test-cluster";c.nodeId=node;c.driver="org.h2.Driver";c.url="jdbc:h2:mem:"+database+";DB_CLOSE_DELAY=-1";c.username="sa";c.password="";c.heartbeatMillis=100;c.leaseMillis=5000;return c;}
    ClusterConfig cluster(String node){return cluster(directory.getFileName().toString(),node);}
    static ControlStore.Node node(String id){ControlStore.Node n=new ControlStore.Node();n.id=id;n.incarnation=UUID.randomUUID().toString();n.version="test";n.jvm=System.getProperty("java.version");n.architecture=System.getProperty("os.arch");n.processors=2;n.heapBytes=128*1048576L;n.taskCapacity=20;n.runningCapacity=2;return n;}
    static ControlStore.Task task(String id){ControlStore.Task task=new ControlStore.Task();task.id=id;task.revision=UUID.randomUUID().toString();task.desired="STOPPED";task.workerState="STOPPED";return task;}
    @SuppressWarnings("unchecked") static List<Map<String,Object>> nodes(ControlStore store)throws Exception{return(List<Map<String,Object>>)store.page("",50).get("nodes");}
    static void expire(ClusterConfig config,String id)throws Exception{try(Connection c=DriverManager.getConnection(config.url,config.username,config.password);PreparedStatement p=c.prepareStatement("UPDATE \"rivomesh_control\".\"rmc_node\" SET lease_until=0 WHERE cluster_id=? AND node_id=?")){p.setString(1,config.id);p.setString(2,id);assertEquals(1,p.executeUpdate());}}
    @Test void separateStoresReadTheSameBoundedRedactedRegistry()throws Exception{
        ClusterConfig a=cluster("a"),b=cluster("b");
        try(ControlStore first=new JdbcControlStore(a);ControlStore second=new JdbcControlStore(b)){
            ControlStore.Node nodeA=node("a"),nodeB=node("b");ControlStore.Lease lease=first.register(nodeA,Arrays.asList(task("orders")));second.register(nodeB,Arrays.asList(task("inventory")));
            assertEquals(2,nodes(first).size());assertEquals(Model.JSON.writeValueAsString(nodes(first)),Model.JSON.writeValueAsString(nodes(second)));
            assertEquals(lease.epoch,((Number)nodes(first).get(0).get("epoch")).longValue());assertEquals("HEALTHY",nodes(first).get(0).get("state"));
            String json=Model.JSON.writeValueAsString(first.page("",20));assertFalse(json.contains(a.url));assertFalse(json.contains("incarnation"));assertFalse(json.contains("password"));assertFalse(json.contains("username"));assertTrue(json.contains("orders"));assertEquals("LOCAL",first.page("",20).get("taskControlScope"));
        }
    }
    @Test void activeCollisionExpiredReplacementAndOldHeartbeatShutdownAreFenced()throws Exception{
        ClusterConfig config=cluster("same");try(ControlStore store=new JdbcControlStore(config)){
            ControlStore.Node original=node("same"),replacement=node("same");ControlStore.Lease old=store.register(original,Collections.emptyList());
            assertEquals(old.epoch,store.register(original,Collections.emptyList()).epoch);assertEquals("NODE_ID_IN_USE",assertThrows(TaskService.Conflict.class,()->store.register(replacement,Collections.emptyList())).getMessage());
            expire(config,"same");assertEquals("STALE",nodes(store).get(0).get("state"));assertThrows(ControlStore.Expired.class,()->store.heartbeat(old,original,Collections.emptyList()));
            ControlStore.Lease next=store.register(replacement,Collections.emptyList());assertEquals(old.epoch+1,next.epoch);assertThrows(ControlStore.Fenced.class,()->store.heartbeat(old,original,Collections.emptyList()));assertThrows(ControlStore.Fenced.class,()->store.stopped(old));
            store.stopped(next);assertEquals("STOPPED",nodes(store).get(0).get("state"));assertEquals(next.epoch+1,store.register(node("same"),Collections.emptyList()).epoch);
        }
    }
    @Test void concurrentClaimOfOneNodeHasOnlyOneWinner()throws Exception{
        ClusterConfig config=cluster("same");try(ControlStore a=new JdbcControlStore(config);ControlStore b=new JdbcControlStore(config)){
            ExecutorService pool=Executors.newFixedThreadPool(2);CountDownLatch start=new CountDownLatch(1);
            try{
                List<Future<Boolean>> results=new ArrayList<>();for(ControlStore store:Arrays.asList(a,b))results.add(pool.submit(()->{start.await();try{store.register(node("same"),Collections.emptyList());return true;}catch(TaskService.Conflict expected){assertEquals("NODE_ID_IN_USE",expected.getMessage());return false;}}));
                start.countDown();int winners=0;for(Future<Boolean> result:results)if(result.get(5,TimeUnit.SECONDS))winners++;assertEquals(1,winners);assertEquals(1,nodes(a).size());
            }finally{pool.shutdownNow();}
        }
    }
    @Test void capacityProtocolAndClusterIsolationAreEnforced()throws Exception{
        ClusterConfig a=cluster("a"),b=cluster("b");a.maximumNodes=b.maximumNodes=1;
        try(ControlStore first=new JdbcControlStore(a);ControlStore second=new JdbcControlStore(b)){
            first.register(node("a"),Collections.emptyList());assertEquals("NODE_CAPACITY_REACHED",assertThrows(TaskService.Conflict.class,()->second.register(node("b"),Collections.emptyList())).getMessage());
            b.id="another-cluster";try(ControlStore isolated=new JdbcControlStore(b)){isolated.register(node("b"),Collections.emptyList());assertEquals("b",nodes(isolated).get(0).get("id"));assertEquals("a",nodes(first).get(0).get("id"));}
            b.id=a.id;b.leaseMillis=2000;assertEquals("CONTROL_STORE_PROTOCOL_MISMATCH",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(b)).getMessage());
        }
    }
    @Test void namespaceModeIsCreatedDirectlyAndNeverAutomaticallyMigratedEvenAfterStop()throws Exception{
        ClusterConfig original=cluster("a");try(ControlStore store=new JdbcControlStore(original)){
            ControlStore.Lease lease=store.register(node("a"),Collections.emptyList());store.stopped(lease);
            ClusterConfig changed=cluster("b");changed.sharedAccess=true;
            assertEquals("CONTROL_STORE_PROTOCOL_MISMATCH",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(changed)).getMessage());
            try(Connection c=DriverManager.getConnection(original.url,original.username,original.password);Statement query=c.createStatement();ResultSet r=query.executeQuery("SELECT format_version FROM \"rivomesh_control\".\"rmc_cluster\"")){assertTrue(r.next());assertEquals(1,r.getInt(1));}
        }
        ClusterConfig current=cluster("direct");current.namespace="current_mode";current.sharedAccess=true;
        try(ControlStore store=new JdbcControlStore(current);Connection c=DriverManager.getConnection(current.url,current.username,current.password);Statement query=c.createStatement();ResultSet r=query.executeQuery("SELECT format_version FROM \"current_mode\".\"rmc_cluster\"")){assertTrue(r.next());assertEquals(2,r.getInt(1));}
    }
    @Test void nodeAndObservationPublishRollBackTogether()throws Exception{
        ClusterConfig config=cluster("node");try(ControlStore store=new JdbcControlStore(config)){
            ControlStore.Node node=node("node");ControlStore.Lease lease=store.register(node,Arrays.asList(task("original")));
            try(Connection c=DriverManager.getConnection(config.url,config.username,config.password);Statement s=c.createStatement()){s.execute("ALTER TABLE \"rivomesh_control\".\"rmc_node_task\" ADD CONSTRAINT reject_task CHECK (task_id <> 'rejected')");}
            node.version="not-committed";assertThrows(ControlStore.Unavailable.class,()->store.heartbeat(lease,node,Arrays.asList(task("rejected"))));
            Map<String,Object> saved=nodes(store).get(0);assertEquals("test",saved.get("version"));assertEquals("original",((ControlStore.Task)((List<?>)saved.get("tasks")).get(0)).id);
        }
    }
    @Test void keysetPaginationAndCursorValidationAreStable()throws Exception{
        for(String id:Arrays.asList("a","b","c")){ClusterConfig config=cluster(id);try(ControlStore store=new JdbcControlStore(config)){store.register(node(id),Collections.emptyList());}}
        try(ControlStore store=new JdbcControlStore(cluster("a"))){Map<String,Object> first=store.page("",2);assertEquals("b",first.get("next"));Map<String,Object> last=store.page((String)first.get("next"),2);assertNull(last.get("next"));assertEquals("c",((Map<?,?>)((List<?>)last.get("nodes")).get(0)).get("id"));assertThrows(IllegalArgumentException.class,()->store.page("../invalid",20));assertThrows(IllegalArgumentException.class,()->store.page("",51));}
    }
    PlatformConfig config(String name){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=PlatformTest.TOKEN;c.pollMillis=100;c.tasksDirectory=directory.resolve(name+"/tasks").toString();c.dataDirectory=directory.resolve(name+"/data").toString();c.cluster=cluster(name);c.users.add(PlatformTest.user("viewer",PlatformConfig.Role.VIEWER,"node-viewer-test-token-at-least-24"));return c;}
    @Test void twoHttpEndpointsShareNodeObservationsWithoutSharingLocalDirectories()throws Exception{
        PlatformConfig a=config("a"),b=config("b");
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());TaskService second=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();first.create(Model.JSON.writeValueAsString(PlatformTest.task("orders")),"node_create_orders");
            String baseOne="http://127.0.0.1:"+one.port(),baseTwo="http://127.0.0.1:"+two.port();
            PlatformTest.await(()->PlatformTest.http(baseTwo,"/api/nodes",PlatformTest.TOKEN,null,null).text.contains("orders"));
            for(String base:Arrays.asList(baseOne,baseTwo)){
                PlatformTest.Response response=PlatformTest.http(base,"/api/nodes",PlatformTest.TOKEN,null,null);assertEquals(200,response.status);assertEquals(2,Model.JSON.readTree(response.text).path("nodes").size());assertTrue(response.text.contains("orders"));assertFalse(response.text.contains(a.dataDirectory));assertFalse(response.text.contains(a.cluster.url));
                assertEquals(403,PlatformTest.http(base,"/api/nodes","node-viewer-test-token-at-least-24",null,null).status);assertEquals(401,PlatformTest.http(base,"/api/nodes",null,null,null).status);assertEquals(400,PlatformTest.http(base,"/api/nodes?limit=51",PlatformTest.TOKEN,null,null).status);
            }
        }
        try(ControlStore store=new JdbcControlStore(a.cluster)){for(Map<String,Object> node:nodes(store))assertEquals("STOPPED",node.get("state"));}
    }
    @Test void standaloneNodeIdSurvivesRestartWithoutClaimingASharedCluster()throws Exception{
        PlatformConfig config=config("local");config.cluster=null;String original;
        try(TaskService service=new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,service)){server.start();original=Model.JSON.readTree(PlatformTest.http("http://127.0.0.1:"+server.port(),"/api/nodes",PlatformTest.TOKEN,null,null).text).path("localNodeId").asText();}
        try(TaskService service=new TaskService(config,new PlatformTest.Backend(),new PlatformTest.Workers());ConsoleServer server=new ConsoleServer(config,service)){server.start();com.fasterxml.jackson.databind.JsonNode page=Model.JSON.readTree(PlatformTest.http("http://127.0.0.1:"+server.port(),"/api/nodes",PlatformTest.TOKEN,null,null).text);assertEquals(original,page.path("localNodeId").asText());assertEquals("STANDALONE",page.path("mode").asText());assertTrue(page.path("clusterId").isNull());}
    }
}
