package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class GlobalResourcesTest {
    @TempDir Path directory;
    GlobalTaskStoreTest fixture;GlobalResources resources;
    @BeforeEach void initialize()throws Exception{fixture=new GlobalTaskStoreTest();fixture.initialize();resources=new GlobalResources(fixture.config.cluster,fixture.a,fixture.access,100);}
    Config task(String id,long serverId){Config task=PlatformTest.task(id);task.source.serverId=serverId;return task;}
    void reserve(Config config)throws Exception{
        String yaml=Model.JSON.writeValueAsString(config);GlobalTaskStore.Record record;try{record=fixture.a.get(config.jobId,fixture.admin);}catch(NoSuchElementException missing){record=fixture.a.create(config.jobId,"a",fixture.directory,yaml,"resource_create_"+config.jobId,fixture.admin);}
        TaskDefinition definition=TaskDefinition.parse(yaml,directory,directory);GlobalTaskStore.Record selected=record;fixture.a.service((c,accounts)->{resources.reserve(c,selected,definition);return null;});
    }
    void sql(String sql)throws Exception{try(Connection c=fixture.connection();Statement s=c.createStatement()){s.execute(sql);}}
    @Test void businessAndMetadataConflictsAreGlobalWhileMetadataSharingIsAllowed()throws Exception{
        Config one=task("one",1);one.target.metadataSchema="shared";reserve(one);Config two=task("two",2);two.target.metadataSchema="shared";reserve(two);
        Config third=task("third",3);third.target.schema="shared";assertEquals("DESTINATION_OVERLAP",assertThrows(TaskService.Conflict.class,()->reserve(third)).getMessage());Config fourth=task("fourth",4);fourth.target.metadataSchema="one";assertEquals("DESTINATION_OVERLAP",assertThrows(TaskService.Conflict.class,()->reserve(fourth)).getMessage());
    }
    @Test void serverIdCollisionsUseEndpointsAndExplicitLogicalSourceIdentity()throws Exception{
        Config one=task("one",1);reserve(one);Config two=task("two",1);assertEquals("REPLICATION_SERVER_ID_COLLISION",assertThrows(TaskService.Conflict.class,()->reserve(two)).getMessage());two.source.host="independent-source";reserve(two);
        Config third=task("third",3);third.source.clusterId="explicit-cluster";reserve(third);Config fourth=task("fourth",3);fourth.source.clusterId="explicit-cluster";fourth.source.host="another-failover-member";assertEquals("REPLICATION_SERVER_ID_COLLISION",assertThrows(TaskService.Conflict.class,()->reserve(fourth)).getMessage());
    }
    @Test void normalizedAddressesAndCaseConservativelyConflictWithoutStoringSecrets()throws Exception{
        Config one=task("one",1);one.target.url="jdbc:postgresql://TARGET/business?password=private-a";one.target.schema="Sales";reserve(one);Config two=task("two",2);two.target.url="jdbc:postgresql://target:5432/business?password=private-b";two.target.schema="sales";assertEquals("DESTINATION_OVERLAP",assertThrows(TaskService.Conflict.class,()->reserve(two)).getMessage());
        try(Connection c=fixture.connection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT resource_key,record_hash FROM rivomesh_control.rmc_task_resource")){while(r.next()){assertFalse(r.getString(1).contains("private"));assertEquals(43,r.getString(1).length());assertEquals(43,r.getString(2).length());}}
    }
    @Test void repeatedReservationsKeepPriorNamespacesAndLostRowsCannotBeReplaced()throws Exception{
        Config one=task("one",1);reserve(one);reserve(one);one.target.schema="new-schema";reserve(one);Config two=task("two",2);two.target.schema="one";assertEquals("DESTINATION_OVERLAP",assertThrows(TaskService.Conflict.class,()->reserve(two)).getMessage());
        sql("DELETE FROM rivomesh_control.rmc_task_resource");assertEquals("RESOURCE_CATALOG_INCOMPLETE",assertThrows(TaskService.Conflict.class,()->reserve(one)).getMessage());sql("DELETE FROM rivomesh_control.rmc_resource_root");assertEquals("RESOURCE_STATE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new GlobalResources(fixture.config.cluster,fixture.a,fixture.access,100)).getMessage());
    }
    @Test void tamperedKindsCannotChangeAnExistingReservation()throws Exception{
        Config one=task("one",1);reserve(one);sql("UPDATE rivomesh_control.rmc_task_resource SET resource_kind=2 WHERE resource_kind=1");Config two=task("two",2);two.target.schema="one";assertEquals("RESOURCE_RECORD_INVALID",assertThrows(TaskService.Conflict.class,()->reserve(two)).getMessage());
    }
    @Test void capacityRefusalDoesNotLeavePartialResourceClaims()throws Exception{
        fixture=new GlobalTaskStoreTest();fixture.initialize();resources=new GlobalResources(fixture.config.cluster,fixture.a,fixture.access,2);assertEquals("RESOURCE_CAPACITY",assertThrows(TaskService.Conflict.class,()->reserve(task("one",1))).getMessage());
        try(Connection c=fixture.connection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM rivomesh_control.rmc_task_resource")){r.next();assertEquals(0,r.getInt(1));}
    }
    @Test void concurrentOwnersCannotReserveOneBusinessNamespace()throws Exception{
        Config one=task("one",1),two=task("two",2);two.target.schema="one";ExecutorService pool=Executors.newFixedThreadPool(2);CountDownLatch start=new CountDownLatch(1);
        try{List<Future<String>> results=new ArrayList<>();for(Config task:Arrays.asList(one,two))results.add(pool.submit(()->{start.await();try{reserve(task);return "OK";}catch(TaskService.Conflict conflict){return conflict.getMessage();}}));start.countDown();List<String> states=Arrays.asList(results.get(0).get(20,TimeUnit.SECONDS),results.get(1).get(20,TimeUnit.SECONDS));assertTrue(states.contains("OK"));assertTrue(states.contains("DESTINATION_OVERLAP"));}finally{pool.shutdownNow();}
    }
}
