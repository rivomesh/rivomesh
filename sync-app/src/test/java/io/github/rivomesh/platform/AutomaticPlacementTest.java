package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class AutomaticPlacementTest {
    private final String directoryB=UUID.randomUUID().toString();
    private AgentDirectoryTest owner;
    private GlobalTaskStoreTest fixture;
    private JdbcControlStore registryB;
    private AgentDirectory agentsB;
    private ControlStore.Lease leaseB;
    private OwnerQueries queriesA,queriesB;
    private GlobalTaskPlane entryA,entryB;

    @BeforeEach void initialize()throws Exception{
        owner=new AgentDirectoryTest();owner.initialize();fixture=owner.fixture;
        var configB=Model.JSON.readValue(Model.JSON.writeValueAsBytes(fixture.config.cluster),ClusterConfig.class);
        configB.nodeId="b";registryB=new JdbcControlStore(configB);
        leaseB=registryB.register(ControlStoreTest.node("b"),List.of());
        owner.sql("UPDATE rivomesh_control.rmc_node SET lease_until=9223372036854775807");
        owner.a.register(owner.lease,fixture.directory);
        agentsB=new AgentDirectory(configB,fixture.access,fixture.b);agentsB.register(leaseB,directoryB);
        queriesA=new OwnerQueries(fixture.config.cluster,fixture.a,owner.a,10,30000);
        queriesB=new OwnerQueries(configB,fixture.b,agentsB,10,30000);
        entryA=new GlobalTaskPlane(fixture.a,new DurableCommands(fixture.config.cluster,fixture.access,fixture.a,10),owner.a,
                new GlobalTaskViews(fixture.config.cluster,fixture.a,owner.a,300000),queriesA,"a");
        entryB=new GlobalTaskPlane(fixture.b,new DurableCommands(configB,fixture.access,fixture.b,10),agentsB,
                new GlobalTaskViews(configB,fixture.b,agentsB,300000),queriesB,"b");
    }
    @AfterEach void close(){if(registryB!=null)registryB.close();if(owner!=null)owner.registry.close();}

    private TaskRequest automatic(){
        var input=TaskRequest.of(TaskRequest.Kind.CREATE,"orders");
        input.placement="AUTO";input.requestId="automatic_placement_01";
        input.yaml="""
                formatVersion: 3
                pipelineId: orders
                source:
                  type: MYSQL
                journal:
                  mode: EXTERNAL
                  product: POSTGRESQL
                  url: jdbc:postgresql://unreachable.test/journal
                  password: synthetic-private-placement-password
                targets:
                - product: MYSQL
                - product: POSTGRESQL
                """;
        return input;
    }

    @Test void noReachableCandidateCreatesNoTaskAndSecondCandidateCanBeAcceptedAndReplayed()throws Exception{
        var request=automatic();
        ExecutorService pool=Executors.newSingleThreadExecutor();
        try{
            var refused=pool.submit(()->entryA.invoke(request,fixture.admin));
            serve(queriesA,owner.lease,fixture.directory,false);
            serve(queriesB,leaseB,directoryB,false);
            var failure=assertThrows(ExecutionException.class,()->refused.get(10,TimeUnit.SECONDS));
            assertEquals("AGENT_PLACEMENT_UNAVAILABLE",failure.getCause().getMessage());
            assertNoCreation();

            // Retry the same creation after connectivity changes. The first probe still fails.
            var accepted=pool.submit(()->entryB.invoke(request,fixture.admin));
            serve(queriesA,owner.lease,fixture.directory,false);
            assertNoCreation();
            serve(queriesB,leaseB,directoryB,true);
            Map<String,Object> created=accepted.get(10,TimeUnit.SECONDS);
            assertEquals("b",created.get("ownerNodeId"));
            assertEquals("UNKNOWN",created.get("workerState"));
            assertFalse(Model.JSON.writeValueAsString(created).contains("synthetic-private-placement-password"));
            assertEquals(1,count("task_authority"));assertEquals(1,count("task_version"));assertEquals(1,count("command"));

            // Committed request replay does not depend on another healthy candidate or transport.
            owner.registry.stopped(owner.lease);registryB.stopped(leaseB);
            assertEquals(created.get("revision"),entryA.invoke(request,fixture.admin).get("revision"));
            assertEquals(1,count("command"));
        }finally{pool.shutdownNow();}
    }

    @Test void aSuccessfulProofCannotAssignAReplacementAgentUnderTheSameNodeName()throws Exception{
        var request=automatic();
        var review=queriesA.submitPlacement(request,fixture.admin,Set.of());
        serve(queriesA,owner.lease,fixture.directory,true);
        assertTrue(queriesA.awaitPlacement(review,request,fixture.admin).eligible());
        owner.registry.stopped(owner.lease);
        ControlStore.Lease next=owner.registry.register(ControlStoreTest.node("a"),List.of());
        owner.a.register(next,fixture.directory);
        assertEquals("AGENT_PLACEMENT_CHANGED",assertThrows(TaskService.Conflict.class,()->fixture.a.allocateQualified(
                request.taskId,request.yaml,request.requestId,fixture.admin,
                (c,actor)->queriesA.qualifiedPlacement(c,review.query(),request,actor),null)).getMessage());
        assertNoCreation();
    }

    @Test void theInstalledWorkerLauncherLoadsAndTheJournalProbeKeepsTlsWithBoundedTimeouts()throws Exception{
        AutomaticPlacement.requireWorker(Map.of());
        Properties values=org.postgresql.Driver.parseURL(AutomaticPlacement.journalProbeUrl(
                "jdbc:postgresql://localhost/test?sslmode=verify-full&socketTimeout=90&loginTimeout=90"),new Properties());
        assertEquals("verify-full",values.getProperty("sslmode"));
        assertEquals("3",values.getProperty("connectTimeout"));
        assertEquals("4",values.getProperty("loginTimeout"));
        assertEquals("4",values.getProperty("socketTimeout"));
    }

    private void serve(OwnerQueries queries,ControlStore.Lease lease,String directory,boolean eligible)throws Exception{
        AtomicReference<String> query=new AtomicReference<>();
        PlatformTest.await(()->{
            var pending=queries.pending(lease,directory,2);
            if(pending.isEmpty())return false;
            query.set(pending.getFirst());return true;
        });
        queries.serve(query.get(),lease,directory,(request,actor)->{
            assertTrue(request.placementProbe());
            assertEquals(lease.nodeId,request.ownerNodeId);
            assertEquals(fixture.admin.id,actor.id);
            if(!eligible)throw new TaskService.Conflict("PLACEMENT_JOURNAL_UNAVAILABLE");
            return AutomaticPlacement.receipt(request);
        });
    }

    private void assertNoCreation()throws Exception{
        assertEquals(0,count("task_authority"));assertEquals(0,count("task_version"));assertEquals(0,count("command"));
        try(var conn=fixture.connection();var statement=conn.createStatement();
            var rows=statement.executeQuery("SELECT envelope FROM rivomesh_control.rmc_owner_query")){
            while(rows.next())assertFalse(rows.getString(1).contains("synthetic-private-placement-password"));
        }
    }
    private int count(String table)throws Exception{
        try(var conn=fixture.connection();var statement=conn.createStatement();
            var rows=statement.executeQuery("SELECT COUNT(*) FROM rivomesh_control.rmc_"+table)){
            assertTrue(rows.next());return rows.getInt(1);
        }
    }
}
