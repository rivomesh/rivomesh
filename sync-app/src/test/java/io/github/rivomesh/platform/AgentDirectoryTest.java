package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class AgentDirectoryTest {
    GlobalTaskStoreTest fixture;JdbcControlStore registry;AgentDirectory a,b;ControlStore.Node node;ControlStore.Lease lease;
    @BeforeEach void initialize()throws Exception{
        fixture=new GlobalTaskStoreTest();fixture.initialize();registry=new JdbcControlStore(fixture.config.cluster);node=ControlStoreTest.node("a");lease=registry.register(node,Collections.emptyList());
        a=new AgentDirectory(fixture.config.cluster,fixture.access,fixture.a);b=new AgentDirectory(fixture.config.cluster,fixture.access,fixture.b);
    }
    void sql(String sql)throws Exception{try(Connection c=fixture.connection();Statement s=c.createStatement()){s.execute(sql);}}
    @Test void directoryBindingIsSharedIdempotentAndRecordedOnce()throws Exception{
        AgentDirectory.Binding first=a.register(lease,fixture.directory),second=b.register(lease,fixture.directory);
        assertEquals(first.boundAt,second.boundAt);assertEquals("HEALTHY",second.state);assertEquals("HEALTHY",b.owner("a",fixture.directory).state);
        try(Connection c=fixture.connection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM rivomesh_control.rmc_audit_event WHERE payload LIKE '%GLOBAL_AGENT_BOUND%'")){r.next();assertEquals(1,r.getInt(1));}
    }
    @Test void unknownOwnerAndRegistryWithoutExecutionAdmissionAreNotHealthyAgents()throws Exception{
        assertEquals("AGENT_NOT_REGISTERED",assertThrows(TaskService.Conflict.class,()->a.owner("a",fixture.directory)).getMessage());
        assertEquals("AGENT_NOT_REGISTERED",assertThrows(TaskService.Conflict.class,()->b.owner("missing",fixture.directory)).getMessage());
    }
    @Test void stoppedOwnerCannotRegisterOrDispatchAndItsDirectoryCannotBeReplaced()throws Exception{
        a.register(lease,fixture.directory);registry.stopped(lease);assertEquals("STOPPED",b.owner("a",fixture.directory).state);assertThrows(ControlStore.Fenced.class,()->a.register(lease,fixture.directory));
        ControlStore.Lease next=registry.register(ControlStoreTest.node("a"),Collections.emptyList());
        assertEquals("REGISTERING",b.owner("a",fixture.directory).state);
        assertEquals("AGENT_DIRECTORY_CHANGED",assertThrows(TaskService.Conflict.class,()->b.register(next,UUID.randomUUID().toString())).getMessage());
        assertEquals(next.epoch,a.register(next,fixture.directory).epoch);assertEquals("HEALTHY",b.owner("a",fixture.directory).state);
        assertThrows(ControlStore.Fenced.class,()->fixture.a.service((c,d)->{a.require(c,lease,fixture.directory);return null;}));
    }
    @Test void expiredAndReplacedLeasesCannotAuthorizeDispatch()throws Exception{
        a.register(lease,fixture.directory);sql("UPDATE rivomesh_control.rmc_node SET lease_until=0");assertEquals("STALE",a.owner("a",fixture.directory).state);
        assertThrows(ControlStore.Expired.class,()->fixture.a.service((c,d)->{a.require(c,lease,fixture.directory);return null;}));
        assertThrows(ControlStore.Expired.class,()->a.register(lease,fixture.directory));
        ControlStore.Lease replacement=registry.register(ControlStoreTest.node("a"),Collections.emptyList());a.register(replacement,fixture.directory);
        assertThrows(ControlStore.Fenced.class,()->b.register(lease,fixture.directory));
        fixture.b.service((c,d)->{b.require(c,replacement,fixture.directory);return null;});
    }
    @Test void foreignNodeAndWrongIncarnationCannotUseAnotherAgentLease()throws Exception{
        a.register(lease,fixture.directory);assertThrows(ControlStore.Fenced.class,()->a.register(new ControlStore.Lease("a",UUID.randomUUID().toString(),lease.epoch),fixture.directory));
        ClusterConfig config=Model.JSON.readValue(Model.JSON.writeValueAsString(fixture.config.cluster),ClusterConfig.class);config.nodeId="b";
        AgentDirectory other=new AgentDirectory(config,fixture.access,fixture.a);assertThrows(ControlStore.Fenced.class,()->other.register(lease,fixture.directory));
        try(JdbcControlStore remote=new JdbcControlStore(config)){
            ControlStore.Lease remoteLease=remote.register(ControlStoreTest.node("b"),Collections.emptyList());other.register(remoteLease,UUID.randomUUID().toString());
            assertThrows(ControlStore.Fenced.class,()->a.register(remoteLease,fixture.directory));
        }
    }
    @Test void alteredDirectoryAndRootCountersAreRefused()throws Exception{
        a.register(lease,fixture.directory);sql("UPDATE rivomesh_control.rmc_agent SET directory_id='"+UUID.randomUUID()+"'");
        assertEquals("AGENT_RECORD_INVALID",assertThrows(TaskService.Conflict.class,()->b.owner("a",fixture.directory)).getMessage());
        sql("UPDATE rivomesh_control.rmc_agent_root SET registered_count=0");assertEquals("AGENT_ROOT_INVALID",assertThrows(TaskService.Conflict.class,()->a.register(lease,fixture.directory)).getMessage());
    }
    @Test void missingCatalogOrRootCannotResetDirectoryOwnership()throws Exception{
        a.register(lease,fixture.directory);sql("DELETE FROM rivomesh_control.rmc_agent");
        assertEquals("AGENT_CATALOG_INCOMPLETE",assertThrows(TaskService.Conflict.class,()->a.register(lease,UUID.randomUUID().toString())).getMessage());
        assertEquals("AGENT_CATALOG_INCOMPLETE",assertThrows(TaskService.Conflict.class,()->new AgentDirectory(fixture.config.cluster,fixture.access,fixture.a)).getMessage());
        sql("DELETE FROM rivomesh_control.rmc_agent_root");
        assertEquals("AGENT_STATE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new AgentDirectory(fixture.config.cluster,fixture.access,fixture.a)).getMessage());
    }
    @Test void currentLeaseIsCheckedAgainAtExecutionAndAuthorityLossFailsClosed()throws Exception{
        a.register(lease,fixture.directory);
        fixture.a.service((c,d)->{a.require(c,lease,fixture.directory);return null;});
        assertEquals("AGENT_DIRECTORY_CHANGED",assertThrows(TaskService.Conflict.class,()->fixture.a.service((c,d)->{a.require(c,lease,UUID.randomUUID().toString());return null;})).getMessage());
        sql("DROP TABLE rivomesh_control.rmc_audit_event");assertThrows(ControlStore.Unavailable.class,()->b.register(lease,fixture.directory));
    }
    @Test void invalidDirectoryNeverCreatesAnAdmission()throws Exception{
        for(String directory:Arrays.asList(null,"../data","1-1-1-1-1",UUID.randomUUID().toString().toUpperCase(Locale.ROOT)))assertEquals("AGENT_DIRECTORY_INVALID",assertThrows(IllegalArgumentException.class,()->a.register(lease,directory)).getMessage());
        assertEquals("AGENT_NOT_REGISTERED",assertThrows(TaskService.Conflict.class,()->a.owner("a",fixture.directory)).getMessage());
    }
}
