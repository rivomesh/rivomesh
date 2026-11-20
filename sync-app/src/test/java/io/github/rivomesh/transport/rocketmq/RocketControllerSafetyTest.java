package io.github.rivomesh.transport.rocketmq;

import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicaIdentity;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class RocketControllerSafetyTest {
    private static final String CONTROLLER="controller:9877", GROUP="raft-group", CLUSTER="cluster", BROKER="broker-a";
    private static final String A="broker-a:10911", B="broker-b:10911", C="broker-c:10911";
    private static final ReplicaIdentity FIRST=fixture(A,1,true), SECOND=fixture(B,2,true), THIRD=fixture(C,3,true);

    private static ReplicaIdentity fixture(String address,long id,boolean alive){
        return new ReplicaIdentity(BROKER,id,address,alive);
    }
    private static RocketControllerSafety.Route view(String nameserver,Long id,String master,int epoch,int sync,
                                                     List<ReplicaIdentity> members,List<ReplicaIdentity> excluded)throws Exception{
        return RocketControllerSafety.route(CONTROLLER,GROUP,BROKER,nameserver,new ReplicasInfo(id,master,epoch,sync,members,excluded));
    }
    private static RocketControllerSafety.Route original()throws Exception{
        return view(A,1L,A,1,2,List.of(FIRST,SECOND,THIRD),List.of());
    }

    @Test void requiresTheElectedControllerAndAMajorityOfTheAdvertisedMembers() throws Exception {
        String first = CONTROLLER, second = "controller-b:9877", third = "controller-c:9877";
        String members = "n0:" + first + ";n1:" + second + ";n2:" + third + ";";
        var safety = controllerSafety(List.of(first, second, third));
        var leader = new GetMetaDataResponseHeader(GROUP, "n0", first, true, members);
        var follower = new GetMetaDataResponseHeader(GROUP, "n0", first, false, members);
        var rival = new GetMetaDataResponseHeader(GROUP, "n2", third, true, members);
        assertEquals(first, safety.leader(Map.of(first, leader, second, follower)::get).address());

        assertRetry(() -> safety.leader(Map.of(first, leader)::get));
        assertRetry(() -> safety.leader(Map.of(second, follower, third, follower)::get));
        assertRetry(() -> safety.leader(Map.of(first, leader, second, follower, third, rival)::get));
        assertRetry(() -> controllerSafety(List.of(first)).leader(Map.of(first, leader)::get));
    }

    @Test void readsStandaloneAndAddressOnlyMembershipWithTheSameRules() throws Exception {
        var single = controllerSafety(List.of(CONTROLLER));
        var embedded = new GetMetaDataResponseHeader(GROUP, "n0", CONTROLLER, true, "n0:" + CONTROLLER + ";");
        assertEquals(CONTROLLER, single.leader(endpoint -> embedded).address());

        String second = "[2001:db8::1]:9877";
        var voters = controllerSafety(List.of(CONTROLLER, second, "controller-c:9877"));
        String remoteMembers = CONTROLLER + ";" + second + ";controller-c:9877;";
        var leader = new GetMetaDataResponseHeader(GROUP, "leader-peer", CONTROLLER, true, remoteMembers);
        var follower = new GetMetaDataResponseHeader(GROUP, "leader-peer", CONTROLLER, false, remoteMembers);
        assertEquals(CONTROLLER, voters.leader(Map.of(CONTROLLER, leader, second, follower)::get).address());

        var mismatched = new GetMetaDataResponseHeader(GROUP, "other-peer", CONTROLLER, false, remoteMembers);
        assertRetry(() -> voters.leader(Map.of(CONTROLLER, leader, second, mismatched)::get));
        var duplicates = new GetMetaDataResponseHeader(GROUP, "leader-peer", CONTROLLER, false,
                CONTROLLER + ";" + second + ";" + second + ";");
        assertRetry(() -> voters.leader(Map.of(CONTROLLER, leader, second, duplicates)::get));
    }

    private static RocketControllerSafety controllerSafety(List<String> endpoints) {
        var settings = new RocketTransport.Settings();
        settings.controllerEndpoints = endpoints;
        return new RocketControllerSafety(settings);
    }

    @Test void routeBindsLiveSyncSetToTheSameMasterAndPersistentReplicaIdentity()throws Exception{
        var first=original();
        var reordered=view(A,1L,A,1,2,List.of(THIRD,SECOND,FIRST),List.of());
        assertEquals(first,reordered);
        var promoted=view(B,2L,B,2,4,List.of(SECOND,THIRD),List.of(fixture(A,1,false)));
        assertFalse(first.sameMaster(promoted));
        assertTrue(promoted.follows(first));
        assertFalse(first.follows(promoted));
        assertFalse(view(A,1L,A,2,5,List.of(FIRST,SECOND,THIRD),List.of()).follows(promoted));
        assertEquals(2,promoted.members().size());
        assertFalse(promoted.members().stream().anyMatch(member->member.address().equals(A)));
        var resynchronized=view(B,2L,B,2,5,List.of(SECOND,THIRD,FIRST),List.of());
        assertTrue(promoted.sameMaster(resynchronized));
        assertNotEquals(promoted,resynchronized,"A SyncStateSet change cannot authorize an in-flight old receipt");
    }

    @Test void rejectsStaleRouteExcessMembersDuplicateIdsAndADeadOrMissingSyncReplica(){
        assertRetry(()->view(A,2L,B,2,4,List.of(SECOND,THIRD),List.of(fixture(A,1,false))));
        assertRetry(()->view(A,1L,A,1,2,List.of(FIRST,fixture(B,2,false)),List.of(THIRD)));
        assertRetry(()->view(A,1L,A,1,2,List.of(FIRST),List.of(SECOND,THIRD)));
        assertRetry(()->view(A,9L,A,1,2,List.of(FIRST,SECOND,THIRD),List.of()));
        assertRetry(()->view(A,1L,A,1,2,List.of(FIRST,fixture(B,1,true),THIRD),List.of()));
        assertRetry(()->view(A,1L,A,1,2,List.of(FIRST,SECOND,fixture("somebody@host:10911",3,true)),List.of()));
        assertPolicy(()->view(A,1L,A,1,2,List.of(FIRST,SECOND,THIRD,fixture("broker-d:10911",4,true)),List.of()));
    }

    @Test void validatesTheIndependentEffectiveBrokerSettingsForMasterAndFollower()throws Exception{
        var route=original();
        var master=new RocketControllerSafety.Replica(A,1);
        var follower=new RocketControllerSafety.Replica(B,2);
        var masterPolicy=policy("SYNC_MASTER","0");
        RocketControllerSafety.verifyBroker(masterPolicy,CLUSTER,BROKER,route,master,87600);
        RocketControllerSafety.verifyBroker(policy("SLAVE","2"),CLUSTER,BROKER,route,follower,87600);
        assertRetry(()->RocketControllerSafety.verifyBroker(policy("SLAVE","3"),CLUSTER,BROKER,route,follower,87600));
        assertRetry(()->RocketControllerSafety.verifyBroker(policy("SLAVE","1"),CLUSTER,BROKER,route,master,87600));
        for(String override:List.of("flushDiskType=ASYNC_FLUSH","enableDLegerCommitLog=true","enableControllerMode=false",
                "allAckInSyncStateSet=false","minInSyncReplicas=1","syncFromLastFile=true","asyncLearner=true",
                "autoCreateTopicEnable=true","transientStorePoolEnable=true","cleanFileForciblyEnable=true",
                "fileReservedTime=876","brokerClusterName=other")){
            String[] field=override.split("=",2);
            Properties changed=policy("SYNC_MASTER","0");
            changed.setProperty(field[0],field[1]);
            assertPolicy(()->RocketControllerSafety.verifyBroker(changed,CLUSTER,BROKER,route,master,87600));
        }
    }

    private static Properties policy(String role,String id){
        Properties p=new Properties();
        p.setProperty("brokerRole",role);
        p.setProperty("brokerId",id);
        p.setProperty("brokerName",BROKER);
        p.setProperty("brokerClusterName",CLUSTER);
        p.setProperty("enableControllerMode","true");
        p.setProperty("enableDLegerCommitLog","false");
        p.setProperty("allAckInSyncStateSet","true");
        p.setProperty("minInSyncReplicas","2");
        p.setProperty("syncFromLastFile","false");
        p.setProperty("asyncLearner","false");
        p.setProperty("flushDiskType","SYNC_FLUSH");
        p.setProperty("transientStorePoolEnable","false");
        p.setProperty("autoCreateTopicEnable","false");
        p.setProperty("cleanFileForciblyEnable","false");
        p.setProperty("fileReservedTime","87600");
        return p;
    }

    private static void assertRetry(org.junit.jupiter.api.function.Executable action){
        RocketTransport.Failure error=assertThrows(RocketTransport.Failure.class,action);
        assertTrue(error.retryable);
        assertEquals("ROCKETMQ_CONTROLLER_ROUTE_RETRY",error.code);
    }
    private static void assertPolicy(org.junit.jupiter.api.function.Executable action){
        RocketTransport.Failure error=assertThrows(RocketTransport.Failure.class,action);
        assertFalse(error.retryable);
        assertEquals("ROCKETMQ_CONTROLLER_POLICY_REQUIRES_PLAN",error.code);
    }
}
