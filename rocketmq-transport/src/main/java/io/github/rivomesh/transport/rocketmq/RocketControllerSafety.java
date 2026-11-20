package io.github.rivomesh.transport.rocketmq;

import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicaIdentity;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/** Read-only safety checks for the three-data-replica, clean-election Controller profile. */
final class RocketControllerSafety {
    record Route(String controller, String group, String master, long masterId, int epoch,
                 int syncEpoch, Set<Replica> members) {
        boolean sameMaster(Route other) {
            return controller.equals(other.controller) && group.equals(other.group)
                    && master.equals(other.master) && masterId == other.masterId && epoch == other.epoch;
        }
        boolean follows(Route earlier) {
            return group.equals(earlier.group) && epoch >= earlier.epoch && syncEpoch >= earlier.syncEpoch
                    && (epoch > earlier.epoch || masterId == earlier.masterId && master.equals(earlier.master));
        }
    }

    record Replica(String address, long id) {}
    record Leader(String address, String group, String id) {}

    @FunctionalInterface
    interface MetadataReader {
        GetMetaDataResponseHeader read(String endpoint) throws Exception;
    }

    private final Set<String> controllers;
    private final String cluster, broker;
    private final int timeout, retention;

    RocketControllerSafety(RocketTransport.Settings settings) {
        controllers = Set.copyOf(settings.controllerEndpoints);
        cluster = settings.cluster;
        broker = settings.broker;
        timeout = settings.requestTimeoutMillis;
        retention = settings.minimumRetentionHours;
    }

    Route inspect(MQClientAPIImpl api, String nameserverMaster) throws Exception {
        Leader leader = leader(api::getControllerMetaData);
        Properties policy;
        ReplicasInfo evidence;
        try {
            policy = api.getControllerConfig(List.of(leader.address), timeout).get(leader.address);
            evidence = api.getInSyncStateData(leader.address, List.of(broker)).getReplicasInfoTable().get(broker);
        } catch (InterruptedException interrupted) {
            throw interrupted;
        } catch (Exception unavailable) {
            throw routeRetry();
        }
        if (policy == null || !"false".equals(policy.getProperty("enableElectUncleanMaster"))) throw policyRejected();
        Route route = route(leader.address, leader.group, broker, nameserverMaster, evidence);
        for (Replica member : route.members) {
            verifyBroker(api.getBrokerConfig(member.address, timeout), cluster, broker, route, member, retention);
        }
        return route;
    }

    Leader leader(MetadataReader metadata) throws Exception {
        Leader observed = null;
        int agreeing = 0;
        boolean leaderConfirmed = false;
        for (String endpoint : controllers) {
            GetMetaDataResponseHeader info;
            try {
                info = metadata.read(endpoint);
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (Exception unavailable) {
                continue;
            }
            if (info == null || info.getControllerLeaderAddress() == null || info.getGroup() == null) continue;
            Leader claim = new Leader(info.getControllerLeaderAddress(), info.getGroup(), info.getControllerLeaderId());
            if (claim.group.isBlank() || claim.id == null || claim.id.isBlank() || !controllers.contains(claim.address)
                    || !controllers.equals(controllerMembers(info.getPeers()))) throw routeRetry();
            if (observed != null && !observed.equals(claim)) throw routeRetry();
            observed = claim;
            agreeing++;
            if (info.isLeader()) {
                if (!endpoint.equals(claim.address)) throw routeRetry();
                leaderConfirmed = true;
            }
        }
        if (!leaderConfirmed || agreeing <= controllers.size() / 2) throw routeRetry();
        return observed;
    }

    private static Set<String> controllerMembers(String peers) throws RocketTransport.Failure {
        if (peers == null) throw routeRetry();
        Set<String> members = new HashSet<>();
        for (String peer : peers.split(";")) {
            // DLedger advertises id:host:port, while JRaft advertises host:port.
            String address = peer;
            if (!nativeAddress(address)) {
                int separator = peer.indexOf(':');
                if (separator < 1) throw routeRetry();
                address = peer.substring(separator + 1);
            }
            if (!nativeAddress(address) || !members.add(address)) throw routeRetry();
        }
        return members;
    }

    static Route route(String controller, String group, String broker, String nameserver, ReplicasInfo evidence)
            throws RocketTransport.Failure {
        if (evidence == null || !Objects.equals(nameserver, evidence.getMasterAddress())
                || nameserver == null || evidence.getMasterBrokerId() == null
                || evidence.getMasterEpoch() < 1 || evidence.getSyncStateSetEpoch() < 1
                || evidence.getInSyncReplicas() == null || evidence.getNotInSyncReplicas() == null) throw routeRetry();
        int registered = evidence.getInSyncReplicas().size() + evidence.getNotInSyncReplicas().size();
        if (registered > 3) throw policyRejected();
        if (registered < 3) throw routeRetry();
        Set<Long> replicaIds = new HashSet<>();
        Set<String> addresses = new HashSet<>();
        Set<Replica> sync = new HashSet<>();
        for (ReplicaIdentity item : evidence.getInSyncReplicas()) {
            Replica replica = replica(item, broker, replicaIds, addresses);
            if (!Boolean.TRUE.equals(item.getAlive())) throw routeRetry();
            sync.add(replica);
        }
        for (ReplicaIdentity item : evidence.getNotInSyncReplicas()) replica(item, broker, replicaIds, addresses);
        if (sync.size() < 2 || !sync.contains(new Replica(nameserver, evidence.getMasterBrokerId()))) throw routeRetry();
        return new Route(controller, group, nameserver, evidence.getMasterBrokerId(), evidence.getMasterEpoch(),
                evidence.getSyncStateSetEpoch(), Set.copyOf(sync));
    }

    private static Replica replica(ReplicaIdentity item, String broker, Set<Long> ids, Set<String> addresses)
            throws RocketTransport.Failure {
        if (item == null || !broker.equals(item.getBrokerName()) || item.getBrokerId() == null
                || item.getBrokerId() < 1 || !nativeAddress(item.getBrokerAddress())
                || !ids.add(item.getBrokerId()) || !addresses.add(item.getBrokerAddress())) throw routeRetry();
        return new Replica(item.getBrokerAddress(), item.getBrokerId());
    }

    private static boolean nativeAddress(String address) {
        try {
            if (address == null) return false;
            var uri = java.net.URI.create("rocketmq://" + address);
            return uri.getHost() != null && uri.getRawUserInfo() == null && uri.getRawPath().isEmpty()
                    && uri.getRawQuery() == null && uri.getRawFragment() == null && uri.getPort() > 0 && uri.getPort() <= 65535;
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }

    static void verifyBroker(Properties properties, String cluster, String name, Route route, Replica replica,
                             int retention) throws RocketTransport.Failure {
        if (properties == null || !cluster.equals(properties.getProperty("brokerClusterName")) || !name.equals(properties.getProperty("brokerName")))
            throw policyRejected();
        if (replica.address.equals(route.master)) {
            if (!"SYNC_MASTER".equals(properties.getProperty("brokerRole")) || !"0".equals(properties.getProperty("brokerId")))
                throw routeRetry();
        } else if (!"SLAVE".equals(properties.getProperty("brokerRole"))
                || !Long.toString(replica.id).equals(properties.getProperty("brokerId"))) throw routeRetry();
        if (!"true".equals(properties.getProperty("enableControllerMode")) || !"false".equals(properties.getProperty("enableDLegerCommitLog"))
                || !"true".equals(properties.getProperty("allAckInSyncStateSet")) || !"2".equals(properties.getProperty("minInSyncReplicas"))
                || !"false".equals(properties.getProperty("syncFromLastFile")) || !"false".equals(properties.getProperty("asyncLearner"))
                || !"SYNC_FLUSH".equals(properties.getProperty("flushDiskType")) || !"false".equals(properties.getProperty("transientStorePoolEnable"))
                || !"false".equals(properties.getProperty("autoCreateTopicEnable")) || !"false".equals(properties.getProperty("cleanFileForciblyEnable")))
            throw policyRejected();
        try {
            if (Long.parseLong(properties.getProperty("fileReservedTime")) < retention) throw policyRejected();
        } catch (NumberFormatException invalid) {
            throw policyRejected();
        }
    }

    static RocketTransport.Failure routeRetry() {
        return new RocketTransport.Failure("ROCKETMQ_CONTROLLER_ROUTE_RETRY", true, null);
    }

    private static RocketTransport.Failure policyRejected() {
        return new RocketTransport.Failure("ROCKETMQ_CONTROLLER_POLICY_REQUIRES_PLAN", false, null);
    }
}
