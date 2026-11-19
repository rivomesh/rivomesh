package io.github.rivomesh.acceptance;

import io.github.rivomesh.Model;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.RocketMessagePublisher;
import io.github.rivomesh.cdc.TargetProduct;
import io.github.rivomesh.transport.rocketmq.RocketTransport;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicaIdentity;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Broker-native offset proof and the current Rivomesh adapter's explicit refusal of Controller mode. */
public final class RocketControllerPromotionAcceptance {
    private static final String NAMESERVER = "rocketnamesrv:9876";
    private static final String CONTROLLER = "rocketnamesrv:9877";
    private static final String CLUSTER = "RivomeshTest";
    private static final String BROKER = "rivomesh-broker";
    private static final String PREFIX = "events";
    private static final String ROLE = "SYNC_MASTER";
    private static final int TIMEOUT = 5000;
    private static final Set<String> NODES = Set.of("rocket-primary:10911", "rocket-replica-a:10911", "rocket-replica-b:10911");
    private final String binding = "rivomesh:" + Codec.sha256(UUID.randomUUID().toString());
    private final String topic = RocketTransport.physicalTopic(PREFIX, binding);
    private final MessageQueue queue = new MessageQueue(topic, BROKER, 0);

    private record Cut(long queueOffset, long commitLogOffset, String contentSha256) {}
    private record Election(String address, long id, int epoch, int syncEpoch, Set<String> liveInSync) {}
    @FunctionalInterface private interface Probe<T> { T read() throws Exception; }

    public static void main(String[] args) throws Exception {
        require(Runtime.version().feature() == 21 && "Eclipse Adoptium".equals(System.getProperty("java.vendor")),
                "Expected the Temurin 21 fixture runtime");
        new RocketControllerPromotionAcceptance().accept(Path.of(args[0]));
    }

    private void accept(Path markers) throws Exception {
        Cut owner, before;
        Election original;
        DefaultMQProducer producer = producer();
        try {
            MQClientAPIImpl api = api(producer);
            Election initial = until(() -> election(api, 1, null), "the distinguished primary");
            require("rocket-primary:10911".equals(initial.address()), "Unexpected initial leader");
            Files.writeString(markers.resolve("leader-ready.json"), Model.JSON.writeValueAsString(Map.of("master", initial.address())));
            original = until(() -> election(api, 3, null), "the original master plus both live in-sync replicas");
            checkPolicies(api, original);
            require(initial.address().equals(original.address()) && initial.id() == original.id(), "Replica enrollment changed the original leader");
            requireRejectedAdapter();
            api.createTopic(original.address(), "TBW102", new TopicConfig(topic, 1, 1, 6), TIMEOUT);
            awaitTopic(api, original.address());
            awaitReplicatedTopic(api, original);

            Message manifest = new Message(topic, binding.getBytes(StandardCharsets.UTF_8));
            manifest.putUserProperty("rivomesh_publication_manifest", binding);
            owner = send(producer, manifest);
            before = send(producer, event("retained-before-promotion"));
            require(owner.queueOffset() == 0 && before.queueOffset() == 1 && before.commitLogOffset() > owner.commitLogOffset(),
                    "The native topic does not have the expected ordered cut");
            awaitHistory(api, original, owner, before);
            Files.writeString(markers.resolve("original-cut.json"), Model.JSON.writeValueAsString(Map.of(
                    "master", original.address(), "masterId", original.id(), "epoch", original.epoch(),
                    "syncStateEpoch", original.syncEpoch(), "liveInSync", original.liveInSync(),
                    "topicConfigurationReplicas", original.liveInSync(), "retained", before)));
            until(() -> Files.isRegularFile(markers.resolve("original-killed")) ? Boolean.TRUE : null, "SIGKILL of the original master");
        } finally {
            producer.shutdown();
        }

        producer = producer();
        try {
            MQClientAPIImpl api = api(producer);
            Election promoted = until(() -> election(api, 2, original), "automatic promotion with a live synchronized follower");
            checkPolicies(api, promoted);
            requireRejectedAdapter();
            awaitHistory(api, promoted, owner, before);
            awaitTopic(api, promoted.address());
            awaitReplicatedTopic(api, promoted);

            Cut after = send(producer, event("appended-after-promotion"));
            require(after.queueOffset() == before.queueOffset() + 1 && after.commitLogOffset() > before.commitLogOffset(),
                    "The promoted group did not append at the next logical and a later physical offset");
            for (String address : promoted.liveInSync()) {
                until(() -> Objects.equals(api.viewMessage(address, topic, after.commitLogOffset(), TIMEOUT).getUserProperty("rivomesh_event_id"),
                        "appended-after-promotion") ? Boolean.TRUE : null, "replication of the new event at " + address);
                verify(api, address, after, "appended-after-promotion");
            }
            Election confirmed = election(api, 2, original);
            require(promoted.equals(confirmed), "Controller and NameServer did not hold a stable promoted route");
            Files.writeString(markers.resolve("promoted-cut.json"), Model.JSON.writeValueAsString(Map.of(
                    "master", promoted.address(), "masterId", promoted.id(), "epoch", promoted.epoch(),
                    "syncStateEpoch", promoted.syncEpoch(), "liveInSync", promoted.liveInSync(),
                    "topicConfigurationReplicas", promoted.liveInSync(),
                    "retained", before, "appended", after, "adapterOutcome", "ROCKETMQ_REPLICATION_PROFILE_REQUIRES_PLAN")));
        } finally {
            producer.shutdown();
        }
    }

    private static DefaultMQProducer producer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("fixture_" + UUID.randomUUID().toString().replace("-", ""));
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(NAMESERVER);
        producer.setVipChannelEnabled(false);
        producer.setSendMsgTimeout(TIMEOUT);
        producer.setRetryTimesWhenSendFailed(0);
        producer.setRetryAnotherBrokerWhenNotStoreOK(false);
        producer.start();
        return producer;
    }

    private static MQClientAPIImpl api(DefaultMQProducer producer) {
        return producer.getDefaultMQProducerImpl().getMqClientFactory().getMQClientAPIImpl();
    }

    private static Election election(MQClientAPIImpl api, int expected, Election previous) throws Exception {
        ReplicasInfo replicas = api.getInSyncStateData(CONTROLLER, List.of(BROKER)).getReplicasInfoTable().get(BROKER);
        if (replicas == null || replicas.getMasterAddress() == null || replicas.getMasterBrokerId() == null) return null;
        BrokerData route = api.getBrokerClusterInfo(TIMEOUT).getBrokerAddrTable().get(BROKER);
        if (route == null || !CLUSTER.equals(route.getCluster()) || !replicas.getMasterAddress().equals(route.getBrokerAddrs().get(0L))) return null;
        Set<String> live = replicas.getInSyncReplicas().stream()
                .filter(replica -> Boolean.TRUE.equals(replica.getAlive()) && BROKER.equals(replica.getBrokerName()))
                .map(ReplicaIdentity::getBrokerAddress).collect(Collectors.toSet());
        if (live.size() != expected || !live.contains(replicas.getMasterAddress()) || !NODES.containsAll(live)) return null;
        Election current = new Election(replicas.getMasterAddress(), replicas.getMasterBrokerId(),
                replicas.getMasterEpoch(), replicas.getSyncStateSetEpoch(), Set.copyOf(live));
        if (previous != null && (current.address().equals(previous.address()) || current.id() == previous.id()
                || current.epoch() <= previous.epoch() || live.contains(previous.address()))) return null;
        return current;
    }

    private static void checkPolicies(MQClientAPIImpl api, Election election) throws Exception {
        Properties controller = api.getControllerConfig(List.of(CONTROLLER), TIMEOUT).get(CONTROLLER);
        require(controller != null && "false".equals(controller.getProperty("enableElectUncleanMaster")),
                "The Controller does not attest clean election");
        for (String address : election.liveInSync()) {
            Properties broker = api.getBrokerConfig(address, TIMEOUT);
            require(broker.entrySet().containsAll(Map.of(
                    "enableControllerMode", "true", "enableDLegerCommitLog", "false",
                    "allAckInSyncStateSet", "true", "minInSyncReplicas", "2", "syncFromLastFile", "false",
                    "asyncLearner", "false", "flushDiskType", "SYNC_FLUSH", "transientStorePoolEnable", "false",
                    "brokerClusterName", CLUSTER, "brokerName", BROKER).entrySet()),
                    "A live broker does not attest the required replica policy at " + address);
            require(address.equals(election.address()) ? ROLE.equals(broker.getProperty("brokerRole")) && "0".equals(broker.getProperty("brokerId"))
                    : "SLAVE".equals(broker.getProperty("brokerRole")), "Live broker configuration does not match the elected role");
        }
    }

    private void awaitTopic(MQClientAPIImpl api, String currentMaster) throws Exception {
        until(() -> {
            var route = api.getTopicRouteInfoFromNameServer(topic, TIMEOUT);
            TopicConfig config = api.getTopicConfig(currentMaster, topic, TIMEOUT);
            return route != null && config != null && config.getReadQueueNums() == 1 && config.getWriteQueueNums() == 1
                    && route.getBrokerDatas().stream().anyMatch(d -> BROKER.equals(d.getBrokerName())
                    && currentMaster.equals(d.getBrokerAddrs().get(0L))) ? Boolean.TRUE : null;
        }, "the one-queue topic route to the active broker");
    }

    private void awaitReplicatedTopic(MQClientAPIImpl api, Election election) throws Exception {
        for (String address : election.liveInSync()) {
            until(() -> {
                var config = api.getTopicConfig(address, topic, TIMEOUT);
                return config != null && config.getReadQueueNums() == 1 && config.getWriteQueueNums() == 1
                        && config.getPerm() == 6 && config.getMappingDetail() == null && config.getAttributes().isEmpty()
                        ? Boolean.TRUE : null;
            }, "the topic configuration replicated to " + address);
        }
    }

    private void awaitHistory(MQClientAPIImpl api, Election election, Cut owner, Cut before) throws Exception {
        for (String address : election.liveInSync()) {
            until(() -> api.getMinOffset(address, queue, TIMEOUT) == 0 && api.getMaxOffset(address, queue, TIMEOUT) >= 2
                    ? Boolean.TRUE : null, "the retained queue on " + address);
            verify(api, address, owner, null);
            verify(api, address, before, "retained-before-promotion");
        }
    }

    private Cut send(DefaultMQProducer producer, Message message) throws Exception {
        message.setWaitStoreMsgOK(true);
        SendResult nativeAck = producer.send(message, queue, TIMEOUT);
        require(nativeAck.getSendStatus() == SendStatus.SEND_OK && queue.equals(nativeAck.getMessageQueue())
                && nativeAck.getOffsetMsgId() != null, "Native send did not confirm a unique stored position");
        return new Cut(nativeAck.getQueueOffset(), MessageDecoder.decodeMessageId(nativeAck.getOffsetMsgId()).getOffset(), hash(message));
    }

    private void verify(MQClientAPIImpl api, String address, Cut receipt, String id) throws Exception {
        MessageExt stored = api.viewMessage(address, topic, receipt.commitLogOffset(), TIMEOUT);
        require(stored != null && topic.equals(stored.getTopic()) && stored.getQueueId() == 0
                && receipt.queueOffset() == stored.getQueueOffset() && receipt.commitLogOffset() == stored.getCommitLogOffset()
                && receipt.contentSha256().equals(hash(stored)), "The native logical/physical receipt changed on " + address);
        if (id == null) {
            require(binding.equals(stored.getUserProperty("rivomesh_publication_manifest")), "The original owner changed");
        } else {
            require(id.equals(stored.getUserProperty("rivomesh_event_id")) && "sales.items".equals(stored.getUserProperty("rivomesh_channel"))
                    && ("key-" + id).equals(stored.getUserProperty("rivomesh_key")), "The retained event envelope changed");
        }
    }

    private Message event(String id) {
        Message message = new Message(topic, ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8));
        message.setKeys(id);
        message.putUserProperty("rivomesh_event_id", id);
        message.putUserProperty("rivomesh_channel", "sales.items");
        message.putUserProperty("rivomesh_key", "key-" + id);
        return message;
    }

    private static String hash(Message message) throws Exception {
        MessageDigest hash = MessageDigest.getInstance("SHA-256");
        String id = message.getUserProperty("rivomesh_event_id");
        byte[] identity = (id == null ? "" : id).getBytes(StandardCharsets.UTF_8);
        hash.update(ByteBuffer.allocate(4).putInt(identity.length).array());
        hash.update(identity);
        hash.update(message.getBody());
        return HexFormat.of().formatHex(hash.digest());
    }

    private static void requireRejectedAdapter() throws Exception {
        PipelineConfig.Target target = new PipelineConfig.Target();
        target.product = TargetProduct.ROCKETMQ;
        target.url = "rocketmq://" + NAMESERVER;
        target.messaging = new PipelineConfig.Messaging();
        target.messaging.cluster = CLUSTER;
        target.messaging.broker = BROKER;
        target.messaging.topic = PREFIX;
        target.messaging.requestTimeoutMillis = TIMEOUT;
        try {
            RocketMessagePublisher.check(target);
            throw new AssertionError("The existing adapter accepted an unsupported replication profile");
        } catch (SyncFailure refusal) {
            require(!refusal.retryable && "ROCKETMQ_REPLICATION_PROFILE_REQUIRES_PLAN".equals(refusal.code),
                    "Unexpected adapter decision: " + refusal.code);
        }
    }

    private static <T> T until(Probe<T> probe, String description) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try {
                T observed = probe.read();
                if (observed != null) return observed;
            } catch (Exception transientFailure) {
                last = transientFailure;
            }
            Thread.sleep(400);
        }
        throw new AssertionError("Timed out waiting for " + description, last);
    }

    private static void require(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
}
