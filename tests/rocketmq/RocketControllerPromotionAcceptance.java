package io.github.rivomesh.acceptance;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.Model;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.ConfirmedPublisher;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.JdbcPublicationStore;
import io.github.rivomesh.cdc.MessageEvent;
import io.github.rivomesh.cdc.TableSchema;
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
import java.net.URI;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
        RocketControllerPromotionAcceptance fixture = new RocketControllerPromotionAcceptance();
        if (args.length == 2 && "--worker".equals(args[1])) fixture.acceptWorker(Path.of(args[0]));
        else if (args.length == 2 && "--product".equals(args[1])) fixture.acceptProduct(Path.of(args[0]));
        else fixture.accept(Path.of(args[0]));
    }

    private String activeController = CONTROLLER;

    private record WorkerCut(String topic, Cut retained, String generation, String snapshotId,
                             long sequence, String transactionId, long epoch, long capturedSequence) {}

    private static void sourceSql(String sql) throws Exception {
        try (var c = JdbcConnections.open("com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://mysql-source:3306/?connectTimeout=3000&sslMode=DISABLED&allowPublicKeyRetrieval=true",
                "root", System.getenv("RM_ROCKET_MYSQL_PASSWORD")); var statement = c.createStatement()) {
            statement.execute(sql);
        }
    }

    private static Process startWorker(Path configuration) throws Exception {
        return new ProcessBuilder(System.getProperty("java.home") + "/bin/java",
                "-Xmx192m", "-XX:ActiveProcessorCount=2", "-jar", "/app.jar", "run", configuration.toString())
                .inheritIO().start();
    }

    private static void stopWorker(Process worker) throws Exception {
        if (worker == null || !worker.isAlive()) return;
        worker.destroyForcibly();
        require(worker.waitFor(15, TimeUnit.SECONDS), "The test Worker did not terminate");
    }

    private static WorkerCut workerCut(PipelineConfig config) throws Exception {
        try (var journal = new JdbcJournal(config.existingJournalSettings())) {
            var status = journal.status();
            var store = new JdbcPublicationStore(journal, "rocket", null);
            var checkpoint = store.checkpoint();
            var snapshot = store.snapshot(TableSchema.identity("sales", "", "items"));
            String token = store.progress();
            var subscription = journal.subscriptions().getFirst();
            if (snapshot == null || !"COMPLETE".equals(snapshot.state) || token == null
                    || checkpoint.sequence != subscription.acknowledged) return null;
            String[] fields = token.split(":", -1);
            require(fields.length == 5 && "ROCKETMQ_OFFSET_V1".equals(fields[0]), "Invalid Worker progress token");
            Cut receipt = new Cut(Long.parseLong(fields[2]), Long.parseLong(fields[3]), fields[4]);
            return new WorkerCut(fields[1], receipt, (String)status.get("generation"), snapshot.snapshotId,
                    checkpoint.sequence, checkpoint.transactionId, checkpoint.epoch, ((Number)status.get("lastSequence")).longValue());
        }
    }

    private static WorkerCut confirmedWorkerCut(PipelineConfig config, long sequence) throws Exception {
        return until(() -> {
            WorkerCut current = workerCut(config);
            return current != null && current.sequence() == sequence && current.capturedSequence() == sequence
                    ? current : null;
        }, "a durable Worker and broker receipt for source sequence " + sequence);
    }

    private static MessageQueue verifyWorkerReceipt(MQClientAPIImpl api, Election owner, WorkerCut cut) throws Exception {
        MessageQueue queue = new MessageQueue(cut.topic(), BROKER, 0);
        for (String address : owner.liveInSync()) {
            until(() -> {
                var topicConfig = api.getTopicConfig(address, cut.topic(), TIMEOUT);
                if (topicConfig == null || api.getMaxOffset(address, queue, TIMEOUT) != cut.retained().queueOffset() + 1)
                    return null;
                MessageExt event = api.viewMessage(address, cut.topic(), cut.retained().commitLogOffset(), TIMEOUT);
                if (event == null) return null;
                var payload = Model.JSON.readValue(event.getBody(), MessageEvent.class);
                require(payload.id.equals(event.getUserProperty("rivomesh_event_id"))
                        && "COMMIT".equals(payload.kind) && cut.sequence() == payload.sequence
                        && cut.generation().equals(payload.generation) && cut.transactionId().equals(payload.transactionId)
                        && payload.totalChanges == 1 && cut.retained().queueOffset() == event.getQueueOffset()
                        && cut.retained().commitLogOffset() == event.getCommitLogOffset()
                        && cut.retained().contentSha256().equals(hash(event)), "The actual Worker COMMIT receipt diverged on " + address);
                return Boolean.TRUE;
            }, "the confirmed Worker commit on " + address);
        }
        return queue;
    }

    private void acceptWorker(Path markers) throws Exception {
        System.setProperty("h2.bindAddress", "127.0.0.1");
        DefaultMQProducer inspector = producer();
        Process worker = null;
        try {
            MQClientAPIImpl api = api(inspector);
            Path configuration = markers.resolve("worker.json");
            PipelineConfig config = PipelineConfig.load(configuration);
            List<String> controllerEndpoints = config.targets.getFirst().messaging.rocketmqControllerEndpoints.stream()
                    .map(URI::create).map(URI::getAuthority).toList();
            activeController = until(() -> controllerLeader(api, controllerEndpoints), "the initial Controller leader");
            Election initial = until(() -> election(api, 1, null), "the distinguished Worker-test primary");
            write(markers, "leader-ready.json", Map.of("master", initial.address()));
            Election original = until(() -> election(api, 3, null), "all three synchronized Worker-test brokers");
            require(initial.address().equals(original.address()), "Initial Worker route moved before start");
            checkPolicies(api, original);
            until(() -> { sourceSql("SELECT 1"); return Boolean.TRUE; }, "the real MySQL source");
            sourceSql("CREATE DATABASE sales");
            sourceSql("CREATE TABLE sales.items(id BIGINT PRIMARY KEY, note VARCHAR(40))");
            sourceSql("INSERT INTO sales.items VALUES(1,'original snapshot')");
            worker = startWorker(configuration);
            WorkerCut snapshot = confirmedWorkerCut(config, 0);
            sourceSql("UPDATE sales.items SET note='before promotion' WHERE id=1");
            WorkerCut before = confirmedWorkerCut(config, 1);
            require(snapshot.snapshotId().equals(before.snapshotId()), "The initial CDC rebuilt the snapshot");
            verifyWorkerReceipt(api, original, before);
            write(markers, "original-cut.json", Map.of("master", original.address(), "masterId", original.id(),
                    "epoch", original.epoch(), "syncStateEpoch", original.syncEpoch(), "liveInSync", original.liveInSync(),
                    "topicConfigurationReplicas", original.liveInSync(), "retained", before.retained(), "worker", before));
            until(() -> Files.isRegularFile(markers.resolve("original-killed")) ? Boolean.TRUE : null,
                    "the original broker to be killed");
            Election promoted = until(() -> election(api, 2, original), "two surviving synchronized brokers");
            checkPolicies(api, promoted);
            verifyWorkerReceipt(api, promoted, before);
            stopWorker(worker);
            sourceSql("INSERT INTO sales.items VALUES(2,'retained after Worker restart')");
            worker = startWorker(configuration);
            WorkerCut after = confirmedWorkerCut(config, 2);
            checkContinuation(before, after);
            require(after.epoch() > before.epoch(), "The restarted Worker did not acquire a new writer epoch");
            MessageQueue queue = verifyWorkerReceipt(api, promoted, after);
            write(markers, "promoted-cut.json", Map.of("master", promoted.address(), "masterId", promoted.id(),
                    "epoch", promoted.epoch(), "syncStateEpoch", promoted.syncEpoch(), "liveInSync", promoted.liveInSync(),
                    "topicConfigurationReplicas", promoted.liveInSync(), "retained", before.retained(), "appended", after.retained(),
                    "adapterOutcome", "CONTROLLER_PROFILE_WORKER_CONTINUED", "worker", after));
            until(() -> Files.isRegularFile(markers.resolve("remaining-follower-killed")) ? Boolean.TRUE : null,
                    "the remaining follower cutoff");
            until(() -> election(api, 1, original), "one live synchronized replica");
            sourceSql("INSERT INTO sales.items VALUES(3,'waits for replicas')");
            until(() -> {
                WorkerCut held = workerCut(config);
                if (held == null || held.capturedSequence() != 3) return null;
                try (var journal = new JdbcJournal(config.existingJournalSettings())) {
                    if (!"ROCKETMQ_CONTROLLER_ROUTE_RETRY".equals(journal.subscriptions().getFirst().errorCode)) return null;
                }
                require(held.sequence() == after.sequence() && held.retained().equals(after.retained())
                        && api.getMaxOffset(promoted.address(), queue, TIMEOUT) == after.retained().queueOffset() + 1,
                        "The declined replica state changed the broker or Worker receipt");
                return Boolean.TRUE;
            }, "an actual queued transaction held below the replica minimum");
            write(markers, "minimum-refused.json", Map.of("status", "PASS", "confirmed", after.retained()));
            until(() -> Files.isRegularFile(markers.resolve("remaining-follower-restarted")) ? Boolean.TRUE : null,
                    "the original follower volume to return");
            Election recovered = until(() -> election(api, 2, original), "the same healthy broker route after recovery");
            require(promoted.address().equals(recovered.address()), "The promoted master changed during follower recovery");
            WorkerCut resumed = confirmedWorkerCut(config, 3);
            checkContinuation(after, resumed);
            require(resumed.epoch() == after.epoch(), "Restoring the follower changed the running Worker epoch");
            verifyWorkerReceipt(api, recovered, resumed);
            write(markers, "worker-recovered.json", Map.of("status", "PASS",
                    "sameRunningWorker", worker.isAlive(), "worker", resumed, "liveInSync", recovered.liveInSync()));
            if (controllerEndpoints.size() > 1) {
                verifyControllerElection(api, markers, config, controllerEndpoints, recovered, resumed);
                require(worker.isAlive(), "The Worker stopped during the Controller election");
            }
        } finally {
            try { stopWorker(worker); }
            finally { inspector.shutdown(); }
        }
    }

    private static String controllerLeader(MQClientAPIImpl api, List<String> endpoints) throws Exception {
        String leader = null;
        for (String endpoint : endpoints) {
            try {
                var metadata = api.getControllerMetaData(endpoint);
                if (metadata != null && metadata.isLeader() && endpoint.equals(metadata.getControllerLeaderAddress())) {
                    if (leader != null) return null;
                    leader = endpoint;
                }
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (Exception unavailable) {
                // A stopped Controller does not prevent the remaining members electing a leader.
            }
        }
        return leader;
    }

    private void verifyControllerElection(MQClientAPIImpl api, Path markers, PipelineConfig config,
                                          List<String> endpoints, Election broker, WorkerCut before) throws Exception {
        String originalLeader = activeController;
        write(markers, "controller-ready.json", Map.of("leader", originalLeader, "worker", before));
        until(() -> Files.isRegularFile(markers.resolve("controller-leader-killed")) ? Boolean.TRUE : null,
                "SIGKILL of the Controller leader");
        activeController = until(() -> {
            String leader = controllerLeader(api, endpoints);
            return Objects.equals(originalLeader, leader) ? null : leader;
        }, "the surviving Controller majority to elect a new leader");
        Election afterElection = until(() -> election(api, 2, null), "the original route under the elected Controller");
        require(broker.equals(afterElection), "The Controller election changed the active broker or replica membership");
        verifyWorkerReceipt(api, afterElection, before);
        sourceSql("INSERT INTO sales.items VALUES(4,'after Controller election')");
        WorkerCut after = confirmedWorkerCut(config, 4);
        checkContinuation(before, after);
        require(before.epoch() == after.epoch(), "The Controller election changed the Worker epoch");
        verifyWorkerReceipt(api, afterElection, after);
        write(markers, "controller-succeeded.json", Map.of("leader", activeController, "worker", after));
    }

    private static void checkContinuation(WorkerCut before, WorkerCut after) {
        require(before.generation().equals(after.generation()) && before.snapshotId().equals(after.snapshotId())
                && before.topic().equals(after.topic()) && after.sequence() == before.sequence() + 1
                && !before.transactionId().equals(after.transactionId())
                && after.retained().queueOffset() == before.retained().queueOffset() + 3
                && after.retained().commitLogOffset() > before.retained().commitLogOffset(),
                "The Worker did not preserve the original journal and publish exactly one BEGIN, ROW and COMMIT");
    }

    private void acceptProduct(Path markers) throws Exception {
        Set<String> channels = Set.of("sales.items");
        DefaultMQProducer inspector = producer();
        try {
            MQClientAPIImpl api = api(inspector);
            Election initial = until(() -> election(api, 1, null), "the distinguished primary");
            require("rocket-primary:10911".equals(initial.address()), "Unexpected initial leader");
            write(markers, "leader-ready.json", Map.of("master", initial.address()));
            Election original = until(() -> election(api, 3, null), "both original synchronized followers");
            require(original.id() == initial.id(), "Enrolling followers changed the initial master");
            checkPolicies(api, original);
            requireRejectedAdapter();

            PipelineConfig.Target target = target();
            target.messaging.replicas = 2;
            target.messaging.rocketmqControllerEndpoints = List.of("rocketmq://" + CONTROLLER);
            RocketMessagePublisher.check(target);
            try (RocketMessagePublisher publisher = new RocketMessagePublisher(target, binding)) {
                until(() -> {
                    try {
                        publisher.prepare(channels);
                        return Boolean.TRUE;
                    } catch (SyncFailure failure) {
                        if (!failure.retryable) throw new AssertionError("Opt-in preparation rejected: " + failure.code, failure);
                        return null;
                    }
                }, "the production publisher creating and verifying the replicated topic");
                Cut owner = cut(publisher.confirmedProgress());
                require(owner.queueOffset() == 0, "Prepared ownership is not present at offset zero");
                awaitReplicatedTopic(api, original);

                publisher.publish(List.of(publication("retained-before-promotion")));
                String oldProgress = publisher.confirmedProgress();
                Cut before = cut(oldProgress);
                require(before.queueOffset() == 1 && before.commitLogOffset() > owner.commitLogOffset(), "Original production receipt is invalid");
                awaitHistory(api, original, owner, before);
                write(markers, "original-cut.json", Map.of("master", original.address(), "masterId", original.id(),
                        "epoch", original.epoch(), "syncStateEpoch", original.syncEpoch(), "liveInSync", original.liveInSync(),
                        "topicConfigurationReplicas", original.liveInSync(), "retained", before));
                until(() -> Files.isRegularFile(markers.resolve("original-killed")) ? Boolean.TRUE : null, "SIGKILL of the original master");

                Election promoted = until(() -> election(api, 2, original), "the promoted master and synchronized survivor");
                checkPolicies(api, promoted);
                requireRejectedAdapter();
                awaitTopic(api, promoted.address());
                awaitReplicatedTopic(api, promoted);
                awaitHistory(api, promoted, owner, before);

                require(oldProgress.equals(publisher.confirmedProgress()), "Checkpoint changed before the retry");
                publisher.publish(List.of(publication("appended-after-promotion")));
                String newProgress = publisher.confirmedProgress();
                Cut after = cut(newProgress);
                require(after.queueOffset() == before.queueOffset() + 1 && after.commitLogOffset() > before.commitLogOffset(),
                        "Production publisher did not append at the next logical and a later physical offset");
                for (String address : promoted.liveInSync()) verify(api, address, after, "appended-after-promotion");
                try (RocketMessagePublisher reopened = new RocketMessagePublisher(target, binding)) {
                    reopened.resumeProgress(newProgress);
                    reopened.require(channels);
                    require(newProgress.equals(reopened.confirmedProgress()), "Reopening did not authenticate the promoted native receipt");
                }
                write(markers, "promoted-cut.json", Map.of("master", promoted.address(), "masterId", promoted.id(),
                        "epoch", promoted.epoch(), "syncStateEpoch", promoted.syncEpoch(), "liveInSync", promoted.liveInSync(),
                        "topicConfigurationReplicas", promoted.liveInSync(), "retained", before, "appended", after,
                        "adapterOutcome", "CONTROLLER_PROFILE_PUBLISHED"));
                until(() -> Files.isRegularFile(markers.resolve("remaining-follower-killed")) ? Boolean.TRUE : null,
                        "the last follower to be killed");
                until(() -> election(api, 1, original), "the live SyncStateSet falling below two");
                try {
                    publisher.publish(List.of(publication("must-not-publish")));
                    throw new AssertionError("Publisher accepted one live replica");
                } catch (SyncFailure failure) {
                    require(failure.retryable && "ROCKETMQ_CONTROLLER_ROUTE_RETRY".equals(failure.code),
                            "Unexpected refusal under minimum: " + failure.code);
                }
                require(newProgress.equals(publisher.confirmedProgress())
                        && api.getMaxOffset(promoted.address(), queue, TIMEOUT) == after.queueOffset() + 1,
                        "Loss of the last follower changed the confirmed receipt or broker queue");
                write(markers, "minimum-refused.json", Map.of("status", "PASS", "confirmed", after));
            }
        } finally {
            inspector.shutdown();
        }
    }

    private Cut cut(String progress) {
        require(progress != null, "Production publisher returned no native position");
        String[] fields = progress.split(":", -1);
        require(fields.length == 5 && "ROCKETMQ_OFFSET_V1".equals(fields[0]) && topic.equals(fields[1]),
                "Production progress format or topic changed");
        return new Cut(Long.parseLong(fields[2]), Long.parseLong(fields[3]), fields[4]);
    }

    private static ConfirmedPublisher.Message publication(String id) {
        ConfirmedPublisher.Message result = new ConfirmedPublisher.Message();
        result.id = id;
        result.destination = "sales.items";
        result.key = "key-" + id;
        result.body = ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8);
        return result;
    }

    private static void write(Path markers, String name, Map<String,?> facts) throws Exception {
        Files.writeString(markers.resolve(name), Model.JSON.writeValueAsString(facts));
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

    private Election election(MQClientAPIImpl api, int expected, Election previous) throws Exception {
        ReplicasInfo replicas = api.getInSyncStateData(activeController, List.of(BROKER)).getReplicasInfoTable().get(BROKER);
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

    private void checkPolicies(MQClientAPIImpl api, Election election) throws Exception {
        Properties controller = api.getControllerConfig(List.of(activeController), TIMEOUT).get(activeController);
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

    private static PipelineConfig.Target target() {
        PipelineConfig.Target target = new PipelineConfig.Target();
        target.product = TargetProduct.ROCKETMQ;
        target.url = "rocketmq://" + NAMESERVER;
        target.messaging = new PipelineConfig.Messaging();
        target.messaging.cluster = CLUSTER;
        target.messaging.broker = BROKER;
        target.messaging.topic = PREFIX;
        target.messaging.requestTimeoutMillis = TIMEOUT;
        return target;
    }

    private static void requireRejectedAdapter() throws Exception {
        try {
            RocketMessagePublisher.check(target());
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
