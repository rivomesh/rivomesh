package io.github.rivomesh.acceptance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.PipelineControls;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.ConfirmedPublisher;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.JdbcPublicationStore;
import io.github.rivomesh.cdc.Journal;
import io.github.rivomesh.cdc.MessageEvent;
import io.github.rivomesh.cdc.MessagePublishers;
import io.github.rivomesh.cdc.MessageSink;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.SubscriberFailure;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;
import io.github.rivomesh.platform.ClusterConfig;
import io.github.rivomesh.platform.ConsoleServer;
import io.github.rivomesh.platform.PlatformConfig;
import io.github.rivomesh.platform.TaskService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;

import java.io.FileOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Exercises the packaged Kafka publisher and JDBC receipts across isolated KRaft broker faults. */
public final class ReplicatedKafkaAcceptance {
    private static final String PIPELINE = "kafka-cluster";
    private static final String SUBSCRIPTION = "kafka";
    private static final String TOPIC = "rivomesh-events";
    private static final String PEER_TOPIC = "rivomesh-recovery-peer";
    private static final String GENERATION = "fixture-generation";
    private static final String UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String FINGERPRINT = Codec.sha256("rivomesh-kafka-cluster-fixture");
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Set<TopicPartition> PARTITIONS = Collections.singleton(PARTITION);
    private static final List<String> BOOTSTRAP = List.of("kafka-a:9092", "kafka-b:9092", "kafka-c:9092", "kafka-d:9092");
    private static final List<Integer> PLACEMENT = List.of(1, 2, 4);
    private static final int MAX_MESSAGE_BYTES = 65536;
    private static final Duration API_TIMEOUT = Duration.ofSeconds(15);
    private static final TableSchema TABLE = table();
    private static final String TASK_ROUTE = "/api/tasks/" + PIPELINE;
    private static final String RECEIPT_ROUTE = TASK_ROUTE + "/recovery-receipts/";
    private static final String REVIEW_ACTION = "REVIEW_KAFKA_RECEIPTS";
    private static final String FIXTURE_ADMIN = "fixture-admin-only-for-loopback-kafka-test";

    private record State(String clusterId, String topicId, int leader, Set<Integer> isr,
                         int metadataLeader, long metadataHighWatermark) {}
    private record Delivery(long offset, String channel, String id, String bodyBase64) {}

    private ReplicatedKafkaAcceptance() {}

    public static void main(String[] args) throws Exception {
        String phase = args[0];
        Set<Integer> active = new TreeSet<>();
        for (String node : args[1].split(",")) active.add(Integer.parseInt(node));
        Set<Integer> expectedIsr = PLACEMENT.stream().filter(active::contains).collect(Collectors.toSet());
        Path home = Path.of(args[2]);
        Map<String, Object> report = new LinkedHashMap<>();
        boolean review = Set.of("recovery-review","managed-recovery-review").contains(phase);

        try (Admin admin = Admin.create(client())) {
            await(() -> ids(get(admin.describeCluster().nodes())), active::equals);
            if ("initial".equals(phase) || review) createTopic(admin, TOPIC, MAX_MESSAGE_BYTES);
            if (review) createTopic(admin, PEER_TOPIC, 2 * MAX_MESSAGE_BYTES);
            State state = await(() -> state(admin), value -> value.isr.equals(expectedIsr)
                    && active.contains(value.leader) && value.metadataLeader <= 3
                    && active.contains(value.metadataLeader));
            report.put("cluster", state);
            if ("below-isr".equals(phase)) {
                // A new metadata commit distinguishes a data-replica shortage from controller quorum loss.
                get(admin.createTopics(List.of(new NewTopic("metadata-proof", 1, (short) 1)
                        .configs(Map.of("min.insync.replicas", "1")))).all());
                State after = await(() -> state(admin), current -> current.isr.equals(expectedIsr)
                        && current.metadataHighWatermark > state.metadataHighWatermark);
                report.put("metadataProofHighWatermark", after.metadataHighWatermark);
            }
        }

        switch (phase) {
            case "initial" -> publish(home, phase, 1, report);
            case "elected" -> publish(home, phase, 2, report);
            case "below-isr", "restored" -> publish(home, phase, 3, report);
            case "offsets" -> { }
            case "recovery-review", "managed-recovery-review" -> reviewPredecessor(home, report,"managed-recovery-review".equals(phase));
            default -> throw new IllegalArgumentException("Unknown fixture phase");
        }
        if (!"below-isr".equals(phase) && !review) consume(home, phase, report);
        System.out.println(Codec.encode(report));
    }

    private static Properties client() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", String.join(",", BOOTSTRAP));
        properties.setProperty("request.timeout.ms", "5000");
        properties.setProperty("default.api.timeout.ms", "15000");
        return properties;
    }

    private static void createTopic(Admin admin, String name, int maximumBytes) throws Exception {
        Map<String, String> settings = Map.of("cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1",
                "min.insync.replicas", "2", "unclean.leader.election.enable", "false",
                "max.message.bytes", Integer.toString(maximumBytes + 4096));
        get(admin.createTopics(List.of(new NewTopic(name, Map.of(0, PLACEMENT)).configs(settings))).all());
    }

    private static State state(Admin admin) throws Exception {
        String clusterId = get(admin.describeCluster().clusterId());
        var topic = get(admin.describeTopics(List.of(TOPIC)).allTopicNames()).get(TOPIC);
        var partition = topic.partitions().getFirst();
        require(topic.partitions().size() == 1 && ids(partition.replicas()).equals(Set.copyOf(PLACEMENT)),
                "Topic placement changed");
        var metadata = get(admin.describeMetadataQuorum().quorumInfo());
        require(Set.of(1, 2, 3).equals(metadata.voters().stream().map(replica -> replica.replicaId())
                .collect(Collectors.toSet())), "KRaft voters changed");
        return new State(clusterId, topic.topicId().toString(), partition.leader() == null ? -1 : partition.leader().id(),
                ids(partition.isr()), metadata.leaderId(), metadata.highWatermark());
    }

    private static Set<Integer> ids(Collection<Node> nodes) {
        return nodes.stream().map(Node::id).collect(Collectors.toCollection(TreeSet::new));
    }

    private static <T> T get(KafkaFuture<T> value) throws Exception {
        return value.get(API_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private interface Read<T> { T get() throws Exception; }

    private static <T> T await(Read<T> action, Predicate<T> accepted) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(70).toNanos();
        Throwable reason = null;
        T last = null;
        do {
            try {
                last = action.get();
                if (accepted.test(last)) return last;
            } catch (ExecutionException | TimeoutException | RetriableException retry) {
                reason = retry;
            }
            Thread.sleep(350);
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Kafka metadata did not converge, last observed " + last, reason);
    }

    private static TableSchema table() {
        TableSchema table = new TableSchema();
        table.id = TableSchema.identity("sales", "", "items");
        table.database = "sales";
        table.name = "items";
        table.primaryKey.add("id");
        TableSchema.Column id = new TableSchema.Column();
        id.name = "id";
        id.nullable = false;
        id.type = TableSchema.Type.INT64;
        TableSchema.Column label = new TableSchema.Column();
        label.name = "label";
        label.type = TableSchema.Type.STRING;
        table.columns.addAll(List.of(id, label));
        return table;
    }

    private static SourcePosition position(int sequence) {
        return new SourcePosition(1, "MYSQL", "fixture-source",
                Map.of("gtidSet", UUID + ":1-" + (100 + sequence), "serverUuid", UUID,
                        "file", "binlog.000001", "offset", Integer.toString(120 + sequence * 100)));
    }

    private static SourceTransaction transaction(int sequence) {
        SourceTransaction tx = new SourceTransaction();
        tx.id = UUID + ":" + (100 + sequence);
        tx.streamGeneration = GENERATION;
        tx.position = position(sequence);
        Change row = new Change();
        row.eventId = tx.id + ":0";
        row.tableId = TABLE.id;
        row.operation = Change.Operation.INSERT;
        row.after = List.of(Cell.value(Integer.toString(sequence + 1)), Cell.value("native-" + sequence));
        tx.changes.add(row);
        return tx;
    }

    private static PipelineConfig.Target configuredTarget() {
        PipelineConfig.Target target = new PipelineConfig.Target();
        target.id = SUBSCRIPTION;
        target.url = "kafka://" + BOOTSTRAP.getFirst();
        target.product = TargetProduct.KAFKA;
        target.mappings.put("sales", "orders");
        target.messaging = new PipelineConfig.Messaging();
        target.messaging.topic = TOPIC;
        target.messaging.endpoints = BOOTSTRAP.subList(1, BOOTSTRAP.size()).stream()
                .map(address -> "kafka://" + address).toList();
        target.messaging.autoCreate = false;
        target.messaging.replicas = 3;
        target.messaging.maxMessageBytes = MAX_MESSAGE_BYTES;
        target.messaging.requestTimeoutMillis = 5000;
        return target;
    }

    private static void publish(Path home, String phase, int sequence, Map<String, Object> report) throws Exception {
        JdbcJournal.Settings jdbc = new JdbcJournal.Settings();
        jdbc.pipelineId = PIPELINE;
        jdbc.driver = "org.h2.Driver";
        jdbc.url = "jdbc:h2:file:" + home.resolve("publication") + ";WRITE_DELAY=0";
        PipelineConfig.Target target=configuredTarget();

        try (JdbcJournal journal = new JdbcJournal(jdbc)) {
            journal.initialize(FINGERPRINT, GENERATION, List.of(SUBSCRIPTION));
            Journal.Lease lease = journal.claim("fixture-" + phase, 120000);
            JdbcPublicationStore store = new JdbcPublicationStore(journal, SUBSCRIPTION, lease);
            try (MessageSink sink = new MessageSink(SUBSCRIPTION, target.mappings, store,
                    () -> MessagePublishers.open(target, MessagePublishers.binding(PIPELINE, SUBSCRIPTION, GENERATION)),
                    MAX_MESSAGE_BYTES)) {
                var before = sink.activate(lease.epoch);
                require(before.sequence == sequence - 1, "Publication checkpoint did not reopen at the prior sequence");
                if (sequence == 1) {
                    prepareSnapshot(journal, sink, lease);
                } else {
                    sink.prepare(lease.epoch, TABLE);
                }
                report.put("snapshotId", store.snapshot(TABLE.id).snapshotId);
                SourceTransaction expected = transaction(sequence);
                if (!"restored".equals(phase)) require(journal.append(lease, expected) == sequence, "Source sequence changed");
                var pending = journal.read(SUBSCRIPTION, 1).getFirst();
                String expectedHash = Codec.sha256(Codec.encode(expected));
                require(pending.sequence == sequence && expectedHash.equals(Codec.sha256(Codec.encode(pending.transaction)))
                        && expected.position.equals(journal.captured()), "Retained source receipt or bytes changed");
                report.put("sourceTransactionId", expected.id);
                report.put("sourceTransactionHash", expectedHash);
                if ("below-isr".equals(phase)) {
                    try {
                        sink.apply(lease.epoch, pending.sequence, pending.transaction);
                        throw new AssertionError("Kafka confirmed while ISR was below min.insync.replicas");
                    } catch (SyncFailure retry) {
                        require(retry.retryable && "KAFKA_RETRY".equals(retry.code), "Unexpected rejection while ISR is below minimum");
                        report.put("retryCode", retry.code);
                    }
                    var blocked = sink.checkpoint();
                    require(before.sequence == blocked.sequence && before.transactionHash.equals(blocked.transactionHash)
                            && before.transactionId.equals(blocked.transactionId), "Unconfirmed publication advanced");
                } else {
                    sink.apply(lease.epoch, pending.sequence, pending.transaction);
                    journal.acknowledge(lease, SUBSCRIPTION, pending.sequence);
                    require(sink.checkpoint().sequence == sequence && expected.id.equals(sink.checkpoint().transactionId)
                            && expectedHash.equals(sink.checkpoint().transactionHash), "Publication receipt differs from source");
                }
                report.put("publicationReceipt", sink.checkpoint());
                report.put("journalAcknowledged", journal.subscriptions().getFirst().acknowledged);
            } finally {
                journal.release(lease);
            }
        }
    }

    private static void prepareSnapshot(JdbcJournal journal, MessageSink sink, Journal.Lease lease) throws Exception {
        journal.saveSchema(lease, TABLE);
        var snapshot = journal.beginSnapshot(lease, "source", TABLE, position(0));
        journal.appendSnapshot(lease, snapshot, 0, List.of(List.of(Cell.value("1"), Cell.value("snapshot"))));
        journal.finishSnapshot(lease, snapshot);
        deliverSnapshot(journal,sink,lease);
    }

    private static void deliverSnapshot(JdbcJournal journal, MessageSink sink, Journal.Lease lease) throws Exception {
        var snapshot = journal.snapshot("source", TABLE.id);
        sink.prepare(lease.epoch, TABLE);
        sink.beginSnapshot(lease.epoch, TABLE, snapshot);
        sink.snapshotPart(lease.epoch, TABLE, snapshot.snapshotId,
                journal.snapshotParts("source", TABLE.id, snapshot.snapshotId, 0, 1).getFirst());
        sink.finishSnapshot(lease.epoch, TABLE, snapshot);
    }

    private record Cli(int status,String body) {}
    private static Cli reviewCommand(Path home, Path configuration, long errorGeneration) throws Exception {
        Path stdout = home.resolve("review.out"), stderr = home.resolve("review.err");
        Process cli = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx128m", "-jar", "/app/rivomesh.jar",
                "review-kafka-recovery", configuration.toString(), SUBSCRIPTION, Long.toString(errorGeneration))
                .redirectOutput(stdout.toFile()).redirectError(stderr.toFile()).start();
        try {
            require(cli.waitFor(40, TimeUnit.SECONDS), "The read-only CLI did not return in its expected time");
            return new Cli(cli.exitValue(), Files.readString(cli.exitValue() == 0 ? stdout : stderr));
        } finally { if (cli.isAlive()) cli.destroyForcibly(); }
    }

    private static PlatformConfig management(Path home,String node){
        PlatformConfig config=new PlatformConfig();
        config.port=0;config.pollMillis=100;config.authToken=FIXTURE_ADMIN;
        config.tasksDirectory=home.resolve(node).resolve("tasks").toString();
        config.dataDirectory=home.resolve(node).resolve("data").toString();
        ClusterConfig cluster=new ClusterConfig();
        cluster.id="kafka-native-commands";cluster.nodeId=node;cluster.heartbeatMillis=250;cluster.leaseMillis=5000;
        cluster.driver="org.h2.Driver";cluster.url="jdbc:h2:mem:kafka-native-command;DB_CLOSE_DELAY=-1";
        cluster.username="sa";cluster.password="";
        cluster.sharedAccess=true;cluster.sharedTasks=true;cluster.recoveryReview=true;
        cluster.taskKey=Base64.getEncoder().encodeToString(new byte[32]);
        config.cluster=cluster;
        return config;
    }

    private static final class ManagedReview {
        private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        private final ConsoleServer entry,owner;

        ManagedReview(ConsoleServer entry,ConsoleServer owner){this.entry=entry;this.owner=owner;}

        JsonNode request(ConsoleServer server,String route,String id,JsonNode data)throws Exception{
            HttpRequest.Builder request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+server.port()+route))
                    .timeout(Duration.ofSeconds(5)).header("Authorization","Bearer "+FIXTURE_ADMIN);
            if(data!=null)request.header("X-Request-ID",id).header("Content-Type","application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(data.toString()));
            HttpResponse<String> reply=http.send(request.build(),HttpResponse.BodyHandlers.ofString());
            require(reply.statusCode()==(data==null?200:202),"Managed HTTP refused "+route+" "+reply.statusCode()+": "+reply.body());
            return Codec.JSON.readTree(reply.body());
        }

        JsonNode task()throws Exception{return request(entry,TASK_ROUTE,null,null);}

        JsonNode awaitOperation(String id)throws Exception{
            return await(this::task,task-> {
                for(JsonNode operation:task.path("operations"))if(id.equals(operation.path("id").asText())
                        &&Set.of("SUCCEEDED","REJECTED","FAILED").contains(operation.path("status").asText()))return true;
                return false;
            });
        }

        void create(PipelineConfig config)throws Exception{
            ObjectNode document=Codec.JSON.valueToTree(config);
            ((ObjectNode)document.get("journal")).remove("path");
            ObjectNode create=Codec.JSON.createObjectNode().put("ownerNodeId","owner");
            create.set("config",document);
            String id="managed_create_kafka";
            request(entry,"/api/tasks",id,create);
            JsonNode task=awaitOperation(id);
            require("owner".equals(task.path("ownerNodeId").asText())&&!task.path("configurationPending").asBoolean(true),
                    "Managed Kafka configuration did not reach the assigned owner");
        }

        ObjectNode action(String action)throws Exception{
            return Codec.JSON.createObjectNode().put("action",action).put("expectedRevision",task().path("revision").asText());
        }

        void pause()throws Exception{
            String id="managed_pause_kafka";
            request(entry,TASK_ROUTE+"/actions",id,action("PAUSE"));
            awaitOperation(id);
            await(this::task,task->"PAUSED".equals(task.path("desired").asText())
                    &&"PAUSED".equals(task.path("status").path("run_state").asText()));
        }

        JsonNode review(long generation,String id,boolean replay)throws Exception{
            ObjectNode body=action(REVIEW_ACTION).put("subscriptionId",SUBSCRIPTION).put("errorCount",generation);
            request(entry,TASK_ROUTE+"/actions",id,body);
            JsonNode result=await(()->request(entry,RECEIPT_ROUTE+id,null,null),
                    receipt->Set.of("SUCCEEDED","FAILED","REJECTED").contains(receipt.path("status").asText()));
            require("owner".equals(result.path("ownerNodeId").asText())&&"legacy-admin".equals(result.path("actorId").asText())
                    &&SUBSCRIPTION.equals(result.path("subscriptionId").asText()),"Managed receipt lost its task, actor or owner binding");
            if(replay){
                request(owner,TASK_ROUTE+"/actions",id,body);
                require(result.equals(request(owner,RECEIPT_ROUTE+id,null,null)),
                        "Identical replay through the second entry changed the historical receipt");
            }
            return result;
        }
    }

    private static void reviewPredecessor(Path home, Map<String,Object> report,boolean managed) throws Exception {
        PipelineConfig config = new PipelineConfig();
        config.pipelineId = PIPELINE;
        config.source.username = "fixture";
        config.source.clusterId = "fixture-source";
        config.databases = List.of("sales", "inventory");
        config.journal.path = home.resolve(managed?"owner/data/journals/"+PIPELINE:"review-journal").toString();
        PipelineConfig.Target target = configuredTarget();
        target.mappings.put("inventory", "catalog");
        config.targets.add(target);
        PipelineConfig.Target peer = configuredTarget();
        peer.id="kafka-peer";peer.messaging.topic=PEER_TOPIC;peer.messaging.maxMessageBytes=2*MAX_MESSAGE_BYTES;
        peer.mappings.put("inventory","catalog");
        config.targets.add(peer);
        config.validate();config.prepareLocalDirectory();
        Path configuration = home.resolve("review.json");
        Files.writeString(configuration, Codec.encode(config));
        if(managed){
            PlatformConfig entryConfig=management(home,"entry"),ownerConfig=management(home,"owner");
            try(TaskService entryTasks=new TaskService(entryConfig);TaskService ownerTasks=new TaskService(ownerConfig);
                ConsoleServer entry=new ConsoleServer(entryConfig,entryTasks);ConsoleServer owner=new ConsoleServer(ownerConfig,ownerTasks)){
                entry.start();owner.start();
                ManagedReview management=new ManagedReview(entry,owner);
                management.create(config);
                String receiptId="managed_verified_kafka";
                reviewPredecessor(home,configuration,config,management,receiptId,report);
                owner.close();
                ownerTasks.close();
                JsonNode historical=management.request(entry,RECEIPT_ROUTE+receiptId,null,null);
                require(historical.path("kafkaReview").equals(report.get("review")),
                        "Historical native proof disappeared when the execution owner went offline");
                report.put("managedHistoricalOwnerOffline",true);
            }
        }else reviewPredecessor(home,configuration,config,null,null,report);
    }

    private static void reviewPredecessor(Path home,Path configuration,PipelineConfig config,ManagedReview managed,
                                          String receiptId,Map<String,Object> report)throws Exception{
        PipelineConfig.Target target=config.targets.getFirst(),peer=config.targets.get(1);
        try (JdbcJournal journal = new JdbcJournal(config.journalSettings())) {
            journal.initialize(config.fingerprint(), GENERATION, config.subscribers());
            Journal.Lease lease = journal.claim("fixture-review", 120000);
            JdbcPublicationStore store = new JdbcPublicationStore(journal, SUBSCRIPTION, lease);
            JdbcPublicationStore peerStore = new JdbcPublicationStore(journal, peer.id, lease);
            SourceTransaction failed = transaction(2);
            Change failedRow=failed.changes.getFirst();
            failedRow.after=List.of(failedRow.after.getFirst(),Cell.value("x".repeat(MAX_MESSAGE_BYTES + 1000)));
            try (MessageSink sink = new MessageSink(SUBSCRIPTION, target.mappings, store,
                    () -> MessagePublishers.open(target, MessagePublishers.binding(PIPELINE, SUBSCRIPTION, GENERATION)),
                    MAX_MESSAGE_BYTES);
                 MessageSink peerSink = new MessageSink(peer.id,peer.mappings,peerStore,
                    () -> MessagePublishers.open(peer,MessagePublishers.binding(PIPELINE,peer.id,GENERATION)),
                    peer.messaging.maxMessageBytes)) {
                sink.activate(lease.epoch);
                peerSink.activate(lease.epoch);
                prepareSnapshot(journal, sink, lease);
                deliverSnapshot(journal,peerSink,lease);
                journal.append(lease, transaction(1));
                SourceTransaction first = journal.normalize(lease, journal.read(SUBSCRIPTION, 1).getFirst(),
                        Map.of(TABLE.id,TABLE), Map.of(TABLE.id,position(0)), (position, table) -> false);
                sink.apply(lease.epoch, 1, first);
                journal.acknowledge(lease, SUBSCRIPTION, 1);
                peerSink.apply(lease.epoch,1,first);
                journal.acknowledge(lease,peer.id,1);
                journal.append(lease, failed);
                SourceTransaction normalized = journal.normalize(lease, journal.read(SUBSCRIPTION, 1).getFirst(),
                        Map.of(TABLE.id,TABLE), Map.of(TABLE.id,position(0)), (position, table) -> false);
                try {
                    sink.apply(lease.epoch, 2, normalized);
                    throw new AssertionError("Oversize payload was sent");
                } catch (IllegalStateException expected) {
                    require("MESSAGE_TOO_LARGE".equals(expected.getMessage()), "Unexpected publication error");
                }
                SubscriberFailure diagnostic=new SubscriberFailure();
                diagnostic.phase=SubscriberFailure.Phase.TARGET_APPLY;
                diagnostic.sequence=2;diagnostic.transactionId=failed.id;
                diagnostic.projectedHash=Codec.sha256(Codec.encode(normalized));
                journal.pauseFailure(lease, SUBSCRIPTION, "MESSAGE_TOO_LARGE", diagnostic);
                peerSink.apply(lease.epoch,2,normalized);
                journal.acknowledge(lease,peer.id,2);
                require(peerSink.checkpoint().sequence==2&&diagnostic.projectedHash.equals(peerSink.checkpoint().transactionHash),
                        "Healthy peer lost the original failed transaction");
            }
            long errorGeneration = journal.subscriptions().getFirst().errorGeneration;
            if(managed==null){
                require(reviewCommand(home,configuration,errorGeneration).body().contains("KAFKA_RECOVERY_REQUIRES_PIPELINE_PAUSED"),
                        "Running pipeline incorrectly admitted");
                PipelineControls.pause(config);
            }else {
                JsonNode refused=managed.review(errorGeneration,"managed_refused_running",false);
                require("REJECTED".equals(refused.path("status").asText())
                        &&"KAFKA_RECOVERY_REQUIRES_PIPELINE_PAUSED".equals(refused.path("code").asText())
                        &&!refused.has("kafkaReview"),"Unpaused managed request produced a verified receipt");
                managed.pause();
            }
            Journal.FailureReview before=journal.inspectFailure(SUBSCRIPTION,errorGeneration,4*1024*1024);
            require(Codec.sha256(Codec.encode(failed)).equals(before.rawHash),"Original failed payload missing");
            Properties properties=client();
            properties.setProperty("group.id","retained-offset-review-fixture");
            properties.setProperty("enable.auto.commit","false");
            properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
            try (KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<>(properties)) {
                consumer.assign(PARTITIONS);
                long brokerEnd=consumer.endOffsets(PARTITIONS,API_TIMEOUT).get(PARTITION);
                TopicPartition peerPartition=new TopicPartition(PEER_TOPIC,0);
                long peerEnd=consumer.endOffsets(Set.of(peerPartition),API_TIMEOUT).get(peerPartition);
                report.put("peerEndBefore",peerEnd);
                List<Delivery> records=read(consumer,0,brokerEnd);
                consumer.commitSync(Map.of(PARTITION,new OffsetAndMetadata(1)),API_TIMEOUT);
                report.put("brokerEndBefore",brokerEnd);
                report.put("consumerOffsetBefore",consumer.committed(PARTITIONS,API_TIMEOUT).get(PARTITION).offset());
                JsonNode proof;
                if(managed==null){
                    Cli verified=reviewCommand(home,configuration,errorGeneration);
                    require(verified.status()==0,"Native predecessor review was rejected: "+verified.body());
                    proof=Codec.JSON.readTree(verified.body());
                    Cli replay=reviewCommand(home,configuration,errorGeneration);
                    require(replay.status()==0&&proof.path("nativeReceipts").equals(Codec.JSON.readTree(replay.body()).path("nativeReceipts")),
                            "Repeated review changed the native receipt evidence");
                }else{
                    JsonNode verified=managed.review(errorGeneration,receiptId,true);
                    require("SUCCEEDED".equals(verified.path("status").asText()),"Managed native predecessor review rejected: "+verified.path("code").asText());
                    proof=verified.path("kafkaReview");
                    report.put("managedActor",verified.path("actorId").asText());
                    report.put("managedOwner",verified.path("ownerNodeId").asText());
                    report.put("managedReplayedAcrossEntries",true);
                }
                require("PRECEDING_KAFKA_RECEIPTS_VERIFIED".equals(proof.path("status").asText()),"Wrong proof outcome");
                for(var receipt:proof.path("nativeReceipts")){
                    Delivery nativeRecord=records.get(Integer.parseInt(receipt.path("offset").asText()));
                    require((receipt.path("namespace").asText()+".__transactions").equals(nativeRecord.channel)
                            && Codec.sha256(Base64.getDecoder().decode(nativeRecord.bodyBase64)).equals(receipt.path("payloadSha256").asText()),
                            "Native predecessor receipt did not return its exact offset and bytes digest");
                }
                report.put("review",proof);
                report.put("brokerEndAfterReview",consumer.endOffsets(PARTITIONS,API_TIMEOUT).get(PARTITION));
                report.put("consumerOffsetAfterReview",consumer.committed(PARTITIONS,API_TIMEOUT).get(PARTITION).offset());
                require(records.equals(read(consumer,0,brokerEnd)),"Review altered native data");

                MessageEvent begin=new MessageEvent();begin.kind="BEGIN";begin.pipelineId=PIPELINE;begin.subscriptionId=SUBSCRIPTION;
                begin.generation=GENERATION;begin.namespace="orders";begin.transactionId=failed.id;begin.position=failed.position;
                begin.sequence=2;begin.id=PIPELINE+"/"+GENERATION+"/"+Codec.sha256(SUBSCRIPTION+":tx:"+failed.id+":orders:BEGIN");
                ConfirmedPublisher.Message message=new ConfirmedPublisher.Message();message.id=begin.id;message.key=failed.id;
                message.destination="orders.__transactions";message.body=Codec.encode(begin).getBytes(StandardCharsets.UTF_8);
                message.headers.put("rivomesh_kind","BEGIN");message.headers.put("rivomesh_event_id",begin.id);
                try (ConfirmedPublisher publisher=MessagePublishers.open(target,MessagePublishers.binding(PIPELINE,SUBSCRIPTION,GENERATION))) {
                    publisher.require(Set.of(message.destination));publisher.publish(List.of(message));
                }
                if(managed==null){
                    Cli partial=reviewCommand(home,configuration,errorGeneration);
                    require(partial.status()!=0&&partial.body().contains("KAFKA_RECOVERY_PUBLICATION_UNCERTAIN"),"Partial native publication was not refused");
                }else{
                    JsonNode partial=managed.review(errorGeneration,"managed_partial_kafka",false);
                    require("FAILED".equals(partial.path("status").asText())
                            &&"KAFKA_RECOVERY_PUBLICATION_UNCERTAIN".equals(partial.path("code").asText())
                            &&!partial.has("kafkaReview"),"Managed review verified a partial publication");
                }
                require(consumer.endOffsets(PARTITIONS,API_TIMEOUT).get(PARTITION)==brokerEnd+1
                        &&consumer.committed(PARTITIONS,API_TIMEOUT).get(PARTITION).offset()==1,"Review mutated broker offsets");
                report.put("peerEndAfter",consumer.endOffsets(Set.of(peerPartition),API_TIMEOUT).get(peerPartition));
                report.put("partialFailureCode","KAFKA_RECOVERY_PUBLICATION_UNCERTAIN");
            }
            Journal.FailureReview after=journal.inspectFailure(SUBSCRIPTION,errorGeneration,4*1024*1024);
            require(before.rawHash.equals(after.rawHash)&&before.normalizedHash.equals(after.normalizedHash),"Review changed original event bytes");
            report.put("journalAcknowledged",after.subscription.acknowledged);
            report.put("publicationAcknowledged",store.checkpoint().sequence);
            report.put("peerAcknowledged",journal.subscriptions().stream().filter(sub -> peer.id.equals(sub.id)).findFirst().orElseThrow().acknowledged);
        }
    }

    private static void consume(Path home, String phase, Map<String, Object> report) throws Exception {
        Properties properties = client();
        properties.setProperty("group.id", "retained-offset-fixture");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("allow.auto.create.topics", "false");
        properties.setProperty("auto.offset.reset", "none");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(PARTITIONS);
            var saved = consumer.committed(PARTITIONS, API_TIMEOUT).get(PARTITION);
            long start = saved == null ? 0 : saved.offset();
            require(("initial".equals(phase) && saved == null) || (!"initial".equals(phase) && saved != null),
                    "Unexpected consumer group offset");
            long end = consumer.endOffsets(PARTITIONS, API_TIMEOUT).get(PARTITION);
            require(start <= end && end <= 100, "Consumer range invalid");
            List<Delivery> all = read(consumer, 0, end);
            List<Delivery> pending = read(consumer, start, end);
            require(pending.equals(all.subList(Math.toIntExact(start), all.size())), "Committed offset resumed at the wrong record");
            byte[] bytes = (Codec.encode(pending) + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileOutputStream file = new FileOutputStream(home.resolve(phase + "-consumer.json").toFile())) {
                file.write(bytes);
                file.getFD().sync();
            }
            if ("initial".equals(phase) || "restored".equals(phase)) {
                consumer.commitSync(Map.of(PARTITION, new OffsetAndMetadata(end)), API_TIMEOUT);
            }
            report.put("storedOffsetBefore", start);
            report.put("storedOffsetAfter", consumer.committed(PARTITIONS, API_TIMEOUT).get(PARTITION).offset());
            report.put("stagedOffsets", pending.stream().map(Delivery::offset).toList());
            report.put("records", all);
        }
    }

    private static List<Delivery> read(KafkaConsumer<String, byte[]> consumer, long start, long end) {
        List<Delivery> records = new ArrayList<>();
        if (start == end) return records;
        consumer.seek(PARTITION, start);
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (start + records.size() < end && System.nanoTime() < deadline) {
            for (var record : consumer.poll(Duration.ofMillis(300))) {
                require(record.offset() == start + records.size() && record.offset() < end, "Unexpected Kafka offset");
                var event = Codec.decode(new String(record.value(), StandardCharsets.UTF_8), MessageEvent.class);
                String id = new String(record.headers().lastHeader("rivomesh_event_id").value(), StandardCharsets.UTF_8);
                String channel = new String(record.headers().lastHeader("rivomesh_channel").value(), StandardCharsets.UTF_8);
                require(id.equals(event.id), "Transport event ID differs from the message envelope");
                records.add(new Delivery(record.offset(), channel, id, Base64.getEncoder().encodeToString(record.value())));
            }
        }
        require(start + records.size() == end, "Kafka fetch did not reach the expected high watermark");
        return records;
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
