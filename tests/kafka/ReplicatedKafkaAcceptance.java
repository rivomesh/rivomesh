package io.github.rivomesh.acceptance;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.JdbcPublicationStore;
import io.github.rivomesh.cdc.Journal;
import io.github.rivomesh.cdc.MessageEvent;
import io.github.rivomesh.cdc.MessagePublishers;
import io.github.rivomesh.cdc.MessageSink;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
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

        try (Admin admin = Admin.create(client())) {
            await(() -> ids(get(admin.describeCluster().nodes())), active::equals);
            if ("initial".equals(phase)) createTopic(admin);
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
            default -> throw new IllegalArgumentException("Unknown fixture phase");
        }
        if (!"below-isr".equals(phase)) consume(home, phase, report);
        System.out.println(Codec.encode(report));
    }

    private static Properties client() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", String.join(",", BOOTSTRAP));
        properties.setProperty("request.timeout.ms", "5000");
        properties.setProperty("default.api.timeout.ms", "15000");
        return properties;
    }

    private static void createTopic(Admin admin) throws Exception {
        Map<String, String> settings = Map.of("cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1",
                "min.insync.replicas", "2", "unclean.leader.election.enable", "false",
                "max.message.bytes", Integer.toString(MAX_MESSAGE_BYTES + 4096));
        get(admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, PLACEMENT)).configs(settings))).all());
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

    private static void publish(Path home, String phase, int sequence, Map<String, Object> report) throws Exception {
        JdbcJournal.Settings jdbc = new JdbcJournal.Settings();
        jdbc.pipelineId = PIPELINE;
        jdbc.driver = "org.h2.Driver";
        jdbc.url = "jdbc:h2:file:" + home.resolve("publication") + ";WRITE_DELAY=0";
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
                    journal.saveSchema(lease, TABLE);
                    var snapshot = journal.beginSnapshot(lease, "source", TABLE, position(0));
                    journal.appendSnapshot(lease, snapshot, 0, List.of(List.of(Cell.value("1"), Cell.value("snapshot"))));
                    journal.finishSnapshot(lease, snapshot);
                    snapshot = journal.snapshot("source", TABLE.id);
                    sink.prepare(lease.epoch, TABLE);
                    sink.beginSnapshot(lease.epoch, TABLE, snapshot);
                    sink.snapshotPart(lease.epoch, TABLE, snapshot.snapshotId,
                            journal.snapshotParts("source", TABLE.id, snapshot.snapshotId, 0, 1).getFirst());
                    sink.finishSnapshot(lease.epoch, TABLE, snapshot);
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
