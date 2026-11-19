package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class TopicIdProducerAcceptance {
    private static final String TOPIC = "rivomesh-fixture-topic-id";
    private static final String TRANSACTION = "rivomesh-fixture-pinned-transaction";
    private static final short VERSION = 13;

    public static void main(String[] args) throws Exception {
        String secret = System.getenv("RIVOMESH_KAFKA_FIXTURE_SECRET");
        require(secret != null && secret.matches("[a-f0-9]{48}"), "Fixture credentials are required");
        Properties adminProps = connection(args[0], "admin", secret);
        try (Admin admin = Admin.create(adminProps)) {
            var features = admin.describeFeatures().featureMetadata().get(15, TimeUnit.SECONDS).finalizedFeatures();
            require(features.get("transaction.version").maxVersionLevel() >= 2, "Transactional v13 requires Kafka transaction version 2");
            require(ApiKeys.PRODUCE.latestVersion() >= VERSION, "The exact fixture SDK lacks Produce v13");
            createTopic(admin);
            admin.createAcls(List.of(acl(ResourceType.TOPIC, TOPIC, AclOperation.WRITE),
                                    acl(ResourceType.TRANSACTIONAL_ID, TRANSACTION, AclOperation.WRITE)))
                    .all().get(12, TimeUnit.SECONDS);
            Uuid approved = awaitTopic(admin, null);
            Probe positive = attempt(args[0], secret, approved, false);
            require(positive.confirmed && positive.inspectCount >= 1 && positive.error == null,
                    "Producer did not write exactly the approved UUID with v13");
            require(offset(admin) > positive.offset && containsCommitted(adminProps, positive.offset),
                    "The approved native Topic has no matching read_committed record at the acknowledged offset");
            System.out.println("PASS standard Kafka transaction committed through a v13-only Produce builder carrying the approved Topic UUID.");

            admin.deleteTopics(List.of(TOPIC)).all().get(12, TimeUnit.SECONDS);
            long absentBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (admin.listTopics().names().get(5, TimeUnit.SECONDS).contains(TOPIC) && System.nanoTime() < absentBy)
                Thread.sleep(100);
            createTopic(admin);
            Uuid replacement = awaitTopic(admin, approved);
            require(!approved.equals(replacement) && offset(admin) == 0, "The replacement fixture must start empty");
            Probe negative = attempt(args[0], secret, approved, false);
            require(!negative.confirmed && "TOPIC_UUID_CHANGED".equals(negative.error), "Replacement did not fail on the approved UUID guard");
            require(offset(admin) == 0, "A Produce reached the same-name replacement");
            System.out.println("PASS the same name resolved to a new UUID. The strict transactional sender refused before Produce and the replacement offset stayed zero.");
            Probe staleWire = attempt(args[0], secret, approved, true);
            require(!staleWire.confirmed && staleWire.inspectCount >= 1 && staleWire.brokerError == Errors.UNKNOWN_TOPIC_ID.code(),
                    "The broker did not return UNKNOWN_TOPIC_ID for an explicit Produce v13 using the retired UUID");
            require(offset(admin) == 0, "A wire request carrying the retired UUID wrote to the replacement");
            System.out.println("PASS the Kafka broker rejected an actual Produce v13 for the retired UUID, leaving the replacement offset zero.");
        }
    }

    private record Probe(boolean confirmed, int inspectCount, String error, long offset, int brokerError) {}

    private static Probe attempt(String bootstrap, String secret, Uuid approved, boolean proveStaleWire) {
        Properties props = connection(bootstrap, "writer", secret);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "rivomesh-fixture-topic-pin");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTION);
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "8000");
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        var config = new ProducerConfig(props);
        var log = new LogContext("[Rivomesh UUID fixture] ");
        var time = Time.SYSTEM;
        var features = new ApiVersions();
        var metadata = new ProducerMetadata(100L, 500L, 1000L, 30_000L, log, new ClusterResourceListeners(), time);
        metadata.bootstrap(ClientUtils.parseAndValidateAddresses(config));
        var failure = new AtomicReference<String>();
        var observed = new AtomicInteger();
        var brokerError = new AtomicInteger(-1);
        var metrics = new Metrics();
        boolean committed = false;
        long acceptedOffset = -1;
        try (NetworkClient underlying = ClientUtils.createNetworkClient(config, metrics, "rivomesh-test",
                log, features, time, 1, metadata, null, null)) {
            var guarded = (KafkaClient)Proxy.newProxyInstance(KafkaClient.class.getClassLoader(), new Class<?>[]{KafkaClient.class},
                    (proxy, method, invocationArgs) -> {
                        if (method.getName().equals("newClientRequest") &&
                                invocationArgs[1] instanceof ProduceRequest.Builder builder) {
                            if (builder.latestAllowedVersion() < VERSION)
                                throw reject(failure, "PRODUCE_V13_UNAVAILABLE");
                            var contents = builder.build(VERSION).data();
                            if (contents.topicData().size() != 1) throw reject(failure, "TOPIC_UUID_CHANGED");
                            if (!contents.topicData().iterator().next().topicId().equals(approved)) {
                                if (!proveStaleWire) throw reject(failure, "TOPIC_UUID_CHANGED");
                                // Wire-level test only. An older metadata cache could still hold the retired UUID.
                                // Submit that UUID explicitly and inspect the actual broker response.
                                var current = contents.topicData().iterator().next();
                                var override = new ProduceRequestData.TopicProduceDataCollection();
                                override.add(new ProduceRequestData.TopicProduceData().setName(TOPIC)
                                        .setTopicId(approved).setPartitionData(current.partitionData()));
                                contents.setTopicData(override);
                                int callback = invocationArgs.length - 1;
                                require(invocationArgs[callback] instanceof RequestCompletionHandler,
                                        "Fixture did not intercept the native Produce response");
                                invocationArgs[callback] = (RequestCompletionHandler) response -> {
                                    if (response.responseBody() instanceof ProduceResponse produce)
                                        produce.data().responses().stream().filter(row -> approved.equals(row.topicId()))
                                                .flatMap(row -> row.partitionResponses().stream())
                                                .forEach(partition -> brokerError.set(partition.errorCode()));
                                };
                            }
                            var partitions = contents.topicData().iterator().next().partitionData();
                            if (partitions.size() != 1 || partitions.getFirst().index() != 0)
                                throw reject(failure, "APPROVED_PARTITION_CHANGED");
                            invocationArgs[1] = new ProduceRequest.Builder(VERSION, VERSION, contents);
                            observed.incrementAndGet();
                        }
                        try {
                            return method.invoke(underlying, invocationArgs);
                        } catch (InvocationTargetException wrapper) {
                            throw wrapper.getCause();
                        }
                    });
            var producer = new KafkaProducer<String, String>(config, null, null, metadata, guarded, null, features, time);
            try {
                producer.initTransactions();
                producer.beginTransaction();
                acceptedOffset = producer.send(new ProducerRecord<>(TOPIC, 0, "scope", "exact-topic"))
                        .get(12, TimeUnit.SECONDS).offset();
                producer.commitTransaction();
                committed = true;
            } catch (Exception rejected) {
                if (failure.get() == null) failure.set(rejected.getClass().getSimpleName());
            } finally {
                producer.close(Duration.ofMillis(500));
            }
        } finally {
            metrics.close();
        }
        return new Probe(committed, observed.get(), failure.get(), acceptedOffset, brokerError.get());
    }

    private static IllegalStateException reject(AtomicReference<String> failure, String code) {
        failure.compareAndSet(null, code);
        return new IllegalStateException(code);
    }

    private static Properties connection(String bootstrap, String role, String secret) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", bootstrap);
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("ssl.endpoint.identification.algorithm", "https");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"fixture-" + role + "\" password=\"" + secret + "-" + role + "\";");
        return props;
    }

    private static AclBinding acl(ResourceType resource, String name, AclOperation operation) {
        return new AclBinding(new ResourcePattern(resource, name, PatternType.LITERAL),
                new AccessControlEntry("User:fixture-writer", "*", operation, AclPermissionType.ALLOW));
    }

    private static void createTopic(Admin admin) throws Exception {
        admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short)1)
                .configs(Map.of("min.insync.replicas", "1", "unclean.leader.election.enable", "false"))))
                .all().get(12, TimeUnit.SECONDS);
    }

    private static Uuid awaitTopic(Admin admin, Uuid replaced) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (System.nanoTime() < deadline) {
            var topic = admin.describeTopics(List.of(TOPIC)).allTopicNames().get(5, TimeUnit.SECONDS).get(TOPIC);
            if (!topic.topicId().equals(Uuid.ZERO_UUID) && !topic.topicId().equals(replaced)
                    && topic.partitions().getFirst().isr().size() == 1) return topic.topicId();
            Thread.sleep(150);
        }
        throw new AssertionError("Timed out waiting for the intended physical Topic");
    }

    private static boolean containsCommitted(Properties adminProps, long offset) {
        var read = new Properties();
        read.putAll(adminProps);
        read.setProperty("isolation.level", "read_committed");
        read.setProperty("enable.auto.commit", "false");
        var partition = new TopicPartition(TOPIC, 0);
        try (var consumer = new KafkaConsumer<String, String>(read, new StringDeserializer(), new StringDeserializer())) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, offset);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (System.nanoTime() < deadline) {
                for (var row : consumer.poll(Duration.ofMillis(150))) {
                    if (row.offset() == offset && "scope".equals(row.key()) && "exact-topic".equals(row.value())) return true;
                }
            }
            return false;
        }
    }

    private static long offset(Admin admin) throws Exception {
        var partition = new TopicPartition(TOPIC, 0);
        return admin.listOffsets(Map.of(partition, OffsetSpec.latest())).partitionResult(partition).get(5, TimeUnit.SECONDS).offset();
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
