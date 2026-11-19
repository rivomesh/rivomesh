package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicReference;

/** Version-specific bridge. Maven relocates this Kafka package with its exact SDK. */
public final class TopicIdKafkaProducer {
    private static final short PRODUCE_TOPIC_ID_VERSION = 13;
    private final KafkaProducer<String, byte[]> producer;
    private final Metrics networkMetrics;
    private final AtomicReference<IllegalStateException> refusal;

    public TopicIdKafkaProducer(ProducerConfig config, String name, Uuid id, String clusterSha256) {
        var time = Time.SYSTEM;
        String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        var log = new LogContext("[" + clientId + "] ");
        var versions = new ApiVersions();
        var metadata = new ProducerMetadata(100L, 500L, 1000L, 30_000L, log, new ClusterResourceListeners(), time);
        metadata.bootstrap(ClientUtils.parseAndValidateAddresses(config));
        refusal = new AtomicReference<>();
        networkMetrics = new Metrics();
        NetworkClient network = null;
        try {
            network = ClientUtils.createNetworkClient(config, networkMetrics, clientId, log,
                    versions, time, 1, metadata, null, null);
            KafkaClient transport = guarded(network, metadata, name, id, clusterSha256);
            producer = new KafkaProducer<>(config, null, null, metadata, transport, null, versions, time);
        } catch (RuntimeException unavailable) {
            try {
                if (network != null) network.close();
            } finally {
                networkMetrics.close();
            }
            throw unavailable;
        }
    }

    public KafkaProducer<String, byte[]> producer() {
        return producer;
    }

    public void checkRefusal() {
        IllegalStateException rejected = refusal.get();
        if (rejected != null) throw rejected;
    }

    public void close(Duration timeout) {
        try {
            producer.close(timeout);
        } finally {
            networkMetrics.close();
        }
    }

    private KafkaClient guarded(NetworkClient network, ProducerMetadata metadata, String name, Uuid id, String clusterSha256) {
        return (KafkaClient) Proxy.newProxyInstance(KafkaClient.class.getClassLoader(), new Class<?>[]{KafkaClient.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("newClientRequest") && args[1] instanceof ProduceRequest.Builder builder) {
                        checkRefusal();
                        String cluster = metadata.fetch().clusterResource().clusterId();
                        if (cluster == null || !clusterSha256.equals(digest("KAFKA:" + cluster)))
                            throw reject("KAFKA_NOTICE_CONTROL_CLUSTER_CHANGED");
                        if (builder.latestAllowedVersion() < PRODUCE_TOPIC_ID_VERSION)
                            throw reject("KAFKA_NOTICE_TOPIC_ID_PRODUCE_REQUIRED");
                        var request = builder.build(PRODUCE_TOPIC_ID_VERSION).data();
                        if (request.topicData().size() != 1)
                            throw reject("KAFKA_NOTICE_CONTROL_TOPIC_CHANGED");
                        var topic = request.topicData().iterator().next();
                        if (!name.equals(topic.name()) || !id.equals(topic.topicId()))
                            throw reject("KAFKA_NOTICE_CONTROL_TOPIC_CHANGED");
                        if (topic.partitionData().size() != 1 || topic.partitionData().getFirst().index() != 0)
                            throw reject("KAFKA_NOTICE_CONTROL_PARTITION_CHANGED");
                        args[1] = new ProduceRequest.Builder(PRODUCE_TOPIC_ID_VERSION, PRODUCE_TOPIC_ID_VERSION, request);
                    }
                    try {
                        return method.invoke(network, args);
                    } catch (InvocationTargetException wrapper) {
                        throw wrapper.getCause();
                    }
                });
    }

    private IllegalStateException reject(String code) {
        refusal.compareAndSet(null, new IllegalStateException(code));
        return refusal.get();
    }

    private static String digest(String value) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    }
}
