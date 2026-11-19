package io.github.rivomesh.kafka.notice;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.TopicIdKafkaProducer;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Kafka transactional notice writes constrained to the approved physical Topic. */
public final class TopicIdTransactions implements AutoCloseable {
    private static final Duration CLOSE = Duration.ofMillis(300);
    private final String name;
    private final TopicIdKafkaProducer transport;
    private boolean sent;

    public TopicIdTransactions(List<String> addresses, String mechanism, String username, String password,
                               String name, String topicId, String clusterSha256, String transactionId) {
        this.name = name;
        var id = Uuid.fromString(topicId);
        if (Uuid.ZERO_UUID.equals(id) || !clusterSha256.matches("[a-f0-9]{64}"))
            throw new IllegalArgumentException("KAFKA_NOTICE_TOPIC_INVALID");
        var settings = new Properties();
        settings.setProperty("bootstrap.servers", String.join(",", addresses));
        configureScram(settings, username, password);
        settings.setProperty("sasl.mechanism", mechanism);
        settings.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "rivomesh-kafka-notice-sender");
        settings.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        settings.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000");
        settings.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        settings.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        settings.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "6000");
        settings.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        settings.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        transport = new TopicIdKafkaProducer(new ProducerConfig(settings), name, id, clusterSha256);
        try {
            transport.producer().initTransactions();
            transport.producer().beginTransaction();
        } catch (RuntimeException failure) {
            close();
            throw failure;
        }
    }

    public long send(String noticeId, byte[] payload, String kind) throws Exception {
        if (sent || payload.length > 64 * 1024)
            throw new IllegalArgumentException("KAFKA_NOTICE_PAYLOAD_CHANGED");
        var record = new ProducerRecord<>(name, 0, noticeId, payload);
        record.headers().add("rivomesh_notice_kind", kind.getBytes(StandardCharsets.US_ASCII));
        var pending = transport.producer().send(record);
        long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < expires) {
            transport.checkRefusal();
            try {
                var result = pending.get(100, TimeUnit.MILLISECONDS);
                transport.checkRefusal();
                if (!result.hasOffset() || result.partition() != 0 || !name.equals(result.topic()))
                    throw new IllegalStateException("KAFKA_NOTICE_NATIVE_OFFSET_INVALID");
                sent = true;
                return result.offset();
            } catch (TimeoutException waiting) {
                // Check the asynchronous address guard even while the Kafka future is still unresolved.
            }
        }
        transport.checkRefusal();
        throw new TimeoutException("KAFKA_NOTICE_NATIVE_SEND_UNKNOWN");
    }

    public void commit() {
        transport.checkRefusal();
        if (!sent) throw new IllegalStateException("KAFKA_NOTICE_NATIVE_OFFSET_REQUIRED");
        transport.producer().commitTransaction();
    }

    @Override public void close() {
        transport.close(CLOSE);
    }

    static void configureScram(Properties settings, String username, String password) {
        settings.setProperty("security.protocol", "SASL_SSL");
        settings.setProperty("ssl.endpoint.identification.algorithm", "https");
        settings.setProperty("sasl.jaas.config", ScramLoginModule.class.getName() + " required username=\""
                + escape(username) + "\" password=\"" + escape(password) + "\";");
        settings.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        settings.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
