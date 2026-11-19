package io.github.rivomesh.kafka.notice;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.TopicIdKafkaProducer;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RetriableException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Business records share the existing wire format and one ordered physical Kafka partition. */
public final class TopicIdBusinessPublisher implements AutoCloseable {
    public record Wire(String key, byte[] body, Map<String, String> headers) {}
    public record Offset(String topic, int partition, long offset) {}

    private final String topic;
    private final TopicIdKafkaProducer transport;

    public TopicIdBusinessPublisher(Properties defaults, String username, String password, String topic,
                                    String topicId, String clusterSha256) {
        this.topic = topic;
        var settings = new Properties();
        settings.putAll(defaults);
        TopicIdTransactions.configureScram(settings, username, password);
        transport = new TopicIdKafkaProducer(new ProducerConfig(settings), topic, Uuid.fromString(topicId), clusterSha256);
    }

    public List<Offset> publish(List<Wire> batch, int requestMillis) throws Exception {
        var sends = new ArrayList<Future<RecordMetadata>>(batch.size());
        for (Wire item : batch) {
            transport.checkRefusal();
            var row = new ProducerRecord<>(topic, 0, item.key(), item.body());
            for (var header : item.headers().entrySet())
                row.headers().add(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8));
            sends.add(transport.producer().send(row));
        }
        var receipts = new ArrayList<Offset>(sends.size());
        for (var pending : sends) {
            var receipt = await(pending, requestMillis);
            if (!receipt.hasOffset() || receipt.partition() != 0 || !topic.equals(receipt.topic()))
                throw new IllegalStateException("KAFKA_PHYSICAL_NATIVE_OFFSET_INVALID");
            receipts.add(new Offset(receipt.topic(), receipt.partition(), receipt.offset()));
        }
        return receipts;
    }

    private RecordMetadata await(Future<RecordMetadata> pending, int requestMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(requestMillis);
        for (;;) {
            transport.checkRefusal();
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("KAFKA_PHYSICAL_SEND_UNCONFIRMED");
            try {
                var confirmed = pending.get(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)), TimeUnit.NANOSECONDS);
                transport.checkRefusal();
                return confirmed;
            } catch (TimeoutException waiting) {
                // Check the asynchronous address guard while awaiting a native result.
            } catch (ExecutionException failed) {
                if (failed.getCause() instanceof RetriableException cause) {
                    var retry = new TimeoutException("KAFKA_PHYSICAL_SEND_UNCONFIRMED");
                    retry.initCause(cause);
                    throw retry;
                }
                throw failed;
            }
        }
    }

    @Override public void close() {
        transport.close(Duration.ofSeconds(1));
    }
}
