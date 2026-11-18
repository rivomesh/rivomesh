package io.github.rivomesh.acceptance;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.MessageEvent;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/** Stages exact broker bytes before acknowledging deliveries on a local fixture connection. */
public final class ReplicatedRabbitAcceptance {
    private record Delivery(String queue, String id, boolean redelivered, String bodyBase64) {}

    private ReplicatedRabbitAcceptance() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !List.of("STAGE", "ACK").contains(args[1]))
            throw new IllegalArgumentException("Expected port, STAGE or ACK, and receipt path");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(Integer.parseInt(args[0]));
        factory.setUsername("rabbit_fixture");
        factory.setPassword(Objects.requireNonNull(System.getenv("RM_RABBIT_PASSWORD")));
        factory.setAutomaticRecoveryEnabled(false);
        factory.setConnectionTimeout(3000);
        factory.setHandshakeTimeout(5000);
        factory.setChannelRpcTimeout(5000);
        List<Delivery> deliveries = new ArrayList<>();
        try (Connection connection = factory.newConnection("rivomesh-fixture-consumer");
             Channel channel = connection.createChannel()) {
            long lastTag = 0;
            for (String queue : List.of("orders.__schema", "orders.__transactions", "orders.items")) {
                GetResponse message;
                while ((message = channel.basicGet(queue, false)) != null) {
                    if (deliveries.size() >= 100) throw new AssertionError("Fixture delivery limit exceeded");
                    byte[] body = message.getBody();
                    MessageEvent event = Codec.decode(new String(body, StandardCharsets.UTF_8), MessageEvent.class);
                    var properties = message.getProps();
                    var envelope = message.getEnvelope();
                    if (!Objects.equals(2, properties.getDeliveryMode()) || event.id == null
                            || !event.id.equals(properties.getMessageId())
                            || !event.id.equals(String.valueOf(properties.getHeaders().get("rivomesh_event_id")))
                            || !queue.equals(envelope.getRoutingKey()))
                        throw new AssertionError("Unexpected transport properties");
                    lastTag = envelope.getDeliveryTag();
                    deliveries.add(new Delivery(queue, event.id, envelope.isRedeliver(),
                            Base64.getEncoder().encodeToString(body)));
                }
            }

            // A transport receipt is staged independently of target business-transaction semantics.
            ByteBuffer bytes = StandardCharsets.UTF_8.encode(Codec.encode(deliveries) + "\n");
            try (FileChannel receipt = FileChannel.open(Path.of(args[2]),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                while (bytes.hasRemaining()) receipt.write(bytes);
                receipt.force(true);
            }
            if ("ACK".equals(args[1]) && lastTag > 0) channel.basicAck(lastTag, true);
        }
    }
}
