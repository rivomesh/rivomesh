package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A private retained Rabbit stream for publisher confirmations. Business deliveries are never read here. */
final class RabbitReceiptStream {
    private static final int LIMIT = 16 * 1024 * 1024, SEGMENT = 1024 * 1024;
    private static final int JSON_LIMIT = 128 * 1024;
    private final String binding, vhost, stream, authorization;
    private final URI management;
    private final Duration timeout;
    private final HttpClient http;
    private String identity;

    RabbitReceiptStream(PipelineConfig.Target target, String binding, String vhost) {
        this.binding = binding;
        this.vhost = vhost;
        stream = binding==null ? null : "rivomesh.receipts." + Codec.sha256(binding);
        management = URI.create(target.messaging.adminUrl);
        authorization = "Basic " + Base64.getEncoder().encodeToString(
                (target.username + ":" + target.password).getBytes(StandardCharsets.UTF_8));
        timeout = Duration.ofMillis(Math.min(5000, target.messaging.requestTimeoutMillis));
        http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(
                Math.min(3000, target.messaging.connectTimeoutMillis))).build();
    }

    String identity() { return identity; }

    void checkBroker(Connection connection) throws Exception {
        identityFor(connection);
    }

    private String identityFor(Connection connection) throws Exception {
        String wire = Objects.toString(connection.getServerProperties().get("cluster_name"), "");
        JsonNode managementInfo = get("/api/overview", false);
        if (wire.isBlank() || !wire.equals(managementInfo.path("cluster_name").asText()))
            throw refused("RABBIT_RECEIPT_MANAGEMENT_BROKER_MISMATCH");
        return "RABBIT_STREAM_V1:" + wire + ":" + vhost + ":" + stream;
    }

    void prepare(Connection connection, Channel sender, boolean allowCreate, int replicas) throws Exception {
        if (identity != null) return;
        String found = identityFor(connection);
        JsonNode metadata = streamMetadata();
        if (metadata == null && allowCreate) {
            sender.queueDeclare(stream, true, false, false, Map.of(
                    "x-queue-type", "stream", "x-max-length-bytes", LIMIT,
                    "x-stream-max-segment-size-bytes", SEGMENT, "x-initial-cluster-size", replicas));
            metadata = streamMetadata();
        }
        checkStream(metadata);
        identity = found;
    }

    private JsonNode streamMetadata() throws Exception {
        String path = "/api/queues/" + URLEncoder.encode(vhost, StandardCharsets.UTF_8).replace("+","%20")
                + "/" + stream + "?disable_stats=true";
        return get(path, true);
    }

    private void checkStream(JsonNode metadata) {
        if (metadata == null) throw refused("RABBIT_RECEIPT_STREAM_MISSING_REQUIRES_PLAN");
        JsonNode arguments = metadata.path("arguments");
        if (!stream.equals(metadata.path("name").asText()) || !vhost.equals(metadata.path("vhost").asText())
                || !"stream".equals(metadata.path("type").asText()) || !metadata.path("durable").asBoolean()
                || metadata.path("auto_delete").asBoolean() || metadata.path("exclusive").asBoolean()
                || arguments.path("x-max-length-bytes").asLong() != LIMIT
                || arguments.path("x-stream-max-segment-size-bytes").asLong() != SEGMENT)
            throw refused("RABBIT_RECEIPT_STREAM_POLICY_CHANGED");
    }

    private JsonNode get(String path, boolean missingAllowed) throws Exception {
        var request = HttpRequest.newBuilder(management.resolve(path)).timeout(timeout)
                .header("Authorization", authorization).header("Accept", "application/json").GET().build();
        var pending = http.sendAsync(request, ignored -> new LimitedBody());
        try {
            var response = pending.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (response.statusCode() == 404 && missingAllowed) return null;
            if (response.statusCode() == 401 || response.statusCode() == 403)
                throw refused("RABBIT_RECEIPT_MANAGEMENT_AUTHORIZATION_REQUIRED");
            if (response.statusCode() != 200) throw managementUnavailable(null);
            return Codec.JSON.readTree(response.body());
        } catch (InterruptedException interrupted) {
            pending.cancel(true);
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (TimeoutException expired) {
            pending.cancel(true);
            throw managementUnavailable(expired);
        } catch (ExecutionException failed) {
            throw managementUnavailable(failed);
        }
    }

    void close() {
        http.shutdownNow();
    }

    private static SyncFailure managementUnavailable(Exception cause) {
        return new SyncFailure("RABBIT_RECEIPT_MANAGEMENT_UNAVAILABLE", "MESSAGE_PUBLICATION", true,
                "RabbitMQ management evidence is temporarily unavailable", cause, null, 0, null);
    }

    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final HttpResponse.BodySubscriber<byte[]> delegate=HttpResponse.BodySubscribers.ofByteArray();
        private Flow.Subscription subscription;
        private int remaining=JSON_LIMIT;

        public CompletionStage<byte[]> getBody(){return delegate.getBody();}
        public void onSubscribe(Flow.Subscription source){subscription=source;delegate.onSubscribe(source);}
        public void onNext(List<ByteBuffer> buffers){
            for(ByteBuffer buffer:buffers){
                remaining-=buffer.remaining();
                if(remaining<0){
                    subscription.cancel();
                    delegate.onError(new IOException("RABBIT_RECEIPT_MANAGEMENT_RESPONSE_LIMIT"));
                    return;
                }
            }
            delegate.onNext(buffers);
        }
        public void onError(Throwable failure){delegate.onError(failure);}
        public void onComplete(){delegate.onComplete();}
    }

    static boolean terminal(ConfirmedPublisher.Message message) {
        String kind = message.headers.get("rivomesh_kind");
        return "COMMIT".equals(kind) || "SNAPSHOT_END".equals(kind);
    }

    String retain(Connection connection, Channel sender, ConfirmedPublisher.Message message, int confirmTimeout) throws Exception {
        if (identity == null) throw refused("RABBIT_RECEIPT_STREAM_NOT_PREPARED");
        String receipt = prefix(message.id, message.headers.get("rivomesh_kind")) + Codec.sha256(message.body) + "\n" + UUID.randomUUID();
        String digest = Codec.sha256(receipt);
        sender.basicPublish("", stream, true, new AMQP.BasicProperties.Builder().messageId(message.id)
                .contentType("text/plain").deliveryMode(2).build(), receipt.getBytes(StandardCharsets.UTF_8));
        sender.waitForConfirmsOrDie(confirmTimeout);
        Delivery retained = read(connection, "last", message.id, digest);
        return "RABBIT_V1:" + offset(retained) + ":" + digest;
    }

    private String prefix(String event, String kind) {
        return "RIVOMESH_RABBIT_RECEIPT_V1\n" + binding + "\n" + event + "\n" + kind + "\n";
    }

    static String[] parseProgress(String progress) {
        if (progress == null || !progress.matches("RABBIT_V1:[0-9]{1,19}:[a-f0-9]{64}"))
            throw uncertain();
        return progress.split(":", 3);
    }

    void verify(Connection connection, String progress, String identityHash, String expectedEvent) throws Exception {
        String[] token = parseProgress(progress);
        String before = identityFor(connection);
        if (!Codec.sha256(before).equals(identityHash)) throw uncertain();
        checkStream(streamMetadata());
        long expectedOffset = Long.parseLong(token[1]);
        Delivery record = read(connection, expectedOffset, expectedEvent, token[2]);
        if (offset(record) != expectedOffset || !new String(record.getBody(), StandardCharsets.UTF_8)
                .startsWith(prefix(expectedEvent, "COMMIT"))) throw uncertain();
        checkStream(streamMetadata());
        if (!before.equals(identityFor(connection))) throw uncertain();
    }

    private static long offset(Delivery message) {
        var headers = message.getProperties().getHeaders();
        Object value = headers == null ? null : headers.get("x-stream-offset");
        if (!(value instanceof Number position) || position.longValue() < 0) throw uncertain();
        return position.longValue();
    }

    private Delivery read(Connection connection, Object start, String event, String digest) throws Exception {
        CompletableFuture<Delivery> match = new CompletableFuture<>();
        Channel reader = connection.createChannel();
        try {
            reader.basicQos(256);
            reader.basicConsume(stream, false, Map.of("x-stream-offset", start), (tag, record) -> {
                try {
                    if (start instanceof Long expected && offset(record) != expected) throw uncertain();
                    if (event.equals(record.getProperties().getMessageId()) && record.getBody().length <= 512
                            && Codec.sha256(record.getBody()).equals(digest)) match.complete(record);
                    else if (start instanceof Long) throw uncertain();
                } catch (IllegalStateException changed) { match.completeExceptionally(changed); }
            }, tag -> match.completeExceptionally(uncertain()));
            return match.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException missing) {
            throw uncertain();
        } finally {
            try{reader.abort();}catch(IOException ignored){/* The probe cannot retain a delivery or mutate its offset. */}
        }
    }

    private static IllegalStateException uncertain() {
        return refused("RABBIT_RECOVERY_PUBLICATION_UNCERTAIN");
    }

    private static IllegalStateException refused(String code) {
        return new IllegalStateException(code);
    }
}
