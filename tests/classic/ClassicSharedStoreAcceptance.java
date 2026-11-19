package io.github.rivomesh.acceptance;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.cdc.ClassicAdminClient;
import io.github.rivomesh.cdc.ClassicMessagePublisher;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.ConfirmedPublisher;
import io.github.rivomesh.cdc.TargetProduct;
import io.github.rivomesh.classic.internal.jms.BytesMessage;
import io.github.rivomesh.classic.internal.jms.Connection;
import io.github.rivomesh.classic.internal.jms.DeliveryMode;
import io.github.rivomesh.classic.internal.jms.Message;
import io.github.rivomesh.classic.internal.jms.MessageConsumer;
import io.github.rivomesh.classic.internal.jms.Session;
import io.github.rivomesh.transport.classic.ClassicTransport;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Compile only against the packaged shaded candidate, including its relocated JMS API. */
public final class ClassicSharedStoreAcceptance {
    private static final String BROKER = "rivomesh-core";
    private static final String CHANNEL = "sales.items";
    private static final Set<String> DESTINATIONS = Set.of(CHANNEL);

    public static void main(String[] args) throws Exception {
        String wireAddress = "127.0.0.1:" + args[0];
        int jolokiaPort = Integer.parseInt(args[1]);
        Path markers = Path.of(args[2]);
        PipelineConfig.Target target = new PipelineConfig.Target();
        target.product = TargetProduct.ACTIVEMQ_CLASSIC;
        target.url = "activemq://" + wireAddress;
        target.username = "admin";
        target.password = System.getenv("RIVO_CLASSIC_TEST_PUBLISHER");
        target.messaging = new PipelineConfig.Messaging();
        target.messaging.stream = "rivomesh";
        target.messaging.broker = BROKER;
        target.messaging.adminUrl = "http://127.0.0.1:" + jolokiaPort + "/api/jolokia/";
        target.messaging.connectTimeoutMillis = 1200;
        target.messaging.requestTimeoutMillis = 2500;
        ClassicAdminClient management = new ClassicAdminClient(target.messaging.adminUrl, target.username, target.password,
                BROKER, target.messaging.connectTimeoutMillis, target.messaging.requestTimeoutMillis);
        String binding = "rivomesh:" + Codec.sha256(UUID.randomUUID().toString());
        String queue = ClassicTransport.physicalQueue(target.messaging.stream, binding);
        String receipts = queue + ".receipts";

        String activeId = brokerId(management);
        ClassicMessagePublisher.check(target);
        require(!management.exists(queue) && !management.exists(receipts), "Connection check created queues");

        String owner;
        Path progress = markers.resolve("confirmed-progress");
        try (ClassicMessagePublisher first = new ClassicMessagePublisher(target, binding)) {
            first.prepare(DESTINATIONS);
            owner = first.identity();
            require(owner != null && owner.startsWith("CLASSIC_OWNER_V1:"), "Owner absent");
            first.publish(List.of(event("before-promotion")));
            consume(wireAddress, queue, "before-promotion");
            require(management.count(queue) == 1, "The EVENT consumer removed the owner or retained a consumed event");
            first.publish(List.of(event("retained-during-promotion")));
            String confirmed = first.confirmedProgress();
            checkReceipt(management, receipts, confirmed);
            require(management.count(queue) == 2 && management.count(receipts) == 3, "Primary queues do not match the original cut");
            Files.writeString(progress, confirmed);
            signal(markers, "primary-confirmed");
            await(markers, "gateway-isolated");
            try {
                first.publish(List.of(event("retry-after-promotion")));
                throw new AssertionError("A disconnected OpenWire/Jolokia route confirmed publication");
            } catch (SyncFailure failure) {
                require(failure.retryable && "MESSAGE_PUBLICATION".equals(failure.stage),
                        "Outage was not a retryable publish failure: " + failure.code);
            }
            require(confirmed.equals(first.confirmedProgress()), "Receipt advanced while both endpoints were isolated");
        }
        signal(markers, "outage-confirmed");
        await(markers, "standby-routed");

        String promotedId = brokerId(management);
        require(!activeId.equals(promotedId), "Management still sees the original broker instance");
        try (ClassicMessagePublisher promoted = new ClassicMessagePublisher(target, binding)) {
            String confirmed = Files.readString(progress);
            promoted.resumeProgress(confirmed);
            promoted.require(DESTINATIONS);
            require(owner.equals(promoted.identity()), "The promoted broker changed the established publication owner");
            checkReceipt(management, receipts, confirmed);
            require(management.count(queue) == 2 && management.count(receipts) == 3, "The shared store cut changed during promotion");
            promoted.publish(List.of(event("retry-after-promotion")));
            String retry = promoted.confirmedProgress();
            require(!confirmed.equals(retry), "Retry did not advance the native receipt");
            checkReceipt(management, receipts, retry);
            consume(wireAddress, queue, "retained-during-promotion", "retry-after-promotion");
            require(management.count(queue) == 1 && management.count(receipts) == 4, "EVENT consumption changed owners or receipts");
            signal(markers, "standby-confirmed");
            await(markers, "old-primary-waiting");

            require(promotedId.equals(brokerId(management)), "Restarted original primary changed the active route");
            promoted.publish(List.of(event("after-old-primary-rejoined")));
            checkReceipt(management, receipts, promoted.confirmedProgress());
            consume(wireAddress, queue, "after-old-primary-rejoined");
            require(owner.equals(promoted.identity()) && management.count(queue) == 1 && management.count(receipts) == 5,
                    "Rejoining the old broker changed the owner or receipt counts");
        }
    }

    private static String brokerId(ClassicAdminClient admin) throws Exception {
        ClassicTransport.Broker broker = admin.broker();
        require(broker.persistent && !broker.slave && "5.19.12".equals(broker.version) && BROKER.equals(broker.name),
                "The management route does not report the active Classic 5.19.12 KahaDB broker");
        return broker.id;
    }

    private static void checkReceipt(ClassicAdminClient admin, String receipts, String progress) throws Exception {
        require(progress != null, "The native confirmation was not recorded");
        String[] fields = progress.split(":", -1);
        require(fields.length == 4 && "CLASSIC_RECEIPT_V1".equals(fields[0]), "Invalid native progress");
        String messageId = new String(Base64.getUrlDecoder().decode(fields[2]), StandardCharsets.UTF_8);
        require(("RIVOMESH_RECEIPT_V1:" + fields[3]).equals(admin.text(receipts, messageId)),
                "The original native receipt and digest are not readable");
    }

    private static ConfirmedPublisher.Message event(String id) {
        ConfirmedPublisher.Message event = new ConfirmedPublisher.Message();
        event.id = id;
        event.destination = CHANNEL;
        event.key = "row-1";
        event.body = ("{\"event\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8);
        return event;
    }

    private static void consume(String address, String queue, String... expected) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + address
                + "?connectionTimeout=1200&soTimeout=2500");
        factory.setWatchTopicAdvisories(false);
        factory.setConnectResponseTimeout(1200);
        factory.setCloseTimeout(2500);
        Connection connection = factory.createConnection("consumer", System.getenv("RIVO_CLASSIC_TEST_READER"));
        try {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queue), ClassicTransport.EVENT_SELECTOR);
            connection.start();
            for (String id : expected) {
                Message received = consumer.receive(8000);
                require(received instanceof BytesMessage, "The selected consumer did not receive an event");
                BytesMessage message = (BytesMessage) received;
                byte[] payload = new byte[(int) message.getBodyLength()];
                require(message.readBytes(payload) == payload.length && Arrays.equals(payload, event(id).body)
                                && id.equals(message.getStringProperty("rivomesh_event_id"))
                                && CHANNEL.equals(message.getStringProperty("rivomesh_channel"))
                                && "row-1".equals(message.getStringProperty("rivomesh_key"))
                                && message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT,
                        "Native EVENT identity, envelope or durability changed");
            }
            require(consumer.receive(300) == null, "Unexpected additional visible event");
            session.commit();
        } finally {
            connection.close();
        }
    }

    private static void signal(Path markers, String phase) throws Exception {
        Files.createFile(markers.resolve(phase));
    }

    private static void await(Path markers, String phase) throws Exception {
        long end = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!Files.exists(markers.resolve(phase)) && System.nanoTime() < end) Thread.sleep(100);
        require(Files.exists(markers.resolve(phase)), "Supervisor did not reach " + phase);
    }

    private static void require(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
}
