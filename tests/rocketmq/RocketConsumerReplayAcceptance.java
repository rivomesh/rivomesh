package io.github.rivomesh.acceptance;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.MessageEvent;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Fixture-only consumer showing local business commit before the native group offset. */
final class RocketConsumerReplayAcceptance {
    private static final String GROUP = "rivomesh_fixture_consumer";

    private record Delivery(String id, String digest, MessageEvent event) {}
    private record Staged(int duplicates, long businessCommits) {}

    static Map<String, Object> verify(MQClientAPIImpl api, String master, Path file, String nameserver, MessageQueue queue,
                                      long startOffset, long middleOffset, long finalOffset, String first, String second) throws Exception {
        var group = new SubscriptionGroupConfig();
        group.setGroupName(GROUP);
        api.createSubscriptionGroup(master, group, 5000);
        String inbox = "jdbc:h2:file:" + file.toAbsolutePath();
        initialize(inbox);
        DefaultMQPullConsumer consumer = open(nameserver);
        try {
            commit(consumer, queue, startOffset);
            Staged firstStage = stage(inbox, pull(consumer, queue, startOffset, middleOffset), first);
            require(firstStage.equals(new Staged(0, 1)), "First business transaction did not stage once");
            require(consumer.fetchConsumeOffset(queue, true) == startOffset,
                    "The broker offset moved before application acknowledgement");
        } finally {
            consumer.shutdown();
        }

        consumer = open(nameserver);
        try {
            long reopened = consumer.fetchConsumeOffset(queue, true);
            require(reopened == startOffset, "New consumer did not reopen the original group offset");
            Staged replay = stage(inbox, pull(consumer, queue, reopened, middleOffset), first);
            require(replay.equals(new Staged(3, 1)), "Redelivery duplicated business state or changed the original bytes");
            Staged next = stage(inbox, pull(consumer, queue, middleOffset, finalOffset), second);
            require(next.equals(new Staged(0, 2)), "The next retained GTID did not commit exactly once");
            commit(consumer, queue, finalOffset);
        } finally {
            consumer.shutdown();
        }

        consumer = open(nameserver);
        try {
            require(consumer.fetchConsumeOffset(queue, true) == finalOffset,
                    "The new group offset was not durable for a fresh consumer");
        } finally {
            consumer.shutdown();
        }
        return Map.of("reopenedOffset", startOffset, "committedOffset", finalOffset,
                "deduplicatedEvents", 3, "committedBusinessTransactions", 2);
    }

    private static DefaultMQPullConsumer open(String nameserver) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(GROUP);
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setNamesrvAddr(nameserver);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        return consumer;
    }

    private static void commit(DefaultMQPullConsumer consumer, MessageQueue queue, long offset) throws Exception {
        consumer.getDefaultMQPullConsumerImpl().updateConsumeOffsetToBroker(queue, offset, false);
        require(consumer.fetchConsumeOffset(queue, true) == offset, "Could not read the committed consumer offset");
    }

    private static List<Delivery> pull(DefaultMQPullConsumer consumer, MessageQueue queue, long start, long end) throws Exception {
        List<Delivery> result = new ArrayList<>();
        long offset = start;
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (offset < end) {
            var response = consumer.pull(queue, "*", offset, (int)(end - offset), 5000);
            if (response.getPullStatus() != PullStatus.FOUND) {
                require(System.nanoTime() < deadline, "The native consumer could not read the retained transaction");
                Thread.sleep(200);
                continue;
            }
            for (var message : response.getMsgFoundList()) {
                require(message.getQueueOffset() == offset++, "The native consumer received a gap or reordered record");
                MessageEvent event = Model.JSON.readValue(message.getBody(), MessageEvent.class);
                require(event.id.equals(message.getUserProperty("rivomesh_event_id")), "Event ID differs from its envelope");
                result.add(new Delivery(event.id, Codec.sha256(message.getBody()), event));
            }
        }
        require(offset == end, "The native consumer crossed the requested transaction boundary");
        return result;
    }

    private static void initialize(String inbox) throws Exception {
        try (Connection db = DriverManager.getConnection(inbox, "sa", ""); var statements = db.createStatement()) {
            statements.execute("CREATE TABLE event_inbox(event_id VARCHAR PRIMARY KEY, digest VARCHAR NOT NULL)");
            statements.execute("CREATE TABLE business_commits(transaction_id VARCHAR PRIMARY KEY)");
        }
    }

    private static Staged stage(String inbox, List<Delivery> deliveries, String expectedTransaction) throws Exception {
        require(deliveries.stream().map(delivery -> delivery.event.kind).toList().equals(List.of("BEGIN", "ROW", "COMMIT"))
                && deliveries.stream().allMatch(delivery -> expectedTransaction.equals(delivery.event.transactionId))
                && deliveries.getLast().event.totalChanges == 1, "The consumer did not receive a complete transaction");
        try (Connection db = DriverManager.getConnection(inbox, "sa", "");
             var check = db.prepareStatement("SELECT digest FROM event_inbox WHERE event_id=?");
             var add = db.prepareStatement("INSERT INTO event_inbox VALUES (?, ?)");
             var apply = db.prepareStatement("INSERT INTO business_commits VALUES (?)");
             var count = db.createStatement()) {
            db.setAutoCommit(false);
            int duplicates = 0;
            for (Delivery delivery : deliveries) {
                check.setString(1, delivery.id);
                try (var existing = check.executeQuery()) {
                    if (existing.next()) {
                        require(delivery.digest.equals(existing.getString(1)), "A replay changed the bytes for an existing event ID");
                        duplicates++;
                    } else {
                        add.setString(1, delivery.id);
                        add.setString(2, delivery.digest);
                        add.executeUpdate();
                    }
                }
            }
            require(duplicates == 0 || duplicates == deliveries.size(), "The durable inbox holds a partial transaction");
            if (duplicates == 0) {
                apply.setString(1, expectedTransaction);
                apply.executeUpdate();
            }
            db.commit();
            try (var rows = count.executeQuery("SELECT COUNT(*) FROM business_commits")) {
                rows.next();
                return new Staged(duplicates, rows.getLong(1));
            }
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
