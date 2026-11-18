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
import io.github.rivomesh.cdc.MessagePublishers;
import io.github.rivomesh.cdc.MessageSink;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Publishes typed records from the original durable journal through a fixed Redis entrance. */
public final class ReplicatedRedisAcceptance {
    private static final String PIPELINE = "redis-cluster";
    private static final String TARGET = "redis";
    private static final String GENERATION = "fixture-generation";
    private static final String UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String FINGERPRINT = Codec.sha256("rivomesh-redis-replication-fixture");
    private static final int MAX_MESSAGE_BYTES = 65536;
    private static final TableSchema TABLE = table();

    private ReplicatedRedisAcceptance() {}

    public static void main(String[] args) throws Exception {
        String phase = args[0];
        var target = target();
        if ("replica-refused".equals(phase)) {
            try {
                MessagePublishers.check(target);
                throw new AssertionError("Read-only Redis replica was accepted as a write endpoint");
            } catch (SyncFailure rejected) {
                require(rejected.retryable && "REDIS_WRITABLE_PRIMARY_REQUIRED".equals(rejected.code),
                        "Unexpected Redis replica admission result");
                System.out.println(Codec.encode(Map.of("retryCode", rejected.code)));
            }
            return;
        }

        JdbcJournal.Settings settings = new JdbcJournal.Settings();
        settings.pipelineId = PIPELINE;
        settings.driver = "org.h2.Driver";
        settings.url = "jdbc:h2:file:" + Path.of(args[1]).resolve("publication") + ";WRITE_DELAY=0";
        try (JdbcJournal journal = new JdbcJournal(settings)) {
            if ("restarted".equals(phase)) {
                journal.verifyIdentity(FINGERPRINT, List.of(TARGET));
                MessagePublishers.check(target);
                var store = new JdbcPublicationStore(journal, TARGET, null);
                require(store.checkpoint().sequence == 2 && journal.subscriptions().getFirst().acknowledged == 2,
                        "Recovered publisher changed its local checkpoint");
                System.out.println(Codec.encode(Map.of("publicationReceipt", store.checkpoint(),
                        "snapshotId", store.snapshot(TABLE.id).snapshotId, "sourcePosition", journal.captured())));
                return;
            }
            journal.initialize(FINGERPRINT, GENERATION, List.of(TARGET));
            Journal.Lease lease = journal.claim("fixture-" + phase, 120000);
            try {
                System.out.println(Codec.encode(publish(journal, lease, phase, target)));
            } finally {
                journal.release(lease);
            }
        }
    }

    private static PipelineConfig.Target target() {
        var target = new PipelineConfig.Target();
        target.id = TARGET;
        target.product = TargetProduct.REDIS_STREAMS;
        target.url = "redis://redis-rw:6379/2";
        target.mappings.put("sales", "orders");
        target.messaging = new PipelineConfig.Messaging();
        target.messaging.stream = "rivomesh-events";
        target.messaging.replicas = 2;
        target.messaging.requestTimeoutMillis = 1800;
        target.messaging.connectTimeoutMillis = 2000;
        return target;
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

    private static Map<String, Object> publish(JdbcJournal journal, Journal.Lease lease, String phase,
                                                PipelineConfig.Target target) throws Exception {
        int sequence = switch (phase) {
            case "initial" -> 1;
            case "no-replica", "restored" -> 2;
            default -> throw new IllegalArgumentException("Unknown Redis fixture phase");
        };
        Map<String, Object> result = new LinkedHashMap<>();
        JdbcPublicationStore store = new JdbcPublicationStore(journal, TARGET, lease);
        try (MessageSink sink = new MessageSink(TARGET, target.mappings, store,
                () -> MessagePublishers.open(target, MessagePublishers.binding(PIPELINE, TARGET, GENERATION)),
                MAX_MESSAGE_BYTES)) {
            var before = sink.activate(lease.epoch);
            require(before.sequence == sequence - 1, "Publication reopened at the wrong sequence");
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
            SourceTransaction expected = transaction(sequence);
            if (!"restored".equals(phase)) require(journal.append(lease, expected) == sequence, "Source sequence changed");
            Journal.Record pending = journal.read(TARGET, 1).getFirst();
            String expectedHash = Codec.sha256(Codec.encode(expected));
            require(pending.sequence == sequence && expectedHash.equals(Codec.sha256(Codec.encode(pending.transaction)))
                    && expected.position.equals(journal.captured()), "Retained source receipt differs");
            if ("no-replica".equals(phase)) {
                try {
                    sink.apply(lease.epoch, pending.sequence, pending.transaction);
                    throw new AssertionError("Publication confirmed without its required replica fsync");
                } catch (SyncFailure unavailable) {
                    require(unavailable.retryable && "REDIS_DURABILITY_NOT_CONFIRMED".equals(unavailable.code),
                            "Unexpected Redis durability failure");
                    result.put("retryCode", unavailable.code);
                }
                var blocked = sink.checkpoint();
                require(blocked.sequence == before.sequence && before.transactionHash.equals(blocked.transactionHash)
                        && before.transactionId.equals(blocked.transactionId), "An unconfirmed publication advanced");
            } else {
                sink.apply(lease.epoch, pending.sequence, pending.transaction);
                journal.acknowledge(lease, TARGET, pending.sequence);
                var confirmed = sink.checkpoint();
                require(confirmed.sequence == sequence && expectedHash.equals(confirmed.transactionHash)
                        && expected.id.equals(confirmed.transactionId), "Publication receipt differs from source");
            }
            result.put("snapshotId", store.snapshot(TABLE.id).snapshotId);
            result.put("sourceTransactionId", expected.id);
            result.put("sourceTransactionHash", expectedHash);
            result.put("publicationReceipt", sink.checkpoint());
            result.put("journalAcknowledged", journal.subscriptions().getFirst().acknowledged);
            return result;
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
