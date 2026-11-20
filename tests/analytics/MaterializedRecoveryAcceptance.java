package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.PipelineSinks;
import io.github.rivomesh.api.SourcePosition;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** The release materializer and journal against a native vendor, with deterministic source events. */
public final class MaterializedRecoveryAcceptance {
    private static final String DATABASE = "sales", DESTINATION = "analytics", PEER = "peer";
    private static final int MESSAGE_BYTES = 8192;

    public static void main(String[] args) throws Exception {
        TargetProduct product = TargetProduct.valueOf(args[0]);
        PipelineConfig config = configuration(product);
        waitForDoris(config);
        TableSchema table = table();
        String generation = UUID.randomUUID().toString();

        try (var journal = new JdbcJournal(config.journalSettings())) {
            journal.initialize(config.fingerprint(), generation, config.subscribers());
            var lease = journal.claim("fixture-source", 600_000);
            var target = config.targets.getFirst();
            var publication = new JdbcPublicationStore(journal, DESTINATION, lease);
            String snapshotReceipt;
            try (var sink = PipelineSinks.open(config, target, generation, journal, lease)) {
                sink.activate(lease.epoch);
                journal.saveSchema(lease, table);
                var snapshot = journal.beginSnapshot(lease, "source", table, position(0));
                journal.finishSnapshot(lease, snapshot);
                sink.prepare(lease.epoch, table);
                sink.beginSnapshot(lease.epoch, table, snapshot);
                sink.finishSnapshot(lease.epoch, table, snapshot);
                snapshotReceipt = publication.progress();
                require(snapshotReceipt != null, "The native snapshot has no receipt");

                SourceTransaction first = append(journal, lease, table, generation, 1, "native predecessor");
                sink.apply(lease.epoch, 1, first);
                journal.acknowledge(lease, DESTINATION, 1);
                journal.acknowledge(lease, PEER, 1);
                SourceTransaction failed = append(journal, lease, table, generation, 2, "x".repeat(16_000));
                try {
                    sink.apply(lease.epoch, 2, failed);
                    throw new AssertionError("Expected to refuse an oversized message before native mutation");
                } catch (IllegalStateException error) {
                    require("MESSAGE_TOO_LARGE".equals(error.getMessage()), "Unexpected native failure");
                }
                var evidence = new SubscriberFailure();
                evidence.phase = SubscriberFailure.Phase.TARGET_APPLY;
                evidence.sequence = 2;
                evidence.transactionId = failed.id;
                evidence.projectedHash = Codec.sha256(Codec.encode(failed));
                journal.pauseFailure(lease, DESTINATION, "MESSAGE_TOO_LARGE", evidence);
                journal.acknowledge(lease, PEER, 2);
            }
            long reviewEpoch = journal.pausePipeline().epoch;

            var paused = new JdbcPublicationStore(journal, DESTINATION, null);
            paused.activate(reviewEpoch);
            var before = journal.inspectFailure(DESTINATION, 1, 100_000);
            String confirmed = paused.progress();
            require(!snapshotReceipt.equals(confirmed), "Incremental commit reused the snapshot receipt");
            Map<String, Object> result = DestinationReceiptReview.inspect(config, DESTINATION, 1);
            require("PRECEDING_RECEIPT_VERIFIED".equals(result.get("status"))
                    && product.name().equals(result.get("product"))
                    && "1".equals(result.get("acknowledgedSequence")) && "2".equals(result.get("failedSequence"))
                    && Codec.sha256(confirmed).equals(result.get("nativeReceiptSha256")), "Wrong native predecessor evidence");
            assertUnchanged(journal, before, paused, confirmed);
            System.out.println("PASS " + product + " verifies the exact native receipt on its original resource.");

            // Deliberately replace the fixture's transport pointer with another real batch from the same target.
            paused.progress(reviewEpoch, confirmed, snapshotReceipt);
            try {
                DestinationReceiptReview.inspect(config, DESTINATION, 1);
                throw new AssertionError("A receipt from a different batch was accepted");
            } catch (IllegalStateException | io.github.rivomesh.SyncFailure error) {
                String code=error instanceof io.github.rivomesh.SyncFailure refused?refused.code:error.getMessage();
                String name=switch(product){
                    case ACTIVEMQ_ARTEMIS->"ARTEMIS";
                    case ACTIVEMQ_CLASSIC->"CLASSIC";
                    default->product.name();
                };
                require((name+"_RECOVERY_PUBLICATION_UNCERTAIN").equals(code),"Wrong refusal");
            } finally {
                paused.progress(reviewEpoch, snapshotReceipt, confirmed);
            }
            assertUnchanged(journal, before, paused, confirmed);
            System.out.println("PASS " + product + " refuses a different native batch and preserves source and peer ACKs.");
        }
    }

    private static void waitForDoris(PipelineConfig config) throws Exception {
        var target = config.targets.getFirst();
        if (target.product != TargetProduct.DORIS) return;
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(4);
        Exception last = null;
        while (System.nanoTime() < deadline) {
            try (var sql = new DorisSqlClient(target.url, target.username, target.password, 3_000)) {
                if (sql.query("SHOW BACKENDS").stream().anyMatch(row -> "true".equalsIgnoreCase(row.get("Alive")))) return;
            } catch (Exception waiting) {
                last = waiting;
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Doris started without a live native backend", last);
    }

    private static PipelineConfig configuration(TargetProduct product) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        var config = new PipelineConfig();
        config.pipelineId = "native-materialized-" + suffix;
        config.databases = List.of(DATABASE);
        config.source.clusterId = "fixture-source";
        config.source.username = "fixture";
        config.journal.path = Files.createTempDirectory("rivomesh-materialized-review-").resolve("journal").toString();
        var target = new PipelineConfig.Target();
        target.id = DESTINATION;
        target.product = product;
        target.metadataSchema = "meta_" + suffix;
        target.mappings = Map.of(DATABASE, "copy_" + suffix);
        switch (product) {
            case CLICKHOUSE -> {
                target.url = "http://clickhouse:8123";
                target.username = "rivomesh";
                target.password = "ClickHouse@Test123";
                target.clickhouse = new PipelineConfig.ClickHouse();
                target.clickhouse.maxMessageBytes = MESSAGE_BYTES;
            }
            case DORIS -> {
                target.url = "jdbc:mysql://doris:9030/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=5000";
                target.username = "root";
                target.password = "";
                target.doris = new PipelineConfig.Doris();
                target.doris.loadUrl = "http://doris:8040";
                target.doris.replicas = 1;
                target.doris.buckets = 1;
                target.doris.maxMessageBytes = MESSAGE_BYTES;
            }
            case HIVE -> {
                target.url = "jdbc:hive2://hive:10000/default;auth=noSasl";
                target.username = "hive";
                target.password = "";
                target.hive = new PipelineConfig.Hive();
                target.hive.maxMessageBytes = MESSAGE_BYTES;
            }
            case ACTIVEMQ_ARTEMIS -> {
                target.url="artemis://artemis:61616";
                target.username="rivomesh";
                target.password="Artemis@Test123";
                target.messaging=new PipelineConfig.Messaging();
                target.messaging.stream="rivomesh";
                target.messaging.broker="rivomesh-artemis";
                target.messaging.requestTimeoutMillis=5000;
                target.messaging.maxMessageBytes=MESSAGE_BYTES;
            }
            case ACTIVEMQ_CLASSIC -> {
                target.url="activemq://classic:61616";
                target.username="admin";
                target.password="Classic@Test123";
                target.messaging=new PipelineConfig.Messaging();
                target.messaging.stream="rivomesh";
                target.messaging.broker="rivomesh-core";
                target.messaging.adminUrl="http://classic:8161/api/jolokia/";
                target.messaging.maxMessageBytes=MESSAGE_BYTES;
            }
            case REDIS_STREAMS -> {
                target.url="redis://redis:6379/0";
                target.messaging=new PipelineConfig.Messaging();
                target.messaging.stream="receipt_"+suffix;
                target.messaging.maxMessageBytes=MESSAGE_BYTES;
            }
            case NATS_JETSTREAM -> {
                target.url="nats://nats:4222";
                target.messaging=new PipelineConfig.Messaging();
                target.messaging.stream="receipt_"+suffix;
                target.messaging.maxMessageBytes=MESSAGE_BYTES;
            }
            case PULSAR -> {
                target.url="pulsar://pulsar:6650";
                target.messaging=new PipelineConfig.Messaging();
                target.messaging.topic="persistent://public/default/receipt_"+suffix;
                target.messaging.cluster="standalone";
                target.messaging.adminUrl="http://pulsar:8080";
                target.messaging.maxMessageBytes=MESSAGE_BYTES;
            }
            case ROCKETMQ -> {
                target.url="rocketmq://rocketnamesrv:9876";
                target.messaging=new PipelineConfig.Messaging();
                target.messaging.topic="receipt_events";
                target.messaging.cluster="RivomeshTest";
                target.messaging.broker="rivomesh-broker";
                target.messaging.requestTimeoutMillis=5000;
                target.messaging.maxMessageBytes=MESSAGE_BYTES;
            }
            default -> throw new IllegalArgumentException("Expected a destination with native receipt review");
        }
        var peer = new PipelineConfig.Target();
        peer.id = PEER;
        peer.product = TargetProduct.POSTGRESQL;
        peer.url = "jdbc:postgresql://127.0.0.1:1/fixture";
        peer.username = "fixture";
        peer.mappings = Map.of(DATABASE, "peer_" + suffix);
        config.targets = List.of(target, peer);
        config.validate();
        config.prepareLocalDirectory();
        return config;
    }

    private static TableSchema table() {
        var table = new TableSchema();
        table.database = DATABASE;
        table.name = "orders";
        table.id = TableSchema.identity(DATABASE, "", table.name);
        var id = new TableSchema.Column();
        id.name = "id";
        id.type = TableSchema.Type.INT64;
        id.nullable = false;
        var payload = new TableSchema.Column();
        payload.name = "payload";
        payload.type = TableSchema.Type.STRING;
        table.columns = List.of(id, payload);
        table.primaryKey = List.of(id.name);
        return table;
    }

    private static SourceTransaction append(JdbcJournal journal, Journal.Lease lease, TableSchema table,
                                             String generation, int sequence, String payload) throws Exception {
        var raw = new SourceTransaction();
        raw.id = "fixture-transaction-" + sequence;
        raw.streamGeneration = generation;
        raw.position = position(sequence);
        var row = new Change();
        row.operation = Change.Operation.INSERT;
        row.eventId = raw.id + ":0";
        row.tableId = table.id;
        row.after = List.of(Cell.value(Integer.toString(sequence)), Cell.value(payload));
        raw.changes.add(row);
        require(journal.append(lease, raw) == sequence, "Unexpected stream sequence");
        return journal.normalize(lease, journal.read(DESTINATION, 1).getFirst(), Map.of(table.id, table),
                Map.of(table.id, position(0)), (position, baseline) -> false);
    }

    private static SourcePosition position(int sequence) {
        String server = "25b043c4-e42d-41ad-960f-53a7f0958a73";
        return new SourcePosition(1, "MYSQL", "fixture-source", Map.of("gtidSet", server + ":1-" + (sequence + 1),
                "serverUuid", server, "file", "binlog.000001", "offset", Integer.toString(104 + sequence)));
    }

    private static void assertUnchanged(JdbcJournal journal, Journal.FailureReview before,
                                        JdbcPublicationStore publication, String progress) throws Exception {
        var after = journal.inspectFailure(DESTINATION, 1, 100_000);
        require(before.rawHash.equals(after.rawHash) && before.normalizedHash.equals(after.normalizedHash)
                && progress.equals(publication.progress()) && publication.checkpoint().sequence == 1
                && journal.subscriptions().stream().allMatch(sub -> sub.acknowledged == (DESTINATION.equals(sub.id) ? 1 : 2)),
                "Review changed a source transaction or destination progress");
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
