package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.api.SourcePosition;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class LocalStateRestoreAcceptance {
    private static final String PIPELINE = "state-restore";
    private static final String SUBSCRIPTION = "target";
    private static final String FINGERPRINT = Codec.sha256("current local restore fixture");
    private static final String SERVER = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final TableSchema TABLE = table();
    private static final Map<String, TableSchema> SCHEMAS = Map.of(TABLE.id, TABLE);
    private static final Map<String, SourcePosition> BASELINES = Map.of(TABLE.id, position(1));

    private record TargetCut(long sequence, String transactionId, String transactionHash,
                             String snapshot, long snapshotRows, List<String> rows) {}
    private record Cut(String generation, String schema, String snapshot, SourcePosition captured,
                       long acknowledged, long normalized, TargetCut target) {}

    private static TableSchema table() {
        TableSchema table = new TableSchema();
        table.database = "sales";
        table.name = "items";
        table.id = TableSchema.identity("sales", "", "items");
        table.primaryKey.add("id");
        TableSchema.Column id = new TableSchema.Column();
        id.name = "id";
        id.type = TableSchema.Type.INT64;
        id.nullable = false;
        table.columns.add(id);
        TableSchema.Column value = new TableSchema.Column();
        value.name = "note";
        value.type = TableSchema.Type.STRING;
        value.length = 50;
        table.columns.add(value);
        return table;
    }

    private static SourcePosition position(int ordinal) {
        return new SourcePosition(1, "MYSQL", "fixture-source", Map.of(
                "serverUuid", SERVER, "gtidSet", SERVER + ":1-" + ordinal,
                "file", "mysql-bin.000001", "offset", Integer.toString(ordinal * 100)));
    }

    private static List<Cell> row(int id) {
        return List.of(Cell.value(Integer.toString(id)), Cell.value("row " + id));
    }

    private static SourceTransaction transaction(String generation, int ordinal) {
        SourceTransaction transaction = new SourceTransaction();
        transaction.id = SERVER + ":" + ordinal;
        transaction.position = position(ordinal);
        transaction.streamGeneration = generation;
        Change change = new Change();
        change.eventId = transaction.id + ":0";
        change.tableId = TABLE.id;
        change.schemaVersion = TABLE.version;
        change.operation = Change.Operation.INSERT;
        change.after = row(ordinal);
        transaction.changes.add(change);
        return transaction;
    }

    private static PipelineConfig local(Path root) {
        PipelineConfig config = new PipelineConfig();
        config.pipelineId = PIPELINE;
        config.nodeId = "fixture";
        config.journal.path = root.resolve("journal").toString();
        return config;
    }

    private static JdbcJournal journal(Path root) throws Exception {
        return new JdbcJournal(local(root).existingJournalSettings());
    }

    private static JdbcSink.Settings target(Path root, String generation, boolean existing) {
        JdbcSink.Settings settings = new JdbcSink.Settings();
        settings.id = SUBSCRIPTION;
        settings.pipelineId = PIPELINE;
        settings.generation = generation;
        settings.fingerprint = FINGERPRINT;
        settings.product = TargetProduct.H2;
        settings.driver = "org.h2.Driver";
        settings.url = "jdbc:h2:file:" + root.resolve("target") + ";WRITE_DELAY=0"
                + (existing ? ";IFEXISTS=TRUE" : "");
        settings.databases.put("sales", "replica");
        return settings;
    }

    private static void require(boolean condition, String check) {
        if (!condition) throw new IllegalStateException("FIXTURE_CHECK_FAILED: " + check);
    }

    private static long append(JdbcJournal journal, Journal.Lease lease, String generation, int ordinal)
            throws Exception {
        SourceTransaction transaction = transaction(generation, ordinal);
        return journal.append(lease, transaction, NormalizationBudget.reserve(transaction, SCHEMAS));
    }

    private static FanoutDispatcher dispatcher(JdbcJournal journal, Journal.Lease lease, JdbcSink sink) {
        return new FanoutDispatcher(journal, lease, List.of(sink), SCHEMAS,
                record -> journal.normalize(lease, record, SCHEMAS, BASELINES, (baseline, id) -> false));
    }

    private static void seed(Path root) throws Exception {
        PipelineConfig config = local(root);
        config.prepareLocalDirectory();
        try (JdbcJournal journal = new JdbcJournal(config.journalSettings())) {
            journal.initialize(FINGERPRINT, null, List.of(SUBSCRIPTION));
            Journal.Lease lease = journal.claim("before-backup", 30000);
            String generation = (String) journal.status().get("generation");
            try (JdbcSink sink = new JdbcSink(target(root, generation, false))) {
                journal.baseline(lease, position(1));
                journal.saveSchema(lease, TABLE);
                Journal.SnapshotState snapshot = journal.beginSnapshot(lease, "source", TABLE, position(1));
                journal.appendSnapshot(lease, snapshot, 0, List.of(row(1)));
                journal.finishSnapshot(lease, snapshot);
                sink.activate(lease.epoch);
                sink.prepare(lease.epoch, TABLE);
                sink.beginSnapshot(lease.epoch, TABLE, snapshot);
                for (Journal.SnapshotPart part : journal.snapshotParts("source", TABLE.id, snapshot.snapshotId, 0, 1)) {
                    sink.snapshotPart(lease.epoch, TABLE, snapshot.snapshotId, part);
                }
                sink.finishSnapshot(lease.epoch, TABLE, snapshot);
                require(append(journal, lease, generation, 2) == 1, "first captured transaction");
                require(dispatcher(journal, lease, sink).drain(1) == 1, "first native commit");
                require(append(journal, lease, generation, 3) == 2, "pending second transaction");
            } finally {
                journal.release(lease);
            }
        }
    }

    private static TargetCut targetCut(Path root, String generation) throws Exception {
        JdbcSink.Settings settings = target(root, generation, true);
        try (JdbcSink sink = JdbcSink.existing(settings);
             var connection = JdbcConnections.open(settings.driver, settings.url, settings.username, settings.password);
             var sql = connection.createStatement()) {
            SinkAdapter.Checkpoint checkpoint = sink.checkpoint();
            List<String> rows = new ArrayList<>();
            try (ResultSet rs = sql.executeQuery("SELECT \"id\",\"note\" FROM \"replica\".\"items\" ORDER BY \"id\"")) {
                while (rs.next()) rows.add(rs.getLong(1) + ":" + rs.getString(2));
            }
            try (ResultSet rs = sql.executeQuery(
                    "SELECT snapshot_id,row_count,snapshot_state FROM \"rivomesh_v3\".\"rm3_sink_table\"")) {
                require(rs.next() && "COMPLETE".equals(rs.getString(3)), "original target snapshot is complete");
                return new TargetCut(checkpoint.sequence, checkpoint.transactionId, checkpoint.transactionHash,
                        rs.getString(1), rs.getLong(2), List.copyOf(rows));
            }
        }
    }

    private static Cut cut(Path root) throws Exception {
        try (JdbcJournal journal = journal(root)) {
            journal.verifyIdentity(FINGERPRINT, List.of(SUBSCRIPTION));
            Map<String, Object> status = journal.status();
            String generation = (String) status.get("generation");
            require("UNCLAIMED".equals(status.get("captureLeaseState")), "journal writer is stopped");
            Journal.SnapshotState snapshot = journal.snapshot("source", TABLE.id);
            require("COMPLETE".equals(snapshot.state), "source snapshot is complete");
            return new Cut(generation, Codec.encode(journal.schemas()), snapshot.snapshotId, journal.captured(),
                    journal.subscriptions().getFirst().acknowledged, (long) status.get("normalizedSequence"),
                    targetCut(root, generation));
        }
    }

    private static Path journalFile(Path root) { return root.resolve("journal/rivomesh.mv.db"); }
    private static Path targetFile(Path root) { return root.resolve("target.mv.db"); }

    private static void coldCopy(Path journalFrom, Path targetFrom, Path to) throws Exception {
        require(!Files.exists(journalFrom.resolve("journal/rivomesh.lock.db")), "journal closed before copy");
        require(!Files.exists(targetFrom.resolve("target.lock.db")), "target closed before copy");
        Files.createDirectories(to.resolve("journal"));
        Files.copy(journalFile(journalFrom), journalFile(to));
        Files.copy(targetFile(targetFrom), targetFile(to));
    }

    private static void resume(Path restored, Cut saved) throws Exception {
        try (JdbcJournal journal = journal(restored);
             JdbcSink sink = JdbcSink.existing(target(restored, saved.generation, true))) {
            Journal.Lease lease = journal.claim("after-restore", 30000);
            try {
                require(journal.read(SUBSCRIPTION, 1).getFirst().transaction.id.equals(SERVER + ":3"),
                        "original retained CDC is next");
                sink.activate(lease.epoch);
                sink.prepare(lease.epoch, TABLE);
                FanoutDispatcher replay = dispatcher(journal, lease, sink);
                require(replay.drain(1) == 1, "apply original pending transaction");
                require(append(journal, lease, saved.generation, 3) == 2, "existing GTID is idempotent");
                require(replay.drain(1) == 0, "deduplicated GTID stays committed once");
                require(append(journal, lease, saved.generation, 4) == 3, "capture continues at next sequence");
                require(replay.drain(1) == 1, "apply new post-restore transaction");
            } finally {
                journal.release(lease);
            }
        }
    }

    private static void stale(Path archive, Path restored, Path mismatch, Cut saved, Cut newest) throws Exception {
        coldCopy(archive, restored, mismatch);
        try (JdbcJournal journal = journal(mismatch);
             JdbcSink sink = JdbcSink.existing(target(mismatch, saved.generation, true))) {
            Journal.Lease lease = journal.claim("mixed-restore", 30000);
            try {
                FanoutDispatcher replay = dispatcher(journal, lease, sink);
                require(replay.drain(1) == 0, "mismatched backup must not apply");
                Journal.Subscription sub = journal.subscriptions().getFirst();
                require("PAUSED".equals(sub.state) && "SINK_JOURNAL_CHECKPOINT_DIVERGED".equals(sub.errorCode),
                        "mixed-time target refusal");
            } finally {
                journal.release(lease);
            }
        }
        Cut refused = cut(mismatch);
        require(refused.target.equals(newest.target), "mixed-time rejection preserves business rows and target receipt");
        require(refused.acknowledged == saved.acknowledged && refused.captured.equals(saved.captured)
                && refused.normalized == saved.normalized && refused.snapshot.equals(saved.snapshot),
                "mixed-time rejection preserves restored journal positions");
    }

    private static void corrupt(Path archive, Path damaged, Cut saved) throws Exception {
        coldCopy(archive, archive, damaged);
        try (JdbcJournal journal = journal(damaged)) {
            try (var connection = journal.connect(); var sql = connection.createStatement()) {
                require(sql.executeUpdate("UPDATE \"rivomesh_v3\".\"rm3_log\" SET payload='damaged' WHERE stream_seq=2") == 1,
                        "mutate only the pending payload in the disposable restore");
            }
            try {
                journal.read(SUBSCRIPTION, 1);
                throw new IllegalStateException("CORRUPTION_NOT_REFUSED");
            } catch (IllegalStateException error) {
                require("JOURNAL_CHECKSUM_MISMATCH".equals(error.getMessage()), "typed journal refusal");
            }
        }
        require(cut(damaged).equals(saved), "checksum refusal leaves source, ACK, snapshots and native receipt intact");
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        Path live = root.resolve("live");
        Path archive = root.resolve("backup");
        Path restored = root.resolve("restored");
        seed(live);
        Cut saved = cut(live);
        require(saved.captured.equals(position(3)) && saved.acknowledged == 1 && saved.normalized == 1
                && saved.target.sequence == 1 && (SERVER + ":2").equals(saved.target.transactionId)
                && saved.target.transactionHash != null && saved.target.snapshot.equals(saved.snapshot)
                && saved.target.snapshotRows == 1 && saved.target.rows.equals(List.of("1:row 1", "2:row 2")),
                "cold backup includes one full snapshot and one committed transaction followed by pending CDC");
        coldCopy(live, live, archive);
        coldCopy(archive, archive, restored);
        require(saved.equals(cut(restored)), "cold copy restores original identities and positions");
        resume(restored, saved);
        Cut newest = cut(restored);
        require(newest.generation.equals(saved.generation) && newest.schema.equals(saved.schema)
                && newest.snapshot.equals(saved.snapshot) && newest.target.snapshot.equals(saved.target.snapshot)
                && newest.target.snapshotRows == 1 && newest.captured.equals(position(4))
                && newest.acknowledged == 3 && newest.normalized == 3 && newest.target.sequence == 3
                && (SERVER + ":4").equals(newest.target.transactionId) && newest.target.transactionHash != null
                && newest.target.rows.equals(List.of("1:row 1", "2:row 2", "3:row 3", "4:row 4")),
                "exact continuation through original and new transactions without re-snapshot");
        stale(archive, restored, root.resolve("mismatch"), saved, newest);
        corrupt(archive, root.resolve("corrupt"), saved);
        require(Files.mismatch(journalFile(live), journalFile(archive)) == -1
                && Files.mismatch(targetFile(live), targetFile(archive)) == -1,
                "preserved bit-for-bit original archive");
        System.out.println(Codec.encode(Map.of("status", "PASS", "source", "synthetic MySQL GTIDs",
                "journal", "LOCAL H2", "target", "H2 JDBC test adapter",
                "generation", saved.generation, "snapshot", saved.snapshot, "savedSequence", 1,
                "resumedSequence", 3, "mixedTimeError", "SINK_JOURNAL_CHECKPOINT_DIVERGED",
                "corruptError", "JOURNAL_CHECKSUM_MISMATCH")));
    }
}
