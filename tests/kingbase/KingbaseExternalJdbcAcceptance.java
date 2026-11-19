package io.github.rivomesh.acceptance;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.Sql;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcSink;
import io.github.rivomesh.cdc.Journal;
import io.github.rivomesh.cdc.KingbaseProfile;
import io.github.rivomesh.cdc.SinkAdapter;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class KingbaseExternalJdbcAcceptance {
    private static final String DRIVER = "com.kingbase8.Driver";
    private static final String NAME = "kb_external";
    private static final String DATA = NAME + "_data";
    private static final String META = NAME + "_meta";
    private static final String TABLE = Sql.pg(DATA) + "." + Sql.pg("items");
    private static final String SOURCE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String AMOUNT = "12345678901234567890123456789012345.123456789012345678901234567890";
    private static final String UNSIGNED = "18446744073709551615";

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static Connection connection(String url, String password) throws SQLException {
        return JdbcConnections.open(DRIVER, url, "system", password);
    }

    private static JdbcSink.Settings settings(String url, String password) {
        JdbcSink.Settings value = new JdbcSink.Settings();
        value.product = TargetProduct.KINGBASE;
        value.driver = DRIVER;
        value.url = url;
        value.username = "system";
        value.password = password;
        value.pipelineId = NAME;
        value.id = "target";
        value.generation = "fixture-generation";
        value.fingerprint = Codec.sha256("exact Kingbase runtime fixture");
        value.metadataSchema = META;
        value.databases.put("fixture", DATA);
        return value;
    }

    private static TableSchema.Column column(String name, TableSchema.Type type) {
        TableSchema.Column column = new TableSchema.Column();
        column.name = name;
        column.type = type;
        column.nullable = false;
        return column;
    }

    private static TableSchema table() {
        TableSchema model = new TableSchema();
        model.id = TableSchema.identity("fixture", "", "items");
        model.database = "fixture";
        model.name = "items";
        model.primaryKey.add("id");
        TableSchema.Column amount = column("amount", TableSchema.Type.DECIMAL);
        amount.precision = 65;
        amount.scale = 30;
        model.columns.addAll(List.of(column("id", TableSchema.Type.INT64),
                column("unsigned_value", TableSchema.Type.UINT64), amount, column("payload", TableSchema.Type.JSON)));
        return model;
    }

    private static List<Cell> row(int id) {
        return List.of(Cell.value(Integer.toString(id)), Cell.value(UNSIGNED), Cell.value(AMOUNT),
                Cell.value("{\"n\":" + UNSIGNED + "}"));
    }

    private static SourceTransaction transaction(TableSchema model, String generation, int sequence, int... ids) {
        SourceTransaction transaction = new SourceTransaction();
        transaction.id = SOURCE + ":" + sequence;
        transaction.position = new SourcePosition(1, "MYSQL", "fixture-source", Map.of("gtidSet", transaction.id));
        transaction.streamGeneration = generation;
        for (int index = 0; index < ids.length; index++) {
            Change change = new Change();
            change.operation = Change.Operation.INSERT;
            change.eventId = transaction.id + ":" + index;
            change.tableId = model.id;
            change.schemaVersion = model.version;
            change.after = row(ids[index]);
            transaction.changes.add(change);
        }
        return transaction;
    }

    private static void confirmRows(Connection connection, int... expected) throws Exception {
        List<Integer> seen = new ArrayList<>();
        try (var query = connection.createStatement();
             var rows = query.executeQuery("SELECT id,unsigned_value,amount,payload->>'n' FROM " + TABLE + " ORDER BY id")) {
            while (rows.next()) {
                require(UNSIGNED.equals(rows.getBigDecimal(2).toPlainString()), "Native UINT64 precision changed");
                require(AMOUNT.equals(rows.getBigDecimal(3).toPlainString()), "Native DECIMAL precision changed");
                require(UNSIGNED.equals(rows.getString(4)), "Native JSON numeric value changed");
                seen.add(rows.getInt(1));
            }
        }
        require(seen.equals(java.util.Arrays.stream(expected).boxed().toList()), "Unexpected native rows " + seen);
    }

    private static String snapshot(Connection connection) throws Exception {
        try (var query = connection.createStatement();
             var rows = query.executeQuery("SELECT snapshot_id,row_count,snapshot_state FROM "
                     + Sql.pg(META) + "." + Sql.pg("rm3_sink_table"))) {
            require(rows.next() && rows.getLong(2) == 1 && "COMPLETE".equals(rows.getString(3)), "Snapshot changed");
            String id = rows.getString(1);
            require(!rows.next(), "Unexpected target snapshot receipt");
            return id;
        }
    }

    private static void signal(Path state, String phase) throws Exception {
        Files.writeString(state.resolve(phase + ".request"), phase);
        long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(state.resolve(phase + ".done"))) return;
            Thread.sleep(150);
        }
        throw new AssertionError("No native response for " + phase);
    }

    public static void main(String[] args) throws Exception {
        if ("absent".equals(args[0])) {
            try (var ignored = connection("jdbc:kingbase8://127.0.0.1:1/fixture", "")) {
                throw new AssertionError("Missing vendor driver unexpectedly connected");
            } catch (SQLException failure) {
                require("JDBC_DRIVER_UNAVAILABLE".equals(failure.getMessage()) && "0A000".equals(failure.getSQLState()),
                        "Missing vendor driver lacks typed refusal");
                System.out.println("KINGBASE_EXTERNAL_MISSING_DRIVER_ACCEPTED");
                return;
            }
        }
        Path expectedDriver = Path.of(args[1]).toRealPath();
        Class<?> type = Class.forName(DRIVER);
        Path origin = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
        require(origin.equals(expectedDriver), "Kingbase JDBC class came from an unexpected JAR");
        String url = args[2];
        String password = System.getenv("RIVOMESH_TEST_KINGBASE_PASSWORD");
        String authenticationRefusal;
        try (var ignored = connection(url, password + "-fixture-invalid")) {
            throw new AssertionError("Wrong password admitted");
        } catch (SQLException rejected) {
            authenticationRefusal = rejected.getSQLState();
            require(authenticationRefusal != null && authenticationRefusal.startsWith("28"),
                    "Expected native authentication refusal but got " + authenticationRefusal);
        }
        try (var session = connection(url, password)) {
            Map<String, String> profile = new LinkedHashMap<>(KingbaseProfile.check(session));
            require("pg".equalsIgnoreCase(profile.get("mode")) && "UTF8".equalsIgnoreCase(profile.get("encoding")),
                    "Expected native PG/UTF8 profile");
            profile.put("driverVersion", session.getMetaData().getDriverVersion());
            profile.put("authenticationRefusal", authenticationRefusal);
            System.out.println("KINGBASE_EXTERNAL_PROFILE: " + Codec.encode(profile));
        }
        System.out.println("KINGBASE_EXTERNAL_ORIGIN_AND_AUTH_ACCEPTED");

        JdbcSink.Settings settings = settings(url, password);
        TableSchema model = table();
        Journal.SnapshotState original = new Journal.SnapshotState();
        original.tableId = model.id;
        original.schemaVersion = model.version;
        original.snapshotId = java.util.UUID.randomUUID().toString();
        original.state = "COMPLETE";
        original.nextPart = 1;
        original.rows = 1;
        Journal.SnapshotPart part = new Journal.SnapshotPart();
        part.rows = List.of(row(1));
        SourceTransaction first = transaction(model, settings.generation, 1, 2);
        SourceTransaction second = transaction(model, settings.generation, 2, 3);
        SinkAdapter.Checkpoint saved;
        try (JdbcSink sink = new JdbcSink(settings)) {
            sink.activate(1);
            sink.prepare(1, model);
            sink.beginSnapshot(1, model, original);
            sink.snapshotPart(1, model, original.snapshotId, part);
            sink.finishSnapshot(1, model, original);
            sink.apply(1, 1, first);
            saved = sink.checkpoint();
            require(saved.sequence == 1 && first.id.equals(saved.transactionId) && saved.transactionHash.length() == 64,
                    "Initial native checkpoint missing");
            try {
                sink.apply(1, 2, transaction(model, settings.generation, 2, 99, 1));
                throw new AssertionError("Duplicate primary key accepted");
            } catch (SQLException rejected) {
                require("23505".equals(rejected.getSQLState()), "Wrong native duplicate-key diagnosis");
            }
            require(sink.checkpoint().sequence == 1 && saved.transactionHash.equals(sink.checkpoint().transactionHash),
                    "Failed update advanced checkpoint");
            try (var session = connection(url, password)) {
                confirmRows(session, 1, 2);
                require(original.snapshotId.equals(snapshot(session)), "Wrong snapshot receipt");
            }
            System.out.println("KINGBASE_EXTERNAL_TYPES_AND_ATOMICITY_ACCEPTED");

            Path phase = Path.of(args[3]);
            signal(phase, "stop");
            try (var ignored = connection(url, password)) {
                throw new AssertionError("Stopped engine unexpectedly accepted a connection");
            } catch (SQLException unavailable) {
                require(Sql.retryable(unavailable), "Endpoint outage did not classify as retryable");
            }
            System.out.println("KINGBASE_EXTERNAL_OUTAGE_ACCEPTED");
            signal(phase, "start");
            SinkAdapter.Checkpoint reopened = sink.activate(2);
            require(reopened.epoch == 2 && reopened.sequence == saved.sequence
                    && reopened.transactionId.equals(saved.transactionId) && reopened.transactionHash.equals(saved.transactionHash),
                    "Native recovery changed the original checkpoint");
            sink.prepare(2, model);
            try {
                sink.apply(1, 2, second);
                throw new AssertionError("Stale writer epoch accepted");
            } catch (IllegalStateException rejected) {
                require("SINK_FENCED".equals(rejected.getMessage()), "Wrong stale-epoch refusal");
            }
            sink.apply(2, 2, second);
            sink.apply(2, 2, second);
            SinkAdapter.Checkpoint finalCut = sink.checkpoint();
            require(finalCut.epoch == 2 && finalCut.sequence == 2 && finalCut.transactionId.equals(second.id)
                    && !finalCut.transactionHash.equals(saved.transactionHash), "Replay advanced native checkpoint incorrectly");
            try (var session = connection(url, password)) {
                confirmRows(session, 1, 2, 3);
                require(original.snapshotId.equals(snapshot(session)), "Reconnection replaced the snapshot");
            }
            System.out.println("KINGBASE_EXTERNAL_RECEIPTS: " + Codec.encode(Map.of(
                    "snapshotId", original.snapshotId, "before", saved, "after", finalCut)));
            System.out.println("KINGBASE_EXTERNAL_RECONNECT_REPLAY_ACCEPTED");
        }
    }
}
