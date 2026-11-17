package io.github.rivomesh;

import io.github.rivomesh.Model.*;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.integration", matches="true")
class TargetStoreIntegrationTest {
    @Test void independentHeartbeatKeepsTheLeaseWhileTheMainThreadIsBlocked() throws Exception {
        Config config = ClusterHarness.config(); config.leaseMillis = 2000; config.heartbeatMillis = 200;
        config.validate(); StateStore store = new StateStore(config);
        try {
            store.initialize(); store.create("test", new Position("bin.000001",4), Collections.emptyList()); store.claim();
            try (LeaseHeartbeat heartbeat = new LeaseHeartbeat(store, config)) {
                Thread.sleep(3000); heartbeat.check();
                assertThrows(StateStore.JobBusyException.class, new StateStore(config)::claim);
                store.heartbeat();
            }
            store.release(); new StateStore(config).claim();
        } finally {
            try(Connection c=store.connect()) {
                Sql.execute(c,"DROP SCHEMA IF EXISTS "+Sql.pg(config.target.schema)+" CASCADE");
                Sql.execute(c,"DROP SCHEMA IF EXISTS "+Sql.pg(config.target.metadataSchema)+" CASCADE");
            }
        }
    }
    @Test void targetTransactionsAndOwnershipRemainAtomic() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Config config = new Config(); config.jobId = "store" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        config.source.database = "test"; config.source.username = "test";
        config.target.schema = config.jobId; config.target.metadataSchema = config.jobId + "_meta";
        config.target.url = System.getProperty("target.url", "jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&socketTimeout=30");
        config.target.type=System.getProperty("target.type","GAUSS");config.target.driver=System.getProperty("target.driver",config.target.driver);
        config.target.username = "sync_test"; config.target.password = "Sync@Test123";
        config.chunkSize = 2;
        StateStore store = new StateStore(config);
        TableProgress progress = new TableProgress(); progress.table = new Table(); progress.table.name = "user";
        String[][] definitions = {{"id", "numeric(20,0)"}, {"name", "varchar(80)"}, {"amount", "numeric(20,6)"},
                {"payload", "bytea"}, {"stamp", "timestamp(6)"}, {"birthday", "date"}, {"body", "text"},
                {"small", "smallint"}, {"integer", "integer"}, {"long", "bigint"}, {"float", "real"}, {"double", "double precision"}};
        for (String[] definition : definitions) {
            Column c = new Column(); c.name = definition[0]; c.targetType = definition[1]; c.nullable = !c.name.equals("id");
            c.keyOrder = c.name.equals("id") ? 1 : 0; progress.table.columns.add(c);
        }
        progress.upperKey = Collections.singletonList("18446744073709551615");
        List<String> first = Arrays.asList("1", "中文'", "12345678901234.123456", "AAH/", "2024-01-01 00:00:00.123456", "2024-01-02", "body", "12", "34", "56", "1.25", "2.5");
        List<String> second = new ArrayList<>(first); second.set(0, "18446744073709551615");
        String target = progress.table.targetName(config.target.schema);
        try {
            store.initialize(); store.create("test-uuid", new Position("bin.000001", 4), Collections.singletonList(progress)); store.claim();
            store.prepare(progress);
            List<String> invalid = new ArrayList<>(second); invalid.set(2, "not-a-number");
            assertThrows(NumberFormatException.class, () -> store.commitChunk(progress, Arrays.asList(first, invalid)));
            assertEquals(0, count(store, target)); assertEquals(0, store.tables().get(0).copiedRows);
            String sid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
            store.commitChunk(progress, Arrays.asList(first, second), sid+":1-3");
            assertTrue(Gtids.contains(store.requiredGtids(), sid+":1-3"));
            assertEquals(2, count(store, target)); assertEquals(2, store.tables().get(0).copiedRows);
            List<String> empty = new ArrayList<>(first); empty.set(1, ""); empty.set(6, "");
            Transaction emptyTx = new Transaction(); emptyTx.position = new Position("bin.000001", 50);
            emptyTx.gtid=sid+":1"; emptyTx.position.gtidSet=emptyTx.gtid;
            emptyTx.changes.add(new Change("user", false, empty)); store.append(emptyTx);
            store.applyNext(Collections.singletonMap("user", progress.table));
            store.append(emptyTx);
            assertEquals(0,((Number)store.status().get("pending_transactions")).intValue(),"Replayed GTIDs must not re-enter a consumed queue");
            try (Connection c = store.connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT name,body FROM " + target + " WHERE id=1")) {
                assertTrue(rs.next()); assertEquals("", rs.getString(1)); assertEquals("", rs.getString(2));
            }
            // A constraint failure must roll back an earlier DELETE in the same replay transaction.
            try (Connection c = store.connect()) { Sql.execute(c, "ALTER TABLE " + target + " ADD CONSTRAINT amount_positive CHECK (amount>=0)"); }
            Transaction tx = new Transaction(); tx.position = new Position("bin.000001", 100);
            tx.gtid=sid+":2"; tx.position.gtidSet=sid+":1-2";
            tx.changes.add(new Change("user", true, first));
            List<String> negative = new ArrayList<>(second); negative.set(2, "-1"); tx.changes.add(new Change("user", false, negative));
            store.append(tx); store.beginCatchup();
            Map<String, Table> tables = Collections.singletonMap("user", progress.table);
            assertThrows(SyncFailure.class, () -> store.applyNext(tables));
            assertEquals(2, count(store, target)); assertEquals(1, ((Number) store.status().get("pending_transactions")).intValue());
            assertTrue(((Number) store.status().get("applied_sequence")).longValue() > 0);
            try (Connection c = store.connect()) { Sql.execute(c, "ALTER TABLE " + target + " DROP CONSTRAINT amount_positive"); }
            store.applyNext(tables); assertEquals(1, count(store, target));
            assertEquals(0, ((Number) store.status().get("pending_transactions")).intValue()); assertEquals("STREAMING", store.phase());
            StateStore other = new StateStore(config); assertThrows(IllegalStateException.class, other::claim);
            try (Connection c = store.connect()) { Sql.execute(c, "UPDATE " + store.name("sync_job") + " SET lease_until=CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE job_id=?", config.jobId); }
            other.claim(); assertThrows(IllegalStateException.class, () -> store.commitChunk(progress, Collections.singletonList(first)));
            assertEquals(1, count(store, target)); other.release();
        } finally {
            try (Connection c = store.connect()) {
                Sql.execute(c, "DROP SCHEMA IF EXISTS " + Sql.pg(config.target.schema) + " CASCADE");
                Sql.execute(c, "DROP SCHEMA IF EXISTS " + Sql.pg(config.target.metadataSchema) + " CASCADE");
            }
        }
    }

    private int count(StateStore store, String target) throws SQLException {
        try (Connection c = store.connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + target)) { rs.next(); return rs.getInt(1); }
    }
}
