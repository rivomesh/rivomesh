package io.github.rivomesh;

import io.github.rivomesh.Model.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.integration", matches="true")
class SyncIntegrationTest {
    private Config config;
    private StateStore store;
    private Process process;
    private Path processLog;

    @BeforeAll static void utc() { TimeZone.setDefault(TimeZone.getTimeZone("UTC")); }
    @BeforeEach void setup() throws Exception {
        String id = "s" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        config = new Config(); config.jobId = id;
        config.source.host = System.getProperty("mysql.host", "mysql");
        config.source.database = id; config.source.username = "root"; config.source.password = "Sync@Test123";
        config.source.serverId = 910001; config.target.schema = id; config.target.metadataSchema = id + "_meta";
        config.target.url = System.getProperty("target.url", "jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=10&socketTimeout=30");
        config.target.type=System.getProperty("target.type","GAUSS");config.target.driver=System.getProperty("target.driver",config.target.driver);
        config.target.username = "sync_test"; config.target.password = "Sync@Test123";
        config.chunkSize = 2; config.pollIntervalMillis = 50; config.retryDelayMillis = 200;
        config.leaseMillis = 6000; config.heartbeatMillis = 500;
        config.source.healthCheckMillis = 1000;
        store = new StateStore(config);
        mysql("CREATE DATABASE " + Sql.mysql(id) + " CHARACTER SET utf8mb4");
        mysql("CREATE TABLE " + table("user") + " (id bigint unsigned PRIMARY KEY,name varchar(80) NOT NULL,amount decimal(20,6),payload varbinary(20),stamp datetime(6),birthday date,body text) ENGINE=InnoDB");
        mysql("CREATE TABLE " + table("compound") + " (a int NOT NULL,b varchar(20) NOT NULL,n int,PRIMARY KEY(a,b)) ENGINE=InnoDB");
        mysql("CREATE TABLE " + table("empty") + " (id int PRIMARY KEY) ENGINE=InnoDB");
        try (Connection c = new MysqlSource(config).connect(); PreparedStatement p = c.prepareStatement("INSERT INTO " + table("user") + " VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (int i = 1; i <= 80; i++) {
                p.setInt(1, i); p.setString(2, i == 3 ? String.join("", Collections.nCopies(80, "中")) : "中文'" + i); p.setBigDecimal(3, new java.math.BigDecimal("12345678901234.123456"));
                p.setBytes(4, new byte[]{0, 1, (byte) 255}); p.setString(5, "2024-01-01 00:00:00.123456");
                p.setString(6, "2024-01-02"); p.setString(7, "text"); p.addBatch();
            }
            p.executeBatch();
        }
        mysql("INSERT INTO " + table("compound") + " VALUES (1,'甲',10),(1,'乙',20),(2,'a',30)");
    }

    @AfterEach void cleanup() throws Exception {
        if (process != null && process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
        if (store != null) try (Connection c = store.connect(); Statement s = c.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS " + Sql.pg(config.target.schema) + " CASCADE");
            s.execute("DROP SCHEMA IF EXISTS " + Sql.pg(config.target.metadataSchema) + " CASCADE");
        }
        if (config != null) mysql("DROP DATABASE IF EXISTS " + Sql.mysql(config.source.database));
    }

    @Test void processCrashDuringSnapshotAndIncrementalRecoversWithoutLosingChanges() throws Exception {
        startProcess();
        await(() -> {
            for (TableProgress t : store.tables()) if (t.table.name.equals("user") && t.copiedRows > 0 && !t.done) return true;
            return false;
        }, 90);
        process.destroyForcibly(); assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        long before = userProgress().copiedRows;
        assertTrue(before > 0 && before < 80);
        // Source remains writable while the sync process is down; keys can move across the scan cursor.
        mysql("UPDATE " + table("user") + " SET name='changed',stamp=NULL,birthday=NULL WHERE id=1");
        mysql("DELETE FROM " + table("user") + " WHERE id=2");
        mysql("UPDATE " + table("user") + " SET id=18446744073709551615 WHERE id=80");
        mysql("INSERT INTO " + table("user") + " (id,name,amount) VALUES (1000,'new',1)");
        mysql("UPDATE " + table("compound") + " SET a=3,b='new' WHERE a=1 AND b='甲'");
        // Force a log rotation while offline, then continue writing.
        mysql("FLUSH BINARY LOGS");
        mysql("INSERT INTO " + table("empty") + " VALUES (9)");
        // Restart immediately: the new process must wait for the real lease without editing metadata.
        startProcess();
        await(() -> "STREAMING".equals(store.phase()) && equalUserData() && targetCount("empty") == 1, 90);
        assertTrue(userProgress().copiedRows >= before);
        assertEquals(3, targetCount("compound"));
        assertEquals(0, targetScalar("SELECT COUNT(*) FROM " + target("compound") + " WHERE a=1 AND b='甲'"));
        assertEquals(1, targetScalar("SELECT COUNT(*) FROM " + target("compound") + " WHERE a=3 AND b='new'"));
        // Kill in incremental mode and verify durable source offsets on the next process too.
        process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS);
        mysql("UPDATE " + table("user") + " SET name='after-second-crash' WHERE id=1");
        mysql("DELETE FROM " + table("empty") + " WHERE id=9");
        startProcess();
        await(() -> equalUserData() && targetCount("empty") == 0, 90);
    }

    @Test void dataAndProgressRollbackTogetherAndOldOwnersAreFenced() throws Exception {
        MysqlSource source = new MysqlSource(config);
        store.initialize(); store.create(source.validateAndIdentity(), source.position(), source.discover()); store.claim();
        TableProgress p = userProgress(); store.prepare(p);
        List<List<String>> rows = source.readChunk(p);
        List<List<String>> broken = new ArrayList<>(); broken.add(rows.get(0));
        List<String> invalid = new ArrayList<>(rows.get(1)); invalid.set(2, "bad-number"); broken.add(invalid);
        assertThrows(NumberFormatException.class, () -> store.commitChunk(p, broken));
        assertEquals(0, targetCount("user")); assertEquals(0, userProgress().copiedRows);
        store.commitChunk(p, rows); assertEquals(2, targetCount("user")); assertEquals(2, userProgress().copiedRows);
        // Fail an incremental transaction after its first operation has executed.
        try (Connection c = store.connect(); Statement s = c.createStatement()) {
            s.execute("ALTER TABLE " + target("user") + " ADD CONSTRAINT positive_amount CHECK (amount>=0)");
        }
        Transaction tx = new Transaction(); tx.position = source.position();
        tx.position.offset += 1; // Synthetic transaction identity for this storage atomicity test.
        tx.changes.add(new Change("user", true, rows.get(0)));
        List<String> negative = new ArrayList<>(rows.get(1)); negative.set(2, "-1");
        tx.changes.add(new Change("user", false, negative)); store.append(tx); store.beginCatchup();
        Map<String, Table> tables = Collections.singletonMap("user", p.table);
        assertThrows(SyncFailure.class, () -> store.applyNext(tables));
        assertEquals(2, targetCount("user"));
        assertEquals(1, ((Number) store.status().get("pending_transactions")).intValue());
        try (Connection c = store.connect(); Statement s = c.createStatement()) { s.execute("ALTER TABLE " + target("user") + " DROP CONSTRAINT positive_amount"); }
        assertTrue(store.applyNext(tables)); assertEquals(1, targetCount("user"));
        assertEquals(0, ((Number) store.status().get("pending_transactions")).intValue());
        StateStore other = new StateStore(config);
        assertThrows(IllegalStateException.class, other::claim);
        try (Connection c = store.connect()) { Sql.execute(c, "UPDATE " + store.name("sync_job") + " SET lease_until=CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE job_id=?", config.jobId); }
        other.claim();
        assertThrows(IllegalStateException.class, () -> store.commitChunk(p, rows));
        assertEquals(1, targetCount("user")); other.release();
    }

    @Test void schemaChangesStopCaptureWithoutAdvancingPastTheDdl() throws Exception {
        config.chunkSize = 1000; startProcess();
        await(() -> "STREAMING".equals(store.phase()), 90);
        Position before = store.captured();
        mysql("ALTER TABLE " + table("user") + " ADD COLUMN unexpected int");
        await(() -> "PAUSED".equals(store.runState()), 20);
        assertTrue(process.isAlive(), "Node stays alive while the shared job is paused");
        Position after = store.captured();
        assertEquals(before.file, after.file); assertEquals(before.offset, after.offset);
        assertEquals(before.gtidSet, after.gtidSet);
        assertTrue(String.valueOf(store.status().get("last_error")).contains("DDL"));
    }

    private void startProcess() throws Exception {
        Path directory = Paths.get("target", "integration"); Files.createDirectories(directory);
        Path yaml = directory.resolve(config.jobId + ".yml");
        new ObjectMapper(new YAMLFactory()).writeValue(yaml.toFile(), config);
        processLog = directory.resolve(config.jobId + "-" + System.nanoTime() + ".log");
        process = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), Main.class.getName(), "run", yaml.toAbsolutePath().toString())
                .redirectErrorStream(true).redirectOutput(processLog.toFile()).start();
    }
    private TableProgress userProgress() throws Exception {
        return store.tables().stream().filter(t -> t.table.name.equals("user")).findFirst().get();
    }
    private interface Check { boolean get() throws Exception; }
    private void await(Check check, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        Exception last = null;
        while (System.nanoTime() < deadline) {
            if (process != null && !process.isAlive()) fail("Sync process exited: " + new String(Files.readAllBytes(processLog), java.nio.charset.StandardCharsets.UTF_8));
            try { if (check.get()) return; }
            catch (SQLException e) { last = e; }
            catch (IllegalStateException e) { if (!"任务不存在".equals(e.getMessage())) throw e; last = e; }
            Thread.sleep(50);
        }
        fail("Timed out; last=" + last + "; log=" + new String(Files.readAllBytes(processLog), java.nio.charset.StandardCharsets.UTF_8));
    }
    private String table(String name) { return Sql.mysql(config.source.database) + "." + Sql.mysql(name); }
    private String target(String name) { return Sql.pg(config.target.schema) + "." + Sql.pg(name); }
    private void mysql(String sql) throws SQLException { try (Connection c = new MysqlSource(config).connect(); Statement s = c.createStatement()) { s.execute(sql); } }
    private int targetCount(String table) throws SQLException { return targetScalar("SELECT COUNT(*) FROM " + target(table)); }
    private int targetScalar(String sql) throws SQLException {
        try (Connection c = store.connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) { rs.next(); return rs.getInt(1); }
    }
    private boolean equalUserData() throws Exception {
        List<List<String>> source = new ArrayList<>(), target = new ArrayList<>();
        Table table = userProgress().table;
        try (Connection c = new MysqlSource(config).connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM " + table("user") + " ORDER BY id")) {
            while (rs.next()) source.add(table.jdbcRow(rs));
        }
        try (Connection c = store.connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM " + target("user") + " ORDER BY id")) {
            while (rs.next()) target.add(table.jdbcRow(rs));
        }
        // MySQL and GaussDB render timestamps with different numbers of trailing fractional zeroes.
        return source.equals(target);
    }
}
