package io.github.rivomesh;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

final class ClusterHarness implements AutoCloseable {
    static final String PASSWORD = "Sync@Test123";
    final Config config;
    final StateStore store;
    final Map<String, Process> processes = new LinkedHashMap<>();
    final Map<String, Path> logs = new LinkedHashMap<>();
    String cleanupHost = "mysql";
    interface Condition { boolean get() throws Exception; }

    static Config config() {
        Config c = new Config(); c.jobId = "ha" + UUID.randomUUID().toString().replace("-", "").substring(0,12);
        c.source.host = "mysql"; c.source.database = c.jobId; c.source.username = "root"; c.source.password = PASSWORD;
        c.source.jdbcOptions = "useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&zeroDateTimeBehavior=EXCEPTION&connectTimeout=1000&socketTimeout=3000";
        c.source.healthCheckMillis = 500; c.source.binlogIdleTimeoutMillis = 5000;
        c.target.url = System.getProperty("target.url","jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10");
        c.target.type=System.getProperty("target.type","GAUSS");c.target.driver=System.getProperty("target.driver",c.target.driver);
        c.target.username = "sync_test"; c.target.password = PASSWORD; c.target.schema = c.jobId; c.target.metadataSchema = c.jobId + "_meta";
        c.leaseMillis = 6000; c.heartbeatMillis = 500; c.pollIntervalMillis = 100;
        c.retryDelayMillis = 200; c.maxRetryDelayMillis = 1000; c.chunkSize = 5;
        return c;
    }
    ClusterHarness(Config config) throws Exception {
        this.config = config; store = new StateStore(config);
        sql("mysql", "CREATE DATABASE " + Sql.mysql(config.source.database));
        sql("mysql", "CREATE TABLE " + sourceTable() + " (id int PRIMARY KEY,amount int NOT NULL,label varchar(80)) ENGINE=InnoDB");
        for (int i = 1; i <= 40; i++) sql("mysql", "INSERT INTO " + sourceTable() + " VALUES (" + i + ",100,'initial')");
    }
    String sourceTable() { return Sql.mysql(config.source.database) + ".items"; }
    String targetTable() { return Sql.pg(config.target.schema) + ".items"; }
    static Connection mysql(String host) throws SQLException {
        return java.sql.DriverManager.getConnection("jdbc:mysql://" + host + ":3306/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&connectTimeout=1000&socketTimeout=3000", "root", PASSWORD);
    }
    static void sql(String host, String sql) throws SQLException {
        try (Connection c = mysql(host); Statement s = c.createStatement()) { s.execute(sql); }
    }
    static String scalar(String host, String sql) throws SQLException {
        try (Connection c = mysql(host); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) { rs.next(); return rs.getString(1); }
    }
    void start(String node) throws Exception {
        Config copy = Model.JSON.readValue(Model.JSON.writeValueAsString(config), Config.class); copy.nodeId = node;
        startConfiguration(node,copy);
    }
    void startPipeline(String node,PipelineConfig pipeline)throws Exception{
        PipelineConfig copy=Model.JSON.readValue(Model.JSON.writeValueAsString(pipeline),PipelineConfig.class);copy.nodeId=node;startConfiguration(node,copy);
    }
    private void startConfiguration(String node,Object copy)throws Exception{
        Path folder = Paths.get("target", "cluster"); Files.createDirectories(folder);
        Path yaml = folder.resolve(config.jobId + "-" + node + ".yml"); new ObjectMapper(new YAMLFactory()).writeValue(yaml.toFile(), copy);
        Path log = folder.resolve(config.jobId + "-" + node + ".log"); logs.put(node, log);
        Process p = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), Main.class.getName(), "run", yaml.toAbsolutePath().toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        processes.put(node, p);
    }
    void kill(String node) throws Exception { Process p = processes.get(node); p.destroyForcibly(); assertTrue(p.waitFor(10, TimeUnit.SECONDS)); }
    String owner() throws Exception { return (String) store.status().get("node_id"); }
    int count(String where) throws SQLException {
        try (Connection c = store.connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + targetTable() + " WHERE " + where)) { rs.next(); return rs.getInt(1); }
    }
    void await(Condition check, int seconds) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds); Throwable last = null;
        while (System.nanoTime() < end) {
            for (Map.Entry<String, Process> entry : processes.entrySet()) {
                if (!entry.getValue().isAlive() && entry.getValue().exitValue() != 137 && entry.getValue().exitValue() != 143)
                    fail("Node exited: " + entry.getKey() + "\n" + new String(Files.readAllBytes(logs.get(entry.getKey())), java.nio.charset.StandardCharsets.UTF_8));
            }
            try { if (check.get()) return; } catch (SQLException | IllegalStateException e) { last = e; }
            Thread.sleep(100);
        }
        StringBuilder context = new StringBuilder("Timeout: " + last);
        try { context.append("\nShared status: ").append(Model.JSON.writeValueAsString(store.status())); } catch(Exception unavailable) { context.append("\nStatus unavailable: ").append(unavailable.getClass().getSimpleName()); }
        for (Path log : logs.values()) {
            List<String> lines = Files.readAllLines(log); context.append('\n').append(log).append('\n').append(String.join("\n", lines.subList(Math.max(0,lines.size()-25), lines.size())));
        }
        fail(context.toString());
    }
    @Override public void close() throws Exception {
        for (Process p : processes.values()) if (p.isAlive()) { p.destroyForcibly(); p.waitFor(10, TimeUnit.SECONDS); }
        try (Connection c = store.connect()) {
            Sql.execute(c, "DROP SCHEMA IF EXISTS " + Sql.pg(config.target.schema) + " CASCADE");
            Sql.execute(c, "DROP SCHEMA IF EXISTS " + Sql.pg(config.target.metadataSchema) + " CASCADE");
        }
        try { sql(cleanupHost, "DROP DATABASE IF EXISTS " + Sql.mysql(config.source.database)); } catch (SQLException ignored) { /* failed source may be offline */ }
    }
}
