package io.github.rivomesh;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Config {
    public String jobId;
    public Source source = new Source();
    public Target target = new Target();
    public int chunkSize = 1000;
    public long pollIntervalMillis = 1000;
    public long retryDelayMillis = 5000;
    public int maxTransactionRows = 100000;
    public String nodeId = "sync-node";
    public long leaseMillis = 30000;
    public long heartbeatMillis = 5000;
    public long maxRetryDelayMillis = 60000;

    public static final class Source {
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String type;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String expectedVersion;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public PostgresqlSource postgresql;
        public String host = "127.0.0.1";
        public int port = 3306;
        public String database;
        public String username;
        public String password;
        public String clusterId = "default";
        public String topology = "AUTO";
        public java.util.List<Endpoint> nodes = new java.util.ArrayList<>();
        public long healthCheckMillis = 5000;
        public long binlogIdleTimeoutMillis = 30000;
        public long serverId = 910001;
        public String sslMode = "DISABLED";
        public String jdbcOptions = "useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&zeroDateTimeBehavior=EXCEPTION&connectTimeout=10000&socketTimeout=60000";
    }

    public static final class PostgresqlSource {
        public String url,publication;
        public String slotPrefix="rivomesh";
    }

    public static final class Endpoint {
        public String host;
        public int port = 3306;
        public Endpoint() {}
        public Endpoint(String host, int port) { this.host = host; this.port = port; }
        @Override public String toString() { return host + ":" + port; }
    }

    public java.util.List<Endpoint> endpoints() {
        return source.nodes.isEmpty() ? java.util.Collections.singletonList(new Endpoint(source.host, source.port)) : source.nodes;
    }

    public static final class Target {
        public String type = "GAUSS";
        public String driver = "io.github.rivomesh.gaussdb.jdbc.Driver";
        public String url;
        public String username;
        public String password;
        public String schema;
        public String metadataSchema = "sync_meta";
        public String effectiveDriver(){return "POSTGRESQL".equals(type)&&"io.github.rivomesh.gaussdb.jdbc.Driver".equals(driver)?"org.postgresql.Driver":driver;}
    }

    public static Config load(Path path) throws Exception {
        return parse(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
    }

    public static Config parse(String yaml) throws Exception {
        // Substitute in parsed strings so passwords containing YAML syntax cannot change configuration.
        Config config = new ObjectMapper(new YAMLFactory()).readValue(yaml, Config.class);
        if (config == null || config.source == null || config.target == null) throw new IllegalArgumentException("源和目标配置不能为空");
        config.source.password = env(config.source.password);
        config.target.password = env(config.target.password);
        config.nodeId = env(config.nodeId);
        config.validate();
        return config;
    }

    static String env(String value) {
        if (value == null) return null;
        Matcher matcher = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}").matcher(value);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String replacement = System.getenv(matcher.group(1));
            if (replacement == null) throw new IllegalArgumentException("未设置环境变量: " + matcher.group(1));
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    public void validate() {
        if (source == null || target == null || source.nodes == null) throw new IllegalArgumentException("源、目标和候选地址列表不能为空");
        required(jobId, "jobId"); required(source.database, "source.database");
        if(source.type!=null&&!"MYSQL".equals(source.type))throw new IllegalArgumentException("SOURCE_ADAPTER_UNAVAILABLE");
        if(source.postgresql!=null)throw new IllegalArgumentException("POSTGRESQL_OPTIONS_REQUIRE_POSTGRESQL_SOURCE");
        if(source.expectedVersion!=null&&!source.expectedVersion.matches("[1-9][0-9]{0,2}\\.(0|[1-9][0-9]{0,2})(\\.(0|[1-9][0-9]{0,3}))?"))
            throw new IllegalArgumentException("SOURCE_VERSION_PROFILE_INVALID");
        required(source.host, "source.host"); required(source.username, "source.username");
        required(target.url, "target.url"); required(target.username, "target.username");
        if(!java.util.Arrays.asList("GAUSS","POSTGRESQL").contains(target.type))throw new IllegalArgumentException("target.type 必须为 GAUSS 或 POSTGRESQL");
        if(!java.util.Arrays.asList("io.github.rivomesh.gaussdb.jdbc.Driver","org.postgresql.Driver").contains(target.driver))throw new IllegalArgumentException("TARGET_DRIVER_NOT_ENABLED");
        required(nodeId, "nodeId"); required(source.clusterId, "source.clusterId");
        if (leaseMillis < 2000 || heartbeatMillis < 100 || heartbeatMillis * 3 > leaseMillis
                || maxRetryDelayMillis < retryDelayMillis || source.healthCheckMillis < 100 || source.binlogIdleTimeoutMillis < 3000
                || nodeId.length() > 128 || source.clusterId.length() > 128)
            throw new IllegalArgumentException("心跳、租约、重试或源健康检查配置无效");
        if (!java.util.Arrays.asList("AUTO", "ASYNC", "GROUP_REPLICATION").contains(source.topology))
            throw new IllegalArgumentException("source.topology 必须为 AUTO、ASYNC 或 GROUP_REPLICATION");
        java.util.Set<String> addresses = new java.util.HashSet<>();
        for (Endpoint endpoint : endpoints()) {
            required(endpoint.host, "source.nodes.host");
            if (endpoint.port < 1 || endpoint.port > 65535 || !addresses.add(endpoint.toString()))
                throw new IllegalArgumentException("源节点地址无效或重复");
        }
        Sql.pg(target.schema); Sql.pg(target.metadataSchema);
        if (target.schema.equals(target.metadataSchema)) throw new IllegalArgumentException("业务 schema 与元数据 schema 必须不同");
        if (jobId.length() > 128 || chunkSize < 1 || chunkSize > 100000 || maxTransactionRows < 1
                || pollIntervalMillis < 10 || retryDelayMillis < 10 || source.port < 1 || source.port > 65535
                || source.serverId < 1 || source.serverId > 4294967295L) {
            throw new IllegalArgumentException("任务配置数值超出范围");
        }
        com.github.shyiko.mysql.binlog.network.SSLMode.valueOf(source.sslMode);
    }

    private static void required(String s, String name) {
        if (s == null || s.trim().isEmpty()) throw new IllegalArgumentException("缺少配置: " + name);
    }

    public String fingerprint() throws Exception {
        String identity="v2\n" + source.clusterId + "\n" + source.topology + "\n" + source.database + "\n" + source.jdbcOptions + "\n" + target.url
                + "\n" + target.schema + "\n" + target.metadataSchema;
        return hash(identity+("GAUSS".equals(target.type)?"":"\n"+target.type));
    }

    static String hash(String text) throws Exception {
        byte[] bytes = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) result.append(String.format("%02x", b & 255));
        return result.toString();
    }
}
