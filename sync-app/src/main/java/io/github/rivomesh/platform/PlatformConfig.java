package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.regex.*;
import java.util.*;

public final class PlatformConfig {
    public String bind = "127.0.0.1";
    public int port = 8080;
    public String authToken;
    public enum AccessMode {CONFIG,MANAGED}
    public AccessMode accessMode=AccessMode.CONFIG;
    public enum Role {VIEWER,OPERATOR,ADMIN}
    public static final class User {public String id,token;public Role role=Role.VIEWER;public List<String> taskIds;}
    public List<User> users=new ArrayList<>();
    public String tasksDirectory = "tasks";
    public String dataDirectory = "../data/platform";
    public int maxTasks = 20;
    public int maxRunningTasks = 2;
    public int workerHeapMiB = 256;
    public int controlThreads = 4;
    public long pollMillis = 3000;
    public boolean autoStart = false;
    public int auditMaxEvents = 1000000;
    public int validationRetentionMinimumDays;
    public String validationArchiveDirectory;
    public boolean validationArchiveReclaim;
    @com.fasterxml.jackson.annotation.JsonIgnore
    public final Set<String> protectedEnvironment=new LinkedHashSet<>();
    public Map<String,List<String>> workerSecretReferences;
    public boolean kafkaPhysicalOwnershipEvidence;
    public List<KafkaNoticeWriter> kafkaNoticeWriters=new ArrayList<>();
    public static final class KafkaNoticeWriter {
        public String taskId,targetId,clusterSha256,topicId,username,password;
        public List<String> bootstrapServers;
    }
    public ClusterConfig cluster;
    public Sessions sessions = new Sessions();
    public Oidc oidc;
    public static final class Oidc {
        public String issuer, authorizationEndpoint, tokenEndpoint, clientId, clientSecret, keyId, rsaPublicKey;
    }

    public static final class Sessions {
        public boolean enabled = true;
        public boolean secureCookie = true;
        public boolean rememberBrowser = false;
        public int idleSeconds = 1800;
        public int absoluteSeconds = 28800;
        public int maximum = 1000;
        /** Exact external origin when TLS terminates at a reverse proxy. Never trust forwarded headers. */
        public String publicOrigin;
        void validate() {
            if (idleSeconds < 30 || idleSeconds > 86400 || absoluteSeconds < idleSeconds || absoluteSeconds > 604800 || maximum < 1 || maximum > 10000)
                throw new IllegalArgumentException("SESSION_LIMITS_INVALID");
            if (publicOrigin != null) {
                java.net.URI uri;
                try { uri = java.net.URI.create(publicOrigin); } catch (Exception invalid) { throw new IllegalArgumentException("SESSION_ORIGIN_INVALID"); }
                if (uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null
                        || !"".equals(uri.getRawPath()) || !(secureCookie ? "https".equals(uri.getScheme()) : "http".equals(uri.getScheme())))
                    throw new IllegalArgumentException("SESSION_ORIGIN_INVALID");
            }
        }
    }

    public static PlatformConfig load(Path path) throws Exception {
        PlatformConfig c = new ObjectMapper(new YAMLFactory()).readValue(Files.readAllBytes(path), PlatformConfig.class);
        c.protectReference(c.authToken);c.authToken=resolveToken(c.authToken);
        if(c.users!=null)for(User user:c.users)if(user!=null){c.protectReference(user.token);user.token=resolveToken(user.token);}
        if(c.cluster!=null){c.protectReference(c.cluster.url);c.protectReference(c.cluster.username);c.protectReference(c.cluster.password);c.protectReference(c.cluster.taskKey);c.cluster.url=resolveToken(c.cluster.url);c.cluster.nodeId=resolveToken(c.cluster.nodeId);c.cluster.advertiseUrl=resolveToken(c.cluster.advertiseUrl);c.cluster.username=resolveToken(c.cluster.username);c.cluster.password=resolveToken(c.cluster.password);c.cluster.taskKey=resolveToken(c.cluster.taskKey);}
        if(c.oidc!=null){
            c.protectReference(c.oidc.clientSecret);
            c.oidc.clientSecret=resolveToken(c.oidc.clientSecret);
        }
        if(c.kafkaNoticeWriters!=null)for(KafkaNoticeWriter writer:c.kafkaNoticeWriters)if(writer!=null){
            c.protectReference(writer.username);c.protectReference(writer.password);
            writer.username=resolveToken(writer.username);writer.password=resolveToken(writer.password);
        }
        Path parent = path.toAbsolutePath().normalize().getParent();
        c.tasksDirectory = parent.resolve(c.tasksDirectory).normalize().toString();
        c.dataDirectory = parent.resolve(c.dataDirectory).normalize().toString();
        if(c.validationArchiveDirectory!=null&&!c.validationArchiveDirectory.isBlank())
            c.validationArchiveDirectory=parent.resolve(c.validationArchiveDirectory).normalize().toString();
        c.validate(); return c;
    }
    public void validate() throws Exception {
        validateAccess();
        if(validationRetentionMinimumDays<0||validationRetentionMinimumDays>io.github.rivomesh.validation.ValidationRetention.MAXIMUM_DAYS)
            throw new IllegalArgumentException("VALIDATION_RETENTION_POLICY_INVALID");
        if(validationArchiveDirectory!=null&&(validationArchiveDirectory.isBlank()
                ||validationArchiveDirectory.chars().anyMatch(character->character<32)))
            throw new IllegalArgumentException("VALIDATION_ARCHIVE_PATH_INVALID");
        if(validationArchiveReclaim&&validationArchiveDirectory==null)
            throw new IllegalArgumentException("VALIDATION_ARCHIVE_PATH_INVALID");
        WorkerSecretReferences.validate(workerSecretReferences,protectedEnvironment);
        validateKafkaNoticeWriters();
        if (sessions == null) throw new IllegalArgumentException("SESSION_CONFIG_REQUIRED");
        sessions.validate();
        if(cluster!=null)cluster.validate();
        if(oidc!=null)OidcBrowserLogin.validate(this);
        if (bind == null || bind.isEmpty()) throw new IllegalArgumentException("BIND_REQUIRED");
        InetAddress.getByName(bind);
        if (port < 0 || port > 65535 || maxTasks < 1 || maxTasks > 200 || maxRunningTasks < 1 || maxRunningTasks > maxTasks || workerHeapMiB < 64
                || workerHeapMiB > 65536 || controlThreads < 1 || controlThreads > 32 || pollMillis < 100 || auditMaxEvents < 1000 || auditMaxEvents > 10000000)
            throw new IllegalArgumentException("PLATFORM_LIMITS_INVALID");
        if (tasksDirectory == null || dataDirectory == null || Paths.get(tasksDirectory).toAbsolutePath().normalize().equals(Paths.get(dataDirectory).toAbsolutePath().normalize()))
            throw new IllegalArgumentException("PLATFORM_DIRECTORIES_INVALID");
    }
    private void validateKafkaNoticeWriters(){
        if(kafkaNoticeWriters==null||kafkaNoticeWriters.size()>200)
            throw new IllegalArgumentException("KAFKA_NOTICE_WRITER_CONFIG_INVALID");
        Set<String> scopes=new HashSet<>();
        for(KafkaNoticeWriter writer:kafkaNoticeWriters){
            if(writer==null||!validNoticeId(writer.taskId)||!validNoticeId(writer.targetId)
                    ||!scopes.add(writer.taskId+"/"+writer.targetId)
                    ||!io.github.rivomesh.cdc.KafkaNoticeWriterReview.validConfiguration(writer.clusterSha256,writer.topicId,
                            writer.bootstrapServers,writer.username,writer.password))
                throw new IllegalArgumentException("KAFKA_NOTICE_WRITER_CONFIG_INVALID");
        }
    }
    private static boolean validNoticeId(String value){return value!=null&&value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");}
    private void protectReference(String value){if(value!=null){Matcher m=Pattern.compile("^\\$\\{([A-Za-z_][A-Za-z0-9_]*)}$").matcher(value);if(m.matches())protectedEnvironment.add(m.group(1));}}
    private static String resolveToken(String value){if(value==null)return null;Matcher m=Pattern.compile("^\\$\\{([A-Za-z_][A-Za-z0-9_]*)}$").matcher(value);return m.matches()?System.getenv(m.group(1)):value;}
    private static void validateToken(String value){if(value==null||value.getBytes(StandardCharsets.UTF_8).length<24||value.length()>4096||value.contains("${")||!value.chars().allMatch(ch->ch>=33&&ch<=126))throw new IllegalArgumentException("RIVOMESH_TOKEN_REQUIRED_MIN_24_BYTES");}
    void validateAccess(){
        if(accessMode==null||users==null||users.size()>32)throw new IllegalArgumentException("PLATFORM_USERS_INVALID");Set<String> ids=new HashSet<>(),tokens=new HashSet<>();boolean administrator=false;
        if(authToken!=null){validateToken(authToken);ids.add("legacy-admin");tokens.add(authToken);administrator=true;}
        for(User user:users){if(user==null||user.id==null||!user.id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")||user.role==null||!ids.add(user.id))throw new IllegalArgumentException("PLATFORM_USERS_INVALID");if(user.taskIds!=null){if(user.role==Role.ADMIN||user.taskIds.size()>10000)throw new IllegalArgumentException("PLATFORM_TASK_SCOPE_INVALID");Set<String> grants=new HashSet<>();for(String task:user.taskIds)if(task==null||!task.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||!grants.add(task))throw new IllegalArgumentException("PLATFORM_TASK_SCOPE_INVALID");}validateToken(user.token);if(!tokens.add(user.token))throw new IllegalArgumentException("PLATFORM_USER_TOKEN_COLLISION");if(user.role==Role.ADMIN)administrator=true;}
        if(!administrator&&!(accessMode==AccessMode.MANAGED&&authToken==null&&users.isEmpty()))throw new IllegalArgumentException("PLATFORM_ADMIN_REQUIRED");
    }
}
