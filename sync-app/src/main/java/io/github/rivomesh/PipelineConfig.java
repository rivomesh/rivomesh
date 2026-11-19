package io.github.rivomesh;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.cdc.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Explicit v3 configuration; v2 task files and state retain their original path. */
public final class PipelineConfig {
    public int formatVersion=3;
    public String pipelineId,nodeId="rivomesh-node";
    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    public String sourceType;
    public Config.Source source=new Config.Source();
    public List<String> databases=new ArrayList<>();
    public List<Target> targets=new ArrayList<>();
    public Storage journal=new Storage();
    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT)
    public boolean kafkaHistoryFromFirstSend;
    public int chunkSize=1000,maxTransactionRows=100000;
    public long leaseMillis=30000,heartbeatMillis=5000,pollIntervalMillis=200,retryDelayMillis=2000;
    private Path base=Paths.get(".").toAbsolutePath();
    public static final class Storage {
        public String mode="LOCAL",path="data/journal",namespace="rivomesh_v3",url,username,password;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String writerAssignment;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT)
        public boolean managedWriter;
        public TargetProduct product=TargetProduct.POSTGRESQL;
        public long maxBytes=1024L*1024*1024;
    }
    public static final class Target {
        public String id,url,username,password,metadataSchema="rivomesh_v3";
        public TargetProduct product;
        public JdbcSink.UniquePolicy uniquePolicy=JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS;
        public Map<String,String> mappings=new TreeMap<>();
        public Messaging messaging;
        public ClickHouse clickhouse;
        public Doris doris;
        public Hive hive;
        public Dm dm;
    }
    public static final class Dm {public boolean acknowledgeTextConversions=false;}
    public static final class ClickHouse {
        public ClickHouseIndexPlan.Mode secondaryIndexes=ClickHouseIndexPlan.Mode.NONE;
        public int connectTimeoutMillis=5000,requestTimeoutMillis=30000,maximumRequestBytes=16*1024*1024,maximumResponseBytes=1024*1024,maxMessageBytes=4*1024*1024;
        public boolean autoCreate=true;
    }
    public static final class Hive {
        public boolean autoCreate=true,acknowledgeIndexOmission=false;
        public int queryTimeoutSeconds=300,maximumSqlBytes=4*1024*1024,maximumResponseBytes=16*1024*1024,maxMessageBytes=1024*1024;
    }
    public static final class Doris {
        public String loadUrl;
        public List<String> redirectOrigins=new ArrayList<>();
        public DorisIndexPlan.Mode secondaryIndexes=DorisIndexPlan.Mode.INVERTED;
        public int replicas=3,buckets=4,connectTimeoutMillis=5000,requestTimeoutMillis=30000,maximumRequestBytes=16*1024*1024,maximumResponseBytes=1024*1024,maxMessageBytes=4*1024*1024;
        public boolean autoCreate=true;
    }
    public static final class Messaging {
        public List<String> endpoints=new ArrayList<>();
        public int connectTimeoutMillis=5000,requestTimeoutMillis=10000,maxMessageBytes=1024*1024,replicas=1,minimumRetentionHours=87600;
        public boolean autoCreate=true;
        public String stream,token,topic,adminUrl,cluster,broker,queueType="quorum",securityProtocol="PLAINTEXT",saslMechanism="PLAIN";
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String tlsTrustCertsFilePath;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public List<String> rocketmqControllerEndpoints;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public KafkaNotice recoveryNotice;
    }
    public static final class KafkaNotice {
        public String controlTopic,clusterSha256,topicId,reservationId;
    }
    private static ObjectMapper yaml(){return new ObjectMapper(new YAMLFactory());}
    public static boolean isV3(Path file)throws Exception{JsonNode tree=yaml().readTree(file.toFile());return tree!=null&&tree.path("formatVersion").asInt()==3;}
    public static PipelineConfig load(Path file)throws Exception{
        return parse(new String(Files.readAllBytes(file),StandardCharsets.UTF_8),file.toAbsolutePath().getParent());
    }
    public static PipelineConfig parse(String value,Path directory)throws Exception{
        PipelineConfig c=yaml().readValue(value,PipelineConfig.class);if(c==null)throw new IllegalArgumentException("PIPELINE_CONFIG_EMPTY");c.base=directory.toAbsolutePath().normalize();
        if(c.source!=null)c.source.password=Config.env(c.source.password);if(c.journal!=null)c.journal.password=Config.env(c.journal.password);if(c.targets!=null)for(Target t:c.targets)if(t!=null){t.password=Config.env(t.password);if(t.messaging!=null)t.messaging.token=Config.env(t.messaging.token);}c.nodeId=Config.env(c.nodeId);c.validate();return c;
    }
    static String driver(TargetProduct product){if(product!=null&&product.mysqlDialect())return "com.mysql.cj.jdbc.Driver";if(product==TargetProduct.POSTGRESQL)return "org.postgresql.Driver";if(product==TargetProduct.GAUSS)return "io.github.rivomesh.gaussdb.jdbc.Driver";if(product==TargetProduct.GBASE_8C)return "com.gbase8c.Driver";if(product==TargetProduct.KINGBASE)return "com.kingbase8.Driver";if(product==TargetProduct.DM)return "dm.jdbc.driver.DmDriver";throw new IllegalArgumentException("PRODUCT_NOT_ENABLED");}
    private static void id(String value){if(value==null||!value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw new IllegalArgumentException("PIPELINE_ID_INVALID");}
    private static void url(TargetProduct product,String url){driver(product);if(url==null||!url.startsWith(product.mysqlDialect()?"jdbc:mysql://":product==TargetProduct.GBASE_8C?"jdbc:gbase8c://":product==TargetProduct.KINGBASE?"jdbc:kingbase8://":product==TargetProduct.DM?"jdbc:dm://":"jdbc:postgresql://"))throw new IllegalArgumentException("TARGET_URL_INVALID");}
    public String effectiveSourceType(){
        String nested=source==null?null:source.type;
        if(nested!=null&&sourceType!=null&&!nested.equals(sourceType))
            throw new IllegalArgumentException("SOURCE_TYPE_CONFLICT");
        return nested!=null?nested:sourceType==null?"MYSQL":sourceType;
    }
    public JournalWriterAssignment.CutProof verifyHandoffCut(JournalWriterAssignment.ExpectedCut expected)throws Exception{
        return JournalWriterAssignment.verifyExistingCut(this,expected,generation->PipelineSources.open(this,generation));
    }
    public void verifyOriginalFreeze(JournalWriterAssignment.CutProof cut,JournalWriterAssignment.OriginalFreeze freeze)throws Exception{
        JournalWriterAssignment.verifyFrozenOriginal(this,cut,freeze,generation->PipelineSources.open(this,generation));
    }
    public JournalWriterAssignment.TargetFence fenceParkedHandoff(java.sql.Connection control,String previousAssignment,
            JournalWriterAssignment.CutProof proof,JournalWriterAssignment.FenceAuthorization authorize)throws Exception{
        return JournalWriterAssignment.fenceParkedRecipient(control,this,previousAssignment,proof,
                generation->PipelineSources.open(this,generation),authorize);
    }
    public void admitParkedHandoff(java.sql.Connection control,String previousAssignment,JournalWriterAssignment.CutProof proof,
            JournalWriterAssignment.FenceAuthorization authorize,JournalWriterAssignment.TargetFence approved)throws Exception{
        JournalWriterAssignment.admitParkedRecipient(control,this,previousAssignment,proof,
                generation->PipelineSources.open(this,generation),authorize,approved);
    }
    public void validate(){
        if(formatVersion!=3||source==null||targets==null||targets.isEmpty()||journal==null||databases==null||databases.isEmpty())throw new IllegalArgumentException("PIPELINE_CONFIG_INVALID");id(pipelineId);id(nodeId);
        Set<String> db=new TreeSet<>();for(String name:databases)if(name==null||name.isEmpty()||name.indexOf('\0')>=0||!db.add(name))throw new IllegalArgumentException("DATABASE_LIST_INVALID");
        if(chunkSize<1||chunkSize>10000||leaseMillis<3000||heartbeatMillis<100||heartbeatMillis>leaseMillis/3||pollIntervalMillis<10||retryDelayMillis<10||journal.maxBytes<1)throw new IllegalArgumentException("PIPELINE_LIMIT_INVALID");
        PipelineSources.validate(this);
        Set<String> ids=new HashSet<>(),destinations=new HashSet<>(),mapped=new TreeSet<>();
        Set<String> kafkaBusinessTopics=new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> kafkaControlTopics=new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for(Target t:targets){
            if(t==null)throw new IllegalArgumentException("TARGET_REQUIRED");id(t.id);if(!ids.add(t.id))throw new IllegalArgumentException("DUPLICATE_TARGET_ID");
            if(t.product!=TargetProduct.DM&&t.dm!=null)throw new IllegalArgumentException("DM_OPTIONS_ON_OTHER_TARGET");if(t.product==TargetProduct.DM&&t.dm==null)t.dm=new Dm();
            boolean messaging=t.product!=null&&t.product.messaging();if(t.product!=TargetProduct.HIVE&&t.hive!=null)throw new IllegalArgumentException("HIVE_OPTIONS_ON_OTHER_TARGET");if(t.product!=TargetProduct.DORIS&&t.doris!=null)throw new IllegalArgumentException("DORIS_OPTIONS_ON_OTHER_TARGET");if(t.product==TargetProduct.HIVE){if(t.hive==null||t.doris!=null||t.clickhouse!=null||t.messaging!=null||t.uniquePolicy!=JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS||t.hive.maxMessageBytes<1024||t.hive.maxMessageBytes>16*1024*1024)throw new IllegalArgumentException("HIVE_CONFIG_INVALID");try(HiveSqlClient ignored=new HiveSqlClient(t.url,t.username,t.password,t.hive.queryTimeoutSeconds,t.hive.maximumSqlBytes,t.hive.maximumResponseBytes)){} }else if(t.product==TargetProduct.DORIS){if(t.doris==null||t.doris.redirectOrigins==null||t.clickhouse!=null||t.messaging!=null||t.uniquePolicy!=JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS||t.doris.secondaryIndexes==null||t.doris.replicas<1||t.doris.replicas>5||t.doris.buckets<1||t.doris.buckets>64||t.doris.maxMessageBytes<1024||t.doris.maxMessageBytes>16*1024*1024)throw new IllegalArgumentException("DORIS_CONFIG_INVALID");try(DorisHttpClient ignored=new DorisHttpClient(dorisHttp(t));DorisSqlClient local=new DorisSqlClient(t.url,t.username,t.password,t.doris.requestTimeoutMillis)){/* Local validation only. */}}else if(t.product==TargetProduct.CLICKHOUSE){if(t.clickhouse==null)t.clickhouse=new ClickHouse();if(t.clickhouse.secondaryIndexes==null||t.messaging!=null||t.uniquePolicy!=JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS||t.clickhouse.maxMessageBytes<1024||t.clickhouse.maxMessageBytes>16*1024*1024)throw new IllegalArgumentException("CLICKHOUSE_CONFIG_INVALID");try(ClickHouseHttpClient ignored=new ClickHouseHttpClient(clickHouseHttp(t))){/* Local validation only. */}catch(Exception invalid){throw new IllegalArgumentException("CLICKHOUSE_CONFIG_INVALID");}}else{if(t.clickhouse!=null)throw new IllegalArgumentException("CLICKHOUSE_OPTIONS_ON_OTHER_TARGET");if(messaging)validateMessaging(t);else{url(t.product,t.url);if(t.messaging!=null)throw new IllegalArgumentException("MESSAGING_OPTIONS_ON_SQL_TARGET");}}
            if((!messaging&&(t.username==null||t.username.isEmpty()))||t.uniquePolicy==null||t.mappings==null||t.mappings.isEmpty())throw new IllegalArgumentException("TARGET_CONFIG_INVALID");Sql.pg(t.metadataSchema);if(t.product==TargetProduct.CLICKHOUSE)ClickHouseMaterializer.q(t.metadataSchema);if(t.product==TargetProduct.DORIS)DorisMaterializer.databaseName(t.metadataSchema);if(t.product==TargetProduct.HIVE)HiveRows.name(t.metadataSchema);
            if((t.product==TargetProduct.KAFKA||t.product==TargetProduct.PULSAR)&&!destinations.add(t.product.name()+":"+t.url.toLowerCase(Locale.ROOT)+":"+t.messaging.topic))throw new IllegalArgumentException("TARGET_MAPPING_COLLISION");
            if(t.product==TargetProduct.KAFKA)kafkaBusinessTopics.add(t.messaging.topic);
            if(t.messaging!=null&&t.messaging.recoveryNotice!=null){
                if(t.product!=TargetProduct.KAFKA)throw new IllegalArgumentException("KAFKA_NOTICE_CONFIG_INVALID");
                KafkaNotice notice=t.messaging.recoveryNotice;
                try{
                    new KafkaNoticePlanning.TopicReservation(notice.reservationId,pipelineId,t.id,notice.controlTopic,notice.clusterSha256,notice.topicId);
                }catch(RuntimeException invalid){throw new IllegalArgumentException("KAFKA_NOTICE_CONFIG_INVALID");}
                if(!kafkaControlTopics.add(notice.controlTopic))throw new IllegalArgumentException("KAFKA_NOTICE_TOPIC_COLLISION");
            }
            if(t.product==TargetProduct.REDIS_STREAMS&&!destinations.add("REDIS_STREAMS:"+t.url.toLowerCase(Locale.ROOT)+":"+t.messaging.stream))throw new IllegalArgumentException("TARGET_MAPPING_COLLISION");
            Set<String> local=new HashSet<>();for(Map.Entry<String,String> mapping:t.mappings.entrySet()){
                if(!db.contains(mapping.getKey()))throw new IllegalArgumentException("UNDECLARED_SOURCE_DATABASE");Sql.pg(mapping.getValue());if(t.product==TargetProduct.CLICKHOUSE)ClickHouseMaterializer.q(mapping.getValue());if(t.product==TargetProduct.DORIS)DorisMaterializer.databaseName(mapping.getValue());if(t.product==TargetProduct.HIVE)HiveRows.name(mapping.getValue());
                if(messaging&&!mapping.getValue().matches("[A-Za-z][A-Za-z0-9_]{0,62}"))throw new IllegalArgumentException("MESSAGE_NAMESPACE_INVALID");
                if((!messaging&&t.metadataSchema.equalsIgnoreCase(mapping.getValue()))||!local.add(mapping.getValue().toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("TARGET_MAPPING_COLLISION");
                if(!t.product.singleMessageLog()&&!destinations.add(t.url.split("\\?",2)[0].toLowerCase(Locale.ROOT)+"\u0000"+mapping.getValue().toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("TARGET_MAPPING_COLLISION");mapped.add(mapping.getKey());
            }
        }
        if(kafkaControlTopics.stream().anyMatch(kafkaBusinessTopics::contains))
            throw new IllegalArgumentException("KAFKA_NOTICE_TOPIC_COLLISION");
        if(!mapped.equals(db))throw new IllegalArgumentException("SOURCE_DATABASE_WITHOUT_TARGET");
        if(kafkaHistoryFromFirstSend&&(!"LOCAL".equals(journal.mode)||kafkaBusinessTopics.isEmpty()))
            throw new IllegalArgumentException("KAFKA_HISTORY_ORIGIN_PROFILE_REQUIRED");
        if("EXTERNAL".equals(journal.mode)){if(!Arrays.asList(TargetProduct.MYSQL,TargetProduct.POSTGRESQL,TargetProduct.GAUSS).contains(journal.product))throw new IllegalArgumentException("JOURNAL_PRODUCT_NOT_ENABLED");url(journal.product,journal.url);Sql.pg(journal.namespace);if(journal.username==null||journal.username.isEmpty())throw new IllegalArgumentException("JOURNAL_USERNAME_REQUIRED");}
        else if("LOCAL".equals(journal.mode)){Path path=localPath();if(path.toString().indexOf(';')>=0||path.toString().chars().anyMatch(ch->ch<32))throw new IllegalArgumentException("LOCAL_JOURNAL_PATH_INVALID");}
        else throw new IllegalArgumentException("JOURNAL_MODE_INVALID");
        if(journal.managedWriter&&journal.writerAssignment==null)throw new IllegalArgumentException("WRITER_ASSIGNMENT_REQUIRED");
        if(journal.writerAssignment!=null&&(!JournalWriterAssignment.validId(journal.writerAssignment)
                ||!"EXTERNAL".equals(journal.mode)||journal.product!=TargetProduct.POSTGRESQL
                ||!"MYSQL".equals(effectiveSourceType())||targets.size()>8
                ||targets.stream().anyMatch(target->target.product!=TargetProduct.POSTGRESQL&&target.product!=TargetProduct.MYSQL)))
            throw new IllegalArgumentException("WRITER_ASSIGNMENT_PROFILE_UNSUPPORTED");
    }
    private static void validateMessaging(Target target){
        Messaging m=target.messaging;if(m==null||m.endpoints==null||m.connectTimeoutMillis<100||m.connectTimeoutMillis>120000||m.requestTimeoutMillis<100||m.requestTimeoutMillis>120000||m.maxMessageBytes<1024||m.maxMessageBytes>16*1024*1024||m.replicas<1||m.replicas>5)throw new IllegalArgumentException("MESSAGING_CONFIG_INVALID");
        List<String> addresses=new ArrayList<>();addresses.add(target.url);addresses.addAll(m.endpoints);
        for(String address:addresses)try{java.net.URI uri=new java.net.URI(address);String scheme=uri.getScheme();List<String> schemes=target.product==TargetProduct.RABBITMQ?Arrays.asList("amqp","amqps"):target.product==TargetProduct.KAFKA?Collections.singletonList("kafka"):target.product==TargetProduct.REDIS_STREAMS?Arrays.asList("redis","rediss"):target.product==TargetProduct.PULSAR?Arrays.asList("pulsar","pulsar+ssl"):target.product==TargetProduct.ROCKETMQ?Collections.singletonList("rocketmq"):target.product==TargetProduct.ACTIVEMQ_CLASSIC?Arrays.asList("activemq","activemqs"):target.product==TargetProduct.ACTIVEMQ_ARTEMIS?Arrays.asList("artemis","artemiss"):Arrays.asList("nats","tls");if(uri.getHost()==null||uri.getUserInfo()!=null||uri.getFragment()!=null||uri.getQuery()!=null||!schemes.contains(scheme)||((target.product==TargetProduct.KAFKA||target.product==TargetProduct.PULSAR||target.product==TargetProduct.ROCKETMQ||target.product==TargetProduct.ACTIVEMQ_ARTEMIS||target.product==TargetProduct.ACTIVEMQ_CLASSIC)&&uri.getPath()!=null&&!uri.getPath().isEmpty()))throw new IllegalArgumentException("MESSAGE_ENDPOINT_INVALID");if(target.product==TargetProduct.REDIS_STREAMS&&uri.getPath()!=null&&!uri.getPath().isEmpty()&&!uri.getPath().matches("/[0-9]{1,5}"))throw new IllegalArgumentException("REDIS_DATABASE_INVALID");}
        catch(Exception e){throw new IllegalArgumentException("MESSAGE_ENDPOINT_INVALID");}
        if(target.product==TargetProduct.RABBITMQ&&(!Arrays.asList("quorum","classic").contains(m.queueType)||target.username==null||target.username.isEmpty()))throw new IllegalArgumentException("RABBIT_CONFIG_INVALID");
        if(target.product==TargetProduct.NATS_JETSTREAM){
            if(m.stream==null||!m.stream.matches("[A-Za-z][A-Za-z0-9_-]{0,127}"))throw new IllegalArgumentException("JETSTREAM_NAME_REQUIRED");
            boolean encrypted=target.url.startsWith("tls:");
            if(m.endpoints.stream().anyMatch(endpoint->endpoint.startsWith("tls:")!=encrypted))
                throw new IllegalArgumentException("JETSTREAM_ENDPOINT_TLS_MISMATCH");
        }
        if(target.product==TargetProduct.REDIS_STREAMS){if(m.stream==null||!m.stream.matches("[A-Za-z][A-Za-z0-9_.:-]{0,127}"))throw new IllegalArgumentException("REDIS_STREAM_NAME_REQUIRED");if(!m.endpoints.isEmpty())throw new IllegalArgumentException("REDIS_SINGLE_ENDPOINT_REQUIRED");}
        if(target.product==TargetProduct.ACTIVEMQ_CLASSIC){
            if(m.stream==null||!m.stream.matches("[A-Za-z][A-Za-z0-9_-]{0,47}")||m.broker==null||!m.broker.matches("[A-Za-z0-9_-]{1,128}")||target.username==null||target.username.isEmpty()||target.password==null||m.replicas!=1||!m.endpoints.isEmpty()||m.token!=null&&!m.token.isEmpty())throw new IllegalArgumentException("CLASSIC_PROFILE_REQUIRES_PLAN");
            try{new ClassicAdminClient(m.adminUrl,target.username,target.password,m.broker,m.connectTimeoutMillis,m.requestTimeoutMillis);if(target.url.startsWith("activemqs:")&&!m.adminUrl.startsWith("https:"))throw new IllegalArgumentException();}catch(Exception invalid){throw new IllegalArgumentException("CLASSIC_ADMIN_CONFIG_INVALID");}
        }
        if(target.product==TargetProduct.ACTIVEMQ_ARTEMIS){
            if(m.stream==null||!m.stream.matches("[A-Za-z][A-Za-z0-9_-]{0,47}")||target.username==null||target.username.isEmpty()||target.password==null||m.broker!=null&&(m.broker.length()>128||m.broker.chars().anyMatch(ch->ch<32)))throw new IllegalArgumentException("ARTEMIS_CONFIG_INVALID");
            if(m.replicas!=1||!m.endpoints.isEmpty()||m.token!=null&&!m.token.isEmpty())throw new IllegalArgumentException("ARTEMIS_PROFILE_REQUIRES_PLAN");
        }
        if(target.product==TargetProduct.ROCKETMQ){
            if(m.topic==null||!m.topic.matches("[A-Za-z][A-Za-z0-9_-]{0,47}")||m.cluster==null||!m.cluster.matches("[A-Za-z0-9_-]{1,127}")||m.broker==null||!m.broker.matches("[A-Za-z0-9_-]{1,127}")||m.minimumRetentionHours<1)throw new IllegalArgumentException("ROCKETMQ_TOPIC_CLUSTER_BROKER_REQUIRED");
            int requiredReplicas=m.rocketmqControllerEndpoints==null?1:2;
            if(m.replicas!=requiredReplicas||target.username!=null&&!target.username.isEmpty()
                    ||target.password!=null&&!target.password.isEmpty()||m.token!=null&&!m.token.isEmpty())
                throw new IllegalArgumentException("ROCKETMQ_PROFILE_REQUIRES_PLAN");
        }
        if(m.rocketmqControllerEndpoints!=null){
            if(target.product!=TargetProduct.ROCKETMQ||m.rocketmqControllerEndpoints.isEmpty()
                    ||m.rocketmqControllerEndpoints.size()>5)
                throw new IllegalArgumentException("ROCKETMQ_CONTROLLER_ENDPOINTS_INVALID");
            Set<String> nativeAddresses=new HashSet<>();
            for(String endpoint:m.rocketmqControllerEndpoints){
                if(!nativeAddresses.add(rocketmqControllerAddress(endpoint)))
                    throw new IllegalArgumentException("ROCKETMQ_CONTROLLER_ENDPOINTS_INVALID");
            }
        }
        if(m.tlsTrustCertsFilePath!=null){
            try{
                if(target.product!=TargetProduct.PULSAR||!target.url.startsWith("pulsar+ssl:")
                        ||!Path.of(m.tlsTrustCertsFilePath).isAbsolute())
                    throw new IllegalArgumentException();
            }catch(IllegalArgumentException invalid){throw new IllegalArgumentException("PULSAR_TLS_TRUST_CONFIG_INVALID");}
        }
        if(target.product==TargetProduct.PULSAR){
            if(m.topic==null||!m.topic.matches("persistent://[A-Za-z0-9][A-Za-z0-9_-]{0,62}/[A-Za-z0-9][A-Za-z0-9_-]{0,62}/[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||m.topic.contains("-partition-")||m.cluster==null||!m.cluster.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}"))throw new IllegalArgumentException("PULSAR_TOPIC_AND_CLUSTER_REQUIRED");
            if(!m.endpoints.isEmpty())throw new IllegalArgumentException("PULSAR_SERVICE_ENDPOINT_REQUIRED");
            if(target.username!=null&&!target.username.isEmpty()||target.password!=null&&!target.password.isEmpty())throw new IllegalArgumentException("PULSAR_TOKEN_AUTH_REQUIRED");
            try{java.net.URI uri=new java.net.URI(m.adminUrl);if(!Arrays.asList("http","https").contains(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||uri.getPath()!=null&&!uri.getPath().isEmpty()&&!uri.getPath().equals("/")||target.url.startsWith("pulsar+ssl:")!=uri.getScheme().equals("https"))throw new IllegalArgumentException();}catch(Exception invalid){throw new IllegalArgumentException("PULSAR_ADMIN_ENDPOINT_INVALID");}
        }
        if(target.product==TargetProduct.KAFKA){
            if(m.topic==null||!m.topic.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,248}")||m.topic.equals(".")||m.topic.equals(".."))throw new IllegalArgumentException("KAFKA_TOPIC_REQUIRED");
            if(!Arrays.asList("PLAINTEXT","SSL","SASL_PLAINTEXT","SASL_SSL").contains(m.securityProtocol)||!Arrays.asList("PLAIN","SCRAM-SHA-256","SCRAM-SHA-512").contains(m.saslMechanism))throw new IllegalArgumentException("KAFKA_SECURITY_INVALID");
            if(m.securityProtocol.startsWith("SASL_")&&(target.username==null||target.username.isEmpty()||target.password==null))throw new IllegalArgumentException("KAFKA_CREDENTIALS_REQUIRED");
        }
    }
    public static String rocketmqControllerAddress(String endpoint){
        try{
            if(endpoint==null||endpoint.length()>384)throw new IllegalArgumentException();
            java.net.URI uri=java.net.URI.create(endpoint);
            if(!"rocketmq".equals(uri.getScheme())||uri.getHost()==null||uri.getRawUserInfo()!=null
                    ||!uri.getRawPath().isEmpty()||uri.getRawQuery()!=null||uri.getRawFragment()!=null
                    ||uri.getPort()<1||uri.getPort()>65535)throw new IllegalArgumentException();
            String host=uri.getHost();
            if(host.indexOf(':')>=0&&!host.startsWith("["))host="["+host+"]";
            return host+":"+uri.getPort();
        }catch(IllegalArgumentException invalid){
            throw new IllegalArgumentException("ROCKETMQ_CONTROLLER_ENDPOINTS_INVALID");
        }
    }
    public Config legacy(){Config c=new Config();c.jobId=pipelineId;c.nodeId=nodeId;c.source=Codec.decode(Codec.encode(source),Config.Source.class);c.source.database=databases.isEmpty()?null:databases.get(0);c.chunkSize=chunkSize;c.maxTransactionRows=maxTransactionRows;c.leaseMillis=leaseMillis;c.heartbeatMillis=heartbeatMillis;c.pollIntervalMillis=pollIntervalMillis;c.retryDelayMillis=retryDelayMillis;c.maxRetryDelayMillis=Math.max(60000,retryDelayMillis);c.target.type="POSTGRESQL";c.target.url="jdbc:postgresql://unused/unused";c.target.username="unused";c.target.schema="unused";return c;}
    private Path localPath(){if(journal.path==null||journal.path.trim().isEmpty())throw new IllegalArgumentException("LOCAL_JOURNAL_PATH_REQUIRED");return base.resolve(journal.path).normalize().toAbsolutePath();}
    public JdbcJournal.Settings journalSettings(){JdbcJournal.Settings s=new JdbcJournal.Settings();s.pipelineId=pipelineId;s.namespace=journal.namespace;s.maxPendingBytes=journal.maxBytes;
        s.writerAssignment=journal.writerAssignment;s.writerNode=nodeId;s.managedWriter=journal.managedWriter;
        s.leaseMillis=leaseMillis;
        s.kafkaPublicationTargets=targets.stream().filter(target->target.product==TargetProduct.KAFKA)
                .map(target->target.id).collect(java.util.stream.Collectors.toUnmodifiableSet());
        s.kafkaPhysicalGrant=kafkaPhysicalGrant;
        String type=effectiveSourceType();
        s.oracleDdlEvidence="ORACLE".equals(type);
        s.deliveryTiming="POSTGRESQL".equals(type)||"MYSQL".equals(type)&&journal.writerAssignment==null
                &&MysqlCdcSource.originalTimingProfile(source);
        if("LOCAL".equals(journal.mode)){s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+localPath().resolve("rivomesh")+";AUTO_SERVER=TRUE;CACHE_SIZE=4096;WRITE_DELAY=0;LOCK_TIMEOUT=10000";}
        else{s.driver=driver(journal.product);s.url=journal.url;s.username=journal.username;s.password=journal.password;}return s;
    }
    @com.fasterxml.jackson.annotation.JsonIgnore
    KafkaPhysicalReceipt.Grant kafkaPhysicalGrant;
    public JdbcSink.Settings targetSettings(Target target,String generation){JdbcSink.Settings s=new JdbcSink.Settings();s.id=target.id;s.pipelineId=pipelineId;s.generation=generation;s.fingerprint=fingerprint();s.driver=driver(target.product);s.product=target.product;s.url=target.url;s.username=target.username;s.password=target.password;s.metadataSchema=target.metadataSchema;s.uniquePolicy=target.uniquePolicy;s.acknowledgeTextConversions=target.dm!=null&&target.dm.acknowledgeTextConversions;s.databases=new TreeMap<>(target.mappings);return s;}
    public static ClickHouseHttpClient.Settings clickHouseHttp(Target target){ClickHouse o=target.clickhouse==null?new ClickHouse():target.clickhouse;ClickHouseHttpClient.Settings s=new ClickHouseHttpClient.Settings();s.url=target.url;s.username=target.username;s.password=target.password;s.connectTimeoutMillis=o.connectTimeoutMillis;s.requestTimeoutMillis=o.requestTimeoutMillis;s.maximumRequestBytes=o.maximumRequestBytes;s.maximumResponseBytes=o.maximumResponseBytes;return s;}
    public ClickHouseMaterializer.Settings clickHouseSettings(Target target,String generation){ClickHouseMaterializer.Settings s=new ClickHouseMaterializer.Settings();s.http=clickHouseHttp(target);s.pipelineId=pipelineId;s.subscriptionId=target.id;s.generation=generation;s.fingerprint=fingerprint();s.metadataDatabase=target.metadataSchema;s.mappings=new TreeMap<>(target.mappings);s.autoCreate=target.clickhouse==null||target.clickhouse.autoCreate;s.secondaryIndexes=target.clickhouse==null?ClickHouseIndexPlan.Mode.NONE:target.clickhouse.secondaryIndexes;return s;}
    public HiveMaterializer.Settings hiveSettings(Target target,String generation){Hive h=target.hive;HiveMaterializer.Settings s=new HiveMaterializer.Settings();s.jdbcUrl=target.url;s.username=target.username;s.password=target.password;s.metadataDatabase=target.metadataSchema;s.pipelineId=pipelineId;s.subscriptionId=target.id;s.generation=generation;s.fingerprint=fingerprint();s.mappings=new TreeMap<>(target.mappings);s.autoCreate=h.autoCreate;s.acknowledgeIndexOmission=h.acknowledgeIndexOmission;s.queryTimeoutSeconds=h.queryTimeoutSeconds;s.maximumSqlBytes=h.maximumSqlBytes;s.maximumResponseBytes=h.maximumResponseBytes;return s;}
    public static DorisHttpClient.Settings dorisHttp(Target target){Doris o=target.doris;DorisHttpClient.Settings s=new DorisHttpClient.Settings();s.url=o.loadUrl;s.username=target.username;s.password=target.password;s.redirectOrigins=new ArrayList<>(o.redirectOrigins);s.connectTimeoutMillis=o.connectTimeoutMillis;s.requestTimeoutMillis=o.requestTimeoutMillis;s.maximumRequestBytes=o.maximumRequestBytes;s.maximumResponseBytes=o.maximumResponseBytes;return s;}
    public DorisMaterializer.Settings dorisSettings(Target target,String generation){DorisMaterializer.Settings s=new DorisMaterializer.Settings();s.http=dorisHttp(target);s.jdbcUrl=target.url;s.username=target.username;s.password=target.password;s.pipelineId=pipelineId;s.subscriptionId=target.id;s.generation=generation;s.fingerprint=fingerprint();s.metadataDatabase=target.metadataSchema;s.mappings=new TreeMap<>(target.mappings);s.autoCreate=target.doris.autoCreate;s.replicas=target.doris.replicas;s.buckets=target.doris.buckets;s.secondaryIndexes=target.doris.secondaryIndexes;return s;}
    public String fingerprint(){
        Map<String,Object> identity=new TreeMap<>();identity.put("version",formatVersion);identity.put("pipelineId",pipelineId);identity.put("sourceId",source.clusterId);identity.put("topology",source.topology);identity.put("jdbcOptions",source.jdbcOptions);identity.put("databases",new TreeSet<>(databases));
        if("POSTGRESQL".equals(effectiveSourceType())){
            identity.put("sourceType","POSTGRESQL");
            identity.put("sourceDatabase",source.database);
            identity.put("sourceUrl",source.postgresql.url);
            identity.put("publication",source.postgresql.publication);
            identity.put("slotPrefix",source.postgresql.slotPrefix);
            if(source.postgresql.failoverSlot)identity.put("failoverSlot",true);
        }else if("ORACLE".equals(effectiveSourceType())){
            identity.put("sourceType","ORACLE");
            identity.put("sourceDatabase",source.database);
            identity.put("sourceRootUrl",source.oracle.root.url);
            identity.put("sourcePdbUrl",source.oracle.pdb.url);
            identity.put("sourceTables",new TreeSet<>(source.oracle.tables));
            if(source.oracle.allowVirtualNumericColumns)identity.put("oracleVirtualNumericColumns",true);
        }
        if(kafkaHistoryFromFirstSend)identity.put("kafkaHistoryFromFirstSend",true);
        Map<String,Object> outputs=new TreeMap<>();for(Target t:targets){Map<String,Object> output=new TreeMap<>();output.put("product",t.product);output.put("url",t.url);output.put("metadataSchema",t.metadataSchema);output.put("mappings",new TreeMap<>(t.mappings));output.put("uniquePolicy",t.uniquePolicy);if(t.product==TargetProduct.DM)output.put("storageProfile","DM_LOSSLESS_V1");if(t.product==TargetProduct.HIVE)output.put("materializationProfile","HIVE_ACID_HISTORY_V1");if(t.product==TargetProduct.DORIS){output.put("materializationProfile","DORIS_MOW_SEQUENCE_V1");output.put("secondaryIndexes",t.doris.secondaryIndexes);output.put("replicas",t.doris.replicas);output.put("buckets",t.doris.buckets);}if(t.product==TargetProduct.CLICKHOUSE){output.put("materializationProfile","CLICKHOUSE_REPLACING_V1");if(t.clickhouse!=null&&t.clickhouse.secondaryIndexes==ClickHouseIndexPlan.Mode.DATA_SKIPPING)output.put("secondaryIndexes","DATA_SKIPPING_V1");}if(t.product!=null&&t.product.messaging()){Map<String,Object> messaging=new TreeMap<>();messaging.put("stream",t.messaging.stream);messaging.put("queueType",t.messaging.queueType);if(t.product==TargetProduct.KAFKA)messaging.put("topic",t.messaging.topic);if(t.product==TargetProduct.REDIS_STREAMS||t.product==TargetProduct.PULSAR)messaging.put("replicas",t.messaging.replicas);if(t.product==TargetProduct.PULSAR){messaging.put("topic",t.messaging.topic);messaging.put("cluster",t.messaging.cluster);}if(t.product==TargetProduct.ROCKETMQ){messaging.put("topicPrefix",t.messaging.topic);messaging.put("cluster",t.messaging.cluster);messaging.put("broker",t.messaging.broker);messaging.put("minimumRetentionHours",t.messaging.minimumRetentionHours);if(t.messaging.rocketmqControllerEndpoints!=null)messaging.put("replicationProfile","ROCKETMQ_CONTROLLER_SYNC_V1");}if(t.product==TargetProduct.ACTIVEMQ_CLASSIC){messaging.put("queuePrefix",t.messaging.stream);messaging.put("broker",t.messaging.broker);messaging.put("profile","CLASSIC_LOCAL_TX_V1");}if(t.product==TargetProduct.ACTIVEMQ_ARTEMIS){messaging.put("broker",t.messaging.broker==null||t.messaging.broker.isEmpty()?null:t.messaging.broker);messaging.put("replicas",t.messaging.replicas);}output.put("messaging",messaging);}outputs.put(t.id,output);}identity.put("targets",outputs);return Codec.sha256(Codec.encode(identity));
    }
    public List<String> subscribers(){List<String> ids=new ArrayList<>();for(Target target:targets)ids.add(target.id);return ids;}
    public Path localDirectory(){return localPath();}
    public void prepareLocalDirectory()throws Exception{if("LOCAL".equals(journal.mode)){Path directory=localPath();if(Files.isSymbolicLink(directory))throw new IllegalArgumentException("LOCAL_JOURNAL_SYMLINK");Files.createDirectories(directory);try{Files.setPosixFilePermissions(directory,java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"));}catch(UnsupportedOperationException ignored){/* platform ACLs apply */}}}
    public JdbcJournal.Settings existingJournalSettings(){JdbcJournal.Settings s=journalSettings();if("LOCAL".equals(journal.mode)){if(!Files.isRegularFile(localPath().resolve("rivomesh.mv.db")))throw new IllegalStateException("JOURNAL_NOT_INITIALIZED");s.url+=";IFEXISTS=TRUE";}return s;}
}
