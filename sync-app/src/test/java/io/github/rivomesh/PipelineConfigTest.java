package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigTest {
    @Test void gbase8cHasAnExplicitVendorFamilyAndDoesNotEnableItsJournal(){
        PipelineConfig c=config();PipelineConfig.Target target=c.targets.get(0);target.product=TargetProduct.GBASE_8C;target.url="jdbc:gbase8c://gbase8c:5432/postgres?connectTimeout=10";c.validate();
        assertEquals("com.gbase8c.Driver",c.targetSettings(target,"generation").driver);String fingerprint=c.fingerprint();target.password="rotated";assertEquals(fingerprint,c.fingerprint());
        target.url="jdbc:postgresql://gbase8c/postgres";assertThrows(IllegalArgumentException.class,c::validate);target.url="jdbc:gbase8c://gbase8c/postgres";
        c.journal.mode="EXTERNAL";c.journal.product=TargetProduct.GBASE_8C;c.journal.url=target.url;c.journal.username="user";assertThrows(IllegalArgumentException.class,c::validate);
        assertThrows(IllegalArgumentException.class,()->TargetProduct.valueOf("GBASE"));
    }
    @Test void dmUsesVendorUrlAndExplicitTextReviewWithoutEnablingItsJournal(){
        PipelineConfig c=config();PipelineConfig.Target target=c.targets.get(0);target.product=TargetProduct.DM;target.url="jdbc:dm://dm:5236?connectTimeout=5000";c.validate();assertNotNull(target.dm);assertFalse(target.dm.acknowledgeTextConversions);
        String fingerprint=c.fingerprint();target.dm.acknowledgeTextConversions=true;assertEquals(fingerprint,c.fingerprint());assertTrue(c.targetSettings(target,"generation").acknowledgeTextConversions);assertEquals("dm.jdbc.driver.DmDriver",c.targetSettings(target,"generation").driver);
        target.url="jdbc:postgresql://dm/test";assertThrows(IllegalArgumentException.class,c::validate);target.url="jdbc:dm://dm:5236";
        c.journal.mode="EXTERNAL";c.journal.product=TargetProduct.DM;c.journal.url=target.url;c.journal.username="user";assertThrows(IllegalArgumentException.class,c::validate);
    }
    @Test void kingbaseUsesVendorDriverAndCannotBeUsedAsAnUnverifiedJournal(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.KINGBASE;t.url="jdbc:kingbase8://kingbase:54321/test?connectTimeout=10&socketTimeout=30";c.validate();
        assertEquals("com.kingbase8.Driver",c.targetSettings(t,"generation").driver);String fingerprint=c.fingerprint();t.password="rotated";assertEquals(fingerprint,c.fingerprint());
        t.url="jdbc:postgresql://kingbase:54321/test";assertThrows(IllegalArgumentException.class,c::validate);t.url="jdbc:kingbase8://kingbase:54321/test";
        c.journal.mode="EXTERNAL";c.journal.product=TargetProduct.KINGBASE;c.journal.url=t.url;c.journal.username="system";assertThrows(IllegalArgumentException.class,c::validate);
    }
    PipelineConfig config(){PipelineConfig c=new PipelineConfig();c.pipelineId="pipeline";c.source.username="reader";c.source.clusterId="source";c.databases=Arrays.asList("orders","inventory");PipelineConfig.Target t=new PipelineConfig.Target();t.id="pg";t.product=TargetProduct.POSTGRESQL;t.url="jdbc:postgresql://target/db";t.username="writer";t.mappings.put("orders","orders");t.mappings.put("inventory","inventory");c.targets.add(t);return c;}
    @Test void sourceProtocolAndVersionHaveExplicitValidationWithoutChangingOldMySqlLineage(){
        PipelineConfig config=config();
        String original=config.fingerprint();
        config.source.type="MYSQL";
        config.source.expectedVersion="8.2.0";
        config.validate();
        assertEquals("MYSQL",config.effectiveSourceType());
        assertEquals(original,config.fingerprint());
        config.source.expectedVersion="8";
        assertEquals("SOURCE_VERSION_PROFILE_INVALID",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
        config.source.expectedVersion=null;
        config.sourceType="POSTGRESQL";
        assertEquals("SOURCE_TYPE_CONFLICT",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
        config.sourceType=null;
        config.source.type="ORACLE";
        assertEquals("ORACLE_SOURCE_CONFIG_INVALID",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
    }
    @Test void sourceEngineRejectsOptionsForAnotherProtocol(){
        PipelineConfig c=config();
        c.source.oracle=new Config.OracleSource();
        assertEquals("ORACLE_OPTIONS_REQUIRE_ORACLE_SOURCE",assertThrows(IllegalArgumentException.class,c::validate).getMessage());
        c.source.type="POSTGRESQL";
        assertEquals("ORACLE_OPTIONS_REQUIRE_ORACLE_SOURCE",assertThrows(IllegalArgumentException.class,c::validate).getMessage());
        c.source.type="ORACLE";
        c.source.postgresql=new Config.PostgresqlSource();
        assertEquals("POSTGRESQL_OPTIONS_REQUIRE_POSTGRESQL_SOURCE",assertThrows(IllegalArgumentException.class,c::validate).getMessage());
    }
    @Test void passwordsAndTuningCanRotateWhileSemanticChangesRequireAMigration(){PipelineConfig c=config();c.validate();String fingerprint=c.fingerprint();c.source.password="rotated";c.targets.get(0).password="rotated-too";c.chunkSize=200;c.nodeId="another-node";c.journal.maxBytes*=2;assertEquals(fingerprint,c.fingerprint());c.targets.get(0).mappings.put("orders","renamed");assertNotEquals(fingerprint,c.fingerprint());}
    @Test void sourceMappingsCannotCollideOrDisappear(){PipelineConfig c=config();c.targets.get(0).mappings.put("inventory","orders");assertThrows(IllegalArgumentException.class,c::validate);c.targets.get(0).mappings.remove("inventory");assertThrows(IllegalArgumentException.class,c::validate);}
    @Test void embeddedSqlDriverAndUrlOptionsCannotBeImportedAsTargetsOrLocalPaths(){PipelineConfig c=config();c.journal.path="data/journal;INIT=RUNSCRIPT FROM 'remote'";assertThrows(IllegalArgumentException.class,c::validate);c=config();c.targets.get(0).product=TargetProduct.H2;assertThrows(IllegalArgumentException.class,c::validate);c=config();c.targets.get(0).url="jdbc:h2:mem:danger";assertThrows(IllegalArgumentException.class,c::validate);}
    @Test void messageSettingsSeparateSecretsFromStreamIdentity(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.NATS_JETSTREAM;t.url="nats://broker:4222";t.username=null;t.messaging=new PipelineConfig.Messaging();t.messaging.stream="SALES";c.validate();String original=c.fingerprint();
        t.messaging.token="rotated";t.messaging.requestTimeoutMillis=12000;t.messaging.endpoints.add("nats://standby:4222");assertEquals(original,c.fingerprint());t.messaging.stream="DIFFERENT";assertNotEquals(original,c.fingerprint());
        t.url="nats://user:secret@broker:4222";assertThrows(IllegalArgumentException.class,c::validate);t.url="nats://broker:4222";t.mappings.put("orders","wild.card");assertThrows(IllegalArgumentException.class,c::validate);
        t.mappings.put("orders","orders");t.product=TargetProduct.RABBITMQ;t.url="amqp://broker:5672/";t.messaging.endpoints.clear();assertThrows(IllegalArgumentException.class,c::validate);t.username="publisher";c.validate();
    }

    @Test void jetStreamFailoverRequiresMatchingTransportSecurity(){
        PipelineConfig c=config();PipelineConfig.Target target=c.targets.get(0);
        target.product=TargetProduct.NATS_JETSTREAM;target.url="tls://primary:4222";
        target.messaging=new PipelineConfig.Messaging();target.messaging.stream="ORDERS";
        target.messaging.endpoints.add("nats://alternate:4222");
        assertEquals("JETSTREAM_ENDPOINT_TLS_MISMATCH",assertThrows(IllegalArgumentException.class,c::validate).getMessage());
        target.messaging.endpoints.set(0,"tls://alternate:4222");c.validate();
    }

    @Test void kafkaTopicAndSecurityProfilesRequireExplicitValidPlans(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.KAFKA;t.url="kafka://broker:9092";t.messaging=new PipelineConfig.Messaging();assertThrows(IllegalArgumentException.class,c::validate);t.messaging.topic="sales.events";c.validate();String fingerprint=c.fingerprint();t.messaging.securityProtocol="SASL_SSL";t.messaging.saslMechanism="SCRAM-SHA-512";assertThrows(IllegalArgumentException.class,c::validate);t.password="rotated-secret";c.validate();assertEquals(fingerprint,c.fingerprint());t.messaging.topic="other.events";assertNotEquals(fingerprint,c.fingerprint());
        t.messaging.saslMechanism="arbitrary-login-class";assertThrows(IllegalArgumentException.class,c::validate);
    }

    @Test void redisRequiresAnExplicitStreamAndDirectEndpoint(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.REDIS_STREAMS;t.url="redis://redis:6379/0";t.username=null;t.messaging=new PipelineConfig.Messaging();assertThrows(IllegalArgumentException.class,c::validate);t.messaging.stream="sales.events";c.validate();String fingerprint=c.fingerprint();t.password="rotated";assertEquals(fingerprint,c.fingerprint());t.messaging.replicas=2;assertNotEquals(fingerprint,c.fingerprint());t.messaging.replicas=1;t.messaging.stream="other.events";assertNotEquals(fingerprint,c.fingerprint());t.messaging.endpoints.add("redis://independent:6379/0");assertThrows(IllegalArgumentException.class,c::validate);t.messaging.endpoints.clear();t.url="redis://redis:6379/not-a-database";assertThrows(IllegalArgumentException.class,c::validate);
    }
    @Test void pulsarRequiresPersistentTopicClusterAdminAndStableDurability(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.PULSAR;t.username=null;t.url="pulsar://broker:6650";t.messaging=new PipelineConfig.Messaging();assertThrows(IllegalArgumentException.class,c::validate);t.messaging.topic="persistent://public/default/sales";t.messaging.cluster="standalone";t.messaging.adminUrl="http://broker:8080";c.validate();String fingerprint=c.fingerprint();t.messaging.token="rotated";t.messaging.adminUrl="http://other-broker:8080";assertEquals(fingerprint,c.fingerprint());t.messaging.replicas=2;assertNotEquals(fingerprint,c.fingerprint());t.messaging.replicas=1;t.messaging.cluster="other";assertNotEquals(fingerprint,c.fingerprint());t.messaging.cluster="standalone";
        t.messaging.topic="non-persistent://public/default/sales";assertThrows(IllegalArgumentException.class,c::validate);t.messaging.topic="persistent://public/default/sales";t.url="pulsar+ssl://broker:6651";assertThrows(IllegalArgumentException.class,c::validate);t.messaging.adminUrl="https://broker:8443";c.validate();t.messaging.adminUrl="https://user:secret@broker:8443";assertThrows(IllegalArgumentException.class,c::validate);t.messaging.adminUrl="https://broker:8443";t.username="unused";assertThrows(IllegalArgumentException.class,c::validate);
    }

    @Test void pulsarPrivateCaRequiresTlsAndDoesNotReassignTheTopic(){
        PipelineConfig config=config();
        PipelineConfig.Target target=config.targets.get(0);
        target.product=TargetProduct.PULSAR;
        target.username=null;
        target.url="pulsar+ssl://broker:6651";
        target.messaging=new PipelineConfig.Messaging();
        target.messaging.adminUrl="https://broker:8443";
        target.messaging.cluster="standalone";
        target.messaging.topic="persistent://public/default/sales";
        config.validate();
        String original=config.fingerprint();
        target.messaging.tlsTrustCertsFilePath="/run/secrets/pulsar-ca.pem";
        config.validate();
        assertEquals(original,config.fingerprint());
        target.messaging.tlsTrustCertsFilePath="pulsar-ca.pem";
        assertEquals("PULSAR_TLS_TRUST_CONFIG_INVALID",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
        target.messaging.tlsTrustCertsFilePath="/run/secrets/pulsar-ca.pem";
        target.url="pulsar://broker:6650";
        target.messaging.adminUrl="http://broker:8080";
        assertEquals("PULSAR_TLS_TRUST_CONFIG_INVALID",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
    }
    @Test void rocketUsesGenerationBoundNamesAndRejectsUnverifiedDurabilityAndCredentials(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.ROCKETMQ;t.url="rocketmq://nameserver:9876";t.username=null;t.messaging=new PipelineConfig.Messaging();t.messaging.topic="sales";t.messaging.cluster="business";t.messaging.broker="broker-a";c.validate();String original=c.fingerprint();t.messaging.endpoints.add("rocketmq://standby:9876");t.messaging.connectTimeoutMillis=8000;assertEquals(original,c.fingerprint());t.messaging.minimumRetentionHours++;assertNotEquals(original,c.fingerprint());t.messaging.minimumRetentionHours--;t.messaging.broker="broker-b";assertNotEquals(original,c.fingerprint());t.messaging.replicas=2;assertThrows(IllegalArgumentException.class,c::validate);t.messaging.replicas=1;t.password="unsupported-secret";assertThrows(IllegalArgumentException.class,c::validate);t.password=null;t.url="rocketmq://nameserver:9876/path";assertThrows(IllegalArgumentException.class,c::validate);
        String first=MessagePublishers.binding(c.pipelineId,t.id,"first"),second=MessagePublishers.binding(c.pipelineId,t.id,"second");assertNotEquals(io.github.rivomesh.transport.rocketmq.RocketTransport.physicalTopic("sales",first),io.github.rivomesh.transport.rocketmq.RocketTransport.physicalTopic("sales",second));assertEquals(io.github.rivomesh.transport.rocketmq.RocketTransport.physicalTopic("sales",first),io.github.rivomesh.transport.rocketmq.RocketTransport.physicalTopic("sales",first));
    }
    @Test void artemisRequiresRetainedQueuePrefixCredentialsAndOneLocalCopy(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.ACTIVEMQ_ARTEMIS;t.url="artemis://broker:61616";t.password="secret";t.messaging=new PipelineConfig.Messaging();t.messaging.stream="rivomesh";c.validate();String fingerprint=c.fingerprint();t.password="rotated";assertEquals(fingerprint,c.fingerprint());t.messaging.stream="other";assertNotEquals(fingerprint,c.fingerprint());t.messaging.stream="rivomesh";t.messaging.replicas=2;assertThrows(IllegalArgumentException.class,c::validate);t.messaging.replicas=1;t.messaging.endpoints.add("artemis://other:61616");assertThrows(IllegalArgumentException.class,c::validate);t.messaging.endpoints.clear();t.url="artemis://user:password@broker:61616";assertThrows(IllegalArgumentException.class,c::validate);
    }
    @Test void classicRequiresFixedAuthenticatedBrokerAndSeparateManagementEndpoint(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.ACTIVEMQ_CLASSIC;t.url="activemq://classic:61616";t.password="secret";t.messaging=new PipelineConfig.Messaging();t.messaging.stream="rivomesh";t.messaging.broker="rivomesh-core";t.messaging.adminUrl="http://classic:8161/api/jolokia/";c.validate();String fingerprint=c.fingerprint();t.password="rotated";t.messaging.adminUrl="https://classic:8162/api/jolokia/";assertEquals(fingerprint,c.fingerprint());t.messaging.stream="other";assertNotEquals(fingerprint,c.fingerprint());t.messaging.stream="rivomesh";t.messaging.endpoints.add("activemq://other:61616");assertThrows(IllegalArgumentException.class,c::validate);t.messaging.endpoints.clear();t.url="activemq://user:password@classic:61616";assertThrows(IllegalArgumentException.class,c::validate);t.url="activemq://classic:61616";t.messaging.adminUrl="http://classic:8161/api/jolokia/?operation=delete";assertThrows(IllegalArgumentException.class,c::validate);
    }
    @Test void clickhouseUsesHttpVersionedRowsWithExplicitLimitsAndStableTuning(){
        PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.CLICKHOUSE;t.url="http://clickhouse:8123";t.password="secret";c.validate();assertNotNull(t.clickhouse);assertEquals(ClickHouseIndexPlan.Mode.NONE,t.clickhouse.secondaryIndexes);String legacy=c.fingerprint();t.clickhouse=null;assertEquals(legacy,c.fingerprint());c.validate();t.clickhouse.secondaryIndexes=ClickHouseIndexPlan.Mode.DATA_SKIPPING;c.validate();String original=c.fingerprint();assertNotEquals(legacy,original);t.clickhouse.secondaryIndexes=null;assertThrows(IllegalArgumentException.class,c::validate);t.clickhouse.secondaryIndexes=ClickHouseIndexPlan.Mode.NONE;assertEquals(legacy,c.fingerprint());t.clickhouse.secondaryIndexes=ClickHouseIndexPlan.Mode.DATA_SKIPPING;assertEquals(original,c.fingerprint());t.password="rotated";t.clickhouse.requestTimeoutMillis=12000;t.clickhouse.maximumRequestBytes=8*1024*1024;c.validate();assertEquals(original,c.fingerprint());
        t.url="http://user:secret@clickhouse:8123";assertThrows(IllegalArgumentException.class,c::validate);t.url="http://clickhouse:8123?query=DROP";assertThrows(IllegalArgumentException.class,c::validate);t.url="jdbc:clickhouse://clickhouse/default";assertThrows(IllegalArgumentException.class,c::validate);t.url="http://clickhouse:8123";t.uniquePolicy=JdbcSink.UniquePolicy.PRESERVE;assertThrows(IllegalArgumentException.class,c::validate);t.uniquePolicy=JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS;t.clickhouse.maximumResponseBytes=1;assertThrows(IllegalArgumentException.class,c::validate);t.clickhouse.maximumResponseBytes=1024;t.product=TargetProduct.POSTGRESQL;t.url="jdbc:postgresql://target/db";assertThrows(IllegalArgumentException.class,c::validate);
    }

}
