package io.github.rivomesh.cdc;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class HiveSqlClientTest {
    @Test void finiteNativeSocketAndRetryBoundsOverrideCallerValues(){String url=HiveSqlClient.boundedUrl("jdbc:hive2://server:10000/default;auth=noSasl;socketTimeout=0;retries=100",300);assertTrue(url.contains("socketTimeout=120000"));assertTrue(url.contains("retries=0"));assertFalse(url.contains("retries=100"));}
    @Test void arbitraryDriverScriptsDiscoveryAndCredentialUrlsAreRejected(){for(String url:new String[]{"jdbc:hive2://user:secret@server:10000/default","jdbc:hive2://server:10000/default;initFile=/tmp/code.sql","jdbc:hive2://server:10000/default;transportMode=http","jdbc:hive2://server:10000/default?hivevar:secret=x","jdbc:hive2://server:10000/default;auth=noSasl;auth=none","jdbc:mysql://server/db"})assertThrows(IllegalArgumentException.class,()->HiveSqlClient.boundedUrl(url,300));}
    @Test void nativeProgressTokensRejectInventedOrTruncatedReceipts(){HiveMaterializer.Settings s=new HiveMaterializer.Settings();s.pipelineId="pipeline";s.subscriptionId="hive";s.generation="generation";s.fingerprint=Codec.sha256("config");s.jdbcUrl="jdbc:hive2://server:10000/default";s.username="writer";s.mappings.put("sales","copy");try(HiveMaterializer hive=new HiveMaterializer(s)){assertThrows(IllegalArgumentException.class,()->hive.resumeProgress("HIVE_RECEIPT_V1:invented"));assertTrue(hive.requiresProgress());}}
}
