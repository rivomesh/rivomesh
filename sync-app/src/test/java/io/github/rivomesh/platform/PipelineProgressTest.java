package io.github.rivomesh.platform;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class PipelineProgressTest {
    @TempDir Path directory;
    @Test void publicProgressSeparatesSqlCommitBrokerAcceptanceAndMaterializerAcknowledgment()throws Exception{
        PipelineConfig config=PlatformTest.pipeline("progress");config.journal.path=directory.resolve("journal").toString();
        for(TargetProduct product:Arrays.asList(TargetProduct.RABBITMQ,TargetProduct.CLICKHOUSE)){
            PipelineConfig.Target target=new PipelineConfig.Target();target.id=product.name().toLowerCase(Locale.ROOT);target.product=product;target.url="http://unused/"+target.id;target.messaging=new PipelineConfig.Messaging();config.targets.add(target);
        }
        config.prepareLocalDirectory();try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
            journal.initialize(config.fingerprint(),"generation",config.subscribers());Map<String,Object> status=PipelineBackend.status(config);assertEquals("0",status.get("covered_sequence"));assertEquals("0",status.get("captured_sequence"));assertFalse(status.containsKey("applied_sequence"));
            @SuppressWarnings("unchecked")List<Map<String,Object>> targets=(List<Map<String,Object>>)status.get("subscriptions");Map<String,String> semantics=new TreeMap<>();for(Map<String,Object> target:targets){semantics.put((String)target.get("id"),(String)target.get("progressSemantics"));assertEquals("0",target.get("safeCoveredSequence"));assertEquals("0",target.get("contiguousDeliveredSequence"));}assertEquals("SQL_COMMITTED",semantics.get("pg"));assertEquals("BROKER_ACCEPTED",semantics.get("rabbitmq"));assertEquals("MATERIALIZER_ACKNOWLEDGED",semantics.get("clickhouse"));
        }
    }
}
