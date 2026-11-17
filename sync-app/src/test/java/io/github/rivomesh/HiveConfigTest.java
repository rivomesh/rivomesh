package io.github.rivomesh;
import io.github.rivomesh.cdc.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class HiveConfigTest {
    @Test void nativeOptionsAreExplicitAndAcknowledgementDoesNotResetIdentity(){PipelineConfig c=new PipelineConfigTest().config();PipelineConfig.Target t=c.targets.get(0);t.product=TargetProduct.HIVE;t.url="jdbc:hive2://hive:10000/default;auth=noSasl";assertThrows(IllegalArgumentException.class,c::validate);t.hive=new PipelineConfig.Hive();c.validate();String fingerprint=c.fingerprint();t.hive.acknowledgeIndexOmission=true;t.hive.queryTimeoutSeconds=600;assertEquals(fingerprint,c.fingerprint());assertTrue(t.product.materialized());assertFalse(t.product.messaging());t.uniquePolicy=JdbcSink.UniquePolicy.PRESERVE;assertThrows(IllegalArgumentException.class,c::validate);t.uniquePolicy=JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS;c.journal.mode="EXTERNAL";c.journal.product=TargetProduct.HIVE;c.journal.url=t.url;assertThrows(IllegalArgumentException.class,c::validate);}
}
