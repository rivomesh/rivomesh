package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OceanBaseConfigTest {
    @Test void explicitProductSelectsMysqlProtocolButNotAnUnverifiedJournal(){
        PipelineConfig c=new PipelineConfigTest().config();PipelineConfig.Target target=c.targets.get(0);target.product=TargetProduct.OCEANBASE;target.url="jdbc:mysql://ob:2881/?connectTimeout=5000&socketTimeout=30000";target.username="rivomesh@business";c.validate();
        assertEquals("com.mysql.cj.jdbc.Driver",c.targetSettings(target,"generation").driver);assertTrue(target.product.mysqlDialect());assertFalse(target.product.messaging());
        String fingerprint=c.fingerprint();target.product=TargetProduct.MYSQL;assertNotEquals(fingerprint,c.fingerprint());target.product=TargetProduct.OCEANBASE;
        target.url="jdbc:postgresql://ob:2881/business";assertThrows(IllegalArgumentException.class,c::validate);target.url="jdbc:mysql://ob:2881/";
        c.journal.mode="EXTERNAL";c.journal.product=TargetProduct.OCEANBASE;c.journal.url=target.url;assertThrows(IllegalArgumentException.class,c::validate);
    }
}
