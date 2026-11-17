package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DorisConfigTest {
    PipelineConfig config(){PipelineConfig c=new PipelineConfigTest().config();PipelineConfig.Target t=c.targets.get(0);t.id="doris";t.product=TargetProduct.DORIS;t.url="jdbc:mysql://target:9030/?connectTimeout=0&socketTimeout=0";t.password="";t.doris=new PipelineConfig.Doris();t.doris.loadUrl="http://target:8040";return c;}
    @Test void typedOptionsSeparateMaterializationIdentityFromConnectionTuning(){PipelineConfig c=config();c.validate();assertEquals(3,c.targets.get(0).doris.replicas);String fingerprint=c.fingerprint();PipelineConfig.Target t=c.targets.get(0);t.password="rotated";t.doris.loadUrl="https://other-be:8040";t.doris.requestTimeoutMillis=5000;assertEquals(fingerprint,c.fingerprint());t.doris.secondaryIndexes=DorisIndexPlan.Mode.NONE;assertNotEquals(fingerprint,c.fingerprint());assertTrue(t.product.materialized());assertFalse(t.product.messaging());}
    @Test void unsupportedJournalsPoliciesAndMalformedEndpointConfigurationsAreRejected(){PipelineConfig c=config();PipelineConfig.Target t=c.targets.get(0);t.doris=null;assertThrows(IllegalArgumentException.class,c::validate);t.doris=new PipelineConfig.Doris();t.doris.loadUrl="http://user:password@target:8040";assertThrows(IllegalArgumentException.class,c::validate);t.doris.loadUrl="http://target:8040";t.doris.redirectOrigins=null;assertThrows(IllegalArgumentException.class,c::validate);t.doris.redirectOrigins=new ArrayList<>();t.doris.replicas=0;assertThrows(IllegalArgumentException.class,c::validate);t.doris.replicas=1;c.validate();c.journal.mode="EXTERNAL";c.journal.product=TargetProduct.DORIS;c.journal.url=t.url;assertThrows(IllegalArgumentException.class,c::validate);}
    @Test void finiteSocketsDisableTheConnectorCancellationTimerEvenWhenUrlRequestsIt()throws Exception{try(DorisSqlClient client=new DorisSqlClient("jdbc:mysql://target:9030/?connectTimeout=0&socketTimeout=0&enableQueryTimeouts=true&useServerPrepStmts=false&emulateUnsupportedPstmts=true","user","password",7000)){java.lang.reflect.Field field=DorisSqlClient.class.getDeclaredField("url");field.setAccessible(true);String effective=(String)field.get(client);assertTrue(effective.contains("connectTimeout=5000"));assertTrue(effective.contains("socketTimeout=7000"));assertTrue(effective.contains("enableQueryTimeouts=false"));assertFalse(effective.contains("enableQueryTimeouts=true"));assertTrue(effective.contains("useServerPrepStmts=true"));assertTrue(effective.contains("emulateUnsupportedPstmts=false"));assertFalse(effective.contains("useServerPrepStmts=false"));assertFalse(effective.contains("emulateUnsupportedPstmts=true"));}}

}
