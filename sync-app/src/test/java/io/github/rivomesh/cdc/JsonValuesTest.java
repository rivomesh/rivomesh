package io.github.rivomesh.cdc;

import io.github.rivomesh.Config;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JsonValuesTest {
    @Test void precisionNullsAndSemanticComparisonAreExplicit(){
        String value=JsonValues.text("{\"n\":12345678901234567890.12345678901234567890,\"u\":18446744073709551615}");
        assertTrue(value.contains("12345678901234567890.12345678901234567890"));assertTrue(value.contains("18446744073709551615"));
        assertFalse(JsonValues.equivalent(null,"null"));assertTrue(JsonValues.equivalent("{\"a\":1,\"b\":[2.0]}","{\"b\":[2],\"a\":1.0}"));
        assertFalse(JsonValues.equivalent("[1,2]","[2,1]"));assertThrows(IllegalArgumentException.class,()->JsonValues.text("{} trailing"));
    }
    @Test void addingAnEmbeddedJournalDoesNotEnableArbitraryDriversThroughTheLegacyTaskApi(){
        String yaml="jobId: blocked\nsource:\n  database: test\n  username: test\ntarget:\n  driver: org.h2.Driver\n  url: jdbc:h2:mem:blocked\n  schema: test\n  username: test\n";
        assertThrows(IllegalArgumentException.class,()->Config.parse(yaml));
    }
}
