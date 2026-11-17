package io.github.rivomesh;

import io.github.rivomesh.Model.Column;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConversionTest {
    @Test void unsignedAndTemporalBinlogValuesPreserveTheirRangeAndPrecision() {
        Column c = new Column(); c.name = "n"; c.mysqlType = "bigint"; c.definition = "bigint unsigned"; c.targetType = "numeric(20,0)";
        assertEquals("18446744073709551615", c.fromBinlog(-1L));
        c.mysqlType = "datetime"; c.targetType = "timestamp(6)";
        assertEquals("2024-01-01 00:00:00.123456", c.fromBinlog(1704067200123456L));
        assertEquals("1969-12-31 23:59:59.999999", c.fromBinlog(-1L));
        assertNull(c.fromBinlog(null));
        assertThrows(IllegalArgumentException.class, () -> c.fromBinlog(Long.MIN_VALUE));
    }

    @Test void identifierQuotingCannotChangeTheSqlStructure() {
        assertEquals("\"a\"\";drop schema x;--\"", Sql.pg("a\";drop schema x;--"));
        assertEquals("`a``b`", Sql.mysql("a`b"));
        assertThrows(IllegalArgumentException.class, () -> Sql.pg(new String(new char[64]).replace('\0', 'a')));
    }
}
