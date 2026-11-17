package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DorisRowsTest {
    static TableSchema.Column column(String name,TableSchema.Type type){TableSchema.Column c=new TableSchema.Column();c.name=name;c.type=type;return c;}
    static TableSchema table(){TableSchema t=new TableSchema();t.database="business";t.name="items";t.id=TableSchema.identity(t.database,"",t.name);return t;}
    @Test void precisionAndWireRepresentationsSurviveNativeJsonEncoding()throws Exception{
        TableSchema t=table();TableSchema.Column decimal=column("amount",TableSchema.Type.DECIMAL);decimal.precision=65;decimal.scale=30;t.columns=Arrays.asList(column("id",TableSchema.Type.UINT64),decimal,column("json",TableSchema.Type.JSON),column("bytes",TableSchema.Type.BYTES));String exact="12345678901234567890123456789012345.123456789012345678901234567890",json="{\"number\":18446744073709551615,\"fraction\":0.123456789012345678901234567890}";
        BigInteger version=BigInteger.ONE.shiftLeft(90).add(BigInteger.ONE);ObjectNode encoded=DorisRows.row(t,Arrays.asList(Cell.value("18446744073709551615"),Cell.value(exact),Cell.value(json),Cell.value("AP+A")),"key",version,false,4096);
        assertEquals("18446744073709551615",encoded.path("id").asText());assertEquals(exact,encoded.path("amount").asText());assertEquals(json,encoded.path("json").asText());assertEquals("AP+A",encoded.path("bytes").asText());assertEquals(version.toString(),encoded.path("_rivomesh_version").asText());assertEquals("DECIMAL(65,30)",DorisRows.type(decimal));
    }
    @Test void missingNullOverflowNonfiniteAndMalformedUtf8CannotReachLoad()throws Exception{
        TableSchema t=table();t.columns=Collections.singletonList(column("v",TableSchema.Type.UINT64));
        for(Cell bad:Arrays.asList(Cell.missing(),Cell.value("-1"),Cell.value("18446744073709551616")))assertThrows(IllegalArgumentException.class,()->DorisRows.row(t,Collections.singletonList(bad),"key",BigInteger.ZERO,false,1024));
        t.columns.get(0).nullable=false;assertThrows(IllegalArgumentException.class,()->DorisRows.row(t,Collections.singletonList(Cell.value(null)),"key",BigInteger.ZERO,false,1024));
        t.columns.get(0).type=TableSchema.Type.FLOAT64;assertThrows(IllegalArgumentException.class,()->DorisRows.row(t,Collections.singletonList(Cell.value("NaN")),"key",BigInteger.ZERO,false,1024));
        t.columns.get(0).type=TableSchema.Type.STRING;
        for(String malformed:Arrays.asList("bad\ud800","bad\udfff")){
            IllegalArgumentException failure=assertThrows(IllegalArgumentException.class,()->DorisRows.row(t,Collections.singletonList(Cell.value(malformed)),"key",BigInteger.ZERO,false,1024));
            assertEquals("DORIS_TEXT_ENCODING_INVALID",failure.getMessage());assertFalse(io.github.rivomesh.Sql.retryable(failure));assertNull(failure.getCause());
        }
        assertEquals("emoji \ud83d\ude03",DorisRows.row(t,Collections.singletonList(Cell.value("emoji \ud83d\ude03")),"key",BigInteger.ZERO,false,1024).path("v").asText());
    }
    @Test void reservedColumnsAndOversizedRowsAreRejected(){TableSchema t=table();t.columns=Collections.singletonList(column("_RIVOMESH_KEY",TableSchema.Type.STRING));assertThrows(IllegalArgumentException.class,()->DorisRows.row(t,Collections.singletonList(Cell.value("override")),"key",BigInteger.ZERO,false,1024));t.columns.get(0).name="text";assertThrows(IllegalArgumentException.class,()->DorisRows.row(t,Collections.singletonList(Cell.value(String.join("",Collections.nCopies(1000,"宽")))),"key",BigInteger.ZERO,false,1024));}
    @Test void indexConversionPreservesIdentityAndReportsLostBusinessSemantics(){TableSchema t=table();t.columns=Arrays.asList(column("id",TableSchema.Type.INT64),column("email",TableSchema.Type.STRING));TableSchema.Index primary=new TableSchema.Index();primary.primary=true;primary.name="PRIMARY";primary.columns=Collections.singletonList("id");TableSchema.Index unique=new TableSchema.Index();unique.name="uq";unique.unique=true;unique.method="BTREE";unique.columns=Collections.singletonList("email");TableSchema.Index gsi=new TableSchema.Index();gsi.name="gsi";gsi.method="GSI";gsi.columns=Arrays.asList("email","id");t.indexes=Arrays.asList(primary,unique,gsi);DorisIndexPlan p=DorisIndexPlan.plan(t,DorisIndexPlan.Mode.INVERTED);p.requireSupported();assertEquals(2,p.indexes.size());assertEquals("KEEP_ROW_IDENTITY",p.decisions.get(0).action);assertEquals("CREATE_NON_UNIQUE_INVERTED",p.decisions.get(1).action);unique.prefixLengths=Collections.singletonList(10);assertThrows(IllegalArgumentException.class,()->DorisIndexPlan.plan(t,DorisIndexPlan.Mode.INVERTED).requireSupported());assertTrue(DorisIndexPlan.plan(t,DorisIndexPlan.Mode.NONE).indexes.isEmpty());}
}
