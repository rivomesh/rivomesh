package io.github.rivomesh.cdc;

import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DmStorageTest {
    TableSchema.Column column(String name,TableSchema.Type type){TableSchema.Column c=new TableSchema.Column();c.name=name;c.type=type;return c;}
    @Test void wideDecimalsAndJsonHaveExplicitLosslessTextPlans(){
        TableSchema t=new TableSchema();t.database="source";t.name="rows";t.id=TableSchema.identity(t.database,"",t.name);
        TableSchema.Column amount=column("amount",TableSchema.Type.DECIMAL);amount.precision=65;amount.scale=30;TableSchema.Column json=column("payload",TableSchema.Type.JSON);t.columns=Arrays.asList(amount,json);
        assertEquals("DM_TEXT_CONVERSION_ACK_REQUIRED",assertThrows(IllegalArgumentException.class,()->DmStorage.plan(t,false)).getMessage());
        assertEquals("VARCHAR(68) [DECIMAL_TEXT_REVIEW_REQUIRED]",DmStorage.plan(t,true).get("amount"));assertEquals("CLOB [JSON_TEXT_REVIEW_REQUIRED]",DmStorage.plan(t,true).get("payload"));
        amount.precision=38;amount.scale=20;assertEquals("DECIMAL(38,20)",DmStorage.type(amount));amount.precision=1001;assertThrows(IllegalArgumentException.class,()->DmStorage.type(amount));
    }
    @Test void stringsAndIndexableBinaryRetainCapacityAndLargeObjectsNeedIndexPlan(){
        TableSchema.Column text=column("text",TableSchema.Type.STRING);text.length=100;assertEquals("VARCHAR(100 CHAR)",DmStorage.type(text));text.length=10000;assertEquals("CLOB",DmStorage.type(text));
        TableSchema.Column bytes=column("id",TableSchema.Type.BYTES);bytes.length=100;assertEquals("VARBINARY(100)",DmStorage.type(bytes));
        TableSchema t=new TableSchema();t.columns=Collections.singletonList(text);t.primaryKey=Collections.singletonList("text");assertEquals("DM_LOB_INDEX_REQUIRES_PLAN",assertThrows(IllegalArgumentException.class,()->DmStorage.plan(t,true)).getMessage());
    }
    @Test void nativeDaySecondIntervalPreservesDurationRatherThanWrappingClock(){
        assertEquals("INTERVAL '-1 01:01:02.123456' DAY(2) TO SECOND(6)",DmStorage.interval("-25:01:02.123456"));
        assertEquals("INTERVAL '-0 00:00:00.000001' DAY(2) TO SECOND(6)",DmStorage.interval("-00:00:00.000001"));
        assertEquals("INTERVAL '34 22:59:59.000000' DAY(2) TO SECOND(6)",DmStorage.interval("838:59:59"));assertNull(DmStorage.interval(null));
        assertThrows(IllegalArgumentException.class,()->DmStorage.interval("839:00:00"));
    }
}
