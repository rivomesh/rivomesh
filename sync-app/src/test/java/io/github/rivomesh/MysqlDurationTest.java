package io.github.rivomesh;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MysqlDurationTest {
    static ByteArrayInputStream bytes(String hex){byte[]data=new byte[hex.length()/2];for(int i=0;i<data.length;i++)data[i]=(byte)Integer.parseInt(hex.substring(i*2,i*2+2),16);return new ByteArrayInputStream(data);}
    @Test void signedDurationsAreCanonicalWithoutClockWrapping(){
        assertNull(MysqlDuration.canonical(null));assertEquals("-25:01:02.123456",MysqlDuration.canonical("-25:01:02.123456"));
        assertEquals("00:00:00.000000",MysqlDuration.canonical("-0:00:00.0"));assertEquals("01:02:03.100000",MysqlDuration.canonical("+1:02:03.1"));
        assertEquals("-838:59:59.000000",MysqlDuration.format(-3020399000000L));assertEquals("838:59:59.000000",MysqlDuration.format(3020399000000L));
        for(String value:Arrays.asList("839:00:00","838:59:59.000001","00:60:00","00:00:60","01:02:03.1234567","P1D","1:2:3"))assertThrows(IllegalArgumentException.class,()->MysqlDuration.canonical(value));
        assertThrows(IllegalArgumentException.class,()->MysqlDuration.format(Long.MIN_VALUE));
    }
    @Test void binaryTime2AllPrecisionsRetainNegativeFractions()throws Exception{
        String[] positive={"819042","8190420a","8190420c","81904204ce","81904204d2","81904201e23a","81904201e240"};
        String[] negative={"7e6fbe","7e6fbdf6","7e6fbdf4","7e6fbdfb32","7e6fbdfb2e","7e6fbdfe1dc6","7e6fbdfe1dc0"};
        for(int fsp=0;fsp<=6;fsp++){
            long quantum=(long)Math.pow(10,6-fsp),micro=123456/quantum*quantum,expected=90062000000L+micro;
            assertEquals(expected,MysqlDuration.time2(fsp,bytes(positive[fsp])));assertEquals(-expected,MysqlDuration.time2(fsp,bytes(negative[fsp])));
        }
        assertEquals(-10000,MysqlDuration.time2(2,bytes("7fffffff")));assertEquals(-990000,MysqlDuration.time2(2,bytes("7fffff9d")));
        assertEquals(-1,MysqlDuration.time2(6,bytes("7fffffffffff")));assertEquals(0,MysqlDuration.time2(6,bytes("800000000000")));
    }
    @Test void legacySigned24BitAndMalformedPayloadsAreExplicit()throws Exception{
        assertEquals(1000000,MysqlDuration.legacy(bytes("010000")));assertEquals(-1000000,MysqlDuration.legacy(bytes("ffffff")));
        assertThrows(IOException.class,()->MysqlDuration.time2(7,bytes("800000")));assertThrows(IOException.class,()->MysqlDuration.time2(6,bytes("80000000")));
        assertThrows(IOException.class,()->MysqlDuration.time2(0,bytes("80003f")));assertThrows(IOException.class,()->MysqlDuration.time2(1,bytes("80000001")));
    }
    @Test void everyRowsEventUsesTheDurationOverrideWithoutChangingOtherCodecs()throws Exception{
        EventDeserializer decoder=MysqlEventDeserializer.create(new HashMap<>());
        for(EventType type:Arrays.asList(EventType.WRITE_ROWS,EventType.UPDATE_ROWS,EventType.DELETE_ROWS,EventType.EXT_WRITE_ROWS,EventType.EXT_UPDATE_ROWS,EventType.EXT_DELETE_ROWS)){
            Object rows=decoder.getEventDataDeserializer(type);Method method=rows.getClass().getDeclaredMethod("deserializeTimeV2",int.class,ByteArrayInputStream.class);method.setAccessible(true);
            assertEquals(-90062123456L,method.invoke(rows,6,bytes("7e6fbdfe1dc0")));
        }
        assertTrue(decoder.getEventDataDeserializer(EventType.TABLE_MAP) instanceof com.github.shyiko.mysql.binlog.event.deserialization.TableMapEventDataDeserializer);
    }
}
