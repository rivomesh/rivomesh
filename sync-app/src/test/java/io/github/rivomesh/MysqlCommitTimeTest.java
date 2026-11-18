package io.github.rivomesh;

import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MysqlCommitTimeTest {
    private static final UUID WRITER=UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");
    private static final long MICROS=1794993905052057L;
    private static final long OPTIONAL_ORIGINAL=1L<<55;

    @Test void pinnedDecoderPreservesDirectAndRelayedWireTimestamps()throws Exception{
        GtidEventData direct=decode(MICROS,null,80413,null);
        assertEquals(MICROS,direct.getOriginalCommitTimestamp());
        assertEquals(MICROS,direct.getImmediateCommitTimestamp());
        assertEquals(Instant.parse("2026-11-18T09:25:05.052057Z"),
                MysqlCdcSource.directCommitTime(direct,WRITER.toString()));

        GtidEventData relayed=decode(MICROS+1000,MICROS,80413,null);
        assertEquals(MICROS,relayed.getOriginalCommitTimestamp());
        assertEquals(MICROS+1000,relayed.getImmediateCommitTimestamp());
        assertNull(MysqlCdcSource.directCommitTime(relayed,WRITER.toString()));
        assertNull(MysqlCdcSource.directCommitTime(decode(MICROS,null,80413,80412),WRITER.toString()));
        assertNull(MysqlCdcSource.directCommitTime(direct,"fedcba98-7654-3210-fedc-ba9876543210"));
    }

    @Test void omittedOrUndefinedTimestampsAndUnverifiedVersionsRemainUnknown()throws Exception{
        GtidEventData legacy=decode(null,null,null,null);
        assertEquals(0,legacy.getOriginalCommitTimestamp());
        assertNull(MysqlCdcSource.directCommitTime(legacy,WRITER.toString()));
        assertNull(MysqlCdcSource.directCommitTime(decode(MICROS,null,null,null),WRITER.toString()));
        assertNull(MysqlCdcSource.directCommitTime(decode(0L,null,80413,null),WRITER.toString()));
        assertNull(MysqlCdcSource.directCommitTime(decode(MICROS,OPTIONAL_ORIGINAL,80413,null),WRITER.toString()));
        assertNull(MysqlCdcSource.directCommitTime(decode(MICROS,null,80046,null),WRITER.toString()));

        Config.Source source=new Config.Source();
        assertFalse(MysqlCdcSource.originalTimingProfile(source));
        source.expectedVersion="8.4.13";
        assertTrue(MysqlCdcSource.originalTimingProfile(source));
        source.topology="GROUP_REPLICATION";
        assertFalse(MysqlCdcSource.originalTimingProfile(source));
    }

    private static GtidEventData decode(Long immediate,Long original,Integer version,Integer originalVersion)throws Exception{
        ByteArrayOutputStream wire=new ByteArrayOutputStream();
        wire.write(0);
        wire.writeBytes(ByteBuffer.allocate(16).putLong(WRITER.getMostSignificantBits())
                .putLong(WRITER.getLeastSignificantBits()).array());
        writeLittleEndian(wire,1,8);
        wire.write(2);
        writeLittleEndian(wire,0,8);
        writeLittleEndian(wire,1,8);
        if(immediate!=null){
            writeLittleEndian(wire,immediate|(original==null?0:OPTIONAL_ORIGINAL),7);
            if(original!=null)writeLittleEndian(wire,original,7);
            wire.write(128);
            if(version!=null){
                writeLittleEndian(wire,version|(originalVersion==null?0:1L<<31),4);
                if(originalVersion!=null)writeLittleEndian(wire,originalVersion,4);
            }
        }
        return new GtidEventDataDeserializer().deserialize(new ByteArrayInputStream(wire.toByteArray()));
    }

    private static void writeLittleEndian(ByteArrayOutputStream wire,long value,int bytes){
        for(int index=0;index<bytes;index++)wire.write((int)(value>>>(index*8))&255);
    }
}
