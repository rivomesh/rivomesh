package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.PipelineFailure;
import io.github.rivomesh.cdc.RowValues;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PgOutputTest {
    private final PostgresSourcePosition positions=new PostgresSourcePosition("cluster","729911","1","rm_slot");

    @Test void lsnUsesUnsignedOrderingAndRequiresScope(){
        long earlier=PostgresSourcePosition.parse("7fffffff/ffffffff");
        long later=PostgresSourcePosition.parse("80000000/0");
        SourcePosition checkpoint=positions.at(later);
        assertTrue(positions.covers(checkpoint,positions.at(earlier)));
        assertTrue(positions.includes(checkpoint,PostgresSourcePosition.transaction(earlier)));
        PostgresSourcePosition otherTimeline=new PostgresSourcePosition("cluster","729911","2","rm_slot");
        assertThrows(IllegalArgumentException.class,()->positions.covers(checkpoint,otherTimeline.at(earlier)));
        assertThrows(IllegalArgumentException.class,()->PostgresSourcePosition.parse("0/100000000"));
    }

    @Test void promotedTimelineAdmitsOnlyTheSharedPrefix(){
        PostgresSourcePosition current=new PostgresSourcePosition("cluster","729911","4","rm_slot",
                PostgresSourcePosition.history("1\t0/1000\tfirst promotion\n3\t0/3000\tsecond promotion\n","4"));
        PostgresSourcePosition original=new PostgresSourcePosition("cluster","729911","1","rm_slot");
        PostgresSourcePosition sibling=new PostgresSourcePosition("cluster","729911","2","rm_slot");
        assertTrue(current.covers(current.at(0x4000),original.at(0x1000)));
        assertThrows(IllegalArgumentException.class,()->current.lsn(original.at(0x1001)));
        assertThrows(IllegalArgumentException.class,()->current.lsn(sibling.at(0x500)));
        assertThrows(IllegalArgumentException.class,()->PostgresSourcePosition.history("1 0/3000\n3 0/1000","4"));
    }

    @Test void uuidUsesTheCanonicalServerRepresentation(){
        TableSchema.Column uuid=new TableSchema.Column();
        uuid.type=TableSchema.Type.STRING;
        uuid.sourceType="2950";
        String value="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        assertEquals(value,PgOutput.value(uuid,value).value);
        assertNull(PgOutput.value(uuid,null).value);
        assertThrows(IllegalStateException.class,()->PgOutput.value(uuid,"a0eebc999c0b4ef8bb6d6bb9bd380a11"));
    }

    @Test void civilDatesShareOneBoundedCanonicalRepresentation(){
        TableSchema.Column day=new TableSchema.Column();
        day.type=TableSchema.Type.DATE;
        day.sourceType=Integer.toString(PostgresDate.OID);
        TableSchema table=new TableSchema();
        table.columns.add(day);
        for(String valid:new String[]{"1000-01-01","1582-10-10","2011-12-30","2024-02-29","9999-12-31"}){
            var value=PgOutput.value(day,valid);
            assertEquals(valid,value.value);
            assertEquals(valid,RowValues.decode(RowValues.canonical(table,Collections.singletonList(value))).get(0).value);
        }
        assertNull(PgOutput.value(day,null).value);
        for(String invalid:new String[]{"infinity","-infinity","0001-12-31 BC","0999-12-31",
                "10000-01-01","2023-02-29","29/02/2024"}){
            assertEquals("POSTGRESQL_DATE_OUT_OF_RANGE",
                    assertThrows(IllegalStateException.class,()->PgOutput.value(day,invalid)).getMessage());
        }
    }

    @Test void civilAndInstantTimestampsPreserveMicrosecondsAndUseUtcForInstants() {
        TableSchema.Column civil = new TableSchema.Column();
        civil.type = TableSchema.Type.DATETIME;
        civil.sourceType = "1114";
        TableSchema.Column instant = new TableSchema.Column();
        instant.type = TableSchema.Type.TIMESTAMP;
        instant.sourceType = "1184";

        assertEquals("2024-03-10 02:30:00.123456", PgOutput.value(civil, "2024-03-10 02:30:00.123456").value);
        assertEquals("9999-12-31 23:59:59.499999", PgOutput.value(civil, "9999-12-31 23:59:59.499999").value);
        assertEquals("2024-11-03 05:30:00.123456", PgOutput.value(instant, "2024-11-03 05:30:00.123456+00").value);
        assertEquals("2024-11-03 06:30:00", PgOutput.value(instant, "2024-11-03 06:30:00.000000+00:00").value);
        assertNull(PgOutput.value(instant, null).value);
        for (String invalid : new String[]{"infinity", "0999-12-31 23:59:59", "10000-01-01 00:00:00",
                "0001-01-01 00:00:00 BC", "2023-02-29 00:00:00", "2024-01-01 24:00:00",
                "2024-01-01 00:00:00.1234567", "9999-12-31 23:59:59.5"}) {
            assertEquals("POSTGRESQL_TIMESTAMP_OUT_OF_RANGE",
                    assertThrows(IllegalStateException.class, () -> PgOutput.value(civil, invalid)).getMessage());
        }
        assertThrows(IllegalStateException.class, () -> PgOutput.value(instant, "2024-11-03 01:30:00-04"));
        assertThrows(IllegalStateException.class, () -> PgOutput.value(instant, "2024-11-03 05:30:00"));
    }

    @Test void civilClockKeepsDistinctMidnightEndpointsAndExactMicroseconds() {
        TableSchema.Column clock = new TableSchema.Column();
        clock.type = TableSchema.Type.TIME;
        clock.sourceType = "1083";
        TableSchema table = new TableSchema();
        table.columns.add(clock);
        assertEquals("00:00:00.000000", PgOutput.value(clock, "00:00:00").value);
        assertEquals("12:34:56.123456", PgOutput.value(clock, "12:34:56.123456").value);
        assertEquals("23:59:59.999999", PgOutput.value(clock, "23:59:59.999999").value);
        assertEquals("24:00:00.000000", PgOutput.value(clock, "24:00:00").value);
        assertNull(PgOutput.value(clock, null).value);
        assertNotEquals(RowValues.canonical(table, java.util.List.of(PgOutput.value(clock, "00:00:00"))),
                RowValues.canonical(table, java.util.List.of(PgOutput.value(clock, "24:00:00"))));
        for(String invalid : new String[]{"-00:01:00", "24:00:00.000001", "12:00:00.1234567", "13:00:00-04"})
            assertEquals("POSTGRESQL_TIME_OUT_OF_RANGE",
                    assertThrows(IllegalArgumentException.class, () -> PgOutput.value(clock, invalid)).getMessage());
    }

    @Test void pgoutputPublishesOnlyCompleteTransactionsAndChecksRelationMetadata()throws Exception{
        TableSchema table=new TableSchema();
        table.id=TableSchema.identity("database","sales","items");
        table.database="sales";
        table.name="items";
        TableSchema.Column column=new TableSchema.Column();
        column.name="id";
        column.type=TableSchema.Type.INT64;
        table.columns.add(column);
        table.properties.put("pg.oid.id","20");
        table.properties.put("pg.modifier.id","-1");
        PgOutput decoder=new PgOutput(positions,"generation",10,Collections.singletonMap(123,table),8);
        assertNull(decoder.accept(frame('B',out->{out.writeLong(16);out.writeLong(0);out.writeInt(7);})));
        assertNull(decoder.accept(relation("items")));
        assertNull(decoder.accept(frame('I',out->{
            out.writeInt(123);out.writeByte('N');out.writeShort(1);out.writeByte('t');out.writeInt(1);out.writeByte('3');
        })));
        SourceTransaction commit=decoder.accept(frame('C',out->{out.writeByte(0);out.writeLong(16);out.writeLong(17);out.writeLong(0);}));
        assertEquals(17,positions.lsn(commit.position));
        assertEquals(Instant.parse("2000-01-01T00:00:00Z"),commit.sourceCommittedAt);
        assertEquals(Change.Operation.INSERT,commit.changes.get(0).operation);
        assertEquals("3",commit.changes.get(0).after.get(0).value);
        PipelineFailure ddl=assertThrows(PipelineFailure.class,()->decoder.accept(relation("renamed")));
        assertEquals(PipelineFailure.Phase.DDL,ddl.phase);
    }

    @Test void commitTimingIsExactAdvisoryMetadataEvenBeforeThePostgresEpoch()throws Exception{
        SourceTransaction beforeEpoch=timedCommit(-1,-1);
        assertEquals(Instant.parse("1999-12-31T23:59:59.999999Z"),beforeEpoch.sourceCommittedAt);
        SourceTransaction offset=timedCommit(1234567L,1234567L);
        assertEquals(Instant.parse("2000-01-01T00:00:01.234567Z"),offset.sourceCommittedAt);
        SourceTransaction inconsistent=timedCommit(4,5);
        assertNull(inconsistent.sourceCommittedAt);
        assertNull(timedCommit(Long.MAX_VALUE,Long.MAX_VALUE).sourceCommittedAt);
        assertNull(timedCommit(Long.MIN_VALUE,Long.MIN_VALUE).sourceCommittedAt);
        assertEquals(Codec.encode(inconsistent),Codec.encode(offset));
    }

    private SourceTransaction timedCommit(long beginMicros,long commitMicros)throws Exception{
        PgOutput decoder=new PgOutput(positions,"generation",10,Collections.emptyMap(),8);
        decoder.accept(frame('B',out->{out.writeLong(16);out.writeLong(beginMicros);out.writeInt(7);}));
        return decoder.accept(frame('C',out->{out.writeByte(0);out.writeLong(16);out.writeLong(17);out.writeLong(commitMicros);}));
    }

    private static ByteBuffer relation(String table)throws Exception{
        return frame('R',out->{
            out.writeInt(123);out.write("sales".getBytes(StandardCharsets.UTF_8));out.writeByte(0);
            out.write(table.getBytes(StandardCharsets.UTF_8));out.writeByte(0);
            out.writeByte('f');out.writeShort(1);out.writeByte(1);
            out.write("id".getBytes(StandardCharsets.US_ASCII));out.writeByte(0);out.writeInt(20);out.writeInt(-1);
        });
    }

    private interface Message {void write(DataOutputStream out)throws Exception;}

    private static ByteBuffer frame(char type,Message message)throws Exception{
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(bytes);
        out.writeByte(type);
        message.write(out);
        return ByteBuffer.wrap(bytes.toByteArray());
    }
}
