package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.PipelineFailure;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        assertEquals(Change.Operation.INSERT,commit.changes.get(0).operation);
        assertEquals("3",commit.changes.get(0).after.get(0).value);
        PipelineFailure ddl=assertThrows(PipelineFailure.class,()->decoder.accept(relation("renamed")));
        assertEquals(PipelineFailure.Phase.DDL,ddl.phase);
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
