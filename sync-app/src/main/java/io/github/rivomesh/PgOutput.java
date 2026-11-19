package io.github.rivomesh;

import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.PipelineFailure;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** One complete transaction at a time, using pgoutput protocol 1 and text tuples. */
final class PgOutput {
    private static final int MAX_FIELD_BYTES=16*1024*1024;
    private static final long MAX_TRANSACTION_BYTES=32L*1024*1024;
    private static final long POSTGRES_EPOCH_SECONDS=946684800L;
    private static final Pattern UUID_TEXT=Pattern.compile("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}");
    private final PostgresSourcePosition positions;
    private final String generation;
    private final int maxRows;
    private final Map<Integer,TableSchema> schemas;
    private final Set<Integer> announced=new HashSet<>();
    private final List<Change> changes=new ArrayList<>();
    private boolean open;
    private long finalLsn,lastCommit,transactionBytes,beginCommitMicros;

    PgOutput(PostgresSourcePosition positions,String generation,int maxRows,
             Map<Integer,TableSchema> schemas,long initialLsn){
        this.positions=positions;
        this.generation=generation;
        this.maxRows=maxRows;
        this.schemas=schemas;
        this.lastCommit=initialLsn;
    }

    SourceTransaction accept(ByteBuffer frame)throws Exception{
        SourceTransaction committed=null;
        int message=Byte.toUnsignedInt(frame.get());
        switch(message){
            case 'B':begin(frame);break;
            case 'C':committed=commit(frame);break;
            case 'R':relation(frame);break;
            case 'I':case 'U':case 'D':row(frame,message);break;
            case 'T':throw schemaFailure("POSTGRESQL_TRUNCATE_REQUIRES_PLAN");
            default:throw invalid("POSTGRESQL_PGOUTPUT_MESSAGE_UNSUPPORTED");
        }
        if(frame.hasRemaining())throw invalid("POSTGRESQL_PGOUTPUT_MESSAGE_TRAILING_DATA");
        return committed;
    }

    private void begin(ByteBuffer frame){
        if(open)throw invalid("POSTGRESQL_TRANSACTION_UNFINISHED");
        finalLsn=frame.getLong();
        beginCommitMicros=frame.getLong();
        frame.getInt(); // xid can wrap and is not the durable transaction identifier
        transactionBytes=0;
        open=true;
    }

    private SourceTransaction commit(ByteBuffer frame){
        if(!open||frame.get()!=0)throw invalid("POSTGRESQL_PGOUTPUT_COMMIT_INVALID");
        long commit=frame.getLong(),end=frame.getLong();
        long commitMicros=frame.getLong();
        if(finalLsn!=commit||Long.compareUnsigned(end,commit)<0||Long.compareUnsigned(end,lastCommit)<=0)
            throw invalid("POSTGRESQL_COMMIT_LSN_INVALID");
        SourceTransaction tx=new SourceTransaction();
        tx.id=PostgresSourcePosition.transaction(end);
        tx.streamGeneration=generation;
        tx.position=positions.at(end);
        if(commitMicros==beginCommitMicros&&commitMicros!=Long.MIN_VALUE&&commitMicros!=Long.MAX_VALUE){
            tx.sourceCommittedAt=Instant.ofEpochSecond(POSTGRES_EPOCH_SECONDS+Math.floorDiv(commitMicros,1000000L),
                    Math.floorMod(commitMicros,1000000L)*1000L);
        }
        for(Change change:changes){
            change.eventId=tx.id+":"+tx.changes.size();
            tx.changes.add(change);
        }
        tx.validate();
        lastCommit=end;
        changes.clear();
        open=false;
        return tx;
    }

    private void relation(ByteBuffer frame)throws Exception{
        int oid=frame.getInt();
        String namespace=cstring(frame),name=cstring(frame);
        int identity=Byte.toUnsignedInt(frame.get()),columns=Short.toUnsignedInt(frame.getShort());
        TableSchema table=schemas.get(oid);
        if(table==null||!table.database.equals(namespace)||!table.name.equals(name)||identity!='f'
                ||columns!=table.columns.size())throw schemaFailure("POSTGRESQL_RELATION_CHANGED_REQUIRES_PLAN");
        for(TableSchema.Column column:table.columns){
            frame.get();
            String columnName=cstring(frame);
            String type=Integer.toUnsignedString(frame.getInt());
            String modifier=Integer.toString(frame.getInt());
            if(!column.name.equals(columnName)||!type.equals(table.properties.get("pg.oid."+column.name))
                    ||!modifier.equals(table.properties.get("pg.modifier."+column.name)))
                throw schemaFailure("POSTGRESQL_RELATION_CHANGED_REQUIRES_PLAN");
        }
        announced.add(oid);
    }

    private void row(ByteBuffer frame,int message)throws Exception{
        if(!open)throw invalid("POSTGRESQL_TRANSACTION_BOUNDARY_REQUIRED");
        int oid=frame.getInt();
        if(!announced.contains(oid))throw invalid("POSTGRESQL_RELATION_REQUIRED");
        if(changes.size()>=maxRows)throw invalid("TRANSACTION_ROW_LIMIT");
        transactionBytes+=frame.remaining();
        if(transactionBytes>MAX_TRANSACTION_BYTES)throw invalid("POSTGRESQL_TRANSACTION_BYTE_LIMIT");
        TableSchema table=schemas.get(oid);
        Change change=new Change();
        change.tableId=table.id;
        change.schemaVersion=table.version;
        if(message=='I'){
            change.operation=Change.Operation.INSERT;
            requireTuple(frame,'N');
            change.after=tuple(frame,table,null);
        }else{
            requireTuple(frame,'O');
            change.before=tuple(frame,table,null);
            change.operation=message=='U'?Change.Operation.UPDATE:Change.Operation.DELETE;
            if(message=='U'){
                requireTuple(frame,'N');
                change.after=tuple(frame,table,change.before);
            }
        }
        changes.add(change);
    }

    private static List<Cell> tuple(ByteBuffer frame,TableSchema table,List<Cell> before)throws Exception{
        if(Short.toUnsignedInt(frame.getShort())!=table.columns.size())throw invalid("POSTGRESQL_TUPLE_WIDTH_CHANGED");
        List<Cell> row=new ArrayList<>(table.columns.size());
        for(int i=0;i<table.columns.size();i++){
            int kind=Byte.toUnsignedInt(frame.get());
            if(kind=='n')row.add(Cell.value(null));
            else if(kind=='u'&&before!=null)row.add(before.get(i));
            else if(kind=='t'){
                int bytes=frame.getInt();
                if(bytes<0||bytes>MAX_FIELD_BYTES||bytes>frame.remaining())throw invalid("POSTGRESQL_TUPLE_FIELD_LIMIT");
                ByteBuffer text=frame.slice();
                text.limit(bytes);
                frame.position(frame.position()+bytes);
                row.add(value(table.columns.get(i),StandardCharsets.UTF_8.newDecoder().decode(text).toString()));
            }else throw invalid("POSTGRESQL_FULL_TEXT_TUPLE_REQUIRED");
        }
        return row;
    }

    static Cell value(TableSchema.Column column,String text){
        if(text==null)return Cell.value(null);
        switch(column.type){
            case BOOLEAN:
                if("t".equals(text)||"true".equals(text))return Cell.value("true");
                if("f".equals(text)||"false".equals(text))return Cell.value("false");
                throw invalid("POSTGRESQL_BOOLEAN_INVALID");
            case INT64:return Cell.value(Long.toString(Long.parseLong(text)));
            case DECIMAL:
                if("NaN".equals(text)||"Infinity".equals(text)||"-Infinity".equals(text))
                    throw invalid("POSTGRESQL_NUMERIC_NONFINITE");
                return Cell.value(new BigDecimal(text).toPlainString());
            case DATE:return Cell.value(PostgresDate.canonicalSource(text));
            case TIME:return Cell.value(PostgresClock.canonical(text));
            case DATETIME:return Cell.value(PostgresTimestamp.canonicalSource(text,false));
            case TIMESTAMP:return Cell.value(PostgresTimestamp.canonicalSource(text,true));
            case BYTES:
                if(!text.startsWith("\\x")||(text.length()&1)!=0)throw invalid("POSTGRESQL_BYTEA_HEX_REQUIRED");
                byte[] bytes=new byte[(text.length()-2)/2];
                for(int i=0;i<bytes.length;i++){
                    int high=Character.digit(text.charAt(i*2+2),16),low=Character.digit(text.charAt(i*2+3),16);
                    if(high<0||low<0)throw invalid("POSTGRESQL_BYTEA_HEX_REQUIRED");
                    bytes[i]=(byte)((high<<4)|low);
                }
                return Cell.value(Base64.getEncoder().encodeToString(bytes));
            case STRING:
                if("2950".equals(column.sourceType)&&!UUID_TEXT.matcher(text).matches())
                    throw invalid("POSTGRESQL_UUID_TEXT_INVALID");
                return Cell.value(text);
            case JSON:return Cell.value(text);
            default:throw invalid("POSTGRESQL_TYPE_UNSUPPORTED");
        }
    }

    private static void requireTuple(ByteBuffer frame,int expected){
        if(Byte.toUnsignedInt(frame.get())!=expected)throw invalid("POSTGRESQL_FULL_REPLICA_IDENTITY_REQUIRED");
    }

    private static String cstring(ByteBuffer frame)throws Exception{
        ByteArrayOutputStream text=new ByteArrayOutputStream();
        for(int i=0;i<256;i++){
            int value=Byte.toUnsignedInt(frame.get());
            if(value==0)return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(text.toByteArray())).toString();
            text.write(value);
        }
        throw invalid("POSTGRESQL_PGOUTPUT_IDENTIFIER_LIMIT");
    }

    private static PipelineFailure schemaFailure(String code){
        return new PipelineFailure(PipelineFailure.Phase.DDL,invalid(code));
    }

    private static IllegalStateException invalid(String code){return new IllegalStateException(code);}
}
