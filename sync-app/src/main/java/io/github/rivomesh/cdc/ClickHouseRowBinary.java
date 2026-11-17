package io.github.rivomesh.cdc;

import java.io.*;
import java.math.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

/** Lossless RowBinary values. Temporal/JSON text stays text; binary stays raw bytes. */
public final class ClickHouseRowBinary {
    private ClickHouseRowBinary(){}
    public static String type(TableSchema.Column column){
        if(column==null||column.type==null)throw new IllegalArgumentException("CLICKHOUSE_COLUMN_INVALID");
        String type;
        switch(column.type){
            case BOOLEAN:type="UInt8";break;
            case INT64:type="Int64";break;
            case UINT64:type="UInt64";break;
            case FLOAT32:type="Float32";break;
            case FLOAT64:type="Float64";break;
            case DECIMAL:
                if(column.precision<1||column.precision>76||column.scale<0||column.scale>column.precision)throw new IllegalArgumentException("CLICKHOUSE_DECIMAL_RANGE_INVALID");
                type="Decimal("+column.precision+","+column.scale+")";break;
            case STRING:case JSON:case BYTES:case GEOMETRY:case DATE:case TIME:case DATETIME:case TIMESTAMP:type="String";break;
            default:throw new IllegalArgumentException("CLICKHOUSE_TYPE_REQUIRES_PLAN");
        }
        return column.nullable?"Nullable("+type+")":type;
    }
    public static final class Buffer extends ByteArrayOutputStream {
        private final int maximum;
        public Buffer(int maximum){super(Math.min(1024,maximum));if(maximum<1)throw new IllegalArgumentException("CLICKHOUSE_ROW_LIMIT");this.maximum=maximum;}
        private void space(int size){if(size<0||size>maximum-count)throw new IllegalArgumentException("CLICKHOUSE_ROW_LIMIT");}
        @Override public synchronized void write(int value){space(1);super.write(value);}
        @Override public synchronized void write(byte[] bytes,int offset,int length){space(length);super.write(bytes,offset,length);}
        public int remaining(){return maximum-count;}
    }
    public static byte[] row(List<TableSchema.Column> columns,List<Cell> values,int maximum)throws Exception{
        Buffer buffer=new Buffer(maximum);writeRow(buffer,columns,values);return buffer.toByteArray();
    }
    public static void writeRow(Buffer output,List<TableSchema.Column> columns,List<Cell> values)throws Exception{
        if(columns==null||values==null||columns.size()!=values.size())throw new IllegalArgumentException("CLICKHOUSE_ROW_WIDTH_INVALID");
        for(int i=0;i<columns.size();i++)write(output,columns.get(i),values.get(i));
    }
    public static void unsigned(Buffer out,BigInteger value,int bytes){
        if(value==null||value.signum()<0||value.bitLength()>bytes*8)throw new IllegalArgumentException("CLICKHOUSE_UNSIGNED_RANGE_INVALID");integer(out,value,bytes);
    }
    private static void integer(Buffer out,BigInteger value,int bytes){for(int n=0;n<bytes;n++){out.write(value.byteValue()&255);value=value.shiftRight(8);}}
    public static void string(Buffer out,String value)throws Exception{
        if(value==null||value.length()>out.remaining())throw new IllegalArgumentException("CLICKHOUSE_ROW_LIMIT");
        ByteBuffer bytes=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
        byte[] data=new byte[bytes.remaining()];bytes.get(data);binary(out,data);
    }
    public static void binary(Buffer out,byte[] value){
        int length=value.length;do{int part=length&127;length>>>=7;out.write(part|(length==0?0:128));}while(length>0);out.write(value,0,value.length);
    }
    private static void write(Buffer out,TableSchema.Column column,Cell cell)throws Exception{
        type(column);if(cell==null||!cell.present)throw new IllegalArgumentException("CLICKHOUSE_FULL_IMAGE_REQUIRED");
        if(column.nullable)out.write(cell.value==null?1:0);
        if(cell.value==null){if(!column.nullable)throw new IllegalArgumentException("CLICKHOUSE_NULLABILITY_MISMATCH");return;}
        String value=cell.value;
        switch(column.type){
            case BOOLEAN:
                if(value.equals("true")||value.equals("1"))out.write(1);else if(value.equals("false")||value.equals("0"))out.write(0);else throw new IllegalArgumentException("CLICKHOUSE_BOOLEAN_INVALID");break;
            case INT64:integer(out,BigInteger.valueOf(Long.parseLong(value)),8);break;
            case UINT64:unsigned(out,new BigInteger(value),8);break;
            case DECIMAL:
                BigDecimal decimal=new BigDecimal(value).setScale(column.scale,RoundingMode.UNNECESSARY);
                if(decimal.precision()>column.precision)throw new IllegalArgumentException("CLICKHOUSE_DECIMAL_RANGE_INVALID");
                integer(out,decimal.unscaledValue(),column.precision<=9?4:column.precision<=18?8:column.precision<=38?16:32);break;
            case FLOAT32:
                float single=Float.parseFloat(value);if(!Float.isFinite(single))throw new IllegalArgumentException("CLICKHOUSE_NONFINITE_VALUE");integer(out,BigInteger.valueOf(Float.floatToRawIntBits(single)),4);break;
            case FLOAT64:
                double number=Double.parseDouble(value);if(!Double.isFinite(number))throw new IllegalArgumentException("CLICKHOUSE_NONFINITE_VALUE");integer(out,BigInteger.valueOf(Double.doubleToRawLongBits(number)),8);break;
            case BYTES:case GEOMETRY:
                if(value.length()>(out.remaining()+2L)/3*4+4)throw new IllegalArgumentException("CLICKHOUSE_ROW_LIMIT");binary(out,Base64.getDecoder().decode(value));break;
            case STRING:case JSON:case DATE:case TIME:case DATETIME:case TIMESTAMP:string(out,value);break;
            default:throw new IllegalArgumentException("CLICKHOUSE_TYPE_REQUIRES_PLAN");
        }
    }
}
