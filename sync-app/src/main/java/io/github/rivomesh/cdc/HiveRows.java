package io.github.rivomesh.cdc;

import java.math.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

/** Hive's decimal limit is explicit; wider decimals and temporal/JSON/binary values retain text. */
public final class HiveRows {
    private HiveRows(){}
    public static void name(String name){if(name==null||!name.matches("[A-Za-z_][A-Za-z0-9_]{0,127}"))throw new IllegalArgumentException("HIVE_IDENTIFIER_PLAN_REQUIRED");}
    public static String q(String name){name(name);return "`"+name+"`";}
    public static Map<String,String> storage(TableSchema table){
        table.validate();name(table.name);Set<String> folded=new HashSet<>();Map<String,String> result=new LinkedHashMap<>();
        for(TableSchema.Column column:table.columns){name(column.name);String normalized=column.name.toLowerCase(Locale.ROOT);if(normalized.startsWith("_rivomesh_"))throw new IllegalArgumentException("HIVE_RESERVED_COLUMN");if(!folded.add(normalized))throw new IllegalArgumentException("HIVE_IDENTIFIER_COLLISION");result.put(column.name,type(column));}return result;
    }
    public static String type(TableSchema.Column column){switch(column.type){case BOOLEAN:return "BOOLEAN";case INT64:return "BIGINT";case UINT64:return "DECIMAL(20,0)";case FLOAT32:return "FLOAT";case FLOAT64:return "DOUBLE";case DECIMAL:if(column.precision<1||column.precision>1000||column.scale<0||column.scale>column.precision)throw new IllegalArgumentException("HIVE_DECIMAL_RANGE_INVALID");return column.precision<=38?"DECIMAL("+column.precision+","+column.scale+")":"STRING";default:return "STRING";}}
    public static String text(String value){if(value==null)return "NULL";try{ByteBuffer bytes=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));byte[] data=new byte[bytes.remaining()];bytes.get(data);return "decode(unbase64('"+Base64.getEncoder().encodeToString(data)+"'),'UTF-8')";}catch(CharacterCodingException invalid){throw new IllegalArgumentException("HIVE_TEXT_ENCODING_INVALID");}}
    public static String value(TableSchema.Column column,Cell cell){
        if(column==null||column.type==null||cell==null||!cell.present)throw new IllegalArgumentException("HIVE_FULL_IMAGE_REQUIRED");type(column);String value=cell.value;if(value==null){if(!column.nullable)throw new IllegalArgumentException("HIVE_NULLABILITY_MISMATCH");return "CAST(NULL AS "+type(column)+")";}
        switch(column.type){case BOOLEAN:if(!Arrays.asList("true","false","1","0").contains(value))throw new IllegalArgumentException("HIVE_BOOLEAN_INVALID");return value.equals("true")||value.equals("1")?"TRUE":"FALSE";
            case INT64:return "CAST("+text(Long.toString(Long.parseLong(value)))+" AS BIGINT)";
            case UINT64:BigInteger n=new BigInteger(value);if(n.signum()<0||n.bitLength()>64)throw new IllegalArgumentException("HIVE_UINT64_INVALID");return "CAST("+text(n.toString())+" AS DECIMAL(20,0))";
            case DECIMAL:BigDecimal d;try{d=new BigDecimal(value).setScale(column.scale,RoundingMode.UNNECESSARY);}catch(ArithmeticException invalid){throw new IllegalArgumentException("HIVE_DECIMAL_RANGE_INVALID");}if(d.precision()>column.precision)throw new IllegalArgumentException("HIVE_DECIMAL_RANGE_INVALID");String encoded=text(d.toPlainString());return column.precision<=38?"CAST("+encoded+" AS "+type(column)+")":encoded;
            case FLOAT32:float f=Float.parseFloat(value);if(!Float.isFinite(f))throw new IllegalArgumentException("HIVE_FLOAT_INVALID");return "CAST("+text(Float.toString(f))+" AS FLOAT)";
            case FLOAT64:double g=Double.parseDouble(value);if(!Double.isFinite(g))throw new IllegalArgumentException("HIVE_FLOAT_INVALID");return "CAST("+text(Double.toString(g))+" AS DOUBLE)";
            case BYTES:case GEOMETRY:Base64.getDecoder().decode(value);break;
            case JSON:JsonValues.text(value);break;
            default:break;
        }return text(value);
    }
    public static List<String> row(TableSchema table,List<Cell> cells,String key,BigInteger version,boolean deleted){
        if(cells==null||cells.size()!=table.columns.size()||key==null||version==null||version.signum()<0||version.toString().length()>38)throw new IllegalArgumentException("HIVE_ROW_INVALID");
        List<String> result=new ArrayList<>(Arrays.asList("'"+Codec.sha256(key)+"'","CAST('"+version+"' AS DECIMAL(38,0))","CAST("+(deleted?1:0)+" AS TINYINT)"));
        for(int i=0;i<cells.size();i++){TableSchema.Column c=table.columns.get(i);q(c.name);if(c.name.toLowerCase(Locale.ROOT).startsWith("_rivomesh_"))throw new IllegalArgumentException("HIVE_RESERVED_COLUMN");result.add(value(c,cells.get(i)));}return result;
    }
}
