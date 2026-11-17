package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.node.*;
import java.math.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

/** Native numeric values; temporal/JSON/base64 payloads retain the source's lossless wire representation. */
public final class DorisRows {
    private DorisRows(){}
    public static String q(String name){if(name==null||name.isEmpty()||name.codePointCount(0,name.length())>64||name.chars().anyMatch(c->c<32||c==127))throw new IllegalArgumentException("DORIS_IDENTIFIER_INVALID");return "`"+name.replace("`","``")+"`";}
    public static String type(TableSchema.Column column){
        if(column==null||column.type==null)throw new IllegalArgumentException("DORIS_COLUMN_INVALID");
        switch(column.type){case BOOLEAN:return "BOOLEAN";case INT64:return "BIGINT";case UINT64:return "LARGEINT";case FLOAT32:return "FLOAT";case FLOAT64:return "DOUBLE";case DECIMAL:if(column.precision<1||column.precision>76||column.scale<0||column.scale>column.precision)throw new IllegalArgumentException("DORIS_DECIMAL_RANGE_INVALID");return "DECIMAL("+column.precision+","+column.scale+")";default:return "STRING";}
    }
    public static ObjectNode row(TableSchema schema,List<Cell> cells,String key,BigInteger version,boolean deleted,int maximum)throws Exception{
        if(schema==null||cells==null||schema.columns.size()!=cells.size()||key==null||version==null||version.signum()<0||version.bitLength()>127)throw new IllegalArgumentException("DORIS_ROW_IDENTITY_INVALID");
        ObjectNode row=Codec.JSON.createObjectNode();row.put("_rivomesh_key",Codec.sha256(key));row.put("_rivomesh_version",version);row.put("_rivomesh_deleted",deleted?1:0);
        for(int i=0;i<cells.size();i++){
            TableSchema.Column column=schema.columns.get(i);if(Arrays.asList("_rivomesh_key","_rivomesh_version","_rivomesh_deleted").contains(column.name.toLowerCase(Locale.ROOT))||column.name.toUpperCase(Locale.ROOT).startsWith("__DORIS_"))throw new IllegalArgumentException("DORIS_RESERVED_COLUMN");type(column);Cell cell=cells.get(i);if(cell==null||!cell.present)throw new IllegalArgumentException("DORIS_FULL_IMAGE_REQUIRED");String value=cell.value;
            if(value==null){if(!column.nullable)throw new IllegalArgumentException("DORIS_NULLABILITY_MISMATCH");row.putNull(column.name);continue;}
            if(value.length()>maximum)throw new IllegalArgumentException("DORIS_ROW_LIMIT");
            switch(column.type){
                case BOOLEAN:if(!Arrays.asList("true","false","1","0").contains(value))throw new IllegalArgumentException("DORIS_BOOLEAN_INVALID");row.put(column.name,value.equals("true")||value.equals("1"));break;
                case INT64:row.put(column.name,Long.parseLong(value));break;
                case UINT64:BigInteger integer=new BigInteger(value);if(integer.signum()<0||integer.bitLength()>64)throw new IllegalArgumentException("DORIS_UINT64_INVALID");row.put(column.name,integer);break;
                case DECIMAL:BigDecimal decimal=new BigDecimal(value).setScale(column.scale,RoundingMode.UNNECESSARY);if(decimal.precision()>column.precision)throw new IllegalArgumentException("DORIS_DECIMAL_RANGE_INVALID");row.put(column.name,decimal);break;
                case FLOAT32:float single=Float.parseFloat(value);if(!Float.isFinite(single))throw new IllegalArgumentException("DORIS_FLOAT_INVALID");row.put(column.name,single);break;
                case FLOAT64:double number=Double.parseDouble(value);if(!Double.isFinite(number))throw new IllegalArgumentException("DORIS_FLOAT_INVALID");row.put(column.name,number);break;
                case BYTES:case GEOMETRY:Base64.getDecoder().decode(value);row.put(column.name,value);break;
                default:try{StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));}catch(CharacterCodingException invalid){throw new IllegalArgumentException("DORIS_TEXT_ENCODING_INVALID");}row.put(column.name,value);
            }
        }
        if(Codec.JSON.writeValueAsBytes(row).length>maximum-2)throw new IllegalArgumentException("DORIS_ROW_LIMIT");return row;
    }
}
