package io.github.rivomesh.cdc;

import io.github.rivomesh.MysqlDuration;
import java.util.*;

/** Lossless value mappings, with explicit acknowledgement where SQL type semantics change. */
public final class DmStorage {
    private DmStorage(){}
    public static boolean textDecimal(TableSchema.Column c){return c.type==TableSchema.Type.DECIMAL&&c.precision>38;}
    public static String type(TableSchema.Column c){
        switch(c.type){
            case BOOLEAN:return "BIT";
            case INT64:return "BIGINT";
            case UINT64:return "DECIMAL(20,0)";
            case DECIMAL:if(c.precision<1||c.precision>1000||c.scale<0||c.scale>c.precision)throw new IllegalArgumentException("DECIMAL_RANGE_INVALID");return textDecimal(c)?"VARCHAR("+(c.precision+3)+")":"DECIMAL("+c.precision+","+c.scale+")";
            case FLOAT32:return "REAL";
            case FLOAT64:return "DOUBLE PRECISION";
            case STRING:return c.length>0&&c.length<=4000?"VARCHAR("+c.length+" CHAR)":"CLOB";
            case BYTES:return c.length>0&&c.length<=4000?"VARBINARY("+c.length+")":"BLOB";
            case GEOMETRY:return "BLOB";
            case JSON:return "CLOB";
            case DATE:return "DATE";
            case DATETIME:case TIMESTAMP:return "TIMESTAMP(6)";
            case TIME:return "INTERVAL DAY(2) TO SECOND(6)";
            default:throw new IllegalArgumentException("DM_TYPE_REQUIRES_PLAN");
        }
    }
    public static Map<String,String> plan(TableSchema table,boolean acknowledge){
        Map<String,String> result=new LinkedHashMap<>();Set<String> indexed=new HashSet<>();indexed.addAll(table.primaryKey);indexed.addAll(table.identityKey);for(TableSchema.Index i:table.indexes)indexed.addAll(i.columns);
        for(TableSchema.Column c:table.columns){
            String target=type(c);if(target.equals("CLOB")||target.equals("BLOB"))if(indexed.contains(c.name))throw new IllegalArgumentException("DM_LOB_INDEX_REQUIRES_PLAN");
            if((textDecimal(c)||c.type==TableSchema.Type.JSON)&&!acknowledge)throw new IllegalArgumentException("DM_TEXT_CONVERSION_ACK_REQUIRED");
            result.put(c.name,target+(textDecimal(c)?" [DECIMAL_TEXT_REVIEW_REQUIRED]":c.type==TableSchema.Type.JSON?" [JSON_TEXT_REVIEW_REQUIRED]":""));
        }return result;
    }
    public static String interval(String value){
        String canonical=MysqlDuration.canonical(value);if(canonical==null)return null;boolean negative=canonical.startsWith("-");String unsigned=negative?canonical.substring(1):canonical;int colon=unsigned.indexOf(':');long hours=Long.parseLong(unsigned.substring(0,colon));
        return "INTERVAL '"+(negative?"-":"")+(hours/24)+" "+String.format(Locale.ROOT,"%02d",hours%24)+unsigned.substring(colon)+"' DAY(2) TO SECOND(6)";
    }
}
