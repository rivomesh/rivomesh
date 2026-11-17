package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;

/** Never decode MySQL binary JSON as UTF-8 or route JSON numbers through double. */
public final class JsonValues {
    private static final ObjectMapper JSON=new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private JsonValues(){}
    public static String text(String value){
        if(value==null)return null;
        try{JsonNode node=JSON.readTree(value);if(node==null)throw new IllegalArgumentException("JSON_VALUE_EMPTY");return JSON.writeValueAsString(node);}
        catch(Exception e){throw new IllegalArgumentException("JSON_VALUE_INVALID",e);}
    }
    public static String mysql(Object value){
        if(value==null)return null;
        try{return text(value instanceof byte[]?JsonBinary.parseAsString((byte[])value):value.toString());}
        catch(Exception e){throw new IllegalArgumentException("MYSQL_JSON_INVALID",e);}
    }
    /** Canonical form for durable row matching, independent of object key order and numeric scale. */
    public static String canonical(String value){
        if(value==null)return null;
        try{return JSON.writeValueAsString(canonical(JSON.readTree(value)));}
        catch(Exception e){throw new IllegalArgumentException("JSON_VALUE_INVALID",e);}
    }
    private static JsonNode canonical(JsonNode node){
        if(node==null)throw new IllegalArgumentException("JSON_VALUE_EMPTY");
        if(node.isNumber())return JsonNodeFactory.instance.numberNode(node.decimalValue().stripTrailingZeros());
        if(node.isObject()){
            com.fasterxml.jackson.databind.node.ObjectNode result=JSON.createObjectNode();java.util.SortedSet<String> keys=new java.util.TreeSet<>();node.fieldNames().forEachRemaining(keys::add);for(String key:keys)result.set(key,canonical(node.get(key)));return result;
        }
        if(node.isArray()){com.fasterxml.jackson.databind.node.ArrayNode result=JSON.createArrayNode();for(JsonNode child:node)result.add(canonical(child));return result;}
        return node;
    }
    public static boolean equivalent(String left,String right){
        if(left==null||right==null)return left==right;
        try{return equal(JSON.readTree(left),JSON.readTree(right));}catch(Exception e){return false;}
    }
    private static boolean equal(JsonNode a,JsonNode b){
        if(a.isNumber()&&b.isNumber())return a.decimalValue().compareTo(b.decimalValue())==0;
        if(a.isObject()&&b.isObject()){
            if(a.size()!=b.size())return false;java.util.Iterator<String> keys=a.fieldNames();while(keys.hasNext()){String key=keys.next();if(!b.has(key)||!equal(a.get(key),b.get(key)))return false;}return true;
        }
        if(a.isArray()&&b.isArray()){if(a.size()!=b.size())return false;for(int i=0;i<a.size();i++)if(!equal(a.get(i),b.get(i)))return false;return true;}
        return a.equals(b);
    }
}
