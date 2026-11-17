package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class Codec {
    public static final ObjectMapper JSON=new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    private Codec(){}
    public static String encode(Object value){try{return JSON.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("ENCODING_FAILED",e);}}
    public static <T>T decode(String value,Class<T> type){try{return JSON.readValue(value,type);}catch(Exception e){throw new IllegalArgumentException("DECODING_FAILED",e);}}
    public static String sha256(String value){
        try{byte[] bytes=MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));StringBuilder out=new StringBuilder();for(byte b:bytes)out.append(String.format("%02x",b&255));return out.toString();}
        catch(Exception e){throw new IllegalStateException(e);}
    }
}
