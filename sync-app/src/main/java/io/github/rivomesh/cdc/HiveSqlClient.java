package io.github.rivomesh.cdc;

import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.transport.hive.HiveTransport;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

/** Bounded binary HiveServer2 protocol; JDBC transaction control is deliberately not used. */
public final class HiveSqlClient implements AutoCloseable {
    private final String url,username,password;
    private final int timeout,maximumSqlBytes,maximumResponseBytes;
    private HiveTransport client;
    public HiveSqlClient(String url,String username,String password,int timeout,int maximumSqlBytes,int maximumResponseBytes){
        if(username==null||username.isEmpty()||timeout<1||timeout>1800||maximumSqlBytes<1024||maximumSqlBytes>32*1024*1024||maximumResponseBytes<1024||maximumResponseBytes>64*1024*1024)throw new IllegalArgumentException("HIVE_CONFIG_INVALID");
        this.url=boundedUrl(url,timeout);this.username=username;this.password=password;this.timeout=timeout;this.maximumSqlBytes=maximumSqlBytes;this.maximumResponseBytes=maximumResponseBytes;
    }
    static String boundedUrl(String input,int timeout){
        try{if(input==null||!input.startsWith("jdbc:hive2://")||input.length()>4096)throw new IllegalArgumentException();String[] fields=input.substring(5).split(";",-1);URI uri=URI.create(fields[0]);if(uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||!uri.getPath().matches("/[A-Za-z][A-Za-z0-9_]{0,127}"))throw new IllegalArgumentException();
            Map<String,String> options=new TreeMap<>();for(int i=1;i<fields.length;i++){String[] pair=fields[i].split("=",-1);if(pair.length!=2||options.put(pair[0],pair[1])!=null)throw new IllegalArgumentException();}
            for(Map.Entry<String,String> option:options.entrySet()){String key=option.getKey(),value=option.getValue();if(key.equals("socketTimeout")||key.equals("retries"))continue;if(!(key.equals("auth")&&Arrays.asList("noSasl","none").contains(value)||key.equals("ssl")&&Arrays.asList("true","false").contains(value)))throw new IllegalArgumentException();}
            options.put("socketTimeout",Integer.toString(Math.max(5000,Math.min(120000,timeout*1000))));options.put("retries","0");StringBuilder result=new StringBuilder("jdbc:").append(fields[0]);for(Map.Entry<String,String> option:options.entrySet())result.append(';').append(option.getKey()).append('=').append(option.getValue());return result.toString();
        }catch(Exception invalid){throw new IllegalArgumentException("HIVE_DIRECT_BINARY_URL_REQUIRED");}
    }
    private HiveTransport client()throws Exception{if(client==null)client=new HiveTransport(url,username,password,timeout,maximumResponseBytes);return client;}
    private void limit(String sql){if(sql==null||sql.getBytes(StandardCharsets.UTF_8).length>maximumSqlBytes)throw new IllegalArgumentException("HIVE_SQL_LIMIT");}
    private Exception classified(Exception error){if(error instanceof java.io.IOException&&error.getMessage()!=null&&error.getMessage().matches("HIVE_SDK_[A-Z_]+"))return new SyncFailure(error.getMessage(),"HIVE_SDK",false,"Hive SDK could not be verified",error,null,0,null);if(error instanceof SQLException){String state=((SQLException)error).getSQLState();boolean retry=state!=null&&(state.trim().startsWith("08")||state.startsWith("40"));return new SyncFailure(retry?"HIVE_CONNECTION_RETRY":"HIVE_SQL_REJECTED","HIVE_SQL",retry,"HIVE_SQL",error,null,0,null);}return error;}
    public List<Map<String,String>> query(String statement)throws Exception{limit(statement);try{return client().query(statement);}catch(Exception failure){close();throw classified(failure);}}
    public void execute(String statement)throws Exception{limit(statement);try{client().execute(statement);}catch(Exception failure){close();throw classified(failure);}}
    @Override public void close(){if(client!=null)try{client.close();}catch(Exception ignored){}finally{client=null;}}
}
