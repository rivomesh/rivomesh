package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.*;
import io.github.rivomesh.SyncFailure;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.net.ssl.SSLException;

/** Bounded HTTP boundary. Callers own idempotency; ambiguous writes are never retried here. */
public final class ClickHouseHttpClient implements AutoCloseable {
    public static final class Settings {
        public String url,username,password;
        public int connectTimeoutMillis=5000,requestTimeoutMillis=30000;
        public int maximumRequestBytes=16*1024*1024,maximumResponseBytes=1024*1024;
    }
    private final URL endpoint;
    private final String authorization;
    private final int connectTimeout,requestTimeout,requestLimit,responseLimit;
    private final ObjectMapper json=new ObjectMapper().enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    public ClickHouseHttpClient(Settings settings)throws Exception{
        if(settings==null||settings.url==null)throw new IllegalArgumentException("CLICKHOUSE_CONFIG_INVALID");
        URI uri=URI.create(settings.url);
        if(!Arrays.asList("http","https").contains(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null
                ||uri.getRawPath()!=null&&!uri.getRawPath().isEmpty()&&!uri.getRawPath().equals("/")
                ||settings.username==null||settings.username.isEmpty()||settings.username.contains(":")||settings.password==null
                ||settings.username.chars().anyMatch(c->c<32||c==127)||settings.password.chars().anyMatch(c->c<32||c==127)
                ||settings.connectTimeoutMillis<100||settings.connectTimeoutMillis>120000||settings.requestTimeoutMillis<100||settings.requestTimeoutMillis>120000
                ||settings.maximumRequestBytes<1024||settings.maximumRequestBytes>64*1024*1024||settings.maximumResponseBytes<1024||settings.maximumResponseBytes>16*1024*1024)
            throw new IllegalArgumentException("CLICKHOUSE_CONFIG_INVALID");
        connectTimeout=settings.connectTimeoutMillis;requestTimeout=settings.requestTimeoutMillis;requestLimit=settings.maximumRequestBytes;responseLimit=settings.maximumResponseBytes;
        String base=uri.toASCIIString();if(!base.endsWith("/"))base+="/";
        endpoint=new URL(base+"?wait_end_of_query=1&send_progress_in_http_headers=0&async_insert=0&wait_for_async_insert=1&buffer_size="+responseLimit);
        authorization="Basic "+Base64.getEncoder().encodeToString((settings.username+":"+settings.password).getBytes(StandardCharsets.UTF_8));
    }
    private static SyncFailure failure(String code,boolean retry,Throwable cause){return new SyncFailure(code,"TARGET_HTTP",retry,code,cause,null,0,null);}
    private byte[] response(InputStream input)throws IOException,SyncFailure{
        if(input==null)return new byte[0];
        try(InputStream in=input;ByteArrayOutputStream output=new ByteArrayOutputStream()){
            byte[] buffer=new byte[8192];int size;while((size=in.read(buffer))!=-1){if(output.size()+size>responseLimit)throw failure("CLICKHOUSE_RESPONSE_LIMIT",false,null);output.write(buffer,0,size);}return output.toByteArray();
        }
    }
    private byte[] request(byte[] body)throws Exception{
        if(body==null||body.length==0||body.length>requestLimit)throw failure("CLICKHOUSE_REQUEST_LIMIT",false,null);
        HttpURLConnection connection=(HttpURLConnection)endpoint.openConnection();
        try{
            connection.setConnectTimeout(connectTimeout);connection.setReadTimeout(requestTimeout);connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("POST");connection.setDoOutput(true);connection.setFixedLengthStreamingMode(body.length);
            connection.setRequestProperty("Authorization",authorization);connection.setRequestProperty("Content-Type","application/octet-stream");connection.setRequestProperty("Accept-Encoding","identity");
            try(OutputStream output=connection.getOutputStream()){output.write(body);}
            int status=connection.getResponseCode();
            if(status>=300&&status<400)throw failure("CLICKHOUSE_REDIRECT_REQUIRES_PLAN",false,null);
            byte[] result=response(status>=400?connection.getErrorStream():connection.getInputStream());
            String code=connection.getHeaderField("X-ClickHouse-Exception-Code");
            if(code!=null&&!code.equals("0"))throw failure(code.matches("[0-9]{1,6}")?"CLICKHOUSE_SERVER_"+code:"CLICKHOUSE_SERVER_ERROR",status==429||status==503||status==504,null);
            if(status!=200)throw failure("CLICKHOUSE_HTTP_"+status,status==408||status==429||status==502||status==503||status==504,null);
            String encoding=connection.getContentEncoding();if(encoding!=null&&!encoding.equalsIgnoreCase("identity"))throw failure("CLICKHOUSE_UNEXPECTED_ENCODING",false,null);
            return result;
        }catch(SSLException rejected){throw failure("CLICKHOUSE_TLS_REJECTED",false,rejected);}
        catch(IOException unavailable){throw failure("CLICKHOUSE_CONNECTION_RETRY",true,unavailable);}
        finally{connection.disconnect();}
    }
    public JsonNode query(String sql)throws Exception{
        byte[] result=request((sql+" FORMAT JSONCompact").getBytes(StandardCharsets.UTF_8));
        try{
            JsonNode document=json.readTree(result);
            if(document==null||!document.isObject()||!document.path("data").isArray()||!document.path("meta").isArray()||!document.path("rows").isIntegralNumber()||!document.path("rows").canConvertToLong()||document.path("rows").asLong()!=document.path("data").size())throw failure("CLICKHOUSE_RESPONSE_INVALID",false,null);
            for(JsonNode row:document.path("data"))if(!row.isArray()||row.size()!=document.path("meta").size())throw failure("CLICKHOUSE_RESPONSE_INVALID",false,null);
            return document;
        }catch(com.fasterxml.jackson.core.JsonProcessingException invalid){throw failure("CLICKHOUSE_RESPONSE_INVALID",false,invalid);}
    }
    public void execute(String sql)throws Exception{execute(sql.getBytes(StandardCharsets.UTF_8));}
    public void execute(byte[] statementAndData)throws Exception{
        byte[] result=request(statementAndData);
        if(!new String(result,StandardCharsets.UTF_8).trim().isEmpty())throw failure("CLICKHOUSE_UNEXPECTED_COMMAND_RESPONSE",false,null);
    }
    @Override public void close(){/* Each request owns and closes its connection. */}
}
