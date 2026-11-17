package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.*;
import io.github.rivomesh.SyncFailure;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.net.ssl.SSLException;

/** Native Stream Load only. Redirect credentials are restricted to explicitly configured origins. */
public final class DorisHttpClient implements AutoCloseable {
    public static final class Settings {
        public String url,username,password;
        public List<String> redirectOrigins=new ArrayList<>();
        public int connectTimeoutMillis=5000,requestTimeoutMillis=30000,maximumRequestBytes=16*1024*1024,maximumResponseBytes=1024*1024;
    }
    private final Settings settings;
    private final URI origin;
    private final Set<String> redirects=new HashSet<>();
    private final String authorization;
    private final ObjectMapper json=new ObjectMapper().enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    public DorisHttpClient(Settings s){
        if(s==null||s.username==null||s.username.isEmpty()||s.username.contains(":")||s.password==null||s.username.chars().anyMatch(c->c<32||c==127)||s.password.chars().anyMatch(c->c<32||c==127)||s.redirectOrigins==null||s.redirectOrigins.size()>64||s.connectTimeoutMillis<100||s.connectTimeoutMillis>120000||s.requestTimeoutMillis<100||s.requestTimeoutMillis>120000||s.maximumRequestBytes<1024||s.maximumRequestBytes>64*1024*1024||s.maximumResponseBytes<1024||s.maximumResponseBytes>16*1024*1024)throw new IllegalArgumentException("DORIS_HTTP_CONFIG_INVALID");
        settings=s;origin=origin(s.url);for(String allowed:s.redirectOrigins){URI target=origin(allowed);if(origin.getScheme().equals("https")&&!target.getScheme().equals("https"))throw new IllegalArgumentException("DORIS_TLS_DOWNGRADE_REJECTED");redirects.add(key(target));}authorization="Basic "+Base64.getEncoder().encodeToString((s.username+":"+s.password).getBytes(StandardCharsets.UTF_8));
    }
    private static URI origin(String value){try{URI uri=URI.create(value);if(!Arrays.asList("http","https").contains(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||uri.getPort()==0||uri.getRawPath()!=null&&!uri.getRawPath().isEmpty()&&!uri.getRawPath().equals("/"))throw new IllegalArgumentException();return uri;}catch(Exception invalid){throw new IllegalArgumentException("DORIS_HTTP_ORIGIN_INVALID");}}
    private static String key(URI uri){return uri.getScheme()+"://"+uri.getHost().toLowerCase(Locale.ROOT)+":"+(uri.getPort()<0?(uri.getScheme().equals("https")?443:80):uri.getPort());}
    static SyncFailure failure(String code,boolean retry,Throwable cause){return new SyncFailure(code,"DORIS_LOAD",retry,code,cause,null,0,null);}
    private byte[] read(InputStream source)throws Exception{if(source==null)return new byte[0];try(InputStream input=source;ByteArrayOutputStream output=new ByteArrayOutputStream()){byte[] bytes=new byte[8192];int size;while((size=input.read(bytes))!=-1){if(output.size()+size>settings.maximumResponseBytes)throw failure("DORIS_RESPONSE_LIMIT",false,null);output.write(bytes,0,size);}return output.toByteArray();}}
    /** Read-only reachability probe. Health endpoints do not establish LOAD/DDL privileges. */
    public void check()throws Exception{
        HttpURLConnection connection=(HttpURLConnection)origin.resolve("/api/health").toURL().openConnection();boolean released=false;
        try{connection.setConnectTimeout(settings.connectTimeoutMillis);connection.setReadTimeout(settings.requestTimeoutMillis);connection.setInstanceFollowRedirects(false);connection.setRequestProperty("Authorization",authorization);connection.setRequestProperty("Accept-Encoding","identity");int status=connection.getResponseCode();byte[] bytes=read(status>=400?connection.getErrorStream():connection.getInputStream());released=true;if(status!=200)throw failure("DORIS_HEALTH_HTTP_"+status,status==408||status==429||status==502||status==503||status==504,null);JsonNode response;try{response=json.readTree(bytes);}catch(com.fasterxml.jackson.core.JsonProcessingException invalid){throw failure("DORIS_HEALTH_RESPONSE_INVALID",false,invalid);}if(response==null||!response.isObject())throw failure("DORIS_HEALTH_RESPONSE_INVALID",false,null);}
        catch(SSLException rejected){throw failure("DORIS_TLS_REJECTED",false,rejected);}catch(IOException unavailable){throw failure("DORIS_CONNECTION_RETRY",true,unavailable);}finally{if(!released)connection.disconnect();}
    }
    public JsonNode load(String database,String table,String label,byte[] body,int rows)throws Exception{
        if(database==null||table==null||!database.matches("[A-Za-z][A-Za-z0-9_]{0,63}")||!table.matches("[A-Za-z][A-Za-z0-9_]{0,127}")||label==null||!label.matches("[A-Za-z0-9_-]{1,128}")||body==null||body.length==0||body.length>settings.maximumRequestBytes||rows<1)throw new IllegalArgumentException("DORIS_LOAD_REQUEST_INVALID");
        String path="/api/"+database+"/"+table+"/_stream_load";URI address=origin.resolve(path);Set<String> visited=new HashSet<>();
        for(int attempt=0;attempt<3;attempt++){
            if(!visited.add(address.toString()))throw failure("DORIS_REDIRECT_LOOP",false,null);
            HttpURLConnection connection=(HttpURLConnection)address.toURL().openConnection();boolean released=false;
            try{
                connection.setConnectTimeout(settings.connectTimeoutMillis);connection.setReadTimeout(settings.requestTimeoutMillis);connection.setInstanceFollowRedirects(false);connection.setRequestMethod("PUT");connection.setDoOutput(true);connection.setFixedLengthStreamingMode(body.length);
                connection.setRequestProperty("Authorization",authorization);connection.setRequestProperty("Content-Type","application/json; charset=utf-8");connection.setRequestProperty("Accept-Encoding","identity");connection.setRequestProperty("label",label);connection.setRequestProperty("format","json");connection.setRequestProperty("strip_outer_array","true");connection.setRequestProperty("read_json_by_line","false");connection.setRequestProperty("num_as_string","true");connection.setRequestProperty("strict_mode","true");connection.setRequestProperty("max_filter_ratio","0");connection.setRequestProperty("timeout",Integer.toString(Math.max(1,(settings.requestTimeoutMillis-100)/1000)));
                try(OutputStream output=connection.getOutputStream()){output.write(body);}
                int status=connection.getResponseCode();
                if(status==307||status==308){read(connection.getInputStream());released=true;String location=connection.getHeaderField("Location");URI next;try{next=address.resolve(location);}catch(Exception invalid){throw failure("DORIS_REDIRECT_REJECTED",false,null);}if(next.getHost()==null||next.getUserInfo()!=null||next.getQuery()!=null||next.getFragment()!=null||!path.equals(next.getRawPath())||!redirects.contains(key(next))||address.getScheme().equals("https")&&!next.getScheme().equals("https"))throw failure("DORIS_REDIRECT_REJECTED",false,null);address=next;continue;}
                byte[] bytes=read(status>=400?connection.getErrorStream():connection.getInputStream());released=true;
                if(status!=200)throw failure("DORIS_HTTP_"+status,status==408||status==429||status==502||status==503||status==504,null);
                if(connection.getContentEncoding()!=null&&!connection.getContentEncoding().equalsIgnoreCase("identity"))throw failure("DORIS_RESPONSE_ENCODING_INVALID",false,null);
                JsonNode result;try{result=json.readTree(bytes);}catch(com.fasterxml.jackson.core.JsonProcessingException invalid){throw failure("DORIS_RESPONSE_INVALID",false,invalid);}
                if(result==null||!result.isObject()||!label.equals(result.path("Label").asText()))throw failure("DORIS_RESPONSE_IDENTITY_MISMATCH",false,null);
                String outcome=result.path("Status").asText();
                if(outcome.equals("Label Already Exists")){if(result.path("ExistingJobStatus").asText().equals("FINISHED"))return result;throw failure(result.path("ExistingJobStatus").asText().equals("RUNNING")?"DORIS_LABEL_PENDING":"DORIS_LABEL_REJECTED",result.path("ExistingJobStatus").asText().equals("RUNNING"),null);}
                if(outcome.equals("Publish Timeout"))throw failure("DORIS_VISIBILITY_PENDING",true,null);
                if(!outcome.equals("Success"))throw failure("DORIS_LOAD_REJECTED",false,null);
                for(String count:Arrays.asList("NumberTotalRows","NumberLoadedRows","NumberFilteredRows","NumberUnselectedRows"))if(!result.path(count).isIntegralNumber()||!result.path(count).canConvertToLong())throw failure("DORIS_LOAD_COUNTS_INVALID",false,null);
                if(result.path("NumberTotalRows").asLong()!=rows||result.path("NumberLoadedRows").asLong()!=rows||result.path("NumberFilteredRows").asLong()!=0||result.path("NumberUnselectedRows").asLong()!=0)throw failure("DORIS_LOAD_ROWS_NOT_CONFIRMED",false,null);
                return result;
            }catch(SSLException rejected){throw failure("DORIS_TLS_REJECTED",false,rejected);}catch(IOException unavailable){throw failure("DORIS_CONNECTION_RETRY",true,unavailable);}finally{if(!released)connection.disconnect();}
        }
        throw failure("DORIS_REDIRECT_LIMIT",false,null);
    }
    @Override public void close(){}
}
