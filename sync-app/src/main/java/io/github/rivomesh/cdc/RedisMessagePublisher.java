package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.*;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.util.KeyValue;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLParameters;

/** Strict direct-standalone Redis>=7.2: XADD NOMKSTREAM + same-connection WAITAOF. */
public final class RedisMessagePublisher implements ConfirmedPublisher {
    public static final String OWNERS="_rivomesh_publication_owners_v1",MARKER_GROUP="_rivomesh_stream_marker_v1";
    private final PipelineConfig.Target target;
    private final String binding;
    private final Jedis client;
    private boolean prepared;
    public RedisMessagePublisher(PipelineConfig.Target target,String binding)throws Exception{
        this(target,binding,connect(target));
    }
    RedisMessagePublisher(PipelineConfig.Target target,String binding,Jedis client)throws Exception{
        this.target=target;this.binding=binding;this.client=client;try{capabilities();}catch(Exception e){try{client.close();}catch(Exception close){e.addSuppressed(close);}throw failure(e);}
    }
    static Jedis connect(PipelineConfig.Target target)throws Exception{
        URI uri=new URI(target.url);String path=uri.getPath();int database=path==null||path.isEmpty()?0:Integer.parseInt(path.substring(1));
        DefaultJedisClientConfig.Builder config=DefaultJedisClientConfig.builder().database(database).autoNegotiateProtocol(false).connectionTimeoutMillis(target.messaging.connectTimeoutMillis).socketTimeoutMillis(target.messaging.requestTimeoutMillis+1000).blockingSocketTimeoutMillis(target.messaging.requestTimeoutMillis+1000).ssl("rediss".equals(uri.getScheme()));
        if(target.username!=null&&!target.username.isEmpty())config.user(target.username);if(target.password!=null)config.password(target.password);
        SSLParameters ssl=new SSLParameters();ssl.setEndpointIdentificationAlgorithm("HTTPS");config.sslParameters(ssl);
        DefaultJedisClientConfig options=config.build();DefaultJedisSocketFactory sockets=new DefaultJedisSocketFactory(new HostAndPort(uri.getHost(),uri.getPort()<0?6379:uri.getPort()),options);AtomicBoolean opened=new AtomicBoolean();
        // Raw Jedis can reopen a socket without rerunning AUTH/SELECT. Never let commands
        // continue on a replacement socket (or accidentally write into default database 0).
        JedisSocketFactory oneConnection=()->{if(opened.getAndSet(true))throw new JedisConnectionException("REDIS_CONNECTION_REOPEN_REQUIRES_NEW_CLIENT");return sockets.createSocket();};
        try{return new Jedis(oneConnection,options);}
        catch(Exception initializationFailure){throw failure(initializationFailure);}
    }
    private static Map<String,String> info(String text){Map<String,String> result=new HashMap<>();for(String line:text.split("\\r?\\n")){int colon=line.indexOf(':');if(colon>0&&!line.startsWith("#"))result.put(line.substring(0,colon),line.substring(colon+1));}return result;}
    private static SyncFailure rejected(String code,String message){return new SyncFailure(code,"MESSAGE_PUBLICATION",false,message);}
    private void capabilities()throws Exception{
        Map<String,String> server=info(client.info("server"));String version=server.getOrDefault("redis_version","");String[] parts=version.split("\\.");
        if(parts.length<2||Integer.parseInt(parts[0])<7||Integer.parseInt(parts[0])==7&&Integer.parseInt(parts[1])<2)throw rejected("REDIS_WAITAOF_REQUIRED","Redis 7.2 or newer is required for the durable stream profile");
        if(!"0".equals(info(client.info("cluster")).get("cluster_enabled")))throw rejected("REDIS_CLUSTER_PROFILE_REQUIRED","Cluster requires explicit same-node routing, unavailable in this profile");
        if(!"master".equals(info(client.info("replication")).get("role")))throw new SyncFailure("REDIS_WRITABLE_PRIMARY_REQUIRED","MESSAGE_PUBLICATION",true,"A writable direct primary endpoint is required");
        if(!"1".equals(info(client.info("persistence")).get("aof_enabled")))throw rejected("REDIS_AOF_REQUIRED","Enable AOF before using this destination");
        if(!"noeviction".equals(client.configGet("maxmemory-policy").get("maxmemory-policy")))throw rejected("REDIS_NOEVICTION_REQUIRED","Eviction can silently discard a confirmed stream");
    }
    public static void check(PipelineConfig.Target target)throws Exception{try(RedisMessagePublisher ignored=new RedisMessagePublisher(target,null)){/* capabilities only; no keys or groups created */}}
    @Override public void prepare(Set<String> destinations)throws Exception{prepare(target.messaging.autoCreate);}
    @Override public void require(Set<String> destinations)throws Exception{prepare(false);}
    private void prepare(boolean allowCreate)throws Exception{
        if(prepared)return;if(binding==null)throw new IllegalStateException("PUBLICATION_BINDING_REQUIRED");String stream=target.messaging.stream;
        try{
            if(client.pttl(OWNERS)>=0)throw rejected("REDIS_OWNER_EXPIRY_REQUIRES_PLAN","Publication ownership must not expire");
            String owner=client.hget(OWNERS,stream);
            if(owner==null){
                if(!allowCreate)throw rejected("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED","An initialized Redis stream owner is missing");
                if(client.exists(stream))throw rejected("MESSAGE_DESTINATION_UNOWNED_REQUIRES_PLAN","An existing unowned stream needs an adoption plan");
                client.hsetnx(OWNERS,stream,binding);owner=client.hget(OWNERS,stream);
            }
            if(!binding.equals(owner))throw rejected("DESTINATION_ALREADY_OWNED","Another publication owns this Redis stream");
            if(!client.exists(stream)){
                if(!allowCreate)throw rejected("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED","An initialized Redis stream is missing");
                try{client.xgroupCreate(stream,MARKER_GROUP,new StreamEntryID(0,0),true);}
                catch(JedisDataException race){if(race.getMessage()==null||!race.getMessage().startsWith("BUSYGROUP"))throw race;}
            }
            if(!"stream".equals(client.type(stream)))throw rejected("REDIS_STREAM_TYPE_REQUIRES_PLAN","The destination key is not a stream");
            if(client.pttl(stream)!=-1||client.pttl(OWNERS)!=-1)throw rejected("REDIS_STREAM_EXPIRY_REQUIRES_PLAN","Stream and ownership keys must not expire");prepared=true;
        }catch(Exception e){throw failure(e);}
    }
    /** A client may reconnect internally. Server run_id also guards client-id reuse on restart. */
    private String connectionToken(){long id=client.clientId();String server=info(client.info("server")).get("run_id");if(server==null||server.isEmpty())throw new IllegalStateException("REDIS_CONNECTION_IDENTITY_UNAVAILABLE");return server+":"+id;}
    @Override public void publish(List<Message> messages)throws Exception{
        try{
            String connection=connectionToken();String stream=target.messaging.stream;
            for(Message message:messages){
                Map<String,String> fields=new LinkedHashMap<>(message.headers);fields.put("rivomesh_event_id",message.id);fields.put("rivomesh_channel",message.destination);if(message.key!=null)fields.put("rivomesh_key",message.key);fields.put("body",new String(message.body,StandardCharsets.UTF_8));
                if(client.xadd(stream,XAddParams.xAddParams().noMkStream().id(StreamEntryID.NEW_ENTRY),fields)==null)throw rejected("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED","The initialized stream disappeared; it was not recreated");
            }
            KeyValue<Long,Long> fsyncs=client.waitAOF(1,target.messaging.replicas-1,target.messaging.requestTimeoutMillis);
            if(!connection.equals(connectionToken()))throw new SyncFailure("REDIS_CONNECTION_CHANGED_DURING_CONFIRM","MESSAGE_PUBLICATION",true,"Publication confirmation used a different physical connection");
            if(fsyncs.getKey()<1||fsyncs.getValue()<target.messaging.replicas-1)throw new SyncFailure("REDIS_DURABILITY_NOT_CONFIRMED","MESSAGE_PUBLICATION",true,"WAITAOF did not confirm the required local and replica fsync counts");
        }catch(Exception e){throw failure(e);}
    }
    private static Exception failure(Exception error){
        if(error instanceof SyncFailure)return error;
        if(error instanceof JedisConnectionException)return new SyncFailure("REDIS_CONNECTION_LOST","MESSAGE_PUBLICATION",true,"Redis connection failed",error,null,0,null);
        if(error instanceof JedisDataException){String message=error.getMessage()==null?"":error.getMessage();boolean retry=message.startsWith("OOM")||message.startsWith("NOREPLICAS")||message.startsWith("READONLY")||message.startsWith("LOADING")||message.startsWith("MISCONF");return new SyncFailure(retry?"REDIS_TEMPORARILY_UNAVAILABLE":"REDIS_COMMAND_REJECTED","MESSAGE_PUBLICATION",retry,"Redis rejected the operation",error,null,0,null);}return error;
    }
    @Override public void close(){client.close();}
}
