package io.github.rivomesh.transport.artemis;

import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import javax.json.*;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Java-8 Core protocol boundary. Management uses a temporary, non-durable reply channel. */
public final class ArtemisTransport implements AutoCloseable {
    public static final class Settings {
        public String url,username,password,queuePrefix,brokerName;
        public int connectTimeoutMillis=5000,requestTimeoutMillis=10000,maxMessageBytes=1024*1024;
    }
    public static final class Publication {public String id,channel,key;public byte[] body;public Map<String,String> headers=new LinkedHashMap<>();}
    public static final class Failure extends Exception {
        public final String code;public final boolean retryable;
        Failure(String code,boolean retryable,Throwable cause){super(code,cause);this.code=code;this.retryable=retryable;}
    }
    private final Settings settings;private final String binding,queue;
    private final ServerLocator locator;private final ClientSessionFactory factory;
    private final ClientSession management,data;private final ClientRequestor requestor;
    private ClientProducer producer;private boolean verified;private String identity;
    private long confirmedMessageId=-1;private String confirmedEventId,confirmedHash;
    public void resumeProgress(String progress)throws Exception{
        if(verified||confirmedMessageId>=0)throw new IllegalStateException("ARTEMIS_PROGRESS_ALREADY_ACTIVE");if(progress==null)return;
        try{String[] fields=progress.split(":",-1);if(fields.length!=5||!"ARTEMIS_RECORD_V1".equals(fields[0])||!queue.equals(fields[1])||!fields[4].matches("[a-f0-9]{64}"))throw new IllegalArgumentException();long nativeId=Long.parseLong(fields[2]);String eventId=new String(Base64.getUrlDecoder().decode(fields[3]),StandardCharsets.UTF_8);if(nativeId<=0||eventId.isEmpty()||eventId.length()>512||!Base64.getUrlEncoder().withoutPadding().encodeToString(eventId.getBytes(StandardCharsets.UTF_8)).equals(fields[3]))throw new IllegalArgumentException();confirmedMessageId=nativeId;confirmedEventId=eventId;confirmedHash=fields[4];}
        catch(Exception invalid){throw reject("ARTEMIS_PROGRESS_INVALID");}
    }
    public String confirmedProgress(){return confirmedHash==null?null:"ARTEMIS_RECORD_V1:"+queue+":"+confirmedMessageId+":"+Base64.getUrlEncoder().withoutPadding().encodeToString(confirmedEventId.getBytes(StandardCharsets.UTF_8))+":"+confirmedHash;}
    private static String contentHash(String id,byte[] body)throws Exception{
        java.security.MessageDigest hash=java.security.MessageDigest.getInstance("SHA-256");byte[] identity=id.getBytes(StandardCharsets.UTF_8);hash.update(java.nio.ByteBuffer.allocate(4).putInt(identity.length).array());hash.update(identity);hash.update(body);StringBuilder result=new StringBuilder();for(byte value:hash.digest())result.append(String.format(Locale.ROOT,"%02x",value&255));return result.toString();
    }
    private long record(String eventId,String digest,long requiredId)throws Exception{
        String filter="rivomesh_event_id = '"+eventId.replace("'","''")+"' AND rivomesh_publication_digest = '"+digest+"'";
        try(ClientConsumer browser=data.createConsumer(queue,filter,true)){long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(settings.requestTimeoutMillis);do{long remaining=java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline-System.nanoTime());if(remaining<=0)break;ClientMessage message=browser.receive(Math.max(1,remaining));if(message==null)break;if(requiredId>0&&message.getMessageID()!=requiredId)continue;byte[] body=new byte[message.getBodyBuffer().readableBytes()];message.getBodyBuffer().readBytes(body);if(!eventId.equals(message.getStringProperty("rivomesh_event_id"))||!digest.equals(contentHash(eventId,body)))throw reject("ARTEMIS_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");if(message.getMessageID()<=0)throw reject("ARTEMIS_NATIVE_MESSAGE_ID_REQUIRED");return message.getMessageID();}while(System.nanoTime()<deadline);}
        throw reject("ARTEMIS_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED");
    }
    private void verifyProgress()throws Exception{if(confirmedHash!=null)record(confirmedEventId,confirmedHash,confirmedMessageId);}

    public static String physicalQueue(String prefix,String binding){if(prefix==null||!prefix.matches("[A-Za-z][A-Za-z0-9_-]{0,47}")||binding==null||!binding.matches("rivomesh:[a-f0-9]{64}"))throw new IllegalArgumentException("ARTEMIS_PUBLICATION_NAME_INVALID");return prefix+"."+binding.substring(9);}
    public ArtemisTransport(Settings settings,String binding)throws Exception{
        this.settings=settings;this.binding=binding;queue=binding==null?null:physicalQueue(settings.queuePrefix,binding);
        URI uri=URI.create(settings.url);if(!Arrays.asList("artemis","artemiss").contains(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||uri.getPath()!=null&&!uri.getPath().isEmpty()||settings.username==null||settings.username.isEmpty()||settings.password==null||settings.connectTimeoutMillis<100||settings.connectTimeoutMillis>120000||settings.requestTimeoutMillis<100||settings.requestTimeoutMillis>120000||settings.maxMessageBytes<1024||settings.maxMessageBytes>16*1024*1024)throw new IllegalArgumentException("ARTEMIS_CONFIG_INVALID");
        Map<String,Object> parameters=new HashMap<>();parameters.put("host",uri.getHost());parameters.put("port",uri.getPort()<0?61616:uri.getPort());parameters.put(TransportConstants.NETTY_CONNECT_TIMEOUT,settings.connectTimeoutMillis);parameters.put("useEpoll",false);parameters.put("useKQueue",false);parameters.put("nioRemotingThreads",1);if(uri.getScheme().equals("artemiss")){parameters.put("sslEnabled",true);parameters.put("verifyHost",true);parameters.put("trustAll",false);parameters.put("useDefaultSslContext",true);parameters.put("sslProvider","JDK");}
        locator=ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(),parameters));locator.setUseGlobalPools(false).setThreadPoolMaxSize(2).setScheduledThreadPoolMaxSize(1).setUseTopologyForLoadBalancing(false).setReconnectAttempts(0).setInitialConnectAttempts(1).setCallTimeout(settings.requestTimeoutMillis).setCallFailoverTimeout(settings.requestTimeoutMillis).setBlockOnDurableSend(true).setBlockOnNonDurableSend(true).setBlockOnAcknowledge(true).setConsumerWindowSize(0).setMinLargeMessageSize(settings.maxMessageBytes+8192);
        ClientSessionFactory opened=null;ClientSession admin=null,sender=null;ClientRequestor requests=null;
        try{opened=locator.createSessionFactory();admin=opened.createSession(settings.username,settings.password,false,true,true,false,0);admin.start();requests=new ClientRequestor(admin,"activemq.management");sender=opened.createSession(settings.username,settings.password,false,false,true,false,0);sender.start();factory=opened;management=admin;requestor=requests;data=sender;}
        catch(Exception failure){if(requests!=null)try{requests.close();}catch(Exception ignored){}if(admin!=null)try{admin.close();}catch(Exception ignored){}if(sender!=null)try{sender.close();}catch(Exception ignored){}if(opened!=null)opened.close();locator.close();throw classify(failure);}
    }
    private static Failure reject(String code){return new Failure(code,false,null);}
    private Object reply(ClientMessage message)throws Exception{ClientMessage response=requestor.request(message,settings.requestTimeoutMillis);if(response==null)throw new Failure("ARTEMIS_MANAGEMENT_TIMEOUT",true,null);if(!ManagementHelper.hasOperationSucceeded(response))throw reject("ARTEMIS_MANAGEMENT_REJECTED");return ManagementHelper.getResult(response);}
    Object attribute(String resource,String name)throws Exception{ClientMessage message=management.createMessage(false);ManagementHelper.putAttribute(message,resource,name);return reply(message);}
    Object operation(String resource,String name,Object... arguments)throws Exception{ClientMessage message=management.createMessage(false);ManagementHelper.putOperationInvocation(message,resource,name,arguments);return reply(message);}
    private static JsonObject object(String value){try(JsonReader reader=Json.createReader(new StringReader(value))){return reader.readObject();}}
    private String capabilities(String address)throws Exception{
        if(!Boolean.TRUE.equals(attribute("broker","active")))throw new Failure("ARTEMIS_BROKER_ACTIVATING",true,null);if(!Boolean.TRUE.equals(attribute("broker","persistenceEnabled"))||!Boolean.TRUE.equals(attribute("broker","journalSyncTransactional"))||!Boolean.TRUE.equals(attribute("broker","journalSyncNonTransactional")))throw reject("ARTEMIS_SYNCHRONOUS_PERSISTENCE_REQUIRED");
        String name=String.valueOf(attribute("broker","name"));if(settings.brokerName!=null&&!settings.brokerName.isEmpty()&&!settings.brokerName.equals(name))throw reject("ARTEMIS_BROKER_BINDING_MISMATCH");String node=String.valueOf(attribute("broker","nodeID"));if(node.isEmpty()||node.equals("null"))throw reject("ARTEMIS_NODE_ID_REQUIRED");
        JsonObject policy=object(String.valueOf(operation("broker","getAddressSettingsAsJSON",address)));
        for(String key:Arrays.asList("autoCreateQueues","autoCreateAddresses","autoDeleteQueues","autoDeleteCreatedQueues","autoDeleteAddresses"))if(policy.getBoolean(key,true))throw reject("ARTEMIS_AUTOMATIC_RESOURCE_CHANGES_MUST_BE_DISABLED");
        boolean noExpiry=policy.getBoolean("noExpiry",false);if(!noExpiry)for(String key:Arrays.asList("expiryDelay","minExpiryDelay","maxExpiryDelay"))if(!policy.containsKey(key)||policy.getJsonNumber(key).longValue()!=-1)throw reject("ARTEMIS_EXPIRY_MUST_BE_DISABLED");
        String full=policy.getString("addressFullMessagePolicy",policy.getString("addressFullPolicy",""));if(!Arrays.asList("PAGE","BLOCK","FAIL").contains(full))throw reject("ARTEMIS_DISCARD_POLICY_REJECTED");
        if(!policy.containsKey("maxDeliveryAttempts")||policy.getInt("maxDeliveryAttempts")!=-1)throw reject("ARTEMIS_DELIVERY_DISCARD_MUST_BE_DISABLED");
        if(policy.containsKey("pageFullMessagePolicy")&&!policy.isNull("pageFullMessagePolicy")&&"DROP".equals(policy.getString("pageFullMessagePolicy")))throw reject("ARTEMIS_DISCARD_POLICY_REJECTED");
        return node;
    }
    public static void check(Settings settings)throws Exception{try(ArtemisTransport transport=new ArtemisTransport(settings,null)){try{transport.capabilities(settings.queuePrefix+"._capability_check");}catch(Exception failure){throw classify(failure);}}}
    private ClientSession.QueueQuery queue()throws Exception{return data.queueQuery(new SimpleString(queue));}
    private void shape(ClientSession.QueueQuery result)throws Exception{
        if(!result.isExists())throw reject("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");if(!result.isDurable()||result.isTemporary()||!Boolean.TRUE.equals(result.isNonDestructive())||result.isPurgeOnNoConsumers()||!Boolean.FALSE.equals(result.isLastValue())||!Boolean.FALSE.equals(result.isAutoDelete())||!Long.valueOf(-1).equals(result.getRingSize())||result.getFilterString()!=null||result.getRoutingType()!=RoutingType.ANYCAST||!queue.equals(result.getAddress().toString()))throw reject("ARTEMIS_QUEUE_CONFIGURATION_REQUIRES_PLAN");
        List<SimpleString> bindings=data.addressQuery(new SimpleString(queue)).getQueueNames();if(bindings.size()!=1||!bindings.contains(new SimpleString(queue)))throw reject("ARTEMIS_ADDRESS_BINDING_REQUIRES_PLAN");
    }
    private void ownership()throws Exception{
        try(ClientConsumer browser=data.createConsumer(queue,true)){ClientMessage marker=browser.receive(settings.requestTimeoutMillis);if(marker==null)throw reject("ARTEMIS_PUBLICATION_MANIFEST_MISSING");byte[] body=new byte[marker.getBodyBuffer().readableBytes()];marker.getBodyBuffer().readBytes(body);if(!binding.equals(marker.getStringProperty("rivomesh_publication_manifest"))||!Arrays.equals(body,binding.getBytes(StandardCharsets.UTF_8)))throw reject("DESTINATION_ALREADY_OWNED");}
    }
    public void prepare(boolean allowCreate)throws Exception{
        if(verified)return;if(queue==null)throw new IllegalStateException("PUBLICATION_BINDING_REQUIRED");
        try{String node=capabilities(queue);ClientSession.QueueQuery result=queue();boolean created=!result.isExists();if(created){if(!allowCreate||confirmedHash!=null)throw reject("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");QueueConfiguration configuration=new QueueConfiguration(queue).setAddress(queue).setRoutingType(RoutingType.ANYCAST).setDurable(true).setTemporary(false).setNonDestructive(true).setAutoDelete(false).setPurgeOnNoConsumers(false).setRingSize(-1L).setAutoCreateAddress(true);try{operation("broker","createQueue",configuration.toJSON());}catch(Exception raced){if(!queue().isExists())throw raced;}result=queue();}shape(result);producer=data.createProducer(queue);
            if(result.getMessageCount()==0){if(!allowCreate||confirmedHash!=null)throw reject("ARTEMIS_PUBLICATION_MANIFEST_MISSING");ClientMessage marker=data.createMessage(Message.BYTES_TYPE,true,0,System.currentTimeMillis(),(byte)4);marker.putStringProperty("rivomesh_publication_manifest",binding);marker.getBodyBuffer().writeBytes(binding.getBytes(StandardCharsets.UTF_8));producer.send(marker);data.commit();}
            ownership();verifyProgress();identity="ARTEMIS_NODE_QUEUE_V1:"+node+":"+attribute("queue."+queue,"ID");verified=true;
        }catch(Exception failure){throw classify(failure);}
    }
    public String identity(){return verified?identity:null;}
    public void publish(List<Publication> messages)throws Exception{
        if(!verified||producer==null)throw new IllegalStateException("PUBLICATION_NOT_PREPARED");
        try{String node=capabilities(queue);shape(queue());String current="ARTEMIS_NODE_QUEUE_V1:"+node+":"+attribute("queue."+queue,"ID");if(!identity.equals(current))throw reject("ARTEMIS_RESOURCE_ID_CHANGED");ownership();verifyProgress();
            for(Publication publication:messages){if(publication.body==null||publication.body.length>settings.maxMessageBytes)throw reject("MESSAGE_TOO_LARGE");ClientMessage message=data.createMessage(Message.BYTES_TYPE,true,0,System.currentTimeMillis(),(byte)4);if(publication.id==null||publication.id.isEmpty()||publication.id.length()>512)throw reject("ARTEMIS_EVENT_ID_INVALID");for(Map.Entry<String,String> header:publication.headers.entrySet())message.putStringProperty(header.getKey(),header.getValue());message.putStringProperty("rivomesh_event_id",publication.id);message.putStringProperty("rivomesh_publication_digest",contentHash(publication.id,publication.body));message.putStringProperty("rivomesh_channel",publication.channel);if(publication.key!=null)message.putStringProperty("rivomesh_key",publication.key);message.getBodyBuffer().writeBytes(publication.body);producer.send(message);}data.commit();shape(queue());if(!identity.equals("ARTEMIS_NODE_QUEUE_V1:"+attribute("broker","nodeID")+":"+attribute("queue."+queue,"ID")))throw reject("ARTEMIS_RESOURCE_ID_CHANGED");ownership();if(!messages.isEmpty()){Publication last=messages.get(messages.size()-1);String digest=contentHash(last.id,last.body);long nativeId=record(last.id,digest,-1);confirmedMessageId=nativeId;confirmedEventId=last.id;confirmedHash=digest;}
        }catch(Exception failure){try{data.rollback();}catch(Exception ignored){}throw classify(failure);}
    }
    private static Exception classify(Exception failure){if(failure instanceof Failure)return failure;if(failure instanceof ActiveMQException){String type=((ActiveMQException)failure).getType().name();boolean retry=Arrays.asList("CONNECTION_TIMEDOUT","NOT_CONNECTED","DISCONNECTED","OBJECT_CLOSED","UNBLOCKED","CONNECTION_DESTROYED","IO_ERROR","PACKET_RESPONSE_TIMEOUT","ADDRESS_FULL","SESSION_CREATION_REJECTED").contains(type);return new Failure("ARTEMIS_"+type,retry,failure);}return failure;}
    @Override public void close(){if(producer!=null)try{producer.close();}catch(Exception ignored){}try{data.close();}catch(Exception ignored){}try{requestor.close();}catch(Exception ignored){}try{management.close();}catch(Exception ignored){}factory.close();locator.close();}
}
