package io.github.rivomesh.cdc;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import io.github.rivomesh.transport.classic.ClassicTransport;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.management.ObjectName;

/** Bounded, authenticated Jolokia POST requests. Never follows redirects or consumes event queues. */
public final class ClassicAdminClient implements ClassicTransport.Management {
    private final URL endpoint;private final String authorization,broker;private final int connectTimeout,requestTimeout;
    private final ObjectMapper json=new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    public ClassicAdminClient(String url,String username,String password,String name,int connectTimeout,int requestTimeout)throws Exception{
        URI uri=new URI(url);if(!Arrays.asList("http","https").contains(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getFragment()!=null||uri.getQuery()!=null||!Arrays.asList("/api/jolokia","/api/jolokia/").contains(uri.getPath())||name==null||!name.matches("[A-Za-z0-9_-]{1,128}")||username==null||username.isEmpty()||password==null)throw reject("CLASSIC_ADMIN_CONFIG_INVALID");
        endpoint=uri.toURL();authorization="Basic "+Base64.getEncoder().encodeToString((username+":"+password).getBytes(StandardCharsets.UTF_8));broker="org.apache.activemq:type=Broker,brokerName="+name;this.connectTimeout=connectTimeout;this.requestTimeout=requestTimeout;
    }
    private static ClassicTransport.Failure reject(String code){return new ClassicTransport.Failure(code,false,null);}
    private JsonNode call(Map<String,Object> request,boolean missing)throws Exception{
        byte[] body=json.writeValueAsBytes(request);if(body.length>65536)throw reject("CLASSIC_ADMIN_REQUEST_LIMIT");HttpURLConnection connection=null;
        try{connection=(HttpURLConnection)endpoint.openConnection();connection.setConnectTimeout(connectTimeout);connection.setReadTimeout(requestTimeout);connection.setInstanceFollowRedirects(false);connection.setRequestMethod("POST");connection.setRequestProperty("Authorization",authorization);connection.setRequestProperty("Content-Type","application/json; charset=utf-8");connection.setRequestProperty("Referer",endpoint.getProtocol()+"://"+endpoint.getAuthority()+"/");connection.setDoOutput(true);connection.setFixedLengthStreamingMode(body.length);try(OutputStream out=connection.getOutputStream()){out.write(body);}
            int status=connection.getResponseCode();if(status==401||status==403)throw reject("CLASSIC_ADMIN_AUTHORIZATION_REJECTED");if(status>=300&&status<400)throw reject("CLASSIC_ADMIN_REDIRECT_REJECTED");if(Arrays.asList(408,429,502,503,504).contains(status))throw new ClassicTransport.Failure("CLASSIC_ADMIN_RETRY",true,null);
            byte[] response;InputStream stream=status>=400?connection.getErrorStream():connection.getInputStream();if(stream==null)throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");try(InputStream input=stream;ByteArrayOutputStream out=new ByteArrayOutputStream()){byte[] buffer=new byte[4096];int count;while((count=input.read(buffer))!=-1){if(count>1024*1024-out.size())throw reject("CLASSIC_ADMIN_RESPONSE_LIMIT");out.write(buffer,0,count);}response=out.toByteArray();}
            JsonNode result;try{result=json.readTree(response);}catch(Exception invalid){throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");}if(result==null||!result.isObject()||!result.path("status").isIntegralNumber())throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");
            int nativeStatus=result.path("status").asInt();if(missing&&nativeStatus==404&&"javax.management.InstanceNotFoundException".equals(result.path("error_type").asText()))return null;
            if(status<200||status>=300||nativeStatus!=200)throw reject("CLASSIC_ADMIN_OPERATION_REJECTED");return result.get("value");
        }catch(javax.net.ssl.SSLException tls){throw new ClassicTransport.Failure("CLASSIC_ADMIN_TLS_REJECTED",false,tls);}catch(IOException network){throw new ClassicTransport.Failure("CLASSIC_ADMIN_CONNECTION_RETRY",true,network);}finally{if(connection!=null)connection.disconnect();}
    }
    private JsonNode read(String bean,boolean missing,String... attributes)throws Exception{Map<String,Object> request=new LinkedHashMap<>();request.put("type","read");request.put("mbean",bean);request.put("attribute",Arrays.asList(attributes));return call(request,missing);}
    private JsonNode exec(String bean,String operation,Object... args)throws Exception{Map<String,Object> request=new LinkedHashMap<>();request.put("type","exec");request.put("mbean",bean);request.put("operation",operation);request.put("arguments",Arrays.asList(args));return call(request,false);}
    private String queueBean(String queue)throws Exception{if(queue==null||!queue.matches("[A-Za-z][A-Za-z0-9_.-]{0,255}"))throw reject("CLASSIC_QUEUE_NAME_INVALID");String value=broker+",destinationType=Queue,destinationName="+queue;new ObjectName(value);return value;}
    private static String bean(JsonNode value)throws Exception{String name=value.isTextual()?value.asText():value.path("objectName").asText();if(name.isEmpty())throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");new ObjectName(name);return name;}
    @Override public ClassicTransport.Broker broker()throws Exception{
        JsonNode value=read(broker,false,"BrokerId","BrokerName","BrokerVersion","Persistent","Slave");if(value==null||!value.path("Persistent").isBoolean()||!value.path("Slave").isBoolean()||!value.path("BrokerId").isTextual()||!value.path("BrokerName").isTextual()||!value.path("BrokerVersion").isTextual())throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");
        ClassicTransport.Broker result=new ClassicTransport.Broker();result.id=value.path("BrokerId").asText();result.name=value.path("BrokerName").asText();result.version=value.path("BrokerVersion").asText();result.persistent=value.path("Persistent").asBoolean();result.slave=value.path("Slave").asBoolean();
        Map<String,Object> query=new LinkedHashMap<>();query.put("type","search");query.put("mbean",broker+",service=PersistenceAdapter,*");JsonNode persistence=call(query,false);if(persistence==null||!persistence.isArray()||persistence.size()!=1||!bean(persistence.get(0)).contains("KahaDBPersistenceAdapter"))throw reject("CLASSIC_KAHADB_PROFILE_REQUIRED");return result;
    }
    @Override public boolean exists(String queue)throws Exception{String expected=queueBean(queue);Map<String,Object> request=new LinkedHashMap<>();request.put("type","search");request.put("mbean",expected);JsonNode result=call(request,false);if(result==null||!result.isArray()||result.size()>1)throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");if(result.size()==0)return false;if(!new ObjectName(expected).equals(new ObjectName(bean(result.get(0)))))throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");return true;}
    @Override public void create(String queue)throws Exception{exec(broker,"addQueue(java.lang.String)",queue);if(!exists(queue))throw reject("CLASSIC_QUEUE_CREATE_NOT_OBSERVED");}
    @Override public long count(String queue)throws Exception{JsonNode value=read(queueBean(queue),false,"QueueSize");if(value==null||!value.path("QueueSize").isIntegralNumber()||value.path("QueueSize").asLong()<0)throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");return value.path("QueueSize").asLong();}
    @Override public void consumers(String events,String receipts)throws Exception{
        for(String name:Arrays.asList(events,receipts)){JsonNode data=read(queueBean(name),false,"Subscriptions");JsonNode subscriptions=data==null?null:data.get("Subscriptions");if(subscriptions==null||!subscriptions.isArray()||subscriptions.size()>128)throw reject("CLASSIC_CONSUMER_PROFILE_REQUIRES_PLAN");for(JsonNode subscription:subscriptions){JsonNode consumer=read(bean(subscription),false,"Selector");if(consumer==null)throw reject("CLASSIC_ADMIN_RESPONSE_INVALID");if(name.equals(receipts)||!ClassicTransport.EVENT_SELECTOR.equals(consumer.path("Selector").asText()))throw reject("CLASSIC_CONSUMER_SELECTOR_REQUIRED");}}
    }
    @Override public String text(String queue,String id)throws Exception{JsonNode value=exec(queueBean(queue),"getMessage(java.lang.String)",id);if(value==null||value.isNull())return null;if(!id.equals(value.path("JMSMessageID").asText())||!value.path("Text").isTextual()||value.path("JMSExpiration").asLong(-1)!=0)throw reject("CLASSIC_NATIVE_MESSAGE_SHAPE_CHANGED");return value.path("Text").asText();}
}
