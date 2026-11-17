package io.github.rivomesh.transport.classic;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/** OpenWire local transactions. Management never consumes the retained owner/receipt records. */
public final class ClassicTransport implements AutoCloseable {
    public static final String EVENT_SELECTOR="rivomesh_record_type = 'EVENT'";
    public static final class Settings {
        public String url,username,password,queuePrefix;
        public int connectTimeoutMillis=5000,requestTimeoutMillis=10000,maxMessageBytes=1024*1024;
    }
    public static final class Broker {public String id,name,version;public boolean persistent,slave;}
    public interface Management {
        Broker broker()throws Exception;
        boolean exists(String queue)throws Exception;
        void create(String queue)throws Exception;
        long count(String queue)throws Exception;
        void consumers(String events,String receipts)throws Exception;
        /** Null means this particular confirmed native message is absent. */
        String text(String queue,String jmsId)throws Exception;
    }
    public static final class Publication {public String id,channel,key;public byte[] body;public Map<String,String> headers=new TreeMap<>();}
    public static final class Failure extends Exception {
        public final String code;public final boolean retryable;
        public Failure(String code,boolean retryable,Throwable cause){super(code,cause);this.code=code;this.retryable=retryable;}
    }
    private final Settings settings;private final Management management;
    private final ActiveMQConnection connection;private final Session writes,browses;
    private final String binding,queue,receipts;
    private MessageProducer producer;private String owner,receiptId,receiptHash;private boolean prepared;
    private static Failure reject(String code){return new Failure(code,false,null);}
    public static String physicalQueue(String prefix,String binding) {
        if(prefix==null||!prefix.matches("[A-Za-z][A-Za-z0-9_-]{0,47}")||binding==null||!binding.matches("rivomesh:[a-f0-9]{64}"))throw new IllegalArgumentException("CLASSIC_BINDING_INVALID");return prefix+"."+binding.substring(9);
    }
    public ClassicTransport(Settings s,Management management,String binding)throws Exception{
        if(s==null||management==null||s.username==null||s.username.isEmpty()||s.password==null||s.connectTimeoutMillis<100||s.requestTimeoutMillis<100||s.maxMessageBytes<1024)throw reject("CLASSIC_CONFIG_INVALID");
        URI uri=new URI(s.url);if(!Arrays.asList("activemq","activemqs").contains(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||uri.getPath()!=null&&!uri.getPath().isEmpty())throw reject("CLASSIC_ENDPOINT_INVALID");
        settings=s;this.management=management;this.binding=binding;queue=binding==null?null:physicalQueue(s.queuePrefix,binding);receipts=queue==null?null:queue+".receipts";
        boolean tls=uri.getScheme().equals("activemqs");String host=uri.getHost();if(host.indexOf(':')>=0&&!host.startsWith("["))host="["+host+"]";
        String endpoint=(tls?"ssl":"tcp")+"://"+host+":"+(uri.getPort()<0?(tls?61617:61616):uri.getPort())+"?connectionTimeout="+s.connectTimeoutMillis+"&soTimeout="+s.requestTimeoutMillis+(tls?"&socket.verifyHostName=true":"");
        ActiveMQConnectionFactory factory=new ActiveMQConnectionFactory(endpoint);factory.setWatchTopicAdvisories(false);factory.setUseAsyncSend(false);factory.setAlwaysSyncSend(true);factory.setSendTimeout(s.requestTimeoutMillis);factory.setConnectResponseTimeout(s.connectTimeoutMillis);factory.setCloseTimeout(s.requestTimeoutMillis);factory.setTrustAllPackages(false);factory.setTrustedPackages(Collections.emptyList());factory.getPrefetchPolicy().setQueueBrowserPrefetch(1);
        ActiveMQConnection opened=null;Session tx=null,read=null;
        try{opened=(ActiveMQConnection)factory.createConnection(s.username,s.password);opened.start();tx=opened.createSession(true,Session.SESSION_TRANSACTED);read=opened.createSession(false,Session.AUTO_ACKNOWLEDGE);connection=opened;writes=tx;browses=read;capabilities();}
        catch(Exception error){if(read!=null)try{read.close();}catch(Exception ignored){}if(tx!=null)try{tx.close();}catch(Exception ignored){}if(opened!=null)try{opened.close();}catch(Exception ignored){}throw classify(error);}
    }
    private void capabilities()throws Exception{
        Broker broker=management.broker();if(!broker.persistent||broker.slave)throw reject("CLASSIC_DURABLE_MASTER_REQUIRED");
        if(connection.getBrokerInfo()==null||!Objects.equals(broker.id,connection.getBrokerInfo().getBrokerId().toString())||!Objects.equals(broker.name,connection.getBrokerInfo().getBrokerName()))throw reject("CLASSIC_ADMIN_WIRE_BROKER_MISMATCH");
    }
    public String queue(){return queue;}public String receiptQueue(){return receipts;}
    public String identity(){return prepared?"CLASSIC_OWNER_V1:"+owner:null;}
    public void resumeProgress(String token)throws Exception{
        if(prepared||receiptId!=null)throw reject("CLASSIC_PROGRESS_ALREADY_ACTIVE");if(token==null)return;
        try{String[] fields=token.split(":",-1);if(fields.length!=4||!fields[0].equals("CLASSIC_RECEIPT_V1")||!fields[1].matches("[a-f0-9]{32}")||!fields[3].matches("[a-f0-9]{64}"))throw new IllegalArgumentException();String id=new String(Base64.getUrlDecoder().decode(fields[2]),StandardCharsets.UTF_8);if(!id.startsWith("ID:")||id.length()>1024||!Base64.getUrlEncoder().withoutPadding().encodeToString(id.getBytes(StandardCharsets.UTF_8)).equals(fields[2]))throw new IllegalArgumentException();owner=fields[1];receiptId=id;receiptHash=fields[3];}catch(Exception invalid){throw reject("CLASSIC_PROGRESS_INVALID");}
    }
    public String confirmedProgress(){return receiptId==null?null:"CLASSIC_RECEIPT_V1:"+owner+":"+Base64.getUrlEncoder().withoutPadding().encodeToString(receiptId.getBytes(StandardCharsets.UTF_8))+":"+receiptHash;}
    private String manifest(){return "RIVOMESH_CLASSIC_V1\n"+binding+"\n"+owner+"\n"+queue+"\n"+receipts;}
    private TextMessage marker(String name)throws Exception{
        QueueBrowser browser=browses.createBrowser(browses.createQueue(name),"rivomesh_record_type = 'OWNER'");try{
            Enumeration<?> messages=browser.getEnumeration();if(!messages.hasMoreElements())return null;Object first=messages.nextElement();if(!(first instanceof TextMessage)||messages.hasMoreElements())throw reject("CLASSIC_PUBLICATION_MANIFEST_CHANGED");TextMessage found=(TextMessage)first;if(found.getJMSDeliveryMode()!=DeliveryMode.PERSISTENT||found.getJMSExpiration()!=0)throw reject("CLASSIC_PUBLICATION_MANIFEST_CHANGED");return found;
        }finally{browser.close();}
    }
    private void ownership()throws Exception{
        for(String name:Arrays.asList(queue,receipts)){if(!management.exists(name))throw reject("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");TextMessage marker=marker(name);if(marker==null||!manifest().equals(marker.getText()))throw reject("CLASSIC_PUBLICATION_MANIFEST_MISSING_OR_CHANGED");}
        management.consumers(queue,receipts);
    }
    private void verifyReceipt()throws Exception{if(receiptId!=null){String actual=management.text(receipts,receiptId);if(actual==null)throw reject("CLASSIC_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED");if(!("RIVOMESH_RECEIPT_V1:"+receiptHash).equals(actual))throw reject("CLASSIC_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");}}
    public void prepare(boolean create)throws Exception{
        if(prepared)return;if(queue==null)throw reject("CLASSIC_BINDING_REQUIRED");
        try{capabilities();for(String name:Arrays.asList(queue,receipts))if(!management.exists(name)){if(!create||receiptId!=null)throw reject("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");management.create(name);}
            TextMessage a=marker(queue),b=marker(receipts);
            if(a==null&&b==null){if(!create||receiptId!=null||management.count(queue)!=0||management.count(receipts)!=0)throw reject("CLASSIC_PUBLICATION_MANIFEST_MISSING_OR_CHANGED");owner=UUID.randomUUID().toString().replace("-","");producer=writes.createProducer(null);for(String name:Arrays.asList(queue,receipts)){TextMessage message=writes.createTextMessage(manifest());message.setStringProperty("rivomesh_record_type","OWNER");producer.send(writes.createQueue(name),message,DeliveryMode.PERSISTENT,4,0);}writes.commit();}
            else{if(a==null||b==null||!a.getText().equals(b.getText()))throw reject("CLASSIC_PUBLICATION_MANIFEST_MISSING_OR_CHANGED");String[] fields=a.getText().split("\n",-1);if(fields.length!=5||!fields[0].equals("RIVOMESH_CLASSIC_V1")||!binding.equals(fields[1])||!fields[2].matches("[a-f0-9]{32}")||!queue.equals(fields[3])||!receipts.equals(fields[4]))throw reject("DESTINATION_ALREADY_OWNED");if(owner!=null&&!owner.equals(fields[2]))throw reject("CLASSIC_RESOURCE_ID_CHANGED");owner=fields[2];}
            if(producer==null)producer=writes.createProducer(null);ownership();verifyReceipt();prepared=true;
        }catch(Exception error){try{writes.rollback();}catch(Exception ignored){}throw classify(error);}
    }
    private static String digest(List<Publication> messages)throws Exception{
        MessageDigest hash=MessageDigest.getInstance("SHA-256");for(Publication message:messages)for(byte[] part:Arrays.asList(message.id.getBytes(StandardCharsets.UTF_8),message.channel.getBytes(StandardCharsets.UTF_8),(message.key==null?"":message.key).getBytes(StandardCharsets.UTF_8),message.body)){hash.update(ByteBuffer.allocate(4).putInt(part.length).array());hash.update(part);}StringBuilder out=new StringBuilder();for(byte b:hash.digest())out.append(String.format(Locale.ROOT,"%02x",b&255));return out.toString();
    }
    public void publish(List<Publication> messages)throws Exception{
        if(!prepared)throw reject("PUBLICATION_NOT_PREPARED");if(messages==null||messages.isEmpty())throw reject("CLASSIC_EMPTY_BATCH");
        for(Publication message:messages)if(message==null||message.id==null||message.id.isEmpty()||message.id.length()>512||message.channel==null||message.channel.length()>512||message.body==null||message.body.length>settings.maxMessageBytes)throw reject("CLASSIC_EVENT_INVALID");
        try{capabilities();ownership();verifyReceipt();String hash=digest(messages);
            for(Publication publication:messages){BytesMessage message=writes.createBytesMessage();message.writeBytes(publication.body);for(Map.Entry<String,String> header:publication.headers.entrySet())message.setStringProperty(header.getKey(),header.getValue());message.setStringProperty("rivomesh_record_type","EVENT");message.setStringProperty("rivomesh_event_id",publication.id);message.setStringProperty("rivomesh_channel",publication.channel);if(publication.key!=null)message.setStringProperty("rivomesh_key",publication.key);producer.send(writes.createQueue(queue),message,DeliveryMode.PERSISTENT,4,0);}
            TextMessage confirmation=writes.createTextMessage("RIVOMESH_RECEIPT_V1:"+hash);confirmation.setStringProperty("rivomesh_record_type","RECEIPT");producer.send(writes.createQueue(receipts),confirmation,DeliveryMode.PERSISTENT,4,0);writes.commit();String id=confirmation.getJMSMessageID();if(id==null||!id.startsWith("ID:")||!("RIVOMESH_RECEIPT_V1:"+hash).equals(management.text(receipts,id)))throw reject("CLASSIC_RECEIPT_NOT_OBSERVED");ownership();receiptId=id;receiptHash=hash;
        }catch(Exception error){try{writes.rollback();}catch(Exception ignored){}throw classify(error);}
    }
    static Exception classify(Exception error){
        if(error instanceof Failure)return error;Deque<Throwable> pending=new ArrayDeque<>();Set<Throwable> seen=Collections.newSetFromMap(new IdentityHashMap<Throwable,Boolean>());pending.add(error);
        while(!pending.isEmpty()&&seen.size()<16){Throwable t=pending.removeFirst();if(!seen.add(t))continue;
            if(t instanceof javax.net.ssl.SSLException||t instanceof JMSSecurityException||t instanceof SecurityException)return new Failure(t instanceof javax.net.ssl.SSLException?"CLASSIC_TLS_REJECTED":"CLASSIC_AUTHORIZATION_REJECTED",false,error);
            if(t instanceof InvalidDestinationException||t instanceof InvalidSelectorException||t instanceof MessageFormatException||t instanceof MessageNotWriteableException)return new Failure("CLASSIC_EVENT_OR_DESTINATION_REJECTED",false,error);
            if(t.getCause()!=null)pending.add(t.getCause());if(t instanceof JMSException&&((JMSException)t).getLinkedException()!=null)pending.add(((JMSException)t).getLinkedException());
        }
        if(error instanceof JMSException)return new Failure(error instanceof ResourceAllocationException?"CLASSIC_CAPACITY_RETRY":"CLASSIC_CONNECTION_RETRY",true,error);return error;
    }
    @Override public void close(){if(producer!=null)try{producer.close();}catch(Exception ignored){}try{browses.close();}catch(Exception ignored){}try{writes.close();}catch(Exception ignored){}try{connection.close();}catch(Exception ignored){}}
}
