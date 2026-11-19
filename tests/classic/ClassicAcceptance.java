package io.github.rivomesh.acceptance;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.*;
import io.github.rivomesh.transport.classic.ClassicTransport;
import io.github.rivomesh.classic.internal.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** External fixture compiled only against the actual shaded release, including its private JMS API. */
public final class ClassicAcceptance {
    static void require(boolean value,String message){if(!value)throw new AssertionError(message);}
    interface Attempt {void run()throws Exception;}
    static void rejected(String code,Attempt attempt)throws Exception{try{attempt.run();throw new AssertionError("Expected "+code);}catch(ClassicTransport.Failure f){require(code.equals(f.code),"Wrong error: "+f.code);}}
    static ClassicTransport.Settings settings(){ClassicTransport.Settings s=new ClassicTransport.Settings();s.url="activemq://classic:61616";s.username="admin";s.password="Classic@Test123";s.queuePrefix="rivomesh";s.requestTimeoutMillis=10000;return s;}
    static ClassicAdminClient admin()throws Exception{return new ClassicAdminClient("http://classic:8161/api/jolokia/","admin","Classic@Test123","rivomesh-core",5000,10000);}
    static ClassicTransport open(String binding)throws Exception{return new ClassicTransport(settings(),admin(),binding);}
    static ClassicTransport.Publication event(String id){ClassicTransport.Publication p=new ClassicTransport.Publication();p.id=id;p.channel="sales.items";p.key="row-1";p.body=("{\"event\":\""+id+"\"}").getBytes(StandardCharsets.UTF_8);return p;}
    static Connection reader()throws Exception{ActiveMQConnectionFactory f=new ActiveMQConnectionFactory("tcp://classic:61616");f.setWatchTopicAdvisories(false);Connection c=f.createConnection("consumer","Consumer@Test123");c.start();return c;}
    static void phase(String phase)throws Exception{Path directory=Paths.get(System.getenv("RIVOMESH_CLASSIC_RESTART_DIR"));Files.write(directory.resolve(phase+".request"),new byte[]{1});long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(180);while(!Files.exists(directory.resolve(phase+".done"))&&System.nanoTime()<end)Thread.sleep(100);require(Files.exists(directory.resolve(phase+".done")),"Supervisor did not "+phase);}
    static void operation(String queue,String name)throws Exception{
        Map<String,Object> request=new LinkedHashMap<>();request.put("type","exec");request.put("mbean",queue==null?"org.apache.activemq:type=Broker,brokerName=rivomesh-core":"org.apache.activemq:type=Broker,brokerName=rivomesh-core,destinationType=Queue,destinationName="+queue);request.put("operation",name);request.put("arguments",Collections.emptyList());
        HttpURLConnection c=(HttpURLConnection)new URL("http://classic:8161/api/jolokia/").openConnection();c.setConnectTimeout(5000);c.setReadTimeout(10000);c.setRequestMethod("POST");c.setDoOutput(true);c.setRequestProperty("Content-Type","application/json");c.setRequestProperty("Referer","http://classic:8161/");c.setRequestProperty("Authorization","Basic "+Base64.getEncoder().encodeToString("admin:Classic@Test123".getBytes(StandardCharsets.UTF_8)));byte[] bytes=Codec.encode(request).getBytes(StandardCharsets.UTF_8);c.setFixedLengthStreamingMode(bytes.length);try{try(OutputStream out=c.getOutputStream()){out.write(bytes);}require(c.getResponseCode()==200,"Management fixture operation failed");JsonNode response=Codec.JSON.readTree(c.getInputStream());require(response.path("status").asInt()==200,"Management fixture rejected operation");}finally{c.disconnect();}
    }
    public static void main(String[] args)throws Exception{
        String binding="rivomesh:"+Codec.sha256(UUID.randomUUID().toString()),queue=ClassicTransport.physicalQueue("rivomesh",binding);ClassicAdminClient management=admin();
        require(!management.exists(queue),"Fixture queue already exists");PipelineConfig.Target target=new PipelineConfig.Target();target.product=TargetProduct.ACTIVEMQ_CLASSIC;target.url=settings().url;target.username=settings().username;target.password=settings().password;target.messaging=new PipelineConfig.Messaging();target.messaging.stream="rivomesh";target.messaging.broker="rivomesh-core";target.messaging.adminUrl="http://classic:8161/api/jolokia/";ClassicMessagePublisher.check(target);require(!management.exists(queue),"Read-only check created a business queue");
        String token,identity;
        try(ClassicTransport transport=open(binding)){
            transport.prepare(true);identity=transport.identity();transport.publish(Collections.singletonList(event("first")));
            Connection c=reader();Session session=c.createSession(false,Session.AUTO_ACKNOWLEDGE);MessageConsumer consumer=session.createConsumer(session.createQueue(queue),ClassicTransport.EVENT_SELECTOR);try{Message message=consumer.receive(5000);require(message instanceof BytesMessage,"Consumer did not receive an event");byte[] bytes=new byte[(int)((BytesMessage)message).getBodyLength()];((BytesMessage)message).readBytes(bytes);require(Arrays.equals(bytes,event("first").body),"Consumer event content changed");require("first".equals(message.getStringProperty("rivomesh_event_id")),"Stable event ID missing");}finally{consumer.close();session.close();c.close();}
            require(management.count(queue)==1,"EVENT selector consumed the owner record");transport.publish(Collections.singletonList(event("second")));token=transport.confirmedProgress();require(token!=null,"No confirmed native receipt");
        }
        String other="rivomesh:"+Codec.sha256("sandbox-"+binding),otherQueue=ClassicTransport.physicalQueue("rivomesh",other);String otherToken;
        try(ClassicTransport sandbox=open(other)){
            sandbox.prepare(true);Connection c=reader();Session session=c.createSession(false,Session.AUTO_ACKNOWLEDGE);MessageConsumer wrong=session.createConsumer(session.createQueue(otherQueue),"JMSPriority = 9");try{rejected("CLASSIC_CONSUMER_SELECTOR_REQUIRED",()->sandbox.publish(Collections.singletonList(event("forbidden"))));require(management.count(otherQueue)==1,"Rejected batch wrote event data");}finally{wrong.close();session.close();c.close();}sandbox.publish(Collections.singletonList(event("sandbox")));otherToken=sandbox.confirmedProgress();
        }
        operation(otherQueue,"purge()");try(ClassicTransport sandbox=open(other)){sandbox.resumeProgress(otherToken);rejected("CLASSIC_PUBLICATION_MANIFEST_MISSING_OR_CHANGED",()->sandbox.prepare(true));require(management.count(otherQueue)==0,"Lost owner was silently recreated");}
        final String corrupt=token.substring(0,token.length()-64)+Codec.sha256("tamper");try(ClassicTransport changed=open(binding)){changed.resumeProgress(corrupt);rejected("CLASSIC_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED",()->changed.prepare(true));}
        phase("restart");String latest;
        try(ClassicTransport resumed=open(binding)){resumed.resumeProgress(token);resumed.prepare(true);require(identity.equals(resumed.identity()),"Owner identity changed across restart");resumed.publish(Collections.singletonList(event("third")));latest=resumed.confirmedProgress();}
        phase("restore");try(ClassicTransport rolledBack=open(binding)){rolledBack.resumeProgress(latest);rejected("CLASSIC_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED",()->rolledBack.prepare(true));}
        System.out.println("CLASSIC_TRANSPORT_ACCEPTED: actual-release Java 21 OpenWire, persistent transactions, EVENT consumer ACK retains owner, invalid selector blocks, purge refusal, content/native receipt checks, broker/client restart and actual older nonempty backup rollback refusal");
    }
}
