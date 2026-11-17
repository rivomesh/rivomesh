package io.github.rivomesh.transport.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Real release-JAR and native broker acceptance; excluded from production artifacts. */
public final class RocketTransportAcceptance {
    interface Checked{void run()throws Exception;}
    static void require(boolean value,String message){if(!value)throw new AssertionError(message);}
    static void retry(Checked operation)throws Exception{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);Exception last=null;
        while(System.nanoTime()<deadline){try{operation.run();return;}catch(RocketTransport.Failure failure){if(!failure.retryable)throw failure;last=failure;}Thread.sleep(250);}throw new AssertionError("RocketMQ did not recover",last);
    }
    static RocketTransport.Publication publication(String id){RocketTransport.Publication p=new RocketTransport.Publication();p.id=id;p.channel="sales.items";p.key="key-"+id;p.body=("{\"id\":\""+id+"\"}").getBytes(StandardCharsets.UTF_8);return p;}
    static List<String> read(RocketTransport transport)throws Exception{
        List<String> ids=new ArrayList<>();for(MessageExt message:transport.read(0,100)){
            if(message.getUserProperty("rivomesh_publication_manifest")!=null)continue;
            String id=message.getUserProperty("rivomesh_event_id");require("sales.items".equals(message.getUserProperty("rivomesh_channel")),"Channel changed");require(("key-"+id).equals(message.getUserProperty("rivomesh_key")),"Key changed");require(new String(message.getBody(),StandardCharsets.UTF_8).equals("{\"id\":\""+id+"\"}"),"Body changed");ids.add(id);
        }return ids;
    }
    static void awaitHistory(RocketTransport transport,String... ids)throws Exception{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(System.nanoTime()<deadline){if(read(transport).containsAll(Arrays.asList(ids)))return;Thread.sleep(100);}throw new AssertionError("Confirmed history missing: "+Arrays.toString(ids));}
    public static void main(String[] args)throws Exception{
        Class.forName("io.github.rivomesh.rocketmq.internal.netty.channel.Channel");Class.forName("io.github.rivomesh.rocketmq.internal.fastjson2.JSON");Class.forName("org.slf4j.Logger");
        RocketTransport.Settings settings=new RocketTransport.Settings();settings.nameservers="rocketnamesrv:9876";settings.cluster="RivomeshTest";settings.broker="rivomesh-broker";settings.topicPrefix="events";settings.requestTimeoutMillis=5000;
        String binding="rivomesh:"+String.format("%064x",new java.math.BigInteger(UUID.randomUUID().toString().replace("-",""),16));String topic=RocketTransport.physicalTopic(settings.topicPrefix,binding);
        Path markers=Paths.get(System.getenv("RIVOMESH_ROCKETMQ_RESTART_DIR"));String savedProgress;
        DefaultMQProducer inspector=new DefaultMQProducer("inspect_"+UUID.randomUUID().toString().replace("-",""));inspector.setNamesrvAddr(settings.nameservers);inspector.setVipChannelEnabled(false);inspector.start();MQClientAPIImpl api=inspector.getDefaultMQProducerImpl().getMqClientFactory().getMQClientAPIImpl();
        try{
            RocketTransport.check(settings);boolean missing=false;try{api.getTopicConfig("rocketbroker:10911",topic,5000);}catch(org.apache.rocketmq.client.exception.MQBrokerException absent){missing=true;}require(missing,"Check created a topic");
            // Simulate interruption after explicit topic creation but before its manifest confirmation.
            api.createTopic("rocketbroker:10911","TBW102",new org.apache.rocketmq.common.TopicConfig(topic,1,1,6),5000);
            try(RocketTransport transport=new RocketTransport(settings,binding)){
                retry(()->transport.prepare(true));transport.publish(Collections.singletonList(publication("first")));awaitHistory(transport,"first");
                Files.write(markers.resolve("restart.request"),new byte[]{1});long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(180);while(!Files.exists(markers.resolve("restart.done"))&&System.nanoTime()<deadline)Thread.sleep(100);require(Files.exists(markers.resolve("restart.done")),"Broker was not restarted");
                retry(()->transport.publish(Collections.singletonList(publication("second"))));awaitHistory(transport,"first","second");savedProgress=transport.confirmedProgress();require(savedProgress!=null,"Native position missing");
            }
            try(RocketTransport resumed=new RocketTransport(settings,binding)){resumed.resumeProgress(savedProgress);retry(()->resumed.prepare(false));require(read(resumed).containsAll(Arrays.asList("first","second")),"Worker restart lost ownership or history");}
            String changedProgress=savedProgress.substring(0,savedProgress.length()-64)+String.join("",Collections.nCopies(64,"0"));
            try(RocketTransport changed=new RocketTransport(settings,binding)){changed.resumeProgress(changedProgress);try{changed.prepare(false);throw new AssertionError("Changed confirmed body accepted");}catch(RocketTransport.Failure failure){require("ROCKETMQ_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED".equals(failure.code),"Wrong content error: "+failure.code);}}
            String foreignBinding="rivomesh:"+String.format("%064x",new java.math.BigInteger(UUID.randomUUID().toString().replace("-",""),16));String foreignTopic=RocketTransport.physicalTopic(settings.topicPrefix,foreignBinding);api.createTopic("rocketbroker:10911","TBW102",new org.apache.rocketmq.common.TopicConfig(foreignTopic,1,1,6),5000);MessageQueue foreignQueue=new MessageQueue(foreignTopic,settings.broker,0);inspector.send(new Message(foreignTopic,"foreign-payload".getBytes(StandardCharsets.UTF_8)),foreignQueue,5000);
            long visible=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(api.getMaxOffset("rocketbroker:10911",foreignQueue,5000)==0&&System.nanoTime()<visible)Thread.sleep(100);require(api.getMaxOffset("rocketbroker:10911",foreignQueue,5000)>0,"Foreign fixture was not dispatched");
            try(RocketTransport foreign=new RocketTransport(settings,foreignBinding)){try{foreign.prepare(true);throw new AssertionError("Foreign data adopted");}catch(RocketTransport.Failure failure){require("DESTINATION_ALREADY_OWNED".equals(failure.code),"Wrong ownership error: "+failure.code);}}
            api.createTopic("rocketbroker:10911","TBW102",new org.apache.rocketmq.common.TopicConfig(topic,2,2,6),5000);
            try(RocketTransport changed=new RocketTransport(settings,binding)){try{changed.prepare(false);throw new AssertionError("Queue growth accepted");}catch(RocketTransport.Failure failure){require("ROCKETMQ_TOPIC_CONFIGURATION_REQUIRES_PLAN".equals(failure.code),"Wrong queue-change error: "+failure.code);}}
            Properties unsafe=new Properties();unsafe.setProperty("flushDiskType","ASYNC_FLUSH");api.updateBrokerConfig("rocketbroker:10911",unsafe,5000);
            try(RocketTransport refused=new RocketTransport(settings,binding)){try{refused.prepare(false);throw new AssertionError("Unsafe broker accepted");}catch(RocketTransport.Failure failure){require("ROCKETMQ_SYNCHRONOUS_DISK_REQUIRED".equals(failure.code),"Wrong durability error: "+failure.code);}}
            Properties restore=new Properties();restore.setProperty("flushDiskType","SYNC_FLUSH");api.updateBrokerConfig("rocketbroker:10911",restore,5000);api.deleteTopicInBroker("rocketbroker:10911",topic,5000);
            try(RocketTransport lost=new RocketTransport(settings,binding)){try{lost.prepare(false);throw new AssertionError("Missing topic recreated");}catch(RocketTransport.Failure failure){require("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED".equals(failure.code),"Wrong missing-topic error: "+failure.code);}}
            Files.write(markers.resolve("restore.request"),new byte[]{1});long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(180);while(!Files.exists(markers.resolve("restore.done"))&&System.nanoTime()<deadline)Thread.sleep(100);require(Files.exists(markers.resolve("restore.done")),"Older volume backup was not restored");
            try(RocketTransport older=new RocketTransport(settings,binding)){retry(()->older.prepare(false));List<String> history=read(older);require(history.contains("first")&&!history.contains("second"),"Fixture did not restore an older nonempty topic");}
            try(RocketTransport protectedClient=new RocketTransport(settings,binding)){protectedClient.resumeProgress(savedProgress);try{protectedClient.prepare(true);throw new AssertionError("Confirmed tail rollback accepted");}catch(RocketTransport.Failure failure){require(Arrays.asList("ROCKETMQ_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED","ROCKETMQ_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED").contains(failure.code),"Wrong rollback error: "+failure.code);}}
            System.out.println("ROCKETMQ_TRANSPORT_ACCEPTED: private dependencies, read-only check, interrupted provisioning, durable confirmation, broker/worker restart, ownership/queue/flush refusal, lost-topic protection, native position/content verification, actual older nonempty volume restore refusal");
        }finally{inspector.shutdown();}
    }
}
