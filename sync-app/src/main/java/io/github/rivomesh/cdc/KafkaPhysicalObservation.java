package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

/** Resolves actual broker offsets back to the same physical topic and exact COMMIT bytes. */
final class KafkaPhysicalObservation {
    private static final Duration WINDOW=Duration.ofSeconds(3),CLOSE=Duration.ofMillis(200);
    private record Expected(String key,String eventId,KafkaPhysicalReceipt.Wire wire){}
    private final KafkaPhysicalReceipt.Topic before;
    private final Set<String> namespaces;
    private final Map<String,Expected> commits=new TreeMap<>();

    private KafkaPhysicalObservation(KafkaPhysicalReceipt.Topic before,Set<String> namespaces){
        this.before=before;this.namespaces=Set.copyOf(namespaces);
    }

    static KafkaPhysicalObservation begin(Admin admin,PipelineConfig.Target target,Set<String> namespaces)throws InterruptedException{
        if(namespaces.isEmpty()||namespaces.size()>KafkaPhysicalReceipt.MAX_WIRE)return null;
        if(target.messaging.physicalTopic!=null)return new KafkaPhysicalObservation(target.messaging.physicalTopic,namespaces);
        try{return new KafkaPhysicalObservation(identity(admin,target,deadline()),namespaces);}
        catch(InterruptException interrupted){throw interrupted();}
        catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw interrupted;}
        catch(Exception unavailable){return null;}
    }

    void confirmed(String topic,ConfirmedPublisher.Message message,String actualTopic,int partition,long offset){
        if(!"COMMIT".equals(message.headers.get("rivomesh_kind")))return;
        final String suffix=".__transactions";
        if(!message.destination.endsWith(suffix)||!topic.equals(actualTopic)||partition!=0
                ||offset<0||message.key==null||message.id==null)throw unavailable();
        String namespace=message.destination.substring(0,message.destination.length()-suffix.length());
        if(!namespaces.contains(namespace)||commits.putIfAbsent(namespace,new Expected(message.key,message.id,
                new KafkaPhysicalReceipt.Wire(namespace,offset,Codec.sha256(message.body))))!=null)throw unavailable();
    }

    KafkaPhysicalReceipt.Observation finish(Admin admin,PipelineConfig.Target target)throws InterruptedException{
        if(!namespaces.equals(commits.keySet()))return null;
        // The UUID-bound producer already received all broker acknowledgements. The independent
        // consumer reads these exact offsets at review time rather than adding a scan to each commit.
        if(target.messaging.physicalTopic!=null)
            return new KafkaPhysicalReceipt.Observation(before,commits.values().stream().map(Expected::wire).toList());
        try{
            long deadline=deadline();
            if(!before.equals(identity(admin,target,deadline)))return null;
            var settings=KafkaMessagePublisher.connectionProperties(target);
            settings.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            settings.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
            settings.setProperty("enable.auto.commit","false");
            settings.setProperty("allow.auto.create.topics","false");
            settings.setProperty("auto.offset.reset","none");
            settings.setProperty("isolation.level","read_committed");
            settings.setProperty("max.poll.records","4");
            String fetchLimit=Integer.toString(target.messaging.maxMessageBytes+4096);
            settings.setProperty("fetch.max.bytes",fetchLimit);settings.setProperty("max.partition.fetch.bytes",fetchLimit);
            var partition=new TopicPartition(target.messaging.topic,0);
            var consumer=new KafkaConsumer<String,byte[]>(settings);
            try{
                consumer.assign(List.of(partition));
                for(Expected proof:commits.values())verify(consumer,partition,proof,deadline);
                if(!before.equals(identity(admin,target,deadline)))return null;
                return new KafkaPhysicalReceipt.Observation(before,commits.values().stream().map(Expected::wire).toList());
            }finally{consumer.close(CLOSE);}
        }catch(InterruptException interrupted){throw interrupted();}
        catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw interrupted;}
        catch(Exception unavailable){return null;}
    }

    private static void verify(KafkaConsumer<String,byte[]> consumer,TopicPartition partition,Expected expected,long deadline){
        long offset=expected.wire().offset();
        consumer.seek(partition,offset);
        for(;;){
            Duration wait=remaining(deadline);
            for(var actual:consumer.poll(wait.compareTo(Duration.ofMillis(100))>0?Duration.ofMillis(100):wait).records(partition)){
                if(actual.offset()!=offset||!expected.key().equals(actual.key())||actual.value()==null
                        ||!expected.wire().payloadSha256().equals(Codec.sha256(actual.value()))
                        ||!expected.eventId().equals(header(actual.headers().lastHeader("rivomesh_event_id")))
                        ||!(expected.wire().namespace()+".__transactions").equals(header(actual.headers().lastHeader("rivomesh_channel")))
                        ||!"COMMIT".equals(header(actual.headers().lastHeader("rivomesh_kind"))))throw unavailable();
                return;
            }
        }
    }

    private static KafkaPhysicalReceipt.Topic identity(Admin admin,PipelineConfig.Target target,long deadline)throws Exception{
        String name=target.messaging.topic;
        var cluster=admin.describeCluster().clusterId();
        var described=admin.describeTopics(List.of(name)).allTopicNames();
        String id=await(cluster,deadline);
        var topic=await(described,deadline).get(name);
        if(id==null||id.isBlank()||topic==null||topic.isInternal()||topic.topicId()==null||topic.partitions().size()!=1
                ||topic.partitions().getFirst().partition()!=0)throw unavailable();
        return new KafkaPhysicalReceipt.Topic(Codec.sha256("KAFKA:"+id),topic.topicId().toString());
    }

    private static String header(org.apache.kafka.common.header.Header header){
        return header==null||header.value()==null?null:new String(header.value(),StandardCharsets.UTF_8);
    }
    private static <T>T await(KafkaFuture<T> future,long deadline)throws Exception{
        return future.get(remaining(deadline).toNanos(),TimeUnit.NANOSECONDS);
    }
    private static long deadline(){return System.nanoTime()+WINDOW.toNanos();}
    private static Duration remaining(long deadline){
        long remaining=deadline-System.nanoTime();
        if(remaining<=0)throw unavailable();
        return Duration.ofNanos(remaining);
    }
    private static IllegalStateException unavailable(){return new IllegalStateException("KAFKA_PHYSICAL_OBSERVATION_UNAVAILABLE");}
    private static InterruptedException interrupted(){Thread.currentThread().interrupt();return new InterruptedException("SUBSCRIBER_STOPPED");}
}
