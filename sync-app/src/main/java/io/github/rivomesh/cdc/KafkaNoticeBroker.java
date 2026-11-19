package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Attempt;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Plan;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Receipt;
import io.github.rivomesh.kafka.notice.TopicIdTransactions;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

/** Prototype transport for a pre-owned Kafka control topic with an exclusive, separate writer entitlement. */
final class KafkaNoticeBroker implements KafkaRecoveryNotice.Broker {
    private static final Duration CLOSE=Duration.ofMillis(300),WINDOW=Duration.ofSeconds(6);
    private static final int MAXIMUM_OFFSETS=512,MAXIMUM_MESSAGE_BYTES=64*1024;
    private final PipelineConfig.Target target;
    private final NoticeWriter writer;

    KafkaNoticeBroker(PipelineConfig.Target target){
        this(target,null);
    }

    static KafkaNoticeBroker physicallyAddressed(PipelineConfig.Target target,List<String> endpoints,String username,String password){
        return new KafkaNoticeBroker(target,new NoticeWriter(endpoints,username,password));
    }

    private KafkaNoticeBroker(PipelineConfig.Target target,NoticeWriter writer){
        if(target==null||target.product!=TargetProduct.KAFKA||target.messaging==null)
            throw rejected("KAFKA_NOTICE_TARGET_UNSUPPORTED");
        this.target=target;
        this.writer=writer;
    }

    private record NoticeWriter(List<String> endpoints,String username,String password) {
        NoticeWriter { endpoints=List.copyOf(endpoints); }
        @Override public String toString(){return "Kafka notice writer";}
    }

    String inspectControlTopic(Plan plan)throws Exception{
        requireTarget(plan);
        Admin admin=Admin.create(KafkaMessagePublisher.connectionProperties(target));
        try{
            long deadline=until();
            String identity=requireTopic(admin,plan,true,deadline);
            if(!identity.equals(requireTopic(admin,plan,false,deadline)))throw rejected("KAFKA_NOTICE_CONTROL_TOPIC_CHANGED");
            return identity;
        }
        finally{admin.close(CLOSE);}
    }

    @Override public Receipt read(Plan plan,Long offset)throws Exception {
        requireTarget(plan);
        long until=until();
        Admin admin=Admin.create(KafkaMessagePublisher.connectionProperties(target));
        try{
            requireTopic(admin,plan,true,until);
            KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<>(consumerProperties("read_committed"));
            try{
                TopicPartition partition=new TopicPartition(plan.topic().name(),0);
                Set<TopicPartition> partitions=Set.of(partition);
                consumer.assign(partitions);
                long begin=consumer.beginningOffsets(partitions,remaining(until)).get(partition);
                long end=consumer.endOffsets(partitions,remaining(until)).get(partition);
                if(offset!=null&&(offset<begin||offset>=end)){
                    requireTopic(admin,plan,false,until);
                    return null;
                }
                if(offset==null&&(begin!=0||end>MAXIMUM_OFFSETS||end!=highWatermark(partitions,partition,until)))
                    throw rejected("KAFKA_NOTICE_HISTORY_UNAVAILABLE");
                long from=offset==null?begin:offset,to=offset==null?end:Math.addExact(offset,1);
                Receipt receipt=readRecords(consumer,partition,plan,from,to,offset!=null,until);
                requireTopic(admin,plan,false,until);
                long currentBegin=consumer.beginningOffsets(partitions,remaining(until)).get(partition);
                if(offset==null&&(currentBegin!=begin||consumer.endOffsets(partitions,remaining(until)).get(partition)!=end
                        ||end!=highWatermark(partitions,partition,until))||offset!=null&&currentBegin>offset)
                    throw rejected("KAFKA_NOTICE_HISTORY_CHANGED");
                return receipt;
            }finally{consumer.close(CLOSE);}
        }finally{admin.close(CLOSE);}
    }

    @Override public boolean confirmedAborted(Plan plan,long offset)throws Exception{
        if(writer==null)return false;
        requireTarget(plan);
        long deadline=until();
        var writerSettings=KafkaMessagePublisher.connectionProperties(target);
        writerSettings.setProperty("bootstrap.servers",String.join(",",writer.endpoints()));
        writerSettings.setProperty("ssl.endpoint.identification.algorithm","https");
        KafkaMessagePublisher.configureSasl(writerSettings,target.messaging.saslMechanism,writer.username(),writer.password());
        var reviewer=Admin.create(KafkaMessagePublisher.connectionProperties(target));
        Admin producerAdmin=null;
        try{
            producerAdmin=Admin.create(writerSettings);
            requireTopic(reviewer,plan,true,deadline);
            if(!plan.topic().clusterSha256().equals(Codec.sha256("KAFKA:"+await(producerAdmin.describeCluster().clusterId(),deadline))))
                throw rejected("KAFKA_NOTICE_CONTROL_CLUSTER_CHANGED");
            String id=plan.transactionalId();
            var description=await(producerAdmin.describeTransactions(List.of(id)).description(id),deadline);
            if(description.state()!=TransactionState.COMPLETE_ABORT)return false;
            var partition=new TopicPartition(plan.topic().name(),0);
            var partitions=Set.of(partition);
            KafkaConsumer<String,byte[]> committed=new KafkaConsumer<>(consumerProperties("read_committed"));
            KafkaConsumer<String,byte[]> uncommitted=null;
            try{
                uncommitted=new KafkaConsumer<>(consumerProperties("read_uncommitted"));
                committed.assign(partitions);
                uncommitted.assign(partitions);
                long begin=committed.beginningOffsets(partitions,remaining(deadline)).get(partition);
                long stable=committed.endOffsets(partitions,remaining(deadline)).get(partition);
                if(offset<begin||offset>=stable)return false;
                long past=Math.addExact(offset,1);
                if(readRecords(committed,partition,plan,offset,past,true,deadline)!=null
                        ||readRecords(uncommitted,partition,plan,offset,past,true,deadline)==null)return false;
                requireTopic(reviewer,plan,false,deadline);
                var after=await(producerAdmin.describeTransactions(List.of(id)).description(id),deadline);
                return description.equals(after);
            }finally{
                try{if(uncommitted!=null)uncommitted.close(CLOSE);}
                finally{committed.close(CLOSE);}
            }
        }finally{
            try{if(producerAdmin!=null)producerAdmin.close(CLOSE);}
            finally{reviewer.close(CLOSE);}
        }
    }

    @Override public Attempt begin(Plan plan)throws Exception{
        requireTarget(plan);
        Admin admin=Admin.create(KafkaMessagePublisher.connectionProperties(target));
        KafkaProducer<String,byte[]> producer=null;
        try{
            requireTopic(admin,plan,true,until());
            if(writer!=null){
                if(!"SASL_SSL".equals(target.messaging.securityProtocol)
                        ||!Set.of("SCRAM-SHA-256","SCRAM-SHA-512").contains(target.messaging.saslMechanism))
                    throw rejected("KAFKA_NOTICE_WRITER_SCRAM_TLS_REQUIRED");
                var topic=plan.topic();
                if(!KafkaNoticeWriterReview.validConfiguration(topic.clusterSha256(),topic.topicId(),writer.endpoints(),writer.username(),writer.password())
                        ||writer.username().equals(target.username)||writer.password().equals(target.password))
                    throw rejected("KAFKA_NOTICE_WRITER_SCOPE_UNVERIFIED");
                var transaction=new TopicIdTransactions(writer.endpoints(),target.messaging.saslMechanism,writer.username(),
                        writer.password(),topic.name(),topic.topicId(),topic.clusterSha256(),plan.transactionalId());
                return new TopicIdAttempt(plan,admin,transaction);
            }
            Properties settings=KafkaMessagePublisher.properties(target);
            settings.setProperty("transactional.id",plan.transactionalId());
            settings.setProperty("max.block.ms","5000");
            settings.setProperty("transaction.timeout.ms","30000");
            producer=new KafkaProducer<>(settings);
            producer.initTransactions();
            producer.beginTransaction();
            return new NativeAttempt(plan,admin,producer);
        }catch(Exception failure){
            try{if(producer!=null)producer.close(CLOSE);}
            finally{admin.close(CLOSE);}
            throw failure;
        }
    }

    private final class TopicIdAttempt implements Attempt {
        private final Plan plan;
        private final Admin admin;
        private final TopicIdTransactions transaction;

        TopicIdAttempt(Plan plan,Admin admin,TopicIdTransactions transaction){
            this.plan=plan;this.admin=admin;this.transaction=transaction;
        }

        @Override public long send(byte[] payload)throws Exception{
            if(!Arrays.equals(plan.payload(),payload))throw rejected("KAFKA_NOTICE_PAYLOAD_CHANGED");
            return transaction.send(plan.id(),payload,KafkaRecoveryNotice.KIND);
        }

        @Override public void commit()throws Exception{
            requireTopic(admin,plan,false,System.nanoTime()+Duration.ofSeconds(2).toNanos());
            transaction.commit();
        }

        @Override public void close(){
            try{transaction.close();}
            finally{admin.close(CLOSE);}
        }
    }

    private final class NativeAttempt implements Attempt {
        private final Plan plan;
        private final Admin admin;
        private final KafkaProducer<String,byte[]> producer;
        private boolean sent;

        private NativeAttempt(Plan plan,Admin admin,KafkaProducer<String,byte[]> producer){
            this.plan=plan;this.admin=admin;this.producer=producer;
        }

        @Override public long send(byte[] payload)throws Exception {
            if(sent||!Arrays.equals(plan.payload(),payload))throw rejected("KAFKA_NOTICE_PAYLOAD_CHANGED");
            ProducerRecord<String,byte[]> record=new ProducerRecord<>(plan.topic().name(),0,plan.id(),payload);
            record.headers().add("rivomesh_notice_kind",KafkaRecoveryNotice.KIND.getBytes(StandardCharsets.US_ASCII));
            RecordMetadata metadata=producer.send(record).get(5,TimeUnit.SECONDS);
            if(!metadata.hasOffset()||metadata.partition()!=0||!plan.topic().name().equals(metadata.topic()))
                throw rejected("KAFKA_NOTICE_NATIVE_OFFSET_INVALID");
            sent=true;
            return metadata.offset();
        }

        @Override public void commit()throws Exception {
            if(!sent)throw rejected("KAFKA_NOTICE_NATIVE_OFFSET_REQUIRED");
            requireTopic(admin,plan,false,until());
            producer.commitTransaction();
        }

        @Override public void close(){
            try{producer.close(CLOSE);}
            finally{admin.close(CLOSE);}
        }
    }

    private Receipt readRecords(KafkaConsumer<String,byte[]> consumer,TopicPartition partition,Plan plan,long from,long to,
                                boolean exactOffset,long until)throws Exception{
        consumer.seek(partition,from);
        Receipt receipt=null;
        while(consumer.position(partition,remaining(until))<to){
            Duration budget=remaining(until);
            Duration poll=budget.compareTo(Duration.ofMillis(100))>0?Duration.ofMillis(100):budget;
            for(ConsumerRecord<String,byte[]> record:consumer.poll(poll).records(partition)){
                if(record.offset()>=to)break;
                if(!plan.id().equals(record.key())){
                    if(exactOffset)throw rejected("KAFKA_NOTICE_NATIVE_RECEIPT_CONFLICT");
                    continue;
                }
                var header=record.headers().lastHeader("rivomesh_notice_kind");
                if(receipt!=null||header==null||!Arrays.equals(header.value(),KafkaRecoveryNotice.KIND.getBytes(StandardCharsets.US_ASCII))
                        ||!Arrays.equals(plan.payload(),record.value()))throw rejected("KAFKA_NOTICE_NATIVE_RECEIPT_CONFLICT");
                receipt=new Receipt(plan.id(),plan.topic(),record.offset(),Codec.sha256(record.value()));
            }
        }
        return receipt;
    }

    private long highWatermark(Set<TopicPartition> partitions,TopicPartition partition,long until)throws Exception {
        KafkaConsumer<String,byte[]> observer=new KafkaConsumer<>(consumerProperties("read_uncommitted"));
        try{return observer.endOffsets(partitions,remaining(until)).get(partition);}
        finally{observer.close(CLOSE);}
    }

    private Properties consumerProperties(String isolation)throws Exception{
        Properties properties=KafkaMessagePublisher.connectionProperties(target);
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty("enable.auto.commit","false");
        properties.setProperty("allow.auto.create.topics","false");
        properties.setProperty("auto.offset.reset","none");
        properties.setProperty("isolation.level",isolation);
        properties.setProperty("max.poll.records","32");
        properties.setProperty("max.partition.fetch.bytes",Integer.toString(MAXIMUM_MESSAGE_BYTES));
        properties.setProperty("fetch.max.bytes",Integer.toString(MAXIMUM_MESSAGE_BYTES));
        return properties;
    }

    private void requireTarget(Plan plan){
        if(!target.id.equals(plan.failure().target())||plan.topic().name().equals(target.messaging.topic))
            throw rejected("KAFKA_NOTICE_CONTROL_TOPIC_REQUIRED");
    }

    private String requireTopic(Admin admin,Plan plan,boolean profile,long until)throws Exception {
        String name=plan.topic().name(),cluster=await(admin.describeCluster().clusterId(),until);
        TopicDescription topic=await(admin.describeTopics(List.of(name)).allTopicNames(),until).get(name);
        if(cluster==null||!Codec.sha256("KAFKA:"+cluster).equals(plan.topic().clusterSha256())
                ||topic==null||topic.isInternal()||!topic.topicId().toString().equals(plan.topic().topicId())
                ||topic.partitions().size()!=1||topic.partitions().get(0).partition()!=0
                ||topic.partitions().get(0).replicas().size()<3)
            throw rejected("KAFKA_NOTICE_CONTROL_TOPIC_CHANGED");
        String identity=KafkaRecoveryReview.topicIdentity(cluster,topic.topicId());
        if(!profile)return identity;
        ConfigResource resource=new ConfigResource(ConfigResource.Type.TOPIC,name);
        List<ConfigResource> resources=new ArrayList<>();
        resources.add(resource);
        for(var broker:await(admin.describeCluster().nodes(),until))
            resources.add(new ConfigResource(ConfigResource.Type.BROKER,Integer.toString(broker.id())));
        Map<ConfigResource,Config> result=await(admin.describeConfigs(resources).all(),until);
        Config config=result.get(resource);
        int replicas=topic.partitions().get(0).replicas().size();
        if(!"delete".equals(value(config,"cleanup.policy"))||!"-1".equals(value(config,"retention.ms"))
                ||!"-1".equals(value(config,"retention.bytes"))||!"false".equals(value(config,"unclean.leader.election.enable"))
                ||!Integer.toString(replicas/2+1).equals(value(config,"min.insync.replicas"))
                ||!"false".equals(value(config,"remote.storage.enable"))
                ||!messageLimit(value(config,"max.message.bytes")))
            throw rejected("KAFKA_NOTICE_CONTROL_TOPIC_PROFILE_UNSUPPORTED");
        for(ConfigResource broker:resources.subList(1,resources.size())){
            if(!"false".equals(value(result.get(broker),"auto.create.topics.enable")))
                throw rejected("KAFKA_NOTICE_AUTOCREATION_MUST_BE_DISABLED");
        }
        return identity;
    }

    private static boolean messageLimit(String value){
        try{
            int bytes=Integer.parseInt(value);
            return bytes>=2048&&bytes<=MAXIMUM_MESSAGE_BYTES;
        }catch(NumberFormatException invalid){return false;}
    }

    private static String value(Config config,String name){
        ConfigEntry item=config==null?null:config.get(name);
        return item==null?null:item.value();
    }

    private static long until(){return System.nanoTime()+WINDOW.toNanos();}
    private static Duration remaining(long until){
        long remaining=until-System.nanoTime();
        if(remaining<=0)throw rejected("KAFKA_NOTICE_NATIVE_READ_TIMED_OUT");
        return Duration.ofNanos(remaining);
    }
    private static <T>T await(KafkaFuture<T> future,long until)throws Exception {
        return future.get(remaining(until).toNanos(),TimeUnit.NANOSECONDS);
    }
    private static IllegalStateException rejected(String code){return new IllegalStateException(code);}
}
