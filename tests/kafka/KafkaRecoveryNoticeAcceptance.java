package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Attempt;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Plan;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Receipt;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Topic;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;

/** Disposable native proof of the internal Kafka control notice. This is never packaged as an entry point. */
public final class KafkaRecoveryNoticeAcceptance {
    private static final String GENERATION="fixture-generation",SOURCE="fixture-source";
    private static final String GTID="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String CONTROL="reviewed-control",BUSINESS="original-business",PEER="independent-business";
    private static final String TARGET="blocked",HEALTHY="peer";
    private static final Duration IO=Duration.ofSeconds(8),CLOSE=Duration.ofMillis(500);
    private static final TopicPartition CONTROL_PARTITION=new TopicPartition(CONTROL,0);
    private static final TopicPartition BUSINESS_PARTITION=new TopicPartition(BUSINESS,0),PEER_PARTITION=new TopicPartition(PEER,0);
    private static final Set<TopicPartition> BUSINESS_PARTITIONS=Set.of(BUSINESS_PARTITION,PEER_PARTITION);
    private static final KafkaRecoveryNotice.Authority TEST_AUTHORITY=()->{};

    private record Business(long targetEnd,long peerEnd,long targetOffset,long peerOffset) {}

    private KafkaRecoveryNoticeAcceptance(){}

    public static void main(String[] args)throws Exception{
        Path directory=Path.of(args[2]);
        PipelineConfig.Target target=target();
        Topic topic=topics(target);
        JdbcJournal.Settings settings=new JdbcJournal.Settings();
        settings.driver="org.h2.Driver";
        settings.url="jdbc:h2:file:"+directory.resolve("notice-journal")+";WRITE_DELAY=0";
        settings.pipelineId="notice-fixture";
        KafkaNoticeBroker broker=new KafkaNoticeBroker(target);
        Plan plan;
        Receipt recovered;
        long oldOffset;
        String beforeRaw,beforeNormalized;
        try(KafkaConsumer<String,byte[]> business=new KafkaConsumer<>(consumerProperties(target,"notice-business-observer"))){
            business.assign(BUSINESS_PARTITIONS);
            publishBusiness(target);
            business.commitSync(Map.of(BUSINESS_PARTITION,new OffsetAndMetadata(1),PEER_PARTITION,new OffsetAndMetadata(2)),IO);
            Business before=business(business);
            require(before.equals(new Business(1,2,1,2)),"Synthetic business baseline differed");

            try(JdbcJournal journal=new JdbcJournal(settings)){
                seed(journal);
                JdbcKafkaNoticeLedger ledger=new JdbcKafkaNoticeLedger(journal);
                ledger.provision();
                plan=ledger.prepare(TARGET,1,topic,Codec.sha256("fixture reviewed prior Kafka record"),TEST_AUTHORITY);
                Journal.FailureReview failure=journal.inspectFailure(TARGET,1,100000);
                beforeRaw=failure.rawHash;beforeNormalized=failure.normalizedHash;
                require(failure.subscription.acknowledged==1&&failure.sequence==2,"Failed source cut differed");

                // Leave the actual first Kafka producer in flight after sending uncommitted bytes.
                ledger.start(plan,TEST_AUTHORITY);
                try(Attempt old=broker.begin(plan)){
                    oldOffset=old.send(plan.payload());
                    require(broker.read(plan,oldOffset)==null,"An uncommitted notice was visible");
                    KafkaRecoveryNotice protocol=new KafkaRecoveryNotice(ledger,lostResponse(broker));
                    try{
                        protocol.publish(plan,plan.hash(),TEST_AUTHORITY);
                        throw new AssertionError("Expected emulated lost commit response");
                    }catch(IOException expected){
                        require("lost native commit response".equals(expected.getMessage()),"Unexpected transport failure");
                    }
                    require(ledger.existing(plan).hasCommitIntent()&&!ledger.existing(plan).verified(),
                            "Broker result was marked verified before native lookup");
                    try{
                        old.commit();
                        throw new AssertionError("The broker accepted the old producer epoch");
                    }catch(ProducerFencedException|InvalidProducerEpochException fenced){
                        // The new writer uses the same transactional ID, so Kafka rejects the old epoch.
                    }
                }
            }

            try(JdbcJournal reopened=new JdbcJournal(settings)){
                KafkaRecoveryNotice protocol=new KafkaRecoveryNotice(new JdbcKafkaNoticeLedger(reopened),receiveOnly(broker));
                recovered=protocol.publish(plan,plan.hash(),TEST_AUTHORITY);
                require(recovered!=null&&recovered.offset()>oldOffset&&recovered.equals(protocol.publish(plan,plan.hash(),TEST_AUTHORITY)),
                        "Reopen or replay failed to recover the exact persisted Kafka offset");
                require(recovered.payloadSha256().equals(plan.payloadSha256()),"Control receipt bytes differed");
                Journal.FailureReview after=reopened.inspectFailure(TARGET,1,100000);
                long peer=reopened.subscriptions().stream().filter(sub->HEALTHY.equals(sub.id)).findFirst().orElseThrow().acknowledged;
                require(after.subscription.acknowledged==1&&peer==2&&beforeRaw.equals(after.rawHash)
                        &&beforeNormalized.equals(after.normalizedHash),"Notice mutated a source payload or destination checkpoint");
            }
            List<Long> visible=visibleControlOffsets(target);
            require(visible.equals(List.of(recovered.offset())),"Broker exposed duplicate or aborted notice rows: "+visible);
            Business after=business(business);
            require(before.equals(after),"Notice changed business topic or consumer positions");
            System.out.println(Codec.encode(Map.of("oldProducerFenced",true,"oldUncommittedOffset",oldOffset,
                    "recoveredOffset",recovered.offset(),"visibleControlOffsets",visible,"businessBefore",before,"businessAfter",after,
                    "failedTargetAcknowledgment",1,"healthyPeerAcknowledgment",2,"sourcePayloadUnchanged",true)));
        }
    }

    private static KafkaRecoveryNotice.Broker lostResponse(KafkaNoticeBroker broker){
        return new KafkaRecoveryNotice.Broker(){
            @Override public Receipt read(Plan plan,Long offset)throws Exception{return broker.read(plan,offset);}
            @Override public Attempt begin(Plan plan)throws Exception{
                Attempt nativeAttempt=broker.begin(plan);
                return new Attempt(){
                    @Override public long send(byte[] payload)throws Exception{return nativeAttempt.send(payload);}
                    @Override public void commit()throws Exception{
                        nativeAttempt.commit();
                        throw new IOException("lost native commit response");
                    }
                    @Override public void close(){nativeAttempt.close();}
                };
            }
        };
    }

    private static KafkaRecoveryNotice.Broker receiveOnly(KafkaNoticeBroker broker){
        return new KafkaRecoveryNotice.Broker(){
            @Override public Receipt read(Plan plan,Long offset)throws Exception{return broker.read(plan,offset);}
            @Override public Attempt begin(Plan plan){throw new AssertionError("Recovery opened a second producer");}
        };
    }

    private static Topic topics(PipelineConfig.Target target)throws Exception{
        Properties properties=KafkaMessagePublisher.connectionProperties(target);
        properties.setProperty("default.api.timeout.ms","25000");
        properties.setProperty("request.timeout.ms","10000");
        Admin admin=Admin.create(properties);
        try{
            long waitUntil=System.nanoTime()+Duration.ofSeconds(70).toNanos();
            int previousLeader=-1;
            while(true){
                try{
                    var quorum=admin.describeMetadataQuorum().quorumInfo().get(4,TimeUnit.SECONDS);
                    int leader=quorum.leaderId();
                    if(leader>0&&quorum.highWatermark()>0&&admin.describeCluster().nodes().get(4,TimeUnit.SECONDS).size()==4){
                        if(leader==previousLeader)break;
                        previousLeader=leader;
                    }
                }catch(ExecutionException|TimeoutException retry){
                    if(System.nanoTime()>=waitUntil)throw retry;
                    previousLeader=-1;
                }
                require(System.nanoTime()<waitUntil,"KRaft brokers did not initialize");
                Thread.sleep(200);
            }
            Map<String,String> configs=Map.of("cleanup.policy","delete","retention.ms","-1","retention.bytes","-1",
                    "unclean.leader.election.enable","false","min.insync.replicas","2","max.message.bytes","65536");
            List<NewTopic> topics=List.of(CONTROL,BUSINESS,PEER).stream()
                    .map(topic->new NewTopic(topic,Map.of(0,List.of(1,2,4))).configs(configs)).toList();
            try{admin.createTopics(topics,new CreateTopicsOptions().timeoutMs(23000)).all().get(24,TimeUnit.SECONDS);}
            catch(ExecutionException existing){if(!(existing.getCause() instanceof TopicExistsException))throw existing;}
            String cluster=admin.describeCluster().clusterId().get(8,TimeUnit.SECONDS);
            long syncDeadline=System.nanoTime()+Duration.ofSeconds(20).toNanos();
            Exception lastMetadataFailure=null;
            while(System.nanoTime()<syncDeadline){
                try{
                    var descriptions=admin.describeTopics(List.of(CONTROL,BUSINESS,PEER)).allTopicNames().get(5,TimeUnit.SECONDS);
                    if(descriptions.size()==3&&descriptions.values().stream()
                            .allMatch(description->description.partitions().size()==1&&description.partitions().getFirst().isr().size()==3))
                        return new Topic(CONTROL,Codec.sha256("KAFKA:"+cluster),descriptions.get(CONTROL).topicId().toString());
                }catch(ExecutionException pending){
                    if(!(pending.getCause() instanceof RetriableException))throw pending;
                    lastMetadataFailure=pending;
                }catch(TimeoutException pending){
                    lastMetadataFailure=pending;
                }
                Thread.sleep(200);
            }
            throw new AssertionError("Control and business topic replicas did not synchronize",lastMetadataFailure);
        }finally{admin.close(CLOSE);}
    }

    private static PipelineConfig.Target target(){
        PipelineConfig.Target target=new PipelineConfig.Target();
        target.id=TARGET;
        target.product=TargetProduct.KAFKA;
        target.url="kafka://kafka-a:9092";
        target.messaging=new PipelineConfig.Messaging();
        target.messaging.topic=BUSINESS;
        target.messaging.endpoints=List.of("kafka://kafka-b:9092","kafka://kafka-c:9092","kafka://kafka-d:9092");
        target.messaging.replicas=3;
        target.messaging.maxMessageBytes=8192;
        return target;
    }

    private static void publishBusiness(PipelineConfig.Target config)throws Exception{
        KafkaProducer<String,byte[]> producer=new KafkaProducer<>(KafkaMessagePublisher.properties(config));
        try{
            for(String topic:List.of(BUSINESS,PEER)){
                producer.send(new ProducerRecord<>(topic,0,"position-one",new byte[]{(byte)0xe7,13})).get(12,TimeUnit.SECONDS);
            }
            producer.send(new ProducerRecord<>(PEER,0,"position-two",new byte[]{32,79})).get(12,TimeUnit.SECONDS);
        }finally{producer.close(CLOSE);}
    }

    private static Business business(KafkaConsumer<String,byte[]> consumer){
        var ends=consumer.endOffsets(BUSINESS_PARTITIONS,IO);
        var offsets=consumer.committed(BUSINESS_PARTITIONS,IO);
        return new Business(ends.get(BUSINESS_PARTITION),ends.get(PEER_PARTITION),
                offsets.get(BUSINESS_PARTITION).offset(),offsets.get(PEER_PARTITION).offset());
    }

    private static List<Long> visibleControlOffsets(PipelineConfig.Target config)throws Exception{
        KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<>(consumerProperties(config,null));
        try{
            Set<TopicPartition> partitions=Set.of(CONTROL_PARTITION);
            consumer.assign(partitions);
            consumer.seek(CONTROL_PARTITION,0);
            long end=consumer.endOffsets(partitions,IO).get(CONTROL_PARTITION);
            long until=System.nanoTime()+IO.toNanos();
            List<Long> offsets=new ArrayList<>();
            while(consumer.position(CONTROL_PARTITION,IO)<end){
                require(System.nanoTime()<until,"Timed out reading settled control topic");
                for(var record:consumer.poll(Duration.ofMillis(100)).records(CONTROL_PARTITION))
                    offsets.add(record.offset());
            }
            return offsets;
        }finally{consumer.close(CLOSE);}
    }

    private static Properties consumerProperties(PipelineConfig.Target target,String group)throws Exception{
        Properties properties=KafkaMessagePublisher.connectionProperties(target);
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty("enable.auto.commit","false");
        properties.setProperty("allow.auto.create.topics","false");
        properties.setProperty("isolation.level","read_committed");
        if(group!=null)properties.setProperty("group.id",group);
        return properties;
    }

    static void seed(JdbcJournal journal)throws Exception{
        journal.initialize(Codec.sha256("notice native fixture"),GENERATION,List.of(TARGET,HEALTHY));
        Journal.Lease writer=journal.claim("fixture-writer",30000);
        TableSchema table=new TableSchema();
        table.id=TableSchema.identity("sales","","orders");table.name="orders";table.database="sales";
        TableSchema.Column id=new TableSchema.Column();id.name="id";id.type=TableSchema.Type.INT64;id.nullable=false;
        table.columns.add(id);table.primaryKey.add("id");
        journal.saveSchema(writer,table);
        for(int sequence=1;sequence<=2;sequence++){
            SourceTransaction operation=transaction(table,sequence);
            require(journal.append(writer,operation)==sequence,"Unexpected journal sequence");
            SourceTransaction normalized=journal.normalize(writer,journal.read(TARGET,1).getFirst(),Map.of(table.id,table),
                    Map.of(table.id,position(0)),(when,baseline)->false);
            journal.acknowledge(writer,HEALTHY,sequence);
            if(sequence==1)journal.acknowledge(writer,TARGET,1);
            else{
                SubscriberFailure failure=new SubscriberFailure();
                failure.phase=SubscriberFailure.Phase.TARGET_APPLY;failure.sequence=2;
                failure.transactionId=operation.id;failure.projectedHash=Codec.sha256(Codec.encode(normalized));
                journal.pauseFailure(writer,TARGET,"KAFKA_TEST_APPLY_FAILURE",failure);
            }
        }
        journal.pausePipeline();
    }

    private static SourceTransaction transaction(TableSchema table,int sequence){
        SourceTransaction operation=new SourceTransaction();
        operation.id=GTID+":"+(100+sequence);
        operation.streamGeneration=GENERATION;
        operation.position=position(sequence);
        Change row=new Change();row.eventId=operation.id+":0";row.tableId=table.id;
        row.operation=Change.Operation.INSERT;row.after=List.of(Cell.value(Integer.toString(sequence)));
        operation.changes.add(row);
        return operation;
    }

    private static SourcePosition position(int sequence){
        return new SourcePosition(1,"MYSQL",SOURCE,Map.of("gtidSet",GTID+":1-"+(100+sequence),"serverUuid",GTID,
                "file","binlog.000001","offset",Integer.toString(120+sequence*100)));
    }

    private static void require(boolean condition,String message){if(!condition)throw new AssertionError(message);}
}
