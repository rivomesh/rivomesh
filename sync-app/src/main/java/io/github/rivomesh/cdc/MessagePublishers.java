package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import com.rabbitmq.client.*;
import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Real broker clients with durable publication acknowledgments. */
public final class MessagePublishers {
    private MessagePublishers(){}
    public static ConfirmedPublisher open(PipelineConfig.Target target,String binding)throws Exception{
        try{if(target.product==TargetProduct.RABBITMQ)return new Rabbit(target,binding);if(target.product==TargetProduct.NATS_JETSTREAM)return new JetStream(target,binding);if(target.product==TargetProduct.KAFKA)return new KafkaMessagePublisher(target);if(target.product==TargetProduct.REDIS_STREAMS)return new RedisMessagePublisher(target,binding);if(target.product==TargetProduct.PULSAR)return new PulsarMessagePublisher(target,binding);if(target.product==TargetProduct.ROCKETMQ)return new RocketMessagePublisher(target,binding);if(target.product==TargetProduct.ACTIVEMQ_ARTEMIS)return new ArtemisMessagePublisher(target,binding);if(target.product==TargetProduct.ACTIVEMQ_CLASSIC)return new ClassicMessagePublisher(target,binding);throw new IllegalArgumentException("MESSAGE_PRODUCT_NOT_ENABLED");}
        catch(Exception e){throw classify(e);}
    }
    private static Exception classify(Exception error){
        if(error instanceof AuthenticationFailureException)return new SyncFailure("BROKER_AUTHENTICATION_REJECTED","MESSAGE_PUBLICATION",false,"Broker rejected authentication",error,null,0,null);
        for(Throwable t=error;t!=null;t=t.getCause())if(t instanceof ShutdownSignalException){Object reason=((ShutdownSignalException)t).getReason();int code=reason instanceof AMQP.Connection.Close?((AMQP.Connection.Close)reason).getReplyCode():reason instanceof AMQP.Channel.Close?((AMQP.Channel.Close)reason).getReplyCode():0;boolean retry=code==0||code==320||code==506||code==541;return new SyncFailure(retry?"BROKER_CONNECTION_LOST":"BROKER_CHANNEL_REJECTED","MESSAGE_PUBLICATION",retry,"Broker channel closed",error,null,0,null);}
        return error;
    }
    public static String binding(String pipeline,String subscription,String generation){return "rivomesh:"+Codec.sha256(pipeline+":"+subscription+":"+generation);}
    /** Connections and capability reads; Artemis also uses an ephemeral management reply channel. No business resources are created. */
    public static void check(PipelineConfig.Target target)throws Exception{
        if(target.product==TargetProduct.PULSAR){PulsarMessagePublisher.check(target);return;}
        if(target.product==TargetProduct.ROCKETMQ){RocketMessagePublisher.check(target);return;}
        if(target.product==TargetProduct.ACTIVEMQ_ARTEMIS){ArtemisMessagePublisher.check(target);return;}
        if(target.product==TargetProduct.ACTIVEMQ_CLASSIC){ClassicMessagePublisher.check(target);return;}
        if(target.product==TargetProduct.KAFKA){KafkaMessagePublisher.check(target);return;}
        if(target.product==TargetProduct.REDIS_STREAMS){RedisMessagePublisher.check(target);return;}
        if(target.product==TargetProduct.RABBITMQ)try(Rabbit publisher=new Rabbit(target,null)){if(publisher.receipts!=null)publisher.receipts.checkBroker(publisher.connection);return;}
        if(target.product==TargetProduct.NATS_JETSTREAM)try(JetStream ignored=new JetStream(target,null)){ignored.management.getAccountStatistics();return;}
        throw new IllegalArgumentException("MESSAGE_PRODUCT_NOT_ENABLED");
    }
    static final class Rabbit implements ConfirmedPublisher {
        final com.rabbitmq.client.Connection connection;final Channel channel;final PipelineConfig.Messaging settings;final AtomicBoolean returned=new AtomicBoolean();final Set<String> prepared=new HashSet<>();
        final RabbitReceiptStream receipts;
        String confirmed;
        Rabbit(PipelineConfig.Target target,String binding)throws Exception{
            settings=target.messaging;ConnectionFactory factory=new ConnectionFactory();factory.setUri(target.url);factory.setUsername(target.username);factory.setPassword(target.password==null?"":target.password);factory.setAutomaticRecoveryEnabled(false);factory.setTopologyRecoveryEnabled(false);factory.setConnectionTimeout(settings.connectTimeoutMillis);factory.setHandshakeTimeout(settings.connectTimeoutMillis);factory.setChannelRpcTimeout(settings.requestTimeoutMillis);factory.setRequestedHeartbeat(20);
            if("amqps".equals(new URI(target.url).getScheme()))factory.enableHostnameVerification();
            List<Address> addresses=new ArrayList<>();List<String> endpoints=new ArrayList<>();endpoints.add(target.url);endpoints.addAll(settings.endpoints);URI first=new URI(target.url);
            for(String endpoint:endpoints){URI uri=new URI(endpoint);if(!Objects.equals(first.getScheme(),uri.getScheme())||!Objects.equals(first.getRawPath(),uri.getRawPath()))throw new IllegalArgumentException("RABBIT_ENDPOINT_VHOST_OR_TLS_MISMATCH");addresses.add(new Address(uri.getHost(),uri.getPort()<0?("amqps".equals(uri.getScheme())?5671:5672):uri.getPort()));}
            receipts=settings.rabbitReceiptStream?new RabbitReceiptStream(target,binding,factory.getVirtualHost()):null;
            try{connection=factory.newConnection(addresses,"rivomesh-"+target.id);}
            catch(Exception failed){if(receipts!=null)receipts.close();throw failed;}
            Channel opened=null;
            try{opened=connection.createChannel();opened.confirmSelect();opened.addReturnListener((ReturnCallback)message->returned.set(true));channel=opened;}
            catch(Exception e){connection.abort();if(receipts!=null)receipts.close();throw e;}
        }
        public void prepare(Set<String> destinations)throws Exception{
            try{if(receipts!=null)receipts.prepare(connection,channel,settings.autoCreate,settings.replicas);
                for(String destination:destinations)if(!prepared.contains(destination)){if(settings.autoCreate){Map<String,Object> args=new HashMap<>();args.put("x-queue-type",settings.queueType);args.put("x-overflow","reject-publish");if("quorum".equals(settings.queueType))args.put("x-quorum-initial-group-size",settings.replicas);channel.queueDeclare(destination,true,false,false,args);}else channel.queueDeclarePassive(destination);prepared.add(destination);}}
            catch(Exception e){throw classify(e);}
        }
        public void require(Set<String> destinations)throws Exception{
            try{if(receipts!=null)receipts.prepare(connection,channel,false,settings.replicas);
                for(String destination:destinations)if(!prepared.contains(destination)){channel.queueDeclarePassive(destination);prepared.add(destination);}}
            catch(Exception e){Exception classified=classify(e);if(classified instanceof SyncFailure&&!((SyncFailure)classified).retryable)throw new SyncFailure("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED","MESSAGE_PUBLICATION",false,"An initialized queue is missing or inaccessible; a recovery plan is required",e,null,0,null);throw classified;}
        }
        public void publish(List<Message> messages)throws Exception{
            try{
            returned.set(false);
            for(Message message:messages){Map<String,Object> headers=new HashMap<>(message.headers);if(message.key!=null)headers.put("rivomesh_key",message.key);AMQP.BasicProperties properties=new AMQP.BasicProperties.Builder().contentType("application/json").contentEncoding("utf-8").deliveryMode(2).messageId(message.id).headers(headers).build();channel.basicPublish("",message.destination,true,properties,message.body);}
            channel.waitForConfirmsOrDie(settings.requestTimeoutMillis);
            if(returned.get())throw new SyncFailure("RABBIT_UNROUTABLE","MESSAGE_PUBLICATION",false,"Mandatory publication was returned; validate consumer history and a recovery plan before resuming");
            if(receipts!=null&&!messages.isEmpty()&&RabbitReceiptStream.terminal(messages.getLast())){
                String next=receipts.retain(connection,channel,messages.getLast(),settings.requestTimeoutMillis);
                if(returned.get())throw new IllegalStateException("RABBIT_RECOVERY_PUBLICATION_UNCERTAIN");
                confirmed=next;
            }
            }catch(Exception e){throw classify(e);}
        }
        @Override public String identity(){return receipts==null?null:receipts.identity();}
        @Override public String confirmedProgress(){return confirmed;}
        @Override public void resumeProgress(String progress){
            if(progress!=null){
                if(receipts==null)throw new IllegalStateException("RABBIT_RECEIPT_STREAM_CONFIGURATION_REQUIRED");
                RabbitReceiptStream.parseProgress(progress);
            }
            confirmed=progress;
        }
        void verifyPrecedingCommit(String progress,String identityHash,String event)throws Exception{
            if(receipts==null)throw new IllegalStateException("RABBIT_RECEIPT_STREAM_CONFIGURATION_REQUIRED");
            receipts.verify(connection,progress,identityHash,event);
        }
        public void close(){try{connection.abort();}finally{if(receipts!=null)receipts.close();}}
    }
    static final class JetStream implements ConfirmedPublisher {
        final io.nats.client.Connection connection;final io.nats.client.JetStream stream;final JetStreamManagement management;final PipelineConfig.Target target;final String binding;
        private String identity,confirmed;
        JetStream(PipelineConfig.Target target,String binding)throws Exception{
            this.target=target;
            this.binding=binding;
            PipelineConfig.Messaging m=target.messaging;
            connection=connect(target,m);
            try{JetStreamOptions js=JetStreamOptions.builder().requestTimeout(Duration.ofMillis(m.requestTimeoutMillis)).build();stream=connection.jetStream(js);management=connection.jetStreamManagement(js);}
            catch(Exception e){connection.close();throw e;}
        }
        private static io.nats.client.Connection connect(PipelineConfig.Target target,PipelineConfig.Messaging settings)throws Exception{
            List<String> servers=new ArrayList<>();
            servers.add(target.url);
            servers.addAll(settings.endpoints);
            // Keep the configured list authoritative, including during a failed TLS handshake.
            Options.Builder options=new Options.Builder().servers(servers.toArray(new String[0]))
                    .ignoreDiscoveredServers().noRandomize()
                    .connectionTimeout(Duration.ofMillis(settings.connectTimeoutMillis)).maxReconnects(0);
            if(target.url.startsWith("tls:")){
                // Resolve for the TCP dial without replacing the TLS peer name with its address.
                options.hostnameResolveMode(Options.HostnameResolveMode.HappyEyeballs)
                        .dataPortType(VerifiedNatsTlsPort.class.getName());
            }
            if(settings.token!=null&&!settings.token.isEmpty())options.token(settings.token.toCharArray());
            else if(target.username!=null&&!target.username.isEmpty())
                options.userInfo(target.username,target.password==null?"":target.password);
            return Nats.connect(options.build());
        }

        public void prepare(Set<String> destinations)throws Exception{prepare(destinations,target.messaging.autoCreate);}
        public void require(Set<String> destinations)throws Exception{prepare(destinations,false);}
        private void prepare(Set<String> destinations,boolean allowCreate)throws Exception{
            if(identity!=null)return;
            StreamInfo info;
            try{info=management.getStreamInfo(target.messaging.stream);}
            catch(JetStreamApiException e){
                if(e.getApiErrorCode()==10059&&!allowCreate)throw new SyncFailure("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED","MESSAGE_PUBLICATION",false,"An initialized stream is missing; a recovery plan is required",e,null,0,null);
                if(e.getApiErrorCode()!=10059||!allowCreate)throw e;Set<String> subjects=new TreeSet<>();for(String namespace:target.mappings.values())subjects.add(namespace+".*");
                try{info=management.addStream(StreamConfiguration.builder().name(target.messaging.stream).description(binding).subjects(subjects).storageType(StorageType.File).retentionPolicy(RetentionPolicy.Limits).discardPolicy(DiscardPolicy.New).replicas(target.messaging.replicas).build());}
                catch(JetStreamApiException race){info=management.getStreamInfo(target.messaging.stream);}
            }
            identity=streamIdentity(info);
        }

        private String streamIdentity(StreamInfo info)throws SyncFailure{
            StreamConfiguration config=info.getConfiguration();Set<String> expected=new TreeSet<>();for(String namespace:target.mappings.values())expected.add(namespace+".*");
            if(config.getStorageType()!=StorageType.File||config.getRetentionPolicy()!=RetentionPolicy.Limits||config.getDiscardPolicy()!=DiscardPolicy.New||!Duration.ZERO.equals(config.getMaxAge())||!new TreeSet<>(config.getSubjects()).equals(expected)||config.getReplicas()!=target.messaging.replicas||!Objects.equals(binding,config.getDescription()))throw new SyncFailure("JETSTREAM_CONFIGURATION_REQUIRES_PLAN","MESSAGE_PUBLICATION",false,"Stream ownership, retention, subjects, file storage, or replica count differs");
            if(info.getCreateTime()==null)throw uncertain();
            return "NATS_STREAM_V1:"+target.messaging.stream+":"+info.getCreateTime().toInstant()+":"+binding;
        }
        public void publish(List<Message> messages)throws Exception{
            String progress=null;
            for(Message message:messages){Headers headers=new Headers();for(Map.Entry<String,String> header:message.headers.entrySet())headers.add(header.getKey(),header.getValue());if(message.key!=null)headers.add("rivomesh_key",message.key);
                try{
                    PublishAck ack=stream.publish(NatsMessage.builder().subject(message.destination).headers(headers).data(message.body).build(),
                            PublishOptions.builder().stream(target.messaging.stream).messageId(message.id).build());
                    if(!target.messaging.stream.equals(ack.getStream())||ack.getSeqno()<1)throw uncertain();
                    progress="NATS_V1:"+ack.getSeqno()+":"+Codec.sha256(message.body);
                }
                catch(JetStreamApiException e){throw new SyncFailure("JETSTREAM_PUBLISH_REJECTED","MESSAGE_PUBLICATION",e.getErrorCode()>=500,"JetStream rejected publication",e,null,0,null);}
                catch(IllegalStateException e){if(connection.getStatus()!=io.nats.client.Connection.Status.CONNECTED)throw new SyncFailure("BROKER_CONNECTION_LOST","MESSAGE_PUBLICATION",true,"NATS connection closed",e,null,0,null);throw e;}
            }
            if(progress!=null)confirmed=progress;
        }
        @Override public String identity(){return identity;}
        @Override public String confirmedProgress(){return confirmed;}
        @Override public void resumeProgress(String progress){
            if(progress!=null)parseProgress(progress);
            confirmed=progress;
        }
        private static String[] parseProgress(String progress){
            if(progress==null||!progress.matches("NATS_V1:[1-9][0-9]{0,18}:[a-f0-9]{64}"))throw uncertain();
            return progress.split(":",3);
        }
        private static IllegalStateException uncertain(){
            return new IllegalStateException("NATS_JETSTREAM_RECOVERY_PUBLICATION_UNCERTAIN");
        }
        void verifyPrecedingCommit(String progress,String identityHash,String event)throws Exception{
            String[] receipt=parseProgress(progress);
            String name=target.messaging.stream;
            String before=streamIdentity(management.getStreamInfo(name));
            if(!Codec.sha256(before).equals(identityHash))throw uncertain();
            MessageInfo message;
            long sequence=Long.parseLong(receipt[1]);
            try{message=management.getMessage(name,sequence);}
            catch(JetStreamApiException failure){if(failure.getErrorCode()==404)throw uncertain();throw failure;}
            Headers headers=message.getHeaders();
            if(message.getSeq()!=sequence||headers==null||!event.equals(headers.getFirst("Nats-Msg-Id"))
                    ||!"COMMIT".equals(headers.getFirst("rivomesh_kind"))||message.getData()==null
                    ||!Codec.sha256(message.getData()).equals(receipt[2])
                    ||!before.equals(streamIdentity(management.getStreamInfo(name))))throw uncertain();
        }
        public void close()throws Exception{connection.close();}
    }
}
