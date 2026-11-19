package io.github.rivomesh.transport.rocketmq;

import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.*;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.impl.*;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Native synchronous TCP profile. One queue, one broker group, explicit finite retention. */
public final class RocketTransport implements AutoCloseable {
    public static final class Settings {
        public String nameservers,cluster,broker,topicPrefix;
        public List<String> controllerEndpoints = List.of();
        public int connectTimeoutMillis=5000,requestTimeoutMillis=10000,maxMessageBytes=1024*1024,minimumRetentionHours=87600;
    }
    public static final class Publication {
        public String id,channel,key;public byte[] body;public Map<String,String> headers=new LinkedHashMap<>();
    }
    public static final class Failure extends Exception {
        public final String code;public final boolean retryable;
        Failure(String code,boolean retryable,Throwable cause){super(code,cause);this.code=code;this.retryable=retryable;}
    }
    private final Settings settings;private final String binding,topic;
    private DefaultMQProducer producer;
    private MQClientAPIImpl api;
    private final RocketControllerSafety controller;
    private RocketControllerSafety.Route controllerRoute;
    private String master;private MessageQueue queue;private boolean verified;
    private boolean restoredProgress;
    private long confirmedOffset=-1,confirmedPhysical=-1;
    private String confirmedHash;
    public void resumeProgress(String progress)throws Exception{
        if(verified||confirmedOffset>=0)throw new IllegalStateException("ROCKETMQ_PROGRESS_ALREADY_ACTIVE");if(progress==null)return;
        try{String[] fields=progress.split(":",-1);if(fields.length!=5||!"ROCKETMQ_OFFSET_V1".equals(fields[0])||!topic.equals(fields[1])||!fields[4].matches("[a-f0-9]{64}"))throw new IllegalArgumentException();confirmedOffset=Long.parseLong(fields[2]);confirmedPhysical=Long.parseLong(fields[3]);if(confirmedOffset<0||confirmedPhysical<0)throw new IllegalArgumentException();confirmedHash=fields[4];restoredProgress=true;}
        catch(Exception invalid){throw reject("ROCKETMQ_PROGRESS_INVALID");}
    }
    public String confirmedProgress(){return confirmedHash==null?null:"ROCKETMQ_OFFSET_V1:"+topic+":"+confirmedOffset+":"+confirmedPhysical+":"+confirmedHash;}
    private static String contentHash(Message message)throws Exception{
        java.security.MessageDigest hash=java.security.MessageDigest.getInstance("SHA-256");String id=message.getUserProperty("rivomesh_event_id");byte[] identity=(id==null?"":id).getBytes(StandardCharsets.UTF_8);hash.update(ByteBuffer.allocate(4).putInt(identity.length).array());hash.update(identity);hash.update(message.getBody());StringBuilder result=new StringBuilder();for(byte value:hash.digest())result.append(String.format(Locale.ROOT,"%02x",value&255));return result.toString();
    }
    private void verifyProgress()throws Exception{
        if(confirmedHash==null)return;MessageExt message;
        try{message=api.viewMessage(master,topic,confirmedPhysical,settings.requestTimeoutMillis);}
        catch(MQBrokerException unavailable){if(unavailable.getResponseCode()==ResponseCode.SYSTEM_ERROR||unavailable.getResponseCode()==ResponseCode.QUERY_NOT_FOUND)throw reject("ROCKETMQ_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED");throw unavailable;}
        if(!matches(message,confirmedPhysical,confirmedOffset,confirmedHash))throw reject("ROCKETMQ_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");
    }
    private boolean matches(MessageExt message,long physical,long offset,String hash)throws Exception{
        return message!=null&&topic.equals(message.getTopic())&&message.getQueueId()==0&&message.getQueueOffset()==offset
                &&message.getCommitLogOffset()==physical&&message.getBody()!=null&&hash.equals(contentHash(message));
    }
    public static String physicalTopic(String prefix,String binding){
        if(prefix==null||!prefix.matches("[A-Za-z][A-Za-z0-9_-]{0,47}")||binding==null||!binding.matches("rivomesh:[a-f0-9]{64}"))throw new IllegalArgumentException("ROCKETMQ_PUBLICATION_NAME_INVALID");
        return prefix+"_"+binding.substring("rivomesh:".length());
    }
    public RocketTransport(Settings settings,String binding)throws Exception{
        this.settings=settings;this.binding=binding;topic=binding==null?null:physicalTopic(settings.topicPrefix,binding);
        if(settings.nameservers==null||settings.nameservers.isEmpty()||settings.cluster==null||!settings.cluster.matches("[A-Za-z0-9_-]{1,127}")||settings.broker==null||!settings.broker.matches("[A-Za-z0-9_-]{1,127}")||settings.minimumRetentionHours<1||settings.connectTimeoutMillis<100||settings.connectTimeoutMillis>120000||settings.requestTimeoutMillis<100||settings.requestTimeoutMillis>120000||settings.maxMessageBytes<1024||settings.maxMessageBytes>16*1024*1024)throw new IllegalArgumentException("ROCKETMQ_CONFIG_INVALID");
        controller=settings.controllerEndpoints.isEmpty()?null:new RocketControllerSafety(settings);
        startProducer();
    }
    private void startProducer()throws Exception{
        producer=new DefaultMQProducer("rivomesh_"+UUID.randomUUID().toString().replace("-",""));producer.setInstanceName(UUID.randomUUID().toString());producer.setNamesrvAddr(settings.nameservers);producer.setVipChannelEnabled(false);producer.setClientCallbackExecutorThreads(1);producer.setSendMsgTimeout(settings.requestTimeoutMillis);producer.setMqClientApiTimeout(settings.requestTimeoutMillis);producer.setMaxMessageSize(settings.maxMessageBytes);producer.setRetryTimesWhenSendFailed(0);producer.setRetryTimesWhenSendAsyncFailed(0);producer.setRetryAnotherBrokerWhenNotStoreOK(false);producer.setAutoBatch(false);producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);
        try{
            // Public SDK lifecycle: configure the private factory before it opens any sockets.
            producer.getDefaultMQProducerImpl().start(false);MQClientInstance factory=producer.getDefaultMQProducerImpl().getMqClientFactory();factory.getNettyClientConfig().setConnectTimeoutMillis(settings.connectTimeoutMillis);factory.getNettyClientConfig().setClientWorkerThreads(1);factory.getNettyClientConfig().setClientCallbackExecutorThreads(1);factory.start();api=factory.getMQClientAPIImpl();
        }catch(Exception failure){producer.shutdown();throw classify(failure);}
    }
    private static Failure reject(String code){return new Failure(code,false,null);}
    private void capabilities()throws Exception{
        ClusterInfo info=api.getBrokerClusterInfo(settings.requestTimeoutMillis);BrokerData broker=info.getBrokerAddrTable().get(settings.broker);
        if(broker==null)throw new Failure("ROCKETMQ_BROKER_UNAVAILABLE",true,null);if(!settings.cluster.equals(broker.getCluster()))throw reject("ROCKETMQ_BROKER_BINDING_MISMATCH");master=broker.getBrokerAddrs().get(0L);if(master==null)throw new Failure("ROCKETMQ_MASTER_UNAVAILABLE",true,null);
        queue=topic==null?null:new MessageQueue(topic,settings.broker,0);
        if(controller!=null){
            RocketControllerSafety.Route observed=controller.inspect(api,master);
            if(controllerRoute!=null&&!observed.follows(controllerRoute))throw RocketControllerSafety.routeRetry();
            if(controllerRoute!=null&&!controllerRoute.sameMaster(observed)){
                close();
                startProducer();
                capabilitiesAfterRestart(observed);
            }
            controllerRoute=observed;
            return;
        }
        Properties properties=api.getBrokerConfig(master,settings.requestTimeoutMillis);
        if(!settings.cluster.equals(properties.getProperty("brokerClusterName"))||!settings.broker.equals(properties.getProperty("brokerName"))||!"0".equals(properties.getProperty("brokerId")))throw reject("ROCKETMQ_BROKER_BINDING_MISMATCH");
        if(!"SYNC_FLUSH".equals(properties.getProperty("flushDiskType"))||!"false".equals(properties.getProperty("transientStorePoolEnable")))throw reject("ROCKETMQ_SYNCHRONOUS_DISK_REQUIRED");
        if(!"false".equals(properties.getProperty("autoCreateTopicEnable")))throw reject("ROCKETMQ_AUTO_CREATION_MUST_BE_DISABLED");
        if(!"false".equals(properties.getProperty("enableDLegerCommitLog"))||!"false".equals(properties.getProperty("enableControllerMode"))||!"ASYNC_MASTER".equals(properties.getProperty("brokerRole")))throw reject("ROCKETMQ_REPLICATION_PROFILE_REQUIRES_PLAN");
        long retention;try{retention=Long.parseLong(properties.getProperty("fileReservedTime"));}catch(Exception invalid){throw reject("ROCKETMQ_RETENTION_REQUIRES_PLAN");}
        if(retention<settings.minimumRetentionHours||!"false".equals(properties.getProperty("cleanFileForciblyEnable")))throw reject("ROCKETMQ_RETENTION_REQUIRES_PLAN");
    }
    private void capabilitiesAfterRestart(RocketControllerSafety.Route observed)throws Exception{
        ClusterInfo info=api.getBrokerClusterInfo(settings.requestTimeoutMillis);
        BrokerData broker=info.getBrokerAddrTable().get(settings.broker);
        if(broker==null||!settings.cluster.equals(broker.getCluster())
                ||!observed.equals(controller.inspect(api,broker.getBrokerAddrs().get(0L))))
            throw RocketControllerSafety.routeRetry();
    }
    /** Capability reads only: no topics, groups, policies or data are created. */
    public static void check(Settings settings)throws Exception{try(RocketTransport transport=new RocketTransport(settings,null)){try{transport.capabilities();}catch(Exception failure){throw classify(failure);}}}
    private TopicConfigAndQueueMapping configuration(String address)throws Exception{
        try{return api.getTopicConfig(address,topic,settings.requestTimeoutMillis);}catch(MQBrokerException missing){if(missing.getResponseCode()==ResponseCode.TOPIC_NOT_EXIST)return null;throw missing;}
    }
    private void verifyTopic(TopicConfigAndQueueMapping config)throws Exception{
        if(config==null)throw reject("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");
        if(config.getReadQueueNums()!=1||config.getWriteQueueNums()!=1||config.getPerm()!=6||config.getMappingDetail()!=null||!config.getAttributes().isEmpty())throw reject("ROCKETMQ_TOPIC_CONFIGURATION_REQUIRES_PLAN");
    }
    private void verifyControllerTopic()throws Exception{
        TopicConfig expected=new TopicConfig(topic,1,1,6);
        for(RocketControllerSafety.Replica replica:controllerRoute.members()){
            TopicConfigAndQueueMapping replicaConfig=configuration(replica.address());
            if(replicaConfig==null)throw metadataPending();
            verifyTopic(replicaConfig);
            if(!expected.equals(new TopicConfig(replicaConfig)))throw reject("ROCKETMQ_TOPIC_CONFIGURATION_REQUIRES_PLAN");
        }
    }
    private static Failure metadataPending(){
        return new Failure("ROCKETMQ_CONTROLLER_TOPIC_METADATA_PENDING",true,null);
    }
    List<MessageExt> read(long offset,int count)throws Exception{
        PullMessageRequestHeader header=new PullMessageRequestHeader();header.setConsumerGroup(MixAll.TOOLS_CONSUMER_GROUP);header.setTopic(topic);header.setQueueId(0);header.setQueueOffset(offset);header.setMaxMsgNums(count);header.setSysFlag(PullSysFlag.buildSysFlag(false,false,true,false));header.setCommitOffset(0L);header.setSuspendTimeoutMillis(0L);header.setSubscription("*");header.setSubVersion(0L);header.setExpressionType("TAG");
        PullResult result=api.pullMessage(master,header,settings.requestTimeoutMillis,CommunicationMode.SYNC,null);
        if(result.getMinOffset()!=0)throw reject("ROCKETMQ_HISTORY_EXPIRED_REBUILD_REQUIRED");
        if(result.getPullStatus()!=PullStatus.FOUND)return Collections.emptyList();byte[] bytes=((PullResultExt)result).getMessageBinary();return MessageDecoder.decodes(ByteBuffer.wrap(bytes));
    }
    private void ownership()throws Exception{
        if(api.getMinOffset(master,queue,settings.requestTimeoutMillis)!=0)throw reject("ROCKETMQ_HISTORY_EXPIRED_REBUILD_REQUIRED");List<MessageExt> first=read(0,1);
        if(first.isEmpty())throw new Failure("ROCKETMQ_MANIFEST_DISPATCH_PENDING",true,null);
        MessageExt marker=first.get(0);if(marker.getQueueOffset()!=0||!binding.equals(marker.getUserProperty("rivomesh_publication_manifest"))||!Arrays.equals(binding.getBytes(StandardCharsets.UTF_8),marker.getBody()))throw reject("DESTINATION_ALREADY_OWNED");
    }
    public void prepare(boolean allowCreate)throws Exception{
        if(verified)return;if(binding==null)throw new IllegalStateException("PUBLICATION_BINDING_REQUIRED");
        try{
            capabilities();
            if(controller==null)verifyProgress();
            TopicConfigAndQueueMapping config=configuration(master);
            if(config==null){
                if(controller!=null){
                    if(!allowCreate||restoredProgress)throw metadataPending();
                    for(RocketControllerSafety.Replica replica:controllerRoute.members()){
                        if(configuration(replica.address())!=null
                                ||api.getMaxOffset(replica.address(),queue,settings.requestTimeoutMillis)!=0)throw metadataPending();
                    }
                    requireSameControllerRoute(controllerRoute);
                }else if(!allowCreate||restoredProgress)throw reject("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");
                api.createTopic(master,"TBW102",new TopicConfig(topic,1,1,6),settings.requestTimeoutMillis);
                config=configuration(master);
            }
            if(controller!=null){
                verifyControllerTopic();
                verifyProgress();
            }else verifyTopic(config);
            long maximum=api.getMaxOffset(master,queue,settings.requestTimeoutMillis);
            if(maximum==0&&confirmedOffset<0){
                if(!allowCreate||restoredProgress)throw reject("ROCKETMQ_MANIFEST_MISSING_REBUILD_REQUIRED");
                // Only deterministic, generation-bound, reserved names can finish an interrupted create.
                Message marker=new Message(topic,binding.getBytes(StandardCharsets.UTF_8));marker.putUserProperty("rivomesh_publication_manifest",binding);confirmed(marker);
            }
            ownership();verified=true;
        }catch(Exception failure){throw classify(failure);}
    }
    private void confirmed(Message message)throws Exception{
        message.setWaitStoreMsgOK(true);
        RocketControllerSafety.Route sentRoute=null;
        SendResult result;
        if(controller==null)result=producer.send(message,queue,settings.requestTimeoutMillis);
        else{
            capabilities();
            verifyControllerTopic();
            verifyProgress();
            if(confirmedHash!=null)ownership();
            sentRoute=controllerRoute;
            requireSameControllerRoute(sentRoute);
            try{
                result=sendToMaster(message,sentRoute.master());
            }catch(MQBrokerException rejected){
                if(rejected.getResponseCode()==ResponseCode.NOT_LEADER_FOR_QUEUE
                        ||rejected.getResponseCode()==ResponseCode.TOPIC_NOT_EXIST)
                    throw RocketControllerSafety.routeRetry();
                throw rejected;
            }
        }
        if(result.getSendStatus()!=SendStatus.SEND_OK)throw new Failure("ROCKETMQ_"+result.getSendStatus().name(),true,null);
        MessageQueue confirmed=result.getMessageQueue();
        if(confirmed==null||!queue.equals(confirmed)||result.getOffsetMsgId()==null)
            throw reject("ROCKETMQ_CONFIRMATION_POSITION_INVALID");
        long physical=MessageDecoder.decodeMessageId(result.getOffsetMsgId()).getOffset(), offset=result.getQueueOffset();
        if(offset<=confirmedOffset||physical<=confirmedPhysical)throw reject("ROCKETMQ_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");
        String hash=contentHash(message);
        if(sentRoute!=null){
            verifyControllerReceipt(sentRoute,physical,offset,hash);
            verifyControllerTopic();
            requireSameControllerRoute(sentRoute);
        }
        confirmedOffset=offset;
        confirmedPhysical=physical;
        confirmedHash=hash;
    }

    private void requireSameControllerRoute(RocketControllerSafety.Route expected)throws Exception{
        capabilities();
        if(!expected.equals(controllerRoute))throw RocketControllerSafety.routeRetry();
    }

    private void verifyControllerReceipt(RocketControllerSafety.Route route,long physical,long offset,String hash)throws Exception{
        try{
            MessageExt stored=api.viewMessage(route.master(),topic,physical,settings.requestTimeoutMillis);
            if(stored==null)throw new Failure("ROCKETMQ_CONTROLLER_RECEIPT_RETRY",true,null);
            if(!matches(stored,physical,offset,hash))throw reject("ROCKETMQ_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");
        }catch(MQBrokerException unavailable){
            int response=unavailable.getResponseCode();
            if(response==ResponseCode.QUERY_NOT_FOUND||response==ResponseCode.SYSTEM_ERROR)
                throw new Failure("ROCKETMQ_CONTROLLER_RECEIPT_RETRY",true,unavailable);
            throw unavailable;
        }
    }

    private SendResult sendToMaster(Message message,String address)throws Exception{
        Validators.checkMessage(message,producer);
        MessageClientIDSetter.setUniqID(message);
        SendMessageRequestHeader request=new SendMessageRequestHeader();
        request.setProducerGroup(producer.getProducerGroup());
        request.setTopic(topic);
        request.setDefaultTopic(producer.getCreateTopicKey());
        request.setDefaultTopicQueueNums(producer.getDefaultTopicQueueNums());
        request.setQueueId(0);
        request.setSysFlag(0);
        request.setBornTimestamp(System.currentTimeMillis());
        request.setFlag(message.getFlag());
        request.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
        request.setReconsumeTimes(0);
        request.setUnitMode(false);
        request.setBatch(false);
        request.setBrokerName(settings.broker);
        return api.sendMessage(address,settings.broker,message,request,settings.requestTimeoutMillis,
                CommunicationMode.SYNC,null,producer.getDefaultMQProducerImpl());
    }
    public void publish(List<Publication> messages)throws Exception{
        if(!verified)throw new IllegalStateException("PUBLICATION_NOT_PREPARED");
        try{
            // Recheck resource identity and durability per batch; no SDK automatic topic creation.
            if(controller==null){capabilities();verifyTopic(configuration(master));verifyProgress();ownership();}
            for(Publication publication:messages){Message message=new Message(topic,publication.body);message.setKeys(publication.id);for(Map.Entry<String,String> entry:publication.headers.entrySet())message.putUserProperty(entry.getKey(),entry.getValue());message.putUserProperty("rivomesh_event_id",publication.id);message.putUserProperty("rivomesh_channel",publication.channel);if(publication.key!=null)message.putUserProperty("rivomesh_key",publication.key);confirmed(message);}
        }catch(Exception failure){throw classify(failure);}
    }
    private static Exception classify(Exception failure){
        if(failure instanceof Failure)return failure;
        if(failure instanceof RemotingException)return new Failure("ROCKETMQ_CONNECTION_RETRY",true,failure);
        if(failure instanceof MQBrokerException){int code=((MQBrokerException)failure).getResponseCode();boolean retry=code==ResponseCode.SYSTEM_ERROR||code==ResponseCode.SYSTEM_BUSY||code==ResponseCode.SERVICE_NOT_AVAILABLE;return new Failure("ROCKETMQ_BROKER_"+code,retry,failure);}
        if(failure instanceof MQClientException){Throwable cause=failure.getCause();if(cause instanceof RemotingException)return new Failure("ROCKETMQ_CONNECTION_RETRY",true,failure);return new Failure("ROCKETMQ_CLIENT_"+((MQClientException)failure).getResponseCode(),false,failure);}
        return failure;
    }
    @Override public void close(){producer.shutdown();}
}
