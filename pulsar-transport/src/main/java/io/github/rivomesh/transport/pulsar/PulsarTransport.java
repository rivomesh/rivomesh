package io.github.rivomesh.transport.pulsar;

import org.apache.pulsar.client.admin.*;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Java-only boundary; SDK logging, crypto, protobuf and Jackson annotations are shaded privately. */
public final class PulsarTransport implements AutoCloseable {
    public static final String OWNER="rivomesh.owner",PROVISION="rivomesh.provision";
    public static final class Settings {
        public String serviceUrl,adminUrl,cluster,topic,token;
        public int connectTimeoutMillis=5000,requestTimeoutMillis=10000,replicas=1;
    }
    public static final class Publication {
        public String id,channel,key;
        public byte[] body;
        public Map<String,String> headers=new LinkedHashMap<>();
    }
    public static final class Failure extends Exception {
        public final String code;public final boolean retryable;
        Failure(String code,boolean retryable,Throwable cause){super(code,cause);this.code=code;this.retryable=retryable;}
    }
    private final Settings settings;
    private final String binding,namespace;
    private final PulsarAdmin admin;
    private final PulsarClient client;
    private Producer<byte[]> producer;
    private boolean verified;
    public PulsarTransport(Settings settings,String binding)throws Exception{
        this.settings=settings;this.binding=binding;validate(settings);String[] topic=settings.topic.substring("persistent://".length()).split("/");namespace=topic[0]+"/"+topic[1];
        admin=admin(settings.adminUrl,settings);
        try{ClientBuilder builder=PulsarClient.builder().serviceUrl(settings.serviceUrl).connectionTimeout(settings.connectTimeoutMillis,TimeUnit.MILLISECONDS).operationTimeout(settings.requestTimeoutMillis,TimeUnit.MILLISECONDS).lookupTimeout(settings.requestTimeoutMillis,TimeUnit.MILLISECONDS).enableTlsHostnameVerification(true).allowTlsInsecureConnection(false).ioThreads(1).listenerThreads(1).statsInterval(0,TimeUnit.SECONDS).memoryLimit(16,org.apache.pulsar.client.api.SizeUnit.MEGA_BYTES);if(settings.token!=null&&!settings.token.isEmpty())builder.authentication(AuthenticationFactory.token(settings.token));client=builder.build();}
        catch(Exception failure){admin.close();throw classify(failure);}
    }
    private static void validate(Settings s){
        if(s==null||s.topic==null||!s.topic.matches("persistent://[A-Za-z0-9][A-Za-z0-9_-]{0,62}/[A-Za-z0-9][A-Za-z0-9_-]{0,62}/[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||s.topic.contains("-partition-")||s.cluster==null||!s.cluster.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")||s.replicas<1||s.replicas>5||s.connectTimeoutMillis<100||s.connectTimeoutMillis>120000||s.requestTimeoutMillis<100||s.requestTimeoutMillis>120000)throw new IllegalArgumentException("PULSAR_CONFIG_INVALID");
        URI service=URI.create(s.serviceUrl),http=URI.create(s.adminUrl);if(!Arrays.asList("pulsar","pulsar+ssl").contains(service.getScheme())||!Arrays.asList("http","https").contains(http.getScheme())||service.getHost()==null||http.getHost()==null||service.getUserInfo()!=null||http.getUserInfo()!=null||service.getQuery()!=null||http.getQuery()!=null||service.getFragment()!=null||http.getFragment()!=null||service.getPath()!=null&&!service.getPath().isEmpty()||http.getPath()!=null&&!http.getPath().isEmpty()&&!http.getPath().equals("/")||service.getScheme().equals("pulsar+ssl")!=http.getScheme().equals("https"))throw new IllegalArgumentException("PULSAR_ENDPOINT_INVALID");
    }
    private static PulsarAdmin admin(String url,Settings settings)throws Exception{
        try{
        PulsarAdminBuilder builder=PulsarAdmin.builder().serviceHttpUrl(url).connectionTimeout(settings.connectTimeoutMillis,TimeUnit.MILLISECONDS).readTimeout(settings.requestTimeoutMillis,TimeUnit.MILLISECONDS).requestTimeout(settings.requestTimeoutMillis,TimeUnit.MILLISECONDS).enableTlsHostnameVerification(true).allowTlsInsecureConnection(false);
        if(settings.token!=null&&!settings.token.isEmpty())builder.authentication(AuthenticationFactory.token(settings.token));return builder.build();
        }catch(Exception failure){throw classify(failure);}
    }
    private static Failure reject(String code){return new Failure(code,false,null);}
    /** Read capabilities only; no producers, topics, subscriptions or policies are created. */
    public static void check(Settings settings)throws Exception{try(PulsarTransport transport=new PulsarTransport(settings,null)){transport.capabilities();}}
    private void capabilities()throws Exception{
        try{
            if(!admin.clusters().getClusters().contains(settings.cluster))throw reject("PULSAR_CLUSTER_NOT_FOUND");
            List<String> clusters=admin.namespaces().getNamespaceReplicationClusters(namespace);if(clusters.size()!=1||!clusters.contains(settings.cluster))throw reject("PULSAR_GEO_REPLICATION_REQUIRES_PLAN");
            AutoTopicCreationOverride override=admin.namespaces().getAutoTopicCreation(namespace);if(override!=null&&override.isAllowAutoTopicCreation())throw reject("PULSAR_AUTO_CREATION_MUST_BE_DISABLED");
            List<String> brokers=admin.brokers().getActiveBrokers(settings.cluster);if(brokers.isEmpty())throw new Failure("PULSAR_BROKER_UNAVAILABLE",true,null);if(brokers.size()>1000)throw reject("PULSAR_BROKER_INVENTORY_LIMIT");
            String scheme=URI.create(settings.adminUrl).getScheme();
            for(String broker:brokers){
                URI address=URI.create(broker.contains("://")?broker:scheme+"://"+broker);if(!scheme.equals(address.getScheme())||address.getHost()==null||address.getUserInfo()!=null||address.getQuery()!=null||address.getFragment()!=null)throw reject("PULSAR_BROKER_ADMIN_ENDPOINT_REJECTED");
                try(PulsarAdmin node=admin(address.toString(),settings)){
                    Map<String,String> runtime=node.brokers().getRuntimeConfigurations();if(!"false".equals(runtime.get("allowAutoTopicCreation")))throw reject("PULSAR_AUTO_CREATION_MUST_BE_DISABLED");
                    if(!"false".equals(runtime.get("brokerDeleteInactiveTopicsEnabled")))throw reject("PULSAR_INACTIVE_DELETION_MUST_BE_DISABLED");
                    if(!"true".equals(runtime.get("topicLevelPoliciesEnabled")))throw reject("PULSAR_TOPIC_POLICIES_REQUIRED");
                }
            }
        }catch(Exception failure){throw classify(failure);}
    }
    public void prepare(boolean allowCreate)throws Exception{
        if(verified)return;if(binding==null)throw new IllegalStateException("PUBLICATION_BINDING_REQUIRED");
        try{
            capabilities();Map<String,String> properties;
            try{properties=admin.topics().getProperties(settings.topic);}
            catch(PulsarAdminException.NotFoundException absent){
                if(!allowCreate)throw reject("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");Map<String,String> initial=new HashMap<>();initial.put(OWNER,binding);initial.put(PROVISION,"BUILDING_V1");
                try{admin.topics().createNonPartitionedTopic(settings.topic,initial);}catch(PulsarAdminException.ConflictException raced){/* Re-read binding; never adopt another publisher's topic. */}
                properties=admin.topics().getProperties(settings.topic);
            }
            if(!binding.equals(properties.get(OWNER)))throw reject(properties.containsKey(OWNER)?"DESTINATION_ALREADY_OWNED":"MESSAGE_DESTINATION_UNOWNED_REQUIRES_PLAN");
            if(admin.topics().getPartitionedTopicMetadata(settings.topic).partitions!=0)throw reject("PULSAR_SINGLE_TOPIC_REQUIRED");
            TopicPolicies policies=admin.topicPolicies();boolean building="BUILDING_V1".equals(properties.get(PROVISION));
            if(building){
                if(!allowCreate)throw reject("PULSAR_PROVISIONING_INCOMPLETE");
                policies.setRetention(settings.topic,new RetentionPolicies(-1,-1));policies.setMessageTTL(settings.topic,0);policies.setInactiveTopicPolicies(settings.topic,new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,0,false));
                policies.setPersistence(settings.topic,new PersistencePolicies(settings.replicas,settings.replicas,settings.replicas,0));
                policies.setBacklogQuota(settings.topic,BacklogQuota.builder().limitSize(-1).limitTime(-1).retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception).build(),BacklogQuota.BacklogQuotaType.destination_storage);
                policies.setBacklogQuota(settings.topic,BacklogQuota.builder().limitSize(-1).limitTime(-1).retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception).build(),BacklogQuota.BacklogQuotaType.message_age);
            }else if(!"READY_V1".equals(properties.get(PROVISION)))throw reject("PULSAR_TOPIC_CONFIGURATION_REQUIRES_PLAN");
            try{verifyPolicies(policies);}catch(Failure policy){if(building)throw new Failure("PULSAR_POLICY_PROPAGATION_PENDING",true,policy);throw policy;}
            if(building){Map<String,String> complete=new HashMap<>(properties);complete.put(PROVISION,"READY_V1");admin.topics().updateProperties(settings.topic,complete);}
            producer=client.newProducer(Schema.BYTES).topic(settings.topic).enableBatching(false).enableChunking(false).maxPendingMessages(100).blockIfQueueFull(false).sendTimeout(settings.requestTimeoutMillis,TimeUnit.MILLISECONDS).create();verified=true;
        }catch(Exception failure){throw classify(failure);}
    }
    private void verifyPolicies(TopicPolicies policies)throws Exception{
        RetentionPolicies retention=policies.getRetention(settings.topic,true);Integer ttl=policies.getMessageTTL(settings.topic,true);InactiveTopicPolicies inactive=policies.getInactiveTopicPolicies(settings.topic,true);PersistencePolicies persistence=policies.getPersistence(settings.topic,true);
        if(retention==null||retention.getRetentionTimeInMinutes()!=-1||retention.getRetentionSizeInMB()!=-1||ttl==null||ttl!=0||inactive==null||inactive.isDeleteWhileInactive()||persistence==null||persistence.getBookkeeperAckQuorum()<settings.replicas||persistence.getBookkeeperWriteQuorum()<persistence.getBookkeeperAckQuorum()||persistence.getBookkeeperEnsemble()<persistence.getBookkeeperWriteQuorum())throw reject("PULSAR_TOPIC_CONFIGURATION_REQUIRES_PLAN");
        Map<BacklogQuota.BacklogQuotaType,BacklogQuota> quotas=policies.getBacklogQuotaMap(settings.topic,true);if(quotas==null||quotas.isEmpty())throw reject("PULSAR_BACKLOG_POLICY_REQUIRES_PLAN");for(BacklogQuota quota:quotas.values())if(quota.getPolicy()==BacklogQuota.RetentionPolicy.consumer_backlog_eviction)throw reject("PULSAR_BACKLOG_POLICY_REQUIRES_PLAN");
    }
    public void publish(List<Publication> messages)throws Exception{
        if(!verified||producer==null)throw new IllegalStateException("PUBLICATION_NOT_PREPARED");
        try{for(Publication message:messages){Map<String,String> properties=new LinkedHashMap<>(message.headers);properties.put("rivomesh_event_id",message.id);properties.put("rivomesh_channel",message.channel);TypedMessageBuilder<byte[]> builder=producer.newMessage().properties(properties).value(message.body);if(message.key!=null)builder.key(message.key);builder.send();}}
        catch(Exception failure){throw classify(failure);}
    }
    static Exception classify(Exception failure){
        if(failure instanceof Failure)return failure;
        if(failure instanceof PulsarAdminException){
            // Generic SDK wrappers default to 500 even without an HTTP response.
            boolean timeout=false,connection=false,authentication=false,tls=false;
            Set<Throwable> seen=Collections.newSetFromMap(new IdentityHashMap<Throwable,Boolean>());
            for(Throwable cause=failure;cause!=null&&seen.size()<16&&seen.add(cause);cause=cause.getCause()){
                timeout|=cause instanceof PulsarAdminException.TimeoutException||cause instanceof java.util.concurrent.TimeoutException||cause instanceof java.net.SocketTimeoutException;
                connection|=cause instanceof PulsarAdminException.ConnectException||cause instanceof java.net.ConnectException||cause instanceof java.net.UnknownHostException||cause instanceof java.net.SocketException;
                authentication|=cause instanceof PulsarAdminException.GettingAuthenticationDataException;
                tls|=cause instanceof javax.net.ssl.SSLException;
            }
            if(authentication)return new Failure("BROKER_AUTHENTICATION_REJECTED",false,failure);
            if(tls)return new Failure("BROKER_TLS_REJECTED",false,failure);
            if(timeout)return new Failure("PULSAR_ADMIN_TIMEOUT",true,failure);
            if(connection)return new Failure("PULSAR_ADMIN_CONNECT_RETRY",true,failure);
            PulsarAdminException adminFailure=(PulsarAdminException)failure;int code=adminFailure.getStatusCode();
            if(failure.getClass()==PulsarAdminException.class&&adminFailure.getHttpError()==null)return new Failure("PULSAR_ADMIN_REQUEST_FAILED",code>=500,failure);
            return new Failure(code==401||code==403?"BROKER_AUTHENTICATION_REJECTED":code==404?"MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED":"PULSAR_ADMIN_HTTP_"+code,code==408||code==409||code==429||code>=500,failure);
        }
        if(failure instanceof PulsarClientException){boolean retry=PulsarClientException.isRetriableError(failure)&&!(failure instanceof PulsarClientException.AuthenticationException);return new Failure(failure instanceof PulsarClientException.TopicDoesNotExistException?"MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED":retry?"PULSAR_RETRY":"PULSAR_PUBLICATION_REJECTED",retry,failure);}
        return failure;
    }
    @Override public void close()throws Exception{Exception problem=null;try{if(producer!=null)producer.close();}catch(Exception failure){problem=failure;}try{client.close();}catch(Exception failure){if(problem==null)problem=failure;else problem.addSuppressed(failure);}finally{admin.close();}if(problem!=null)throw classify(problem);}
}
