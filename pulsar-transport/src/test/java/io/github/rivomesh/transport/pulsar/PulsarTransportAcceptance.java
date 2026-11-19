package io.github.rivomesh.transport.pulsar;

import org.apache.pulsar.client.admin.*;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Executed against the actual shaded artifacts and a real broker, never included in the release. */
public final class PulsarTransportAcceptance {
    interface Checked {void run()throws Exception;}
    static void require(boolean condition,String message){if(!condition)throw new AssertionError(message);}
    static void retry(Checked call)throws Exception{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);Exception last=null;
        while(System.nanoTime()<deadline){try{call.run();return;}catch(PulsarTransport.Failure failure){if(!failure.retryable)throw failure;last=failure;}catch(PulsarAdminException failure){if(failure.getStatusCode()<500)throw failure;last=failure;}Thread.sleep(250);}throw new AssertionError("Pulsar operation did not recover",last);
    }
    static PulsarTransport.Publication publication(String id){PulsarTransport.Publication p=new PulsarTransport.Publication();p.id=id;p.channel="sales.items";p.key="key-"+id;p.body=("{\"id\":\""+id+"\"}").getBytes(StandardCharsets.UTF_8);return p;}
    static List<String> read(PulsarTransport.Settings settings)throws Exception{
        ClientBuilder builder=PulsarClient.builder().serviceUrl(settings.serviceUrl).operationTimeout(10,TimeUnit.SECONDS)
                .enableTlsHostnameVerification(true).allowTlsInsecureConnection(false);
        if(settings.token!=null)builder.authentication(AuthenticationFactory.token(settings.token));
        if(settings.tlsTrustCertsFilePath!=null)builder.tlsTrustCertsFilePath(settings.tlsTrustCertsFilePath);
        List<String> result=new ArrayList<>();try(PulsarClient client=builder.build();Reader<byte[]> reader=client.newReader(Schema.BYTES).topic(settings.topic).startMessageId(MessageId.earliest).create()){
            for(int count=0;count<20;count++){Message<byte[]> message=reader.readNext(2,TimeUnit.SECONDS);if(message==null)break;require("sales.items".equals(message.getProperty("rivomesh_channel")),"Logical channel missing");require(("key-"+message.getProperty("rivomesh_event_id")).equals(message.getKey()),"Publication key changed");require(new String(message.getValue(),StandardCharsets.UTF_8).equals("{\"id\":\""+message.getProperty("rivomesh_event_id")+"\"}"),"Immutable body changed");result.add(message.getProperty("rivomesh_event_id"));}
        }return result;
    }
    private static void rejected(PulsarTransport.Settings settings,String code)throws Exception{
        try{PulsarTransport.check(settings);throw new AssertionError("Invalid secure connection was accepted");}
        catch(PulsarTransport.Failure refused){require(!refused.retryable&&code.equals(refused.code),"Expected "+code+" but received "+refused.code);}
    }
    private static void tls()throws Exception{
        PulsarTransport.Settings settings=new PulsarTransport.Settings();
        settings.serviceUrl="pulsar+ssl://pulsar:6651";
        settings.adminUrl="https://pulsar:8443";
        settings.cluster="standalone";
        settings.topic="persistent://public/default/rivomesh_tls_"+UUID.randomUUID().toString().replace("-","");
        settings.connectTimeoutMillis=2500;
        settings.requestTimeoutMillis=6000;
        settings.tlsTrustCertsFilePath="/fixture/ca.pem";
        settings.token=new String(Files.readAllBytes(Paths.get("/fixture/token")),StandardCharsets.US_ASCII).trim();
        String validToken=settings.token;
        settings.token=null;
        rejected(settings,"BROKER_AUTHENTICATION_REJECTED");
        settings.token=validToken;
        settings.tlsTrustCertsFilePath="/fixture/untrusted.pem";
        rejected(settings,"BROKER_TLS_REJECTED");
        settings.tlsTrustCertsFilePath="/fixture/ca.pem";
        settings.adminUrl="https://pulsar-other:8443";
        rejected(settings,"BROKER_TLS_REJECTED");
        settings.adminUrl="https://pulsar:8443";
        PulsarTransport.check(settings);
        System.out.println("PASS Pulsar TLS rejects absent authentication, untrusted CA and a mismatched hostname");
        String binding="tls-publication";
        try(PulsarTransport transport=new PulsarTransport(settings,binding)){
            retry(()->transport.prepare(true));
            transport.publish(Collections.singletonList(publication("first")));
            require(read(settings).equals(Collections.singletonList("first")),"TLS publication differs");
        }
        System.out.println("PASS Native TLS and JWT persist and read the exact Pulsar message");
        Path marker=Paths.get(System.getenv("RIVOMESH_PULSAR_RESTART_DIR"));
        Files.write(marker.resolve("restart.request"),new byte[]{1});
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(120);
        while(!Files.exists(marker.resolve("restart.done"))&&System.nanoTime()<deadline)Thread.sleep(100);
        require(Files.exists(marker.resolve("restart.done")),"TLS broker restart not executed");
        try(PulsarTransport transport=new PulsarTransport(settings,binding)){
            retry(()->transport.prepare(false));
            transport.publish(Collections.singletonList(publication("second")));
            require(read(settings).equals(Arrays.asList("first","second")),"TLS restart changed durable history");
        }
        System.out.println("PASS Trusted TLS reconnect keeps the existing topic identity and exact confirmed history");
    }
    private static PulsarTransport.Settings quorumSettings(String broker,String topic){
        PulsarTransport.Settings settings=new PulsarTransport.Settings();
        settings.serviceUrl="pulsar://"+broker+":6650";
        settings.adminUrl="http://"+broker+":8080";
        settings.cluster="local-quorum";
        settings.topic=topic;
        settings.replicas=2;
        settings.connectTimeoutMillis=2500;
        settings.requestTimeoutMillis=8000;
        return settings;
    }
    private static void stage(Path markers,String name)throws Exception{
        Files.createFile(markers.resolve(name+".request"));
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(130);
        while(!Files.exists(markers.resolve(name+".ready"))&&System.nanoTime()<deadline)Thread.sleep(100);
        require(Files.exists(markers.resolve(name+".ready")),"Fault coordination timed out: "+name);
    }
    private static void quorum()throws Exception{
        Path markers=Paths.get(System.getenv("RIVOMESH_PULSAR_RESTART_DIR"));
        String topic="persistent://public/default/rivomesh-quorum";
        String binding="bookkeeper-quorum-acceptance";
        PulsarTransport.Settings primary=quorumSettings("broker-a",topic);
        try(PulsarTransport transport=new PulsarTransport(primary,binding)){
            retry(()->transport.prepare(true));
            transport.publish(Collections.singletonList(publication("first")));
        }
        require(read(primary).equals(Collections.singletonList("first")),"Initial confirmation missing");
        System.out.println("PASS An active broker confirms a message with the configured two-BookKeeper quorum");
        stage(markers,"second-broker");
        PulsarTransport.Settings stable=quorumSettings("broker-b",topic);
        try(PulsarAdmin admin=PulsarAdmin.builder().serviceHttpUrl(stable.adminUrl).build()){
            require(admin.brokers().getActiveBrokers(stable.cluster).size()==2,"Expected two live brokers");
            require("pulsar://broker-a:6650".equals(admin.lookups().lookupTopic(topic)),"Initial topic owner differs");
            Map<String,String> identity=admin.topics().getProperties(topic);
            require(binding.equals(identity.get(PulsarTransport.OWNER))&&"READY_V1".equals(identity.get(PulsarTransport.PROVISION)),
                    "Topic ownership or provisioning proof missing");
            org.apache.pulsar.common.policies.data.PersistencePolicies persistence=admin.topicPolicies().getPersistence(topic,true);
            require(persistence.getBookkeeperEnsemble()==2&&persistence.getBookkeeperWriteQuorum()==2
                    &&persistence.getBookkeeperAckQuorum()==2,"The topic does not require two bookies");
            boolean confirmedAfterBrokerFailure=false;
            try(PulsarTransport original=new PulsarTransport(stable,binding)){
                retry(()->original.prepare(false));
                stage(markers,"kill-owner");
                long transferDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
                String owner=null;
                while(System.nanoTime()<transferDeadline){
                    try{owner=admin.lookups().lookupTopic(topic);}
                    catch(PulsarAdminException transientLookup){
                        if(transientLookup.getStatusCode()<500)throw transientLookup;
                    }
                    if("pulsar://broker-b:6650".equals(owner))break;
                    Thread.sleep(250);
                }
                require("pulsar://broker-b:6650".equals(owner),"New topic owner differs");
                try{
                    original.publish(Collections.singletonList(publication("after-broker")));
                    confirmedAfterBrokerFailure=true;
                }catch(PulsarTransport.Failure staleProducer){
                    require(staleProducer.retryable,"Owner loss did not report a retryable failure: "+staleProducer.code);
                }
            }
            List<String> confirmedHistory;
            try(PulsarTransport transport=new PulsarTransport(stable,binding)){
                retry(()->transport.prepare(false));
                if(!confirmedAfterBrokerFailure)transport.publish(Collections.singletonList(publication("after-broker")));
                confirmedHistory=read(stable);
                require(confirmedHistory.size()>=2&&"first".equals(confirmedHistory.get(0))
                        &&confirmedHistory.subList(1,confirmedHistory.size()).stream().allMatch("after-broker"::equals),
                        "Broker handoff changed confirmed history or the pending stable identity");
                require(identity.equals(admin.topics().getProperties(topic)),"Topic binding changed after broker handoff");
                System.out.println("PASS After the owning broker is killed the retry path confirms at the original topic binding through the survivor");
                stage(markers,"lose-quorum");
                try{
                    transport.publish(Collections.singletonList(publication("quorum-retry")));
                    throw new AssertionError("Publication confirmed below the BookKeeper quorum");
                }catch(PulsarTransport.Failure failure){
                    require(failure.retryable,"BookKeeper disruption was not classified as retryable: "+failure.code);
                }
                System.out.println("PASS One surviving bookie cannot confirm a publication requiring two acknowledgements");
            }
            stage(markers,"restore-quorum");
            try(PulsarTransport reopened=new PulsarTransport(stable,binding)){
                retry(()->reopened.prepare(false));
                reopened.publish(Collections.singletonList(publication("quorum-retry")));
            }
            List<String> history=read(stable);
            require(history.size()>confirmedHistory.size()
                    &&history.subList(0,confirmedHistory.size()).equals(confirmedHistory)
                    &&history.subList(confirmedHistory.size(),history.size()).stream().allMatch("quorum-retry"::equals),
                    "Recovery changed a confirmed message or the retried event identity");
            require(identity.equals(admin.topics().getProperties(topic)),"Recovery recreated or changed the topic binding");
            System.out.println("PASS Restarted bookies recover exact confirmed history and the pending stable event identity");
        }
    }
    public static void main(String[] args)throws Exception{
        if(args.length==1&&"tls".equals(args[0])){tls();return;}
        if(args.length==1&&"quorum".equals(args[0])){quorum();return;}
        require(Class.forName("org.slf4j.Logger").getProtectionDomain().getCodeSource().getLocation().toString().contains("rivomesh-app"),"Core logging API was replaced");
        Class.forName("io.github.rivomesh.pulsar.internal.slf4j.Logger").getMethod("atDebug");
        Class.forName("com.fasterxml.jackson.annotation.JsonProperty");Class.forName("io.github.rivomesh.pulsar.internal.jackson.annotation.JsonProperty");
        Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider");Class.forName("io.github.rivomesh.pulsar.internal.bouncycastle.jce.provider.BouncyCastleProvider");
        Exception timeout=PulsarTransport.classify(new PulsarAdminException(new java.util.concurrent.CompletionException(new java.net.SocketTimeoutException("secret-not-for-diagnostics"))));
        require(timeout instanceof PulsarTransport.Failure&&((PulsarTransport.Failure)timeout).retryable&&"PULSAR_ADMIN_TIMEOUT".equals(((PulsarTransport.Failure)timeout).code),"Wrapped client timeout mislabeled");
        Exception tls=PulsarTransport.classify(new PulsarAdminException.ConnectException(new javax.net.ssl.SSLHandshakeException("secret-not-for-diagnostics")));
        require(tls instanceof PulsarTransport.Failure&&!((PulsarTransport.Failure)tls).retryable&&"BROKER_TLS_REJECTED".equals(((PulsarTransport.Failure)tls).code),"TLS rejection treated as transient connection failure");
        Exception http=PulsarTransport.classify(new PulsarAdminException.ServerSideErrorException(null,"server error","server error",500));
        require(http instanceof PulsarTransport.Failure&&((PulsarTransport.Failure)http).retryable&&"PULSAR_ADMIN_HTTP_500".equals(((PulsarTransport.Failure)http).code),"Actual server status lost");
        PulsarTransport.Settings settings=new PulsarTransport.Settings();settings.serviceUrl="pulsar://pulsar:6650";settings.adminUrl="http://pulsar:8080";settings.cluster="standalone";settings.topic="persistent://public/default/rivomesh_"+UUID.randomUUID().toString().replace("-","");settings.requestTimeoutMillis=5000;
        PulsarTransport.Settings unreachable=new PulsarTransport.Settings();unreachable.serviceUrl=settings.serviceUrl;unreachable.adminUrl="http://127.0.0.1:9";unreachable.cluster=settings.cluster;unreachable.topic=settings.topic;unreachable.connectTimeoutMillis=500;unreachable.requestTimeoutMillis=1000;
        try{PulsarTransport.check(unreachable);throw new AssertionError("Unreachable admin accepted");}catch(PulsarTransport.Failure refused){require(refused.retryable&&Arrays.asList("PULSAR_ADMIN_CONNECT_RETRY","PULSAR_ADMIN_TIMEOUT").contains(refused.code),"Client connection failure mislabeled as HTTP: "+refused.code);}
        String binding="isolated-publication";Path marker=Paths.get(System.getenv("RIVOMESH_PULSAR_RESTART_DIR"));
        try(PulsarAdmin admin=PulsarAdmin.builder().serviceHttpUrl(settings.adminUrl).build()){
            List<String> before=admin.topics().getList("public/default");PulsarTransport.check(settings);require(before.equals(admin.topics().getList("public/default")),"Check created resources");
            try(PulsarTransport transport=new PulsarTransport(settings,binding)){
                retry(()->transport.prepare(true));transport.publish(Collections.singletonList(publication("first")));require(read(settings).equals(Collections.singletonList("first")),"Confirmed event missing");
                try(PulsarTransport other=new PulsarTransport(settings,"other-owner")){try{other.prepare(false);throw new AssertionError("Ownership was ignored");}catch(PulsarTransport.Failure denied){require("DESTINATION_ALREADY_OWNED".equals(denied.code),"Wrong ownership error");}}
                Files.write(marker.resolve("restart.request"),new byte[]{1});long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);while(!Files.exists(marker.resolve("restart.done"))&&System.nanoTime()<deadline)Thread.sleep(100);require(Files.exists(marker.resolve("restart.done")),"Broker restart not executed");
                retry(()->transport.publish(Collections.singletonList(publication("second"))));List<String> restored=read(settings);require(restored.contains("first")&&restored.contains("second"),"Broker restart lost confirmed history");
            }
            admin.topicPolicies().setRetention(settings.topic,new RetentionPolicies(1,1));
            long changed=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);while(admin.topicPolicies().getRetention(settings.topic,true).getRetentionTimeInMinutes()!=1&&System.nanoTime()<changed)Thread.sleep(100);
            try(PulsarTransport incompatible=new PulsarTransport(settings,binding)){try{incompatible.prepare(false);throw new AssertionError("Unsafe retention was ignored");}catch(PulsarTransport.Failure denied){require("PULSAR_TOPIC_CONFIGURATION_REQUIRES_PLAN".equals(denied.code),"Wrong policy error");}}
            admin.topics().delete(settings.topic,true);
            try(PulsarTransport lost=new PulsarTransport(settings,binding)){try{lost.prepare(false);throw new AssertionError("Missing initialized topic was recreated");}catch(PulsarTransport.Failure denied){require("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED".equals(denied.code),"Wrong missing-resource error");}}require(!admin.topics().getList("public/default").contains(settings.topic),"Deleted topic was recreated");
            System.out.println("PULSAR_TRANSPORT_ACCEPTED: private SDK, read-only check, binding, confirmation, broker restart, retention refusal, lost-topic protection");
        }finally{/* Compose removes this test's isolated broker storage. */}
    }
}
