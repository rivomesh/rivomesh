package io.github.rivomesh.acceptance;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.Sql;
import io.github.rivomesh.cdc.ConfirmedPublisher;
import io.github.rivomesh.cdc.MessagePublishers;
import io.github.rivomesh.cdc.TargetProduct;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StreamInfo;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public final class ReplicatedJetStreamAcceptance {
    private static final String[] ENDPOINTS={"tls://nats-a:4222","tls://nats-b:4222","tls://nats-c:4222"};
    private static final String STREAM="RIVOMESH_NATIVE";
    private static final String BINDING=MessagePublishers.binding("pipeline","js","generation");
    private static final String SUBJECT="orders.events";
    private static final String TOKEN=System.getenv("RIVOMESH_NATS_FIXTURE_TOKEN");

    static PipelineConfig.Target target(){
        PipelineConfig.Target target=new PipelineConfig.Target();
        target.product=TargetProduct.NATS_JETSTREAM;
        target.id="js";
        target.url=ENDPOINTS[0];
        target.mappings.put("sales","orders");
        target.messaging=new PipelineConfig.Messaging();
        target.messaging.endpoints=Arrays.asList(ENDPOINTS[1],ENDPOINTS[2]);
        target.messaging.token=TOKEN;
        target.messaging.stream=STREAM;
        target.messaging.replicas=3;
        target.messaging.connectTimeoutMillis=2000;
        target.messaging.requestTimeoutMillis=2000;
        return target;
    }

    static ConfirmedPublisher.Message event(int position){
        ConfirmedPublisher.Message event=new ConfirmedPublisher.Message();
        event.id="native-event-"+position;
        event.destination=SUBJECT;
        event.key="orders";
        event.body=("synthetic-content-"+position).getBytes(StandardCharsets.UTF_8);
        return event;
    }

    static Connection inspect()throws Exception{
        return Nats.connect(new Options.Builder().servers(ENDPOINTS).ignoreDiscoveredServers().token(TOKEN.toCharArray())
                .connectionTimeout(Duration.ofSeconds(2)).maxReconnects(0).build());
    }

    static String publish(int position,boolean create,int count)throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(40);
        Exception last=null;
        while(System.nanoTime()<until){
            try(ConfirmedPublisher publisher=MessagePublishers.open(target(),BINDING)){
                if(create)publisher.prepare(Collections.singleton(SUBJECT));
                else publisher.require(Collections.singleton(SUBJECT));
                publisher.publish(Collections.singletonList(event(position)));
                try(Connection check=inspect()){
                    StreamInfo info=check.jetStreamManagement().getStreamInfo(STREAM);
                    if(info.getConfiguration().getReplicas()!=3||info.getStreamState().getMsgCount()!=count)
                        throw new AssertionError("Replicated stream count or replica setting differs");
                    for(int i=1;i<=count;i++){
                        byte[] expected=event(i).body;
                        byte[] actual=check.jetStreamManagement().getMessage(STREAM,i).getData();
                        if(!Arrays.equals(expected,actual))throw new AssertionError("Durable message content changed");
                    }
                    return info.getClusterInfo().getLeader();
                }
            }catch(Exception retry){last=retry;Thread.sleep(250);}
        }
        throw new IllegalStateException("JETSTREAM_LEADER_UNAVAILABLE",last);
    }

    static void refusalWithoutQuorum()throws Exception{
        try(ConfirmedPublisher publisher=MessagePublishers.open(target(),BINDING)){
            publisher.require(Collections.singleton(SUBJECT));
            publisher.publish(Collections.singletonList(event(3)));
        }catch(Exception unavailable){
            if(!Sql.retryable(unavailable))throw unavailable;
            return;
        }
        throw new AssertionError("JetStream confirmed a write without quorum");
    }

    static void refusesCertificate(PipelineConfig.Target target)throws Exception{
        try(ConfirmedPublisher ignored=MessagePublishers.open(target,BINDING)){
            throw new AssertionError("Untrusted cluster certificate was accepted");
        }catch(java.io.IOException rejected){
            // The fixture uses correct credentials and an invalid trust anchor or server name.
        }
    }

    public static void main(String[] args)throws Exception{
        switch(args[0]){
            case "untrusted":refusesCertificate(target());break;
            case "wrong-host":
                PipelineConfig.Target wrongHost=target();
                wrongHost.url="tls://alias-a:4222";
                wrongHost.messaging.endpoints=Collections.emptyList();
                refusesCertificate(wrongHost);
                break;
            case "initial":System.out.println("LEADER "+publish(1,true,1));break;
            case "promoted":System.out.println("LEADER "+publish(2,false,2));break;
            case "no-quorum":refusalWithoutQuorum();break;
            case "restored":
                publish(3,false,3);
                System.out.println("LEADER "+publish(3,false,3));
                break;
            default:throw new IllegalArgumentException("Invalid fixture phase");
        }
    }
}
