package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.transport.rocketmq.RocketTransport;
import java.net.URI;
import java.util.*;

/** Java-only boundary between the fan-out engine and the native RocketMQ SDK. */
public final class RocketMessagePublisher implements ConfirmedPublisher {
    private final PipelineConfig.Target target;private final RocketTransport transport;
    public RocketMessagePublisher(PipelineConfig.Target target,String binding)throws Exception{this.target=target;try{transport=new RocketTransport(settings(target),binding);}catch(Exception failure){throw translated(failure);}}
    private static RocketTransport.Settings settings(PipelineConfig.Target target){
        PipelineConfig.Messaging m=target.messaging;RocketTransport.Settings s=new RocketTransport.Settings();List<String> endpoints=new ArrayList<>();endpoints.add(target.url);endpoints.addAll(m.endpoints);List<String> servers=new ArrayList<>();for(String endpoint:endpoints){URI uri=URI.create(endpoint);String host=uri.getHost();if(host.indexOf(':')>=0&&!host.startsWith("["))host="["+host+"]";servers.add(host+":"+(uri.getPort()<0?9876:uri.getPort()));}s.nameservers=String.join(";",servers);s.cluster=m.cluster;s.broker=m.broker;s.topicPrefix=m.topic;s.minimumRetentionHours=m.minimumRetentionHours;s.maxMessageBytes=m.maxMessageBytes;s.connectTimeoutMillis=m.connectTimeoutMillis;s.requestTimeoutMillis=m.requestTimeoutMillis;return s;
    }
    private static Exception translated(Exception failure){if(failure instanceof RocketTransport.Failure){RocketTransport.Failure error=(RocketTransport.Failure)failure;return new SyncFailure(error.code,"MESSAGE_PUBLICATION",error.retryable,"RocketMQ operation was not confirmed",failure,null,0,null);}return failure;}
    public static void check(PipelineConfig.Target target)throws Exception{try{RocketTransport.check(settings(target));}catch(Exception failure){throw translated(failure);}}
    @Override public boolean requiresProgress(){return true;}
    @Override public void resumeProgress(String progress)throws Exception{try{transport.resumeProgress(progress);}catch(Exception failure){throw translated(failure);}}
    @Override public String confirmedProgress(){return transport.confirmedProgress();}
    @Override public void prepare(Set<String> destinations)throws Exception{try{transport.prepare(target.messaging.autoCreate);}catch(Exception failure){throw translated(failure);}}
    @Override public void require(Set<String> destinations)throws Exception{try{transport.prepare(false);}catch(Exception failure){throw translated(failure);}}
    @Override public void publish(List<Message> messages)throws Exception{
        List<RocketTransport.Publication> batch=new ArrayList<>();for(Message message:messages){RocketTransport.Publication p=new RocketTransport.Publication();p.id=message.id;p.channel=message.destination;p.key=message.key;p.body=message.body;p.headers.putAll(message.headers);batch.add(p);}try{transport.publish(batch);}catch(Exception failure){throw translated(failure);}
    }
    @Override public void close(){transport.close();}
}
