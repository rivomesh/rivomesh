package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.transport.artemis.ArtemisTransport;
import java.util.*;

/** Core protocol confirmations and durable non-destructive queues. */
public final class ArtemisMessagePublisher implements ConfirmedPublisher {
    private final PipelineConfig.Target target;private final ArtemisTransport transport;
    public ArtemisMessagePublisher(PipelineConfig.Target target,String binding)throws Exception{this.target=target;try{transport=new ArtemisTransport(settings(target),binding);}catch(Exception failure){throw translated(failure);}}
    private static ArtemisTransport.Settings settings(PipelineConfig.Target target){PipelineConfig.Messaging m=target.messaging;ArtemisTransport.Settings s=new ArtemisTransport.Settings();s.url=target.url;s.username=target.username;s.password=target.password;s.queuePrefix=m.stream;s.brokerName=m.broker;s.maxMessageBytes=m.maxMessageBytes;s.connectTimeoutMillis=m.connectTimeoutMillis;s.requestTimeoutMillis=m.requestTimeoutMillis;return s;}
    private static Exception translated(Exception failure){if(failure instanceof ArtemisTransport.Failure){ArtemisTransport.Failure error=(ArtemisTransport.Failure)failure;return new SyncFailure(error.code,"MESSAGE_PUBLICATION",error.retryable,"Artemis operation was not confirmed",failure,null,0,null);}return failure;}
    public static void check(PipelineConfig.Target target)throws Exception{try{ArtemisTransport.check(settings(target));}catch(Exception failure){throw translated(failure);}}
    @Override public boolean requiresProgress(){return true;}
    @Override public void resumeProgress(String progress)throws Exception{try{transport.resumeProgress(progress);}catch(Exception failure){throw translated(failure);}}
    @Override public String confirmedProgress(){return transport.confirmedProgress();}
    @Override public String identity(){return transport.identity();}
    @Override public void prepare(Set<String> destinations)throws Exception{try{transport.prepare(target.messaging.autoCreate);}catch(Exception failure){throw translated(failure);}}
    @Override public void require(Set<String> destinations)throws Exception{try{transport.prepare(false);}catch(Exception failure){throw translated(failure);}}
    @Override public void publish(List<Message> messages)throws Exception{List<ArtemisTransport.Publication> batch=new ArrayList<>();for(Message message:messages){ArtemisTransport.Publication p=new ArtemisTransport.Publication();p.id=message.id;p.channel=message.destination;p.key=message.key;p.body=message.body;p.headers.putAll(message.headers);batch.add(p);}try{transport.publish(batch);}catch(Exception failure){throw translated(failure);}}
    @Override public void close(){transport.close();}
}
