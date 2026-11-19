package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.transport.classic.ClassicTransport;
import java.util.*;

/** OpenWire transactions with retained ownership and receipt records. */
public final class ClassicMessagePublisher implements ConfirmedPublisher {
    private final PipelineConfig.Target target;private final ClassicTransport transport;
    public ClassicMessagePublisher(PipelineConfig.Target target,String binding)throws Exception{this.target=target;try{transport=new ClassicTransport(settings(target),admin(target),binding);}catch(Exception failure){throw translated(failure);}}
    private static ClassicTransport.Settings settings(PipelineConfig.Target target){PipelineConfig.Messaging m=target.messaging;ClassicTransport.Settings s=new ClassicTransport.Settings();s.url=target.url;s.username=target.username;s.password=target.password;s.queuePrefix=m.stream;s.maxMessageBytes=m.maxMessageBytes;s.connectTimeoutMillis=m.connectTimeoutMillis;s.requestTimeoutMillis=m.requestTimeoutMillis;return s;}
    private static ClassicAdminClient admin(PipelineConfig.Target target)throws Exception{return new ClassicAdminClient(target.messaging.adminUrl,target.username,target.password,target.messaging.broker,target.messaging.connectTimeoutMillis,target.messaging.requestTimeoutMillis);}
    private static Exception translated(Exception failure){if(failure instanceof ClassicTransport.Failure){ClassicTransport.Failure error=(ClassicTransport.Failure)failure;return new SyncFailure(error.code,"MESSAGE_PUBLICATION",error.retryable,"ActiveMQ Classic operation was not confirmed",failure,null,0,null);}return failure;}
    public static void check(PipelineConfig.Target target)throws Exception{try(ClassicTransport ignored=new ClassicTransport(settings(target),admin(target),null)){/* No business queues created. */}catch(Exception failure){throw translated(failure);}}
    @Override public boolean requiresProgress(){return true;}
    @Override public void resumeProgress(String progress)throws Exception{try{transport.resumeProgress(progress);}catch(Exception failure){throw translated(failure);}}
    @Override public String confirmedProgress(){return transport.confirmedProgress();}
    @Override public String identity(){return transport.identity();}
    @Override public void prepare(Set<String> destinations)throws Exception{try{transport.prepare(target.messaging.autoCreate);}catch(Exception failure){throw translated(failure);}}
    @Override public void require(Set<String> destinations)throws Exception{try{transport.prepare(false);}catch(Exception failure){throw translated(failure);}}
    @Override public void publish(List<Message> messages)throws Exception{List<ClassicTransport.Publication> batch=new ArrayList<>();for(Message message:messages){ClassicTransport.Publication p=new ClassicTransport.Publication();p.id=message.id;p.channel=message.destination;p.key=message.key;p.body=message.body;p.headers.putAll(message.headers);batch.add(p);}try{transport.publish(batch);}catch(Exception failure){throw translated(failure);}}
    void verifyPrecedingCommit(String progress,String expectedIdentityHash,String eventId)throws Exception{
        try{
            String identity=transport.verifyPrecedingCommit(progress,eventId);
            if(!expectedIdentityHash.equals(Codec.sha256(identity)))throw new IllegalStateException("CLASSIC_RESOURCE_ID_CHANGED");
        }catch(Exception failure){
            throw translated(failure);
        }
    }
    @Override public void close(){transport.close();}
}
