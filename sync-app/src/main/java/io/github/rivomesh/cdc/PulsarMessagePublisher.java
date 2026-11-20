package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.transport.pulsar.PulsarTransport;
import java.util.*;

/** Synchronous persistent-topic confirmations through the isolated Pulsar SDK. */
public final class PulsarMessagePublisher implements ConfirmedPublisher {
    private final PipelineConfig.Target target;
    private final PulsarTransport transport;
    public PulsarMessagePublisher(PipelineConfig.Target target,String binding)throws Exception{
        this.target=target;try{transport=new PulsarTransport(settings(target),binding);}catch(Exception failure){throw translated(failure);}
    }
    private static PulsarTransport.Settings settings(PipelineConfig.Target target){
        PipelineConfig.Messaging m=target.messaging;PulsarTransport.Settings s=new PulsarTransport.Settings();s.serviceUrl=target.url;s.adminUrl=m.adminUrl;s.cluster=m.cluster;s.topic=m.topic;s.token=m.token;s.tlsTrustCertsFilePath=m.tlsTrustCertsFilePath;s.replicas=m.replicas;s.connectTimeoutMillis=m.connectTimeoutMillis;s.requestTimeoutMillis=m.requestTimeoutMillis;return s;
    }
    private static Exception translated(Exception failure){if(failure instanceof PulsarTransport.Failure){PulsarTransport.Failure error=(PulsarTransport.Failure)failure;return new SyncFailure(error.code,"MESSAGE_PUBLICATION",error.retryable,"Pulsar operation was not confirmed",failure,null,0,null);}return failure;}
    public static void check(PipelineConfig.Target target)throws Exception{try{PulsarTransport.check(settings(target));}catch(Exception failure){throw translated(failure);}}
    @Override public void prepare(Set<String> destinations)throws Exception{try{transport.prepare(target.messaging.autoCreate);}catch(Exception failure){throw translated(failure);}}
    @Override public void require(Set<String> destinations)throws Exception{try{transport.prepare(false);}catch(Exception failure){throw translated(failure);}}
    @Override public void publish(List<Message> messages)throws Exception{
        List<PulsarTransport.Publication> batch=new ArrayList<>();for(Message message:messages){PulsarTransport.Publication publication=new PulsarTransport.Publication();publication.id=message.id;publication.channel=message.destination;publication.key=message.key;publication.body=message.body;publication.headers.putAll(message.headers);batch.add(publication);}try{transport.publish(batch);}catch(Exception failure){throw translated(failure);}
    }
    @Override public String identity(){return transport.identity();}
    @Override public String confirmedProgress(){return transport.confirmedProgress();}
    @Override public void resumeProgress(String progress)throws Exception{
        try{transport.resumeProgress(progress);}catch(Exception failure){throw translated(failure);}
    }
    void verifyPrecedingCommit(String progress,String identityHash,String event)throws Exception{
        try{
            if(!Codec.sha256(transport.verifyPrecedingCommit(progress,event)).equals(identityHash))
                throw new IllegalStateException("PULSAR_RECOVERY_PUBLICATION_UNCERTAIN");
        }catch(Exception failure){throw translated(failure);}
    }
    @Override public void close()throws Exception{try{transport.close();}catch(Exception failure){throw translated(failure);}}
}
