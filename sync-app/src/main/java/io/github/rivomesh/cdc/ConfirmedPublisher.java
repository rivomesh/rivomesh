package io.github.rivomesh.cdc;

import java.util.*;

/** Implementations return after their native publication/materialization confirmation. */
public interface ConfirmedPublisher extends AutoCloseable {
    final class Message {
        public String destination,key,id;
        public byte[] body;
        public Map<String,String> headers=new TreeMap<>();
    }
    void prepare(Set<String> destinations)throws Exception;
    /** Previously acknowledged destinations must already exist; never recreate them at an old checkpoint. */
    void require(Set<String> destinations)throws Exception;
    /** Stable cluster identity when the protocol exposes one; never a transient server/node ID. */
    default String identity()throws Exception{return null;}
    /** Restore the last durable native confirmation before resource preparation or any writes. */
    default void resumeProgress(String progress)throws Exception{if(progress!=null)throw new IllegalStateException("PUBLICATION_TRANSPORT_PROGRESS_UNSUPPORTED");}
    default boolean requiresProgress(){return false;}
    default String confirmedProgress()throws Exception{return null;}
    void publish(List<Message> messages)throws Exception;
    @Override void close()throws Exception;
}
