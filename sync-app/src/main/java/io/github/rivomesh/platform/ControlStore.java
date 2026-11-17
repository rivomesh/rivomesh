package io.github.rivomesh.platform;

import java.util.*;

/** Shared metadata authority. Node observations do not grant Worker execution ownership. */
interface ControlStore extends AutoCloseable {
    final class Unavailable extends IllegalStateException { Unavailable(){super("CONTROL_STORE_UNAVAILABLE");} }
    final class Fenced extends IllegalStateException { Fenced(){super("NODE_REGISTRATION_FENCED");} }
    final class Expired extends IllegalStateException { Expired(){super("NODE_REGISTRATION_EXPIRED");} }
    final class Node {
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String role;
        public String id, incarnation, version, artifactSha256, jvm, architecture, advertiseUrl;
        public int processors, taskCapacity, runningCapacity;
        public long heapBytes;
        String roleName(){return role==null?"CONTROLLER_WORKER":role;}
        boolean executes(){return Arrays.asList("CONTROLLER_WORKER","AGENT").contains(roleName());}
    }
    final class Task {
        public String id, revision, desired, workerState, observedAt;
    }
    final class Lease {
        final String nodeId,incarnation; final long epoch;
        Lease(String nodeId,String incarnation,long epoch){this.nodeId=nodeId;this.incarnation=incarnation;this.epoch=epoch;}
    }
    Lease register(Node node,List<Task> tasks)throws Exception;
    void heartbeat(Lease lease,Node node,List<Task> tasks)throws Exception;
    void stopped(Lease lease)throws Exception;
    Map<String,Object> page(String after,int limit)throws Exception;
    void close();
}
