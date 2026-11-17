package io.github.rivomesh.platform;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Independent registry heartbeat; local task scheduling never holds a network-storage lock. */
final class NodeDirectory implements AutoCloseable {
    private final PlatformConfig config;
    private final TaskService tasks;
    private final ControlStore store;
    private final ControlStore.Node node=new ControlStore.Node();
    private final ScheduledExecutorService heartbeat;
    private final AtomicBoolean closed=new AtomicBoolean();
    private volatile ControlStore.Lease lease;
    private volatile long confirmedAt;
    private volatile String problem;
    private volatile boolean fenced;
    NodeDirectory(PlatformConfig config,TaskService tasks)throws Exception{
        this.config=config;this.tasks=tasks;
        node.id=config.cluster==null?localId(Paths.get(config.dataDirectory)):config.cluster.nodeId;
        node.incarnation=UUID.randomUUID().toString();node.version=version();node.artifactSha256=artifact();
        node.jvm=System.getProperty("java.runtime.version",System.getProperty("java.version"));node.architecture=System.getProperty("os.arch");
        node.processors=Runtime.getRuntime().availableProcessors();node.heapBytes=Runtime.getRuntime().maxMemory();boolean executes=config.cluster==null||config.cluster.executes();node.taskCapacity=executes?config.maxTasks:0;node.runningCapacity=executes?config.maxRunningTasks:0;
        node.role=config.cluster!=null&&config.cluster.separateExecution?config.cluster.roleName():null;
        node.advertiseUrl=config.cluster==null?null:config.cluster.advertiseUrl;
        store=config.cluster==null?null:new JdbcControlStore(config.cluster);
        heartbeat=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"rivomesh-node-heartbeat");t.setDaemon(true);return t;});
    }
    private static String localId(Path data)throws Exception{
        Path file=data.resolve("node-id");String id;
        if(Files.isSymbolicLink(file))throw new IOException("NODE_ID_INVALID");
        if(Files.exists(file)){id=new String(PrivateFiles.read(file,100),StandardCharsets.US_ASCII).trim();if(!id.matches("local-[a-f0-9-]{36}"))throw new IOException("NODE_ID_INVALID");}
        else{id="local-"+UUID.randomUUID();PrivateFiles.write(file,id.getBytes(StandardCharsets.US_ASCII));}return id;
    }
    private static String version()throws IOException{
        Properties p=new Properties();try(InputStream in=NodeDirectory.class.getResourceAsStream("/META-INF/maven/io.github.rivomesh/rivomesh-app/pom.properties")){if(in!=null)p.load(in);}return p.getProperty("version","DEVELOPMENT");
    }
    private static String cachedArtifact;
    private static synchronized String artifact()throws Exception{
        if(cachedArtifact!=null)return cachedArtifact;
        Path code=Paths.get(NodeDirectory.class.getProtectionDomain().getCodeSource().getLocation().toURI());if(!Files.isRegularFile(code))return cachedArtifact="DEVELOPMENT";
        MessageDigest digest=MessageDigest.getInstance("SHA-256");try(InputStream in=Files.newInputStream(code)){byte[] buffer=new byte[65536];int n;while((n=in.read(buffer))!=-1)digest.update(buffer,0,n);}
        StringBuilder value=new StringBuilder();for(byte b:digest.digest())value.append(String.format(Locale.ROOT,"%02x",b&255));return cachedArtifact=value.toString();
    }
    void start()throws Exception{
        if(store==null)return;
        long started=System.nanoTime();lease=store.register(node,tasks.nodeObservations());confirmedAt=started;
        heartbeat.scheduleWithFixedDelay(this::beat,config.cluster.heartbeatMillis,config.cluster.heartbeatMillis,TimeUnit.MILLISECONDS);
    }
    private void beat(){
        if(closed.get()||fenced)return;long started=System.nanoTime();
        try{
            tasks.checkAccessAuthority();
            try{store.heartbeat(lease,node,tasks.nodeObservations());}
            catch(ControlStore.Expired expired){lease=store.register(node,tasks.nodeObservations());}
            confirmedAt=started;problem=null;
        }catch(ControlStore.Fenced lost){fenced=true;problem="NODE_REGISTRATION_FENCED";}
        catch(TaskService.Conflict conflict){fenced=true;problem="NODE_ID_IN_USE";}
        catch(Exception unavailable){problem="CONTROL_STORE_UNAVAILABLE";}
    }
    boolean ready(){return !closed.get()&&(store==null||!fenced&&confirmedAt!=0&&System.nanoTime()-confirmedAt<TimeUnit.MILLISECONDS.toNanos(config.cluster.leaseMillis));}
    ControlStore.Lease currentLease(){
        ControlStore.Lease current=lease;
        if(current==null||!ready())throw new ControlStore.Fenced();
        return current;
    }
    Map<String,Object> page(String after,int limit)throws Exception{
        if(store!=null){Map<String,Object> page=store.page(after,limit);page.put("localNodeId",node.id);page.put("localNodeEpoch",lease==null?null:lease.epoch);page.put("localProblem",problem);return page;}
        if(after==null||!after.isEmpty()||limit<1||limit>50)throw new IllegalArgumentException("NODE_CURSOR_INVALID");
        Map<String,Object> row=new LinkedHashMap<>();row.put("id",node.id);row.put("epoch",1L);row.put("state","HEALTHY");row.put("role","CONTROLLER_WORKER");row.put("version",node.version);row.put("artifactSha256",node.artifactSha256);row.put("jvm",node.jvm);row.put("architecture",node.architecture);row.put("processors",node.processors);row.put("heapBytes",node.heapBytes);row.put("taskCapacity",node.taskCapacity);row.put("runningCapacity",node.runningCapacity);row.put("lastHeartbeat",Instant.now().toString());row.put("leaseExpiresAt",null);row.put("tasks",tasks.nodeObservations());
        Map<String,Object> page=new LinkedHashMap<>();page.put("clusterId",null);page.put("mode","STANDALONE");page.put("localNodeId",node.id);page.put("observedAt",Instant.now().toString());page.put("nodes",Collections.singletonList(row));page.put("next",null);page.put("taskControlScope","LOCAL");return page;
    }
    public void close(){
        if(!closed.compareAndSet(false,true))return;heartbeat.shutdownNow();
        try{heartbeat.awaitTermination(6,TimeUnit.SECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}
        if(store!=null){try{if(lease!=null)store.stopped(lease);}catch(Exception ignored){/* The database lease expires if graceful marking cannot be committed. */}store.close();}
    }
}
