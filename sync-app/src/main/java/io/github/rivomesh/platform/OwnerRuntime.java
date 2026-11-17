package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Internal owner lifecycle; public global-mode migration/admission remains a separate gate. */
final class OwnerRuntime implements AutoCloseable {
    private final PlatformConfig config;
    private final TaskService local;
    private final GlobalTaskStore authority;
    private final DurableCommands durable;
    private final AgentDirectory agents;
    private final GlobalTaskViews views;
    private final OwnerQueries queries;
    private final Supplier<ControlStore.Lease> leases;
    private final String directory;
    private final OwnerExecution execution;
    private final ScheduledExecutorService commandLoop,queryLoop;
    private final ThreadPoolExecutor readers;
    private final Set<String> queued=ConcurrentHashMap.newKeySet();
    private final AtomicBoolean started=new AtomicBoolean(),closed=new AtomicBoolean();
    private volatile String commandProblem,queryProblem;

    OwnerRuntime(PlatformConfig config,TaskService local,GlobalTaskStore authority,DurableCommands commands,AgentDirectory agents,GlobalTaskViews views,OwnerQueries queries,Supplier<ControlStore.Lease> leases,String directory){
        this.config=config;this.local=local;this.authority=authority;durable=commands;this.agents=agents;this.views=views;this.queries=queries;this.leases=leases;this.directory=directory;execution=new OwnerExecution(commands,agents,leases,directory,local);
        commandLoop=Executors.newSingleThreadScheduledExecutor(threads("rivomesh-owner-commands"));queryLoop=Executors.newSingleThreadScheduledExecutor(threads("rivomesh-owner-queries"));readers=new ThreadPoolExecutor(2,2,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(20),threads("rivomesh-owner-reader"),new ThreadPoolExecutor.AbortPolicy());
    }
    private static ThreadFactory threads(String name){return run->{Thread thread=new Thread(run,name);thread.setDaemon(true);return thread;};}
    void start()throws Exception{
        if(closed.get()||!started.compareAndSet(false,true))throw new TaskService.Conflict("OWNER_RUNTIME_ALREADY_STARTED");
        try{
            agents.register(leases.get(),directory);execution.restoreReceipts();verifyDefinitions();execution.prepareStartupStops();publish();local.beginOwnerScheduling();
            commandLoop.scheduleWithFixedDelay(this::commands,0,Math.max(100,Math.min(config.pollMillis,1000)),TimeUnit.MILLISECONDS);
            queryLoop.scheduleWithFixedDelay(this::queries,0,100,TimeUnit.MILLISECONDS);
        }catch(Exception failure){commandProblem=code(failure);close();throw failure;}
    }
    private void verifyDefinitions()throws Exception{
        Map<String,String> documents=local.ownerDocuments();
        authority.service((c,accounts)->{
            ControlStore.Lease lease=leases.get();agents.require(c,lease,directory);ObjectMapper yaml=new ObjectMapper(new YAMLFactory());
            for(Map.Entry<String,String> entry:documents.entrySet()){
                GlobalTaskStore.Record record;try{record=authority.record(c,entry.getKey());}catch(NoSuchElementException missing){throw new TaskService.Conflict("OWNER_TASK_NOT_REGISTERED");}
                if(!lease.nodeId.equals(record.owner)||!directory.equals(record.directory))throw new TaskService.Conflict("COMMAND_OWNER_CHANGED");
                TaskDefinition expected=TaskDefinition.parse(authority.activeYaml(c,record),Paths.get(config.tasksDirectory),Paths.get(config.dataDirectory));
                if(!yaml.readTree(expected.workerYaml).equals(yaml.readTree(entry.getValue())))throw new TaskService.Conflict("OWNER_CONFIGURATION_PROJECTION_CHANGED");
                durable.reserveExisting(c,record,expected);
            }agents.require(c,lease,directory);return null;
        });
    }
    @SuppressWarnings("unchecked") private void publish()throws Exception{
        List<Map<String,Object>> tasks=(List<Map<String,Object>>)local.list(null).get("tasks");
        for(int offset=0;offset<tasks.size();offset+=20)views.publish(leases.get(),directory,tasks.subList(offset,Math.min(tasks.size(),offset+20)));
    }
    private void commands(){
        if(closed.get())return;try{execution.cycle();publish();commandProblem=null;}catch(Exception failure){commandProblem=code(failure);}
    }
    private void queries(){
        if(closed.get())return;
        try{
            List<String> pending=queries.pending(leases.get(),directory,20);queryProblem=null;
            for(String id:pending)if(queued.add(id)){
                try{readers.execute(()->{try{queries.serve(id,leases.get(),directory,new LocalTaskPlane(local));}catch(ControlStore.Unavailable unavailable){queryProblem=code(unavailable);}catch(Exception rejected){/* Per-request expiry/epoch/rejection does not impersonate a cluster outage. */}finally{queued.remove(id);}});}
                catch(RejectedExecutionException full){queued.remove(id);}
            }
        }catch(Exception failure){queryProblem=code(failure);}
    }
    private static String code(Exception failure){String message=failure.getMessage();return message!=null&&message.matches("[A-Z0-9_]{1,120}")?message:"OWNER_RUNTIME_UNAVAILABLE";}
    boolean ready(){if(!started.get()||closed.get()||problem()!=null)return false;try{leases.get();return local.accessReady();}catch(Exception unavailable){return false;}}
    String problem(){return commandProblem!=null?commandProblem:queryProblem;}
    public void close(){
        if(!closed.compareAndSet(false,true))return;commandLoop.shutdownNow();queryLoop.shutdownNow();readers.shutdownNow();
        for(ExecutorService executor:Arrays.asList(commandLoop,queryLoop,readers))try{executor.awaitTermination(5,TimeUnit.SECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();return;}
        queued.clear();
    }
}
