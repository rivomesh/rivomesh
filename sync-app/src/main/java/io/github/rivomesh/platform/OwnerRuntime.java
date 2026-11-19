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
    private Set<String> retiredTasks=Set.of();
    private String recipientCursor="";
    private long nextRecipientPoll;
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
    private void importCommittedWriter(String task,ControlStore.Lease lease)throws Exception{
        ManagedRecipientImport.Receipt imported=ManagedRecipientImport.install(authority,agents,local,lease,directory,task,plan->{
            HandoffCandidateAdmission.ReadyConfig frozen=HandoffCandidateAdmission.verifyCommitted(
                    Paths.get(config.dataDirectory).toAbsolutePath().normalize(),plan,authority.commandCipher());
            HandoffCandidateAdmission.Ready receipt=frozen.receipt();
            return new ManagedRecipientImport.FrozenConfig(receipt.preparation(),receipt.binding(),receipt.desired(),
                    receipt.configurationSha256(),receipt.stagingSha256(),frozen.successorYaml());
        });
        authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            authority.recordRecipientProjection(c,task,lease,directory,imported);
            return null;
        });
    }
    private void adoptRecipientEvidence(String task,String preparation,ControlStore.Lease lease)throws Exception{
        ManagedRecipientImport.Grant grant=authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            ManagedRecipientImport.Grant verified=authority.verifiedEvidenceRecipient(c,task,preparation,lease,directory);
            agents.require(c,lease,directory);
            return verified;
        });
        HandoffEvidenceAdoption.Receipt adopted=local.installAdoptedHandoffEvidence(grant);
        authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            authority.recordRecipientEvidence(c,task,lease,directory,adopted);
            agents.require(c,lease,directory);
            return null;
        });
    }
    private void verifyDefinitions()throws Exception{
        Map<String,String> documents=local.ownerDocuments();
        Set<String> retired=authority.service((c,accounts)->{
            ControlStore.Lease lease=leases.get();agents.require(c,lease,directory);ObjectMapper yaml=new ObjectMapper(new YAMLFactory());
            Set<String> prior=new HashSet<>();
            for(Map.Entry<String,String> entry:documents.entrySet()){
                GlobalTaskStore.Record record;try{record=authority.record(c,entry.getKey());}catch(NoSuchElementException missing){throw new TaskService.Conflict("OWNER_TASK_NOT_REGISTERED");}
                String historical=authority.retiredConfiguration(c,record,lease.nodeId,directory);
                String expectedYaml=historical==null?authority.activeYaml(c,record):historical;
                TaskDefinition expected=TaskDefinition.parse(expectedYaml,Paths.get(config.tasksDirectory),Paths.get(config.dataDirectory),lease.nodeId);
                if(!yaml.readTree(expected.workerYaml).equals(yaml.readTree(entry.getValue())))throw new TaskService.Conflict("OWNER_CONFIGURATION_PROJECTION_CHANGED");
                if(historical==null)durable.reserveExisting(c,record,expected);
                else prior.add(record.id);
            }
            agents.require(c,lease,directory);
            return prior;
        });
        local.retainRetiredOwners(retired);
        retiredTasks=Set.copyOf(retired);
    }
    @SuppressWarnings("unchecked") private void publish()throws Exception{
        List<Map<String,Object>> tasks=(List<Map<String,Object>>)local.list(null).get("tasks");
        if(!retiredTasks.isEmpty())tasks=tasks.stream().filter(t->!retiredTasks.contains(t.get("id"))).toList();
        for(int offset=0;offset<tasks.size();offset+=20)views.publish(leases.get(),directory,tasks.subList(offset,Math.min(tasks.size(),offset+20)));
    }
    private void commands(){
        if(closed.get())return;
        try{
            execution.cycle();
            try{publish();}
            catch(ControlStore.Fenced changed){verifyDefinitions();publish();}
            commandProblem=null;
        }catch(Exception failure){commandProblem=code(failure);}
    }
    private void queueRecipients(ControlStore.Lease lease)throws Exception{
        if(!config.cluster.separateExecution)return;
        long now=System.nanoTime();
        if(nextRecipientPoll!=0&&now-nextRecipientPoll<0)return;
        nextRecipientPoll=now+TimeUnit.SECONDS.toNanos(1);
        List<HandoffPreparations.PendingRecipient> pending=authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            return authority.preparations().pendingRecipient(c,lease.nodeId,directory,recipientCursor,10);
        });
        recipientCursor=pending.size()<10?"":pending.getLast().preparation();
        for(HandoffPreparations.PendingRecipient task:pending){
            String id="handoff:"+task.preparation();
            if(!queued.add(id))continue;
            try{readers.execute(()->{
                try{
                    ControlStore.Lease current=leases.get();
                    if("PROJECTED".equals(task.phase()))adoptRecipientEvidence(task.task(),task.preparation(),current);
                    else importCommittedWriter(task.task(),current);
                }
                catch(ControlStore.Unavailable unavailable){queryProblem=code(unavailable);}
                catch(Exception deferred){/* The committed receipt stays pending and all new commands remain blocked. */}
                finally{queued.remove(id);}
            });}catch(RejectedExecutionException full){queued.remove(id);}
        }
    }
    private void queries(){
        if(closed.get())return;
        try{
            ControlStore.Lease lease=leases.get();
            List<String> pending=queries.pending(lease,directory,20);queryProblem=null;
            queueRecipients(lease);
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
