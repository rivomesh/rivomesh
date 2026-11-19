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
    private final ConcurrentMap<String,Long> handoffRetries=new ConcurrentHashMap<>();
    private Set<String> retiredTasks=Set.of();
    private String recipientCursor="",handoffResumeCursor="";
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
    private void processParkedHandoff(String task,String preparation,ControlStore.Lease lease,boolean starting)throws Exception{
        ManagedRecipientImport.Grant expected=authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            var verified=authority.verifiedEvidenceRecipient(c,task,preparation,lease,directory);
            requireHandoffAuthor(accounts,verified.committed(),starting);
            return verified;
        });
        local.installAdoptedHandoffEvidence(expected);
        var config=local.parkedFenceConfig(expected);
        authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            var current=authority.verifiedEvidenceRecipient(c,task,preparation,lease,directory);
            if(!expected.equals(current))throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
            requireHandoffAuthor(accounts,current.committed(),starting);
            if(starting)authority.admitParkedWorker(c,current,config,()->agents.require(c,lease,directory));
            else authority.fenceParkedTargets(c,current,config,()->agents.require(c,lease,directory));
            return null;
        });
    }
    private void projectStartedWorker(String task,String preparation,ControlStore.Lease lease)throws Exception{
        var grant=authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            var current=authority.admittedRecipient(c,authority.record(c,task),lease,directory);
            if(current==null||!preparation.equals(current.committed().id()))
                throw new TaskService.Conflict("HANDOFF_WORKER_START_REQUEST_CHANGED");
            return current;
        });
        String request=local.projectStartedRecipient(grant);
        authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            authority.recordStartedRecipient(c,task,lease,directory,request);
            return null;
        });
    }
    private static void requireHandoffAuthor(AccountDirectory.State accounts,HandoffPreparations.Plan plan,boolean starting){
        if(!(starting?"START_PENDING":"FENCE_PENDING").equals(plan.phase()))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
        String id=starting?plan.workerStart().actor():plan.targetFence().actor();
        String revision=starting?plan.workerStart().actorRevision():plan.targetFence().actorRevision();
        AccountDirectory.User author=accounts.users.get(id);
        if(!Objects.equals(accounts.revision,revision)||author==null||!author.enabled||author.deleted)
            throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        var actor=new AccessControl.Principal(author.id,author.role,author.taskIds,accounts.revision,author.nodeIds);
        if(!actor.administers()||!actor.task(plan.task())||!actor.node(plan.evidence().candidate().node())
                ||!actor.node(plan.evidence().original().node()))throw new SecurityException("FORBIDDEN");
    }
    private void verifyDefinitions()throws Exception{
        Map<String,String> documents=local.ownerDocuments();
        Map<String,String> starts=local.ownerRecipientStarts();
        Set<String> retired=authority.service((c,accounts)->{
            ControlStore.Lease lease=leases.get();agents.require(c,lease,directory);ObjectMapper yaml=new ObjectMapper(new YAMLFactory());
            Set<String> prior=new HashSet<>();
            for(Map.Entry<String,String> entry:documents.entrySet()){
                GlobalTaskStore.Record record;try{record=authority.record(c,entry.getKey());}catch(NoSuchElementException missing){throw new TaskService.Conflict("OWNER_TASK_NOT_REGISTERED");}
                String historical=authority.retiredConfiguration(c,record,lease.nodeId,directory);
                String expectedYaml=historical==null?authority.activeYaml(c,record):historical;
                TaskDefinition expected=TaskDefinition.parse(expectedYaml,Paths.get(config.tasksDirectory),Paths.get(config.dataDirectory),lease.nodeId);
                if(!yaml.readTree(expected.workerYaml).equals(yaml.readTree(entry.getValue())))throw new TaskService.Conflict("OWNER_CONFIGURATION_PROJECTION_CHANGED");
                if(historical!=null){prior.add(record.id);continue;}
                String request=starts.get(record.id);
                var admitted=authority.admittedRecipient(c,record,lease,directory);
                if(request!=null&&(admitted==null||!request.equals(admitted.committed().workerStart().id()))
                        ||admitted!=null&&(!starts.containsKey(record.id)
                        ||request==null&&"WORKER_PROJECTED".equals(admitted.committed().phase())))
                    throw new TaskService.Conflict("HANDOFF_WORKER_LOCAL_ADMISSION_CHANGED");
                durable.reserveExisting(c,record,expected);
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
        queuePreparedHandoff();
        for(HandoffPreparations.PendingRecipient task:pending){
            String id="handoff:"+task.preparation();
            boolean finishing=Set.of("FENCE_PENDING","START_PENDING","WORKER_ADMITTED").contains(task.phase());
            Long retryAt=handoffRetries.get(id);
            if(finishing&&retryAt!=null&&System.nanoTime()-retryAt<0)continue;
            if(!queued.add(id))continue;
            try{readers.execute(()->{
                try{
                    ControlStore.Lease current=leases.get();
                    switch(task.phase()){
                        case "FENCE_PENDING","START_PENDING" -> processParkedHandoff(task.task(),task.preparation(),current,"START_PENDING".equals(task.phase()));
                        case "WORKER_ADMITTED" -> projectStartedWorker(task.task(),task.preparation(),current);
                        case "PROJECTED" -> adoptRecipientEvidence(task.task(),task.preparation(),current);
                        default -> importCommittedWriter(task.task(),current);
                    }
                    handoffRetries.remove(id);
                }
                catch(ControlStore.Unavailable unavailable){queryProblem=code(unavailable);}
                catch(Exception deferred){
                    if(finishing){
                        handoffRetries.put(id,System.nanoTime()+TimeUnit.SECONDS.toNanos(6));
                        String operation="FENCE_PENDING".equals(task.phase())?"HANDOFF_TARGET_FENCE":"HANDOFF_"+task.phase();
                        String problem=HandoffPreparations.safeProblem(code(deferred));
                        System.err.println(operation+" "+problem);
                        if(task.approval()!=null)rememberPendingProblem(task,problem);
                    }
                    // The committed receipt stays pending and new generic commands remain blocked.
                }
                finally{queued.remove(id);}
            });}catch(RejectedExecutionException full){queued.remove(id);}
        }
    }
    private void queuePreparedHandoff(){
        String key="handoff-resume";
        if(!queued.add(key))return;
        try{readers.execute(()->{
            try{
                ControlStore.Lease lease=leases.get();
                String request=queries.nextHandoffResume(lease,directory,handoffResumeCursor);
                handoffResumeCursor=request==null?"":request;
                if(request!=null)queries.resumeHandoff(request,lease,directory,local::recheckPreparedHandoff);
            }catch(ControlStore.Unavailable unavailable){queryProblem=code(unavailable);}
            catch(Exception pending){/* Only current signed readiness and the exact native cut can commit ownership. */}
            finally{queued.remove(key);}
        });}catch(RejectedExecutionException full){queued.remove(key);}
    }
    private void rememberPendingProblem(HandoffPreparations.PendingRecipient task,String problem){
        try{
            ControlStore.Lease current=leases.get();
            authority.service((c,accounts)->{
                agents.require(c,current,directory);
                var record=authority.record(c,task.task());
                if(!record.owner.equals(current.nodeId)||!record.directory.equals(directory))throw new ControlStore.Fenced();
                var plans=authority.preparations();
                long now=authority.database().now(c);
                var plan=plans.latest(c,task.task(),now);
                plans.recordPendingProblem(c,plan,task,problem,now);
                return null;
            });
        }catch(Exception unavailable){queryProblem=code(unavailable);}
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
        queued.clear();handoffRetries.clear();
    }
}
