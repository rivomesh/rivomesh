package io.github.rivomesh.platform;

import java.util.*;
import java.util.function.Supplier;

/** Fixed-owner command execution. No scheduling transfer or Worker failover is inferred. */
final class OwnerExecution {
    private final DurableCommands commands;
    private final AgentDirectory agents;
    private final Supplier<ControlStore.Lease> leases;
    private final String directory;
    private final TaskService tasks;
    private final LocalTaskPlane local;
    private ControlStore.Lease admitted;
    private final Map<String,String> startupStops=new LinkedHashMap<>();

    OwnerExecution(DurableCommands commands,AgentDirectory agents,Supplier<ControlStore.Lease> leases,String directory,TaskService tasks){
        this.commands=commands;this.agents=agents;this.leases=leases;this.directory=directory;this.tasks=tasks;local=new LocalTaskPlane(tasks);
    }
    synchronized void cycle()throws Exception{
        ControlStore.Lease lease=leases.get();
        if(admitted==null||admitted.epoch!=lease.epoch||!admitted.incarnation.equals(lease.incarnation)){agents.register(lease,directory);admitted=lease;}
        // A busy operation must be allowed to finish without restoring a Worker ahead of an offline STOP.
        for(String id:new ArrayList<>(startupStops.keySet()))step(id);
        for(String id:commands.pending(agents,lease,directory,50))step(id);
    }
    synchronized void prepareStartupStops()throws Exception{
        ControlStore.Lease lease=leases.get();String cursor="";
        while(true){
            List<DurableCommands.Stored> page=commands.outstanding(agents,lease,directory,cursor,true);if(page.isEmpty())break;
            for(DurableCommands.Stored command:page)if(command.request.kind==TaskRequest.Kind.ACTION&&Arrays.asList("STOP","PAUSE").contains(command.request.action)){
                startupStops.put(command.id,command.task);tasks.holdOwnerStart(command.task,true);
            }
            cursor=page.get(page.size()-1).id;
        }
        for(String id:new ArrayList<>(startupStops.keySet()))step(id);
    }
    private void releaseStartupStop(String id){String task=startupStops.remove(id);if(task!=null&&!startupStops.containsValue(task))tasks.holdOwnerStart(task,false);}
    synchronized void restoreReceipts()throws Exception{
        ControlStore.Lease lease=leases.get();agents.register(lease,directory);String cursor="";
        while(true){
            List<DurableCommands.Stored> page=commands.recoverable(agents,lease,directory,cursor);if(page.isEmpty())return;
            for(DurableCommands.Stored command:page){
                TaskService.OwnerReceipt receipt=command.localRequest==null?null:tasks.ownerReceipt(command.localRequest,command.actor,command.actorRole);
                if(receipt!=null){tasks.pinOwnerRequest(command.id);step(command.id);}
                else if("ACCEPTED".equals(command.phase)||"UNKNOWN".equals(command.phase))throw new TaskService.Conflict("OWNER_COMMAND_EVIDENCE_REQUIRED");
            }cursor=page.get(page.size()-1).id;
        }
    }
    synchronized void step(String id)throws Exception{
        ControlStore.Lease lease=leases.get();DurableCommands.Dispatch dispatch=commands.claim(id,agents,lease,directory);if(dispatch==null){releaseStartupStop(id);return;}
        tasks.pinOwnerRequest(id);
        try{
            TaskRequest request=dispatch.command.localRequest;
            if(request==null){
                if("UNKNOWN".equals(dispatch.command.phase)){finish(dispatch,"UNKNOWN","OWNER_ACCEPTANCE_EVIDENCE_REQUIRED",lease);return;}
                if(dispatch.actor==null){finish(dispatch,"REJECTED","AUTHORIZATION_CHANGED",lease);return;}
                request=commands.bindLocal(dispatch,tasks.prepareOwnerRequest(dispatch.command.executionTemplate==null?dispatch.command.request:dispatch.command.executionTemplate),agents,lease,directory);
            }
            TaskService.OwnerReceipt receipt=tasks.ownerReceipt(request,dispatch.command.actor,dispatch.command.actorRole);
            if(receipt!=null){finish(dispatch,receipt.phase,receipt.code,lease);return;}
            if("ACCEPTED".equals(dispatch.command.phase)||"UNKNOWN".equals(dispatch.command.phase)){
                finish(dispatch,"UNKNOWN","OWNER_ACCEPTANCE_EVIDENCE_REQUIRED",lease);return;
            }
            if(dispatch.actor==null){finish(dispatch,"REJECTED","AUTHORIZATION_CHANGED",lease);return;}
            commands.verifyDispatch(dispatch,agents,lease,directory);
            commands.reserveResources(dispatch,tasks.ownerResources(request),agents,lease,directory);
            try{local.invoke(request,dispatch.actor);}
            catch(Exception failure){
                // A reply can fail after the private controller registry is committed.
                receipt=tasks.ownerReceipt(request,dispatch.command.actor,dispatch.command.actorRole);
                if(receipt!=null){finish(dispatch,receipt.phase,receipt.code,lease);return;}throw failure;
            }
            receipt=tasks.ownerReceipt(request,dispatch.command.actor,dispatch.command.actorRole);
            if(receipt==null)finish(dispatch,"UNKNOWN","OWNER_ACCEPTANCE_UNCONFIRMED",lease);
            else finish(dispatch,receipt.phase,receipt.code,lease);
        }catch(ControlStore.Unavailable|ControlStore.Fenced|ControlStore.Expired unavailable){throw unavailable;}
        catch(SecurityException denied){finish(dispatch,"REJECTED","AUTHORIZATION_CHANGED",lease);}
        catch(TaskService.Conflict conflict){
            String code=conflict.getMessage();
            if(Arrays.asList("TASK_BUSY","OPERATION_PENDING").contains(code)){finish(dispatch,"DISPATCHED","OWNER_WAITING_LOCAL_OPERATION",lease);return;}
            boolean uncertain=Arrays.asList("REVISION_CHANGED","OWNER_RECEIPT_MISMATCH","TASK_EXISTS","CONFIG_FILE_EXISTS").contains(code);
            finish(dispatch,uncertain?"UNKNOWN":"REJECTED",safe(code,"OWNER_EXECUTION_REJECTED"),lease);
        }
        catch(IllegalArgumentException invalid){finish(dispatch,"REJECTED",safe(invalid.getMessage(),"OWNER_REQUEST_INVALID"),lease);}
        catch(NoSuchElementException absent){finish(dispatch,"DISPATCHED".equals(dispatch.command.phase)?"REJECTED":"UNKNOWN","OWNER_TASK_REQUIRED",lease);}
        catch(Exception uncertain){finish(dispatch,"UNKNOWN","OWNER_EXECUTION_UNCONFIRMED",lease);}
    }
    private static String safe(String value,String fallback){return value!=null&&value.matches("[A-Z0-9_]{1,120}")?value:fallback;}
    private void finish(DurableCommands.Dispatch dispatch,String phase,String code,ControlStore.Lease lease)throws Exception{
        commands.complete(dispatch,phase,safe(code,"OWNER_OPERATION_RESULT"),agents,lease,directory);
        if(Arrays.asList("ACCEPTED","SUCCEEDED","FAILED","REJECTED").contains(phase))releaseStartupStop(dispatch.command.id);
        if(Arrays.asList("SUCCEEDED","FAILED","REJECTED").contains(phase))tasks.releaseOwnerRequest(dispatch.command.id);
    }
}
