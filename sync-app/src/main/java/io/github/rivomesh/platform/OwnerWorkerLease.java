package io.github.rivomesh.platform;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/** A local start fence for managed journal workers under one shared control lease. */
final class OwnerWorkerLease {
    private final Supplier<ControlStore.Lease> leases;
    private final Map<TaskWorker,Boolean> workers=new IdentityHashMap<>();
    private ControlStore.Lease admitted;

    OwnerWorkerLease(Supplier<ControlStore.Lease> leases){
        this.leases=leases;
    }

    private static boolean same(ControlStore.Lease a,ControlStore.Lease b){
        return a!=null&&b!=null&&a.epoch==b.epoch&&a.nodeId.equals(b.nodeId)&&a.incarnation.equals(b.incarnation);
    }

    private ControlStore.Lease live(){
        try{return leases.get();}
        catch(ControlStore.Fenced unavailable){return null;}
    }

    synchronized boolean admitted(ControlStore.Lease lease){
        return same(admitted,lease);
    }

    synchronized boolean permitsStart(){
        return same(admitted,live());
    }

    synchronized TaskWorker launch(Callable<TaskWorker> start)throws Exception{
        if(!permitsStart())return null;
        TaskWorker worker=start.call();
        workers.put(worker,false);
        monitor();
        return worker;
    }

    synchronized void monitor(){
        if(admitted!=null&&!permitsStart())suspend();
    }

    synchronized void suspend(){
        admitted=null;
        for(Map.Entry<TaskWorker,Boolean> entry:workers.entrySet()){
            if(!entry.getValue()&&entry.getKey().alive()){
                entry.getKey().fence();
                entry.setValue(true);
            }
        }
    }

    synchronized boolean quiescent(){
        return workers.keySet().stream().noneMatch(TaskWorker::alive);
    }

    synchronized boolean admit(ControlStore.Lease checked){
        if(!quiescent()||!same(checked,live()))return false;
        admitted=checked;
        return true;
    }

    synchronized boolean retired(TaskWorker worker){
        return Boolean.TRUE.equals(workers.remove(worker));
    }
}
