package io.github.rivomesh.platform;

/** Opt-in fixed-owner global control. Existing local tasks require a separate reviewed import. */
final class GlobalControl implements AutoCloseable {
    final GlobalTaskPlane plane;
    private final OwnerRuntime runtime;
    GlobalControl(PlatformConfig config,TaskService tasks,NodeDirectory nodes)throws Exception{
        if(config.cluster==null||!config.cluster.sharedTasks)throw new IllegalArgumentException("GLOBAL_TASK_AUTHORITY_REQUIRED");
        SharedAccessStore access=tasks.sharedAccess();String directory=tasks.ownerDirectoryIdentity();GlobalTaskStore store=new GlobalTaskStore(config.cluster,access,config.cluster.taskKey,tasks.globalTaskIdentity(),config.cluster.maximumTasks);
        AgentDirectory agents=new AgentDirectory(config.cluster,access,store);DurableCommands commands=new DurableCommands(config.cluster,access,store,config.cluster.maximumCommands);GlobalTaskViews views=new GlobalTaskViews(config.cluster,store,agents,Math.min(300000,Math.max(15000,config.cluster.leaseMillis)));OwnerQueries queries=new OwnerQueries(config.cluster,store,agents,1000,30000);
        if(!config.cluster.executes())store.requireControllerWithoutTasks(config.cluster.nodeId);
        tasks.bindGlobal(store.identity,directory);plane=new GlobalTaskPlane(store,commands,agents,views,queries,config.cluster.nodeId,config.cluster.executes());runtime=config.cluster.executes()?new OwnerRuntime(config,tasks,store,commands,agents,views,queries,nodes::currentLease,directory):null;
    }
    void start()throws Exception{if(runtime!=null)runtime.start();}
    boolean ready(){return runtime==null||runtime.ready();}
    public void close(){if(runtime!=null)runtime.close();}
}
