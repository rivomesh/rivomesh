package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.file.Path;
import java.util.Map;

/** Disposable Controllers call the internal workflow through the same sealed SQL and authenticated Agent RPC. */
public final class ManagedHandoffPrivateFixture {
    private ManagedHandoffPrivateFixture(){}
    public static void main(String[] args)throws Exception{
        if(args.length!=7||!("bind".equals(args[6])||"discard".equals(args[6])||"observe".equals(args[6])))
            throw new IllegalArgumentException("HANDOFF_FIXTURE_ARGUMENTS_INVALID");
        PlatformConfig config=PlatformConfig.load(Path.of(args[0]));
        ClusterConfig cluster=config.cluster;
        if(cluster==null||!"global-cdc".equals(cluster.id)||cluster.role!=ClusterConfig.Role.CONTROLLER
                ||!"jdbc:postgresql://store:5432/rivomesh_control".equals(cluster.url))
            throw new IllegalArgumentException("HANDOFF_FIXTURE_REQUIRES_DISPOSABLE_CONTROL_STORE");
        SharedAccessStore access=new SharedAccessStore(config,AccountDirectory.configured(config),null,Map.of());
        GlobalTaskStore tasks=new GlobalTaskStore(cluster,access,cluster.taskKey,null,cluster.maximumTasks);
        AgentDirectory agents=new AgentDirectory(cluster,access,tasks);
        DurableCommands commands=new DurableCommands(cluster,access,tasks,cluster.maximumCommands);
        GlobalTaskViews views=new GlobalTaskViews(cluster,tasks,agents,Math.min(300000,Math.max(15000,cluster.leaseMillis)));
        ManagedHandoff workflow=new ManagedHandoff(cluster,access,tasks,commands,agents,views);
        OwnerQueries transport=new OwnerQueries(cluster,tasks,agents,1000,30000,workflow);
        AccessControl.Principal admin=AccountDirectory.authenticate(access.accounts(),"Bearer "+config.authToken);
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.HANDOFF_STAGE,args[1]);
        request.reference=args[2];request.revision=args[3];request.identityHash=args[4];request.requestId=args[5];
        if("bind".equals(args[6])){
            if(workflow.bindOrReplayExecution(request,admin)!=null)
                throw new IllegalStateException("HANDOFF_FIXTURE_EXPECTED_UNCOMMITTED_INTENT");
            return;
        }
        Map<String,Object> committed=workflow.orchestratePreparedWriter(request,admin,transport);
        if("observe".equals(args[6]))System.out.println(Model.JSON.writeValueAsString(committed));
    }
}
