package io.github.rivomesh.platform;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.Journal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Exercise former leases against the disposable PostgreSQL store after the original Agent rejoins. */
public final class OriginalAgentOutageFixture {
    private static final String STORE="jdbc:postgresql://store:5432/rivomesh_control";
    private static final String NODE="node-worker";
    private static final String TASK="global_cdc";

    private OriginalAgentOutageFixture(){}

    public static void main(String[] args)throws Exception{
        if(args.length!=6)throw new IllegalArgumentException("OUTAGE_FIXTURE_ARGUMENTS_INVALID");
        PlatformConfig platform=PlatformConfig.load(Path.of(args[0]));
        ClusterConfig cluster=platform.cluster;
        PipelineConfig pipeline=PipelineConfig.load(Path.of(args[1]));
        if(cluster==null||!"global-cdc".equals(cluster.id)||!NODE.equals(cluster.nodeId)
                ||cluster.role!=ClusterConfig.Role.AGENT||!STORE.equals(cluster.url)
                ||!TASK.equals(pipeline.pipelineId)||!NODE.equals(pipeline.nodeId)
                ||!pipeline.journal.managedWriter||!STORE.equals(pipeline.journal.url)
                ||!"transfer_journal".equals(pipeline.journal.namespace))
            throw new IllegalArgumentException("OUTAGE_FIXTURE_REQUIRES_DISPOSABLE_POSTGRESQL");
        SharedAccessStore access=new SharedAccessStore(platform,AccountDirectory.configured(platform),null,Map.of());
        GlobalTaskStore tasks=new GlobalTaskStore(cluster,access,cluster.taskKey,null,cluster.maximumTasks);
        AgentDirectory agents=new AgentDirectory(cluster,access,tasks);
        GlobalTaskViews views=new GlobalTaskViews(cluster,tasks,agents,15000);
        String directory=Files.readString(Path.of(platform.dataDirectory,"owner-directory-id")).trim();
        ControlStore.Lease control=new ControlStore.Lease(NODE,args[2],Long.parseLong(args[3]));
        try{
            views.publish(control,directory,List.of(Map.of("id",TASK)));
            throw new IllegalStateException("ORIGINAL_AGENT_PUBLICATION_ACCEPTED_UNDER_FORMER_LEASE");
        }catch(ControlStore.Fenced expected){
            // The rejoined Agent registered a new incarnation and generation.
        }
        Journal.Lease capture=new Journal.Lease(args[4],Long.parseLong(args[5]));
        try(JdbcJournal journal=new JdbcJournal(pipeline.existingJournalSettings())){
            journal.verifyIdentity(pipeline.fingerprint(),pipeline.subscribers());
            try{
                journal.heartbeat(capture,5000);
                throw new IllegalStateException("ORIGINAL_CAPTURE_HEARTBEAT_ACCEPTED_UNDER_FORMER_LEASE");
            }catch(JdbcJournal.Fenced expected){
                // The persisted capture generation is already held by the new Worker.
            }
        }
        System.out.println("ORIGINAL_AGENT_GENERATIONS_FENCED");
    }
}
