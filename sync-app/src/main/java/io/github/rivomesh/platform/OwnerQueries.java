package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.JournalWriterAssignment;
import java.sql.*;
import java.util.*;

/** Expiring encrypted reads and isolated custody staging. Task mutations use DurableCommands. */
final class OwnerQueries {
    static final class Entry {
        public int version=1;
        public String id,task,owner,directory,incarnation,actor,actorRevision,phase="PENDING",code;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String handoffConfiguration;
        public long epoch,expiresAt;
        public TaskRequest request;
        public Map<String,Object> response;
    }
    record HandoffQueries(String candidate,String originalEvidence){}
    record PlacementReview(String query,String candidate,GlobalTaskStore.Record replay){}
    record PlacementResult(boolean eligible,GlobalTaskStore.Record replay){}
    private static final long PLACEMENT_QUERY_MILLIS=8500;
    private record Execution(TaskRequest request,AccessControl.Principal actor,KafkaNoticeCatalog.Selection kafkaCatalog){
        Execution(TaskRequest request,AccessControl.Principal actor){this(request,actor,null);}
    }
    private record PreparedResume(Execution execution,ManagedHandoff.CandidateAdmission admission){}
    @FunctionalInterface interface PreparedRecheck {
        JournalWriterAssignment.CutProof verify(TaskRequest request,AccessControl.Principal actor,
                                                ManagedHandoff.CandidateAdmission admission)throws Exception;
    }
    private static final String HANDOFF_READY="HANDOFF_READY";
    private final GlobalTaskStore tasks;
    private final JdbcControlStore db;
    private final AgentDirectory agents;
    private final ControlCipher cipher;
    private final String cluster;
    private final int maximum;
    private final long lifetime;
    private final boolean recoveryReview,recoverySkip;
    private final boolean sharedHandoffEvidence;
    private final ManagedHandoff handoff;

    OwnerQueries(ClusterConfig config,GlobalTaskStore tasks,AgentDirectory agents,int maximum,long lifetime)throws Exception{
        this(config,tasks,agents,maximum,lifetime,null);
    }
    OwnerQueries(ClusterConfig config,GlobalTaskStore tasks,AgentDirectory agents,int maximum,long lifetime,ManagedHandoff handoff)throws Exception{
        if(maximum<1||maximum>10000||lifetime<1000||lifetime>60000)throw new IllegalArgumentException("OWNER_QUERY_POLICY_INVALID");
        this.tasks=tasks;this.agents=agents;this.maximum=maximum;this.lifetime=lifetime;db=tasks.database();cipher=tasks.commandCipher();cluster=config.id;recoveryReview=config.recoveryReview;recoverySkip=config.recoverySkip;
        this.handoff=handoff;
        sharedHandoffEvidence=config.handoffEvidenceRoot!=null;
        try(Connection c=db.connect()){
            db.bootstrapLock(c);JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("owner_query")+" (cluster_id VARCHAR(128) NOT NULL,query_id VARCHAR(36) NOT NULL,task_id VARCHAR(128) NOT NULL,owner_id VARCHAR(64) NOT NULL,node_epoch BIGINT NOT NULL,phase VARCHAR(16) NOT NULL,expires_at BIGINT NOT NULL,envelope "+(db.mysql()?"LONGTEXT":"TEXT")+" NOT NULL,PRIMARY KEY(cluster_id,query_id))"+(db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":""));
            db.index(c,"owner_query","rmc_owner_query_pending","cluster_id, owner_id, node_epoch, phase, expires_at, query_id");
            db.index(c,"owner_query","rmc_owner_query_expiry","cluster_id, expires_at");
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME='rmc_owner_query'",config.namespace);ResultSet r=p.executeQuery()){
                if(!r.next()||!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new TaskService.Conflict("CONTROL_STORE_ENGINE_INVALID");if(!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))throw new TaskService.Conflict("CONTROL_STORE_COLLATION_INVALID");
            }
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    private static void readonly(TaskRequest request){
        if(request.mutation()||request.kind==TaskRequest.Kind.LIST)throw new IllegalArgumentException("OWNER_QUERY_READ_REQUIRED");
    }
    private static void authorize(TaskRequest request,AccessControl.Principal actor){if(request.admin()&&!actor.administers())throw new SecurityException("FORBIDDEN");}
    private Entry read(Connection c,String id)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("owner_query")+" WHERE cluster_id=? AND query_id=?",cluster,id);ResultSet r=p.executeQuery()){
            if(!r.next())throw new NoSuchElementException("OWNER_QUERY_NOT_FOUND");Entry entry=Model.JSON.readValue(cipher.open("owner-query/"+id,r.getString("envelope")),Entry.class);
            if(entry.version!=1||!id.equals(entry.id)||!Objects.equals(entry.task,r.getString("task_id"))||!Objects.equals(entry.owner,r.getString("owner_id"))||entry.epoch!=r.getLong("node_epoch")||!Objects.equals(entry.phase,r.getString("phase"))||entry.expiresAt!=r.getLong("expires_at")||entry.request==null||!entry.task.equals(entry.request.taskId))throw new TaskService.Conflict("OWNER_QUERY_RECORD_INVALID");readonly(entry.request);
            if((entry.request.preparedHandoffRpc()||entry.request.originalFreezeRpc()||entry.request.offlineReadyRpc())&&(entry.request.handoffStaging!=null
                    ||entry.request.handoffRawStaging!=null||entry.request.handoffPreparation!=null
                    ||entry.request.handoffClosedReceipts!=null||entry.request.yaml!=null))
                throw new TaskService.Conflict("OWNER_QUERY_RECORD_INVALID");
            if(HANDOFF_READY.equals(entry.phase)&&(entry.request.kind!=TaskRequest.Kind.HANDOFF_READINESS_RPC
                    ||entry.response==null||entry.response.get("rawCapture")!=null||entry.response.get("failureRecords")!=null||entry.code!=null))
                throw new TaskService.Conflict("OWNER_QUERY_RECORD_INVALID");
            return entry;
        }
    }
    private void write(Connection c,Entry entry)throws Exception{
        if(db.update(c,"UPDATE "+db.table("owner_query")+" SET phase=?,envelope=? WHERE cluster_id=? AND query_id=?",entry.phase,cipher.seal("owner-query/"+entry.id,Model.JSON.writeValueAsString(entry)),cluster,entry.id)!=1)throw new TaskService.Conflict("OWNER_QUERY_REQUIRED");
    }
    String submit(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();readonly(request);if(Model.JSON.writeValueAsBytes(request).length>512*1024)throw new IllegalArgumentException("OWNER_QUERY_REQUEST_LIMIT");
        if(request.handoffRpc())throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
        if(request.placementProbe())throw new IllegalArgumentException("PLACEMENT_PROBE_INVALID");
        if(request.adoptedEvidence!=null)throw new IllegalArgumentException("HANDOFF_ADOPTED_SCOPE_CHANGED");
        if(request.skipRecovery()&&!recoverySkip)throw new TaskService.Conflict("SKIP_DISABLED");
        if(request.failureReview()&&!recoveryReview)throw new TaskService.Conflict("FAILURE_REVIEW_DISABLED");
        return tasks.transaction(request.taskId,actor,(c,task,current)->{
            authorize(request,current);if(request.readAtRevision()&&!Objects.equals(request.revision,task.revision))throw new TaskService.Conflict("REVISION_CHANGED");
            if(request.kind==TaskRequest.Kind.KAFKA_PHYSICAL_RECEIPT)requireKafkaPhysicalScope(task,current,request);
            AgentDirectory.Binding owner=agents.owner(c,task.owner,task.directory);
            if(!"HEALTHY".equals(owner.state))throw new TaskService.Conflict("OWNER_QUERY_UNAVAILABLE");
            if(request.readsAdoptedEvidence())request.adoptedEvidence=adoptedEvidence(c,task);
            return insert(c,request,current,owner,reserve(c,1),null);
        });
    }
    PlacementReview submitPlacement(TaskRequest create,AccessControl.Principal actor,Set<String> excluded)throws Exception{
        return tasks.reviewAutomaticCreate(create,actor,(c,current,replay)->{
            if(replay!=null)return new PlacementReview(null,null,replay);
            var candidate=agents.allocate(c,current,excluded);
            TaskRequest probe=TaskRequest.of(TaskRequest.Kind.AUTO_PLACEMENT_PROBE,create.taskId);
            probe.ownerNodeId=candidate.node;probe.requestId=create.requestId;probe.yaml=create.yaml;
            probe.identityHash=AuditStore.hash(create.yaml);probe.validate();
            return new PlacementReview(insert(c,probe,current,candidate,reserve(c,1),null),candidate.node,null);
        });
    }

    PlacementResult awaitPlacement(PlacementReview attempt,TaskRequest create,AccessControl.Principal actor)throws Exception{
        long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Math.min(lifetime,PLACEMENT_QUERY_MILLIS));
        while(System.nanoTime()<deadline){
            PlacementResult ready=tasks.reviewAutomaticCreate(create,actor,(c,current,replay)->{
                if(replay!=null)return new PlacementResult(false,replay);
                Entry entry;
                try{entry=read(c,attempt.query());}
                catch(NoSuchElementException expired){return new PlacementResult(false,null);}
                bindPlacement(entry,create,current);
                if(entry.expiresAt<=db.now(c)||"REJECTED".equals(entry.phase))return new PlacementResult(false,null);
                return "SUCCEEDED".equals(entry.phase)?new PlacementResult(true,null):null;
            });
            if(ready!=null)return ready;
            Thread.sleep(50);
        }
        return new PlacementResult(false,null);
    }

    AgentDirectory.Binding qualifiedPlacement(Connection c,String query,TaskRequest create,AccessControl.Principal actor)throws Exception{
        Entry entry;
        try{entry=read(c,query);}
        catch(NoSuchElementException expired){throw new TaskService.Conflict("AGENT_PLACEMENT_CHANGED");}
        bindPlacement(entry,create,actor);
        if(!"SUCCEEDED".equals(entry.phase)||entry.expiresAt<=db.now(c)
                ||!AutomaticPlacement.receipt(entry.request).equals(entry.response))
            throw new TaskService.Conflict("AGENT_PLACEMENT_CHANGED");
        return agents.requirePlacement(c,actor,entry.owner,entry.directory,entry.incarnation,entry.epoch);
    }

    private static void bindPlacement(Entry entry,TaskRequest create,AccessControl.Principal actor){
        if(!actor.id.equals(entry.actor)||!actor.authorizationRevision.equals(entry.actorRevision))
            throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        if(!entry.request.placementProbe()||!entry.task.equals(create.taskId)
                ||!entry.request.requestId.equals(create.requestId)||!entry.request.identityHash.equals(AuditStore.hash(create.yaml)))
            throw new TaskService.Conflict("PLACEMENT_PROBE_INVALID");
    }

    private void verifyPlacementRoute(Connection c,Entry entry,AccessControl.Principal actor)throws Exception{
        if(!entry.owner.equals(entry.request.ownerNodeId)||!entry.request.identityHash.equals(AuditStore.hash(entry.request.yaml)))
            throw new TaskService.Conflict("PLACEMENT_PROBE_INVALID");
        tasks.requireNewTask(c,entry.task);
        agents.requirePlacement(c,actor,entry.owner,entry.directory,entry.incarnation,entry.epoch);
    }

    HandoffQueries submitHandoff(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(handoff==null||request.kind!=TaskRequest.Kind.HANDOFF_PROBE)
            throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
        return tasks.transaction(request.taskId,actor,Connection.TRANSACTION_READ_COMMITTED,(c,task,current)->{
            ManagedHandoff.ProbeRoute route=handoff.probeRoute(c,task,current,request.reference);
            long now=reserve(c,2);
            TaskRequest candidate=TaskRequest.of(TaskRequest.Kind.HANDOFF_CANDIDATE_PROBE,task.id);
            candidate.reference=request.reference;candidate.revision=route.revision();
            TaskRequest evidence=TaskRequest.of(TaskRequest.Kind.HANDOFF_EVIDENCE_PROBE,task.id);
            evidence.reference=request.reference;evidence.revision=route.revision();
            return new HandoffQueries(insert(c,candidate,current,route.candidate(),now,route.activeVersion()),
                    insert(c,evidence,current,route.original(),now,route.activeVersion()));
        });
    }
    String submitHandoffCustody(TaskRequest supplied,AccessControl.Principal actor,
                               SharedHandoffEvidence.Archive archive)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(handoff==null||request.kind!=TaskRequest.Kind.HANDOFF_PROBE)
            throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
        return tasks.transaction(request.taskId,actor,Connection.TRANSACTION_READ_COMMITTED,(c,task,current)->{
            ManagedHandoff.ProbeRoute route=handoff.probeRoute(c,task,current,request.reference);
            TaskRequest verify=TaskRequest.of(TaskRequest.Kind.HANDOFF_CUSTODY_PROBE,task.id);
            verify.reference=request.reference;verify.revision=route.revision();verify.handoffArchive=archive;
            return insert(c,verify,current,route.candidate(),reserve(c,1),route.activeVersion());
        });
    }
    String submitFreezeOrigin(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();
        boolean cancelling=request.kind==TaskRequest.Kind.HANDOFF_CANCEL_FREEZE;
        if(handoff==null||!cancelling&&request.kind!=TaskRequest.Kind.HANDOFF_FREEZE_ORIGINAL)
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_REQUEST_INVALID");
        return tasks.transaction(request.taskId,actor,Connection.TRANSACTION_READ_COMMITTED,(c,task,current)->{
            TaskRequest scoped=TaskRequest.of(cancelling?TaskRequest.Kind.HANDOFF_CANCEL_FREEZE_RPC:
                    TaskRequest.Kind.HANDOFF_FREEZE_ORIGINAL_RPC,task.id);
            scoped.reference=request.reference;scoped.revision=request.revision;scoped.identityHash=request.identityHash;
            scoped.handoffInventorySha256=request.handoffInventorySha256;scoped.requestId=request.requestId;
            var route=handoff.freezeOriginRoute(c,task,current,scoped,cancelling);
            return insert(c,scoped,current,route.original(),reserve(c,1),route.plan().evidence().configuration());
        });
    }
    String submitHandoffStaging(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        return submitPreparedHandoff(supplied,actor,TaskRequest.Kind.HANDOFF_STAGING_RPC);
    }
    ManagedHandoff.CandidateAdmission candidateReadiness(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        String query=submitHandoffReadiness(supplied,actor);
        return Model.JSON.convertValue(await(query,actor),ManagedHandoff.CandidateAdmission.class);
    }
    String submitHandoffReadiness(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        return submitPreparedHandoff(supplied,actor,TaskRequest.Kind.HANDOFF_READINESS_RPC);
    }
    private String submitPreparedHandoff(TaskRequest supplied,AccessControl.Principal actor,TaskRequest.Kind kind)throws Exception{
        TaskRequest request=supplied.snapshot();
        TaskRequest.Kind operation=kind==TaskRequest.Kind.HANDOFF_READINESS_RPC?TaskRequest.Kind.HANDOFF_COMMIT_OWNER:TaskRequest.Kind.HANDOFF_STAGE;
        if(handoff==null||request.kind!=operation)throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
        if(request.yaml!=null)throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
        return tasks.transaction(request.taskId,actor,Connection.TRANSACTION_READ_COMMITTED,(c,task,current)->{
            TaskRequest scoped=TaskRequest.of(kind,task.id);
            scoped.reference=request.reference;scoped.revision=request.revision;scoped.identityHash=request.identityHash;
            scoped.requestId=request.requestId;
            ManagedHandoff.StageRoute route=handoff.stageRoute(c,task,current,scoped);
            return insert(c,scoped,current,route.candidate(),reserve(c,1),route.binding().scope().configuration());
        });
    }
    ManagedHandoff.CandidateAdmission offlineCandidateReadiness(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(handoff==null||request.kind!=TaskRequest.Kind.HANDOFF_OFFLINE_OWNER_COMMIT||request.yaml!=null||request.document!=null)
            throw new IllegalArgumentException("HANDOFF_OFFLINE_COMMIT_REQUEST_INVALID");
        String query=tasks.transaction(request.taskId,actor,Connection.TRANSACTION_READ_COMMITTED,(c,task,current)->{
            var route=handoff.offlineRoute(c,task,current,request);
            TaskRequest scoped=TaskRequest.of(TaskRequest.Kind.HANDOFF_OFFLINE_READY_RPC,task.id);
            scoped.reference=request.reference;scoped.revision=request.revision;scoped.identityHash=request.identityHash;
            scoped.requestId=request.requestId;scoped.handoffInventorySha256=request.handoffInventorySha256;
            scoped.confirmOriginalUnavailable=request.confirmOriginalUnavailable;
            return insert(c,scoped,current,route.candidate(),reserve(c,1),route.plan().evidence().configuration());
        });
        return Model.JSON.convertValue(await(query,actor),ManagedHandoff.CandidateAdmission.class);
    }

    private long reserve(Connection c,int slots)throws SQLException{
        long now=db.now(c);
        db.update(c,"DELETE FROM "+db.table("owner_query")+" WHERE cluster_id=? AND expires_at<=?",cluster,now);
        try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("owner_query")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){
            r.next();if(r.getLong(1)>maximum-slots)throw new TaskService.Conflict("OWNER_QUERY_CAPACITY");
        }
        return now;
    }
    private String insert(Connection c,TaskRequest request,AccessControl.Principal actor,AgentDirectory.Binding target,
                          long now,String activeVersion)throws Exception{
        Entry entry=new Entry();entry.id=UUID.randomUUID().toString();entry.task=request.taskId;
        entry.owner=target.node;entry.directory=target.directory;entry.epoch=target.epoch;entry.incarnation=target.incarnation;
        entry.actor=actor.id;entry.actorRevision=actor.authorizationRevision;entry.request=request;
        entry.expiresAt=now+(request.placementProbe()?Math.min(lifetime,PLACEMENT_QUERY_MILLIS):lifetime);
        entry.handoffConfiguration=activeVersion;
        db.update(c,"INSERT INTO "+db.table("owner_query")
                +" (cluster_id,query_id,task_id,owner_id,node_epoch,phase,expires_at,envelope) VALUES (?,?,?,?,?,?,?,?)",
                cluster,entry.id,entry.task,entry.owner,entry.epoch,entry.phase,entry.expiresAt,
                cipher.seal("owner-query/"+entry.id,Model.JSON.writeValueAsString(entry)));
        return entry.id;
    }
    private ManagedHandoff.ProbeRoute verifyHandoff(Connection c,GlobalTaskStore.Record record,
                                                    AccessControl.Principal actor,Entry entry)throws Exception{
        if(handoff==null||entry.handoffConfiguration==null)throw new TaskService.Conflict("HANDOFF_REQUEST_INVALID");
        ManagedHandoff.ProbeRoute route=handoff.probeRoute(c,record,actor,entry.request.reference);
        if(!route.activeVersion().equals(entry.handoffConfiguration)||!route.revision().equals(entry.request.revision))
            throw new TaskService.Conflict("HANDOFF_ASSESSMENT_CHANGED");
        AgentDirectory.Binding target=entry.request.kind==TaskRequest.Kind.HANDOFF_EVIDENCE_PROBE?route.original():route.candidate();
        requireHandoffAgent(entry,target);
        return route;
    }
    private ManagedHandoff.FreezeOriginRoute verifyFreezeOrigin(Connection c,GlobalTaskStore.Record record,
                                                               AccessControl.Principal actor,Entry entry)throws Exception{
        if(handoff==null||entry.handoffConfiguration==null)throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        var route=handoff.freezeOriginRoute(c,record,actor,entry.request,entry.request.kind==TaskRequest.Kind.HANDOFF_CANCEL_FREEZE_RPC);
        if(!entry.handoffConfiguration.equals(route.plan().evidence().configuration()))
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        requireHandoffAgent(entry,route.original());
        return route;
    }

    private ManagedHandoff.OfflineRoute verifyOfflineCandidate(Connection c,GlobalTaskStore.Record record,
                                                               AccessControl.Principal actor,Entry entry)throws Exception{
        if(handoff==null||entry.handoffConfiguration==null)
            throw new TaskService.Conflict("HANDOFF_OFFLINE_COMMIT_FREEZE_REQUIRED");
        var route=handoff.offlineRoute(c,record,actor,entry.request);
        if(!entry.handoffConfiguration.equals(route.plan().evidence().configuration()))
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        requireHandoffAgent(entry,route.candidate());
        return route;
    }

    private ManagedHandoff.StageRoute verifyPreparedHandoff(Connection c,GlobalTaskStore.Record record,
                                                       AccessControl.Principal actor,Entry entry)throws Exception{
        if(handoff==null||entry.handoffConfiguration==null)throw new TaskService.Conflict("HANDOFF_REQUEST_INVALID");
        ManagedHandoff.StageRoute stage=handoff.stageRoute(c,record,actor,entry.request);
        if(!entry.handoffConfiguration.equals(stage.binding().scope().configuration()))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
        requireHandoffAgent(entry,stage.candidate());
        return stage;
    }
    private HandoffEvidenceAdoption.Receipt adoptedEvidence(Connection c,GlobalTaskStore.Record record)throws Exception{
        HandoffPreparations.Plan plan=tasks.preparations().latest(c,record.id,db.now(c));
        if(!HandoffPreparations.evidenceInstalled(plan))return null;
        var receipt=plan.evidenceAdoption();
        if(receipt==null||!record.revision.equals(receipt.committedRevision())&&!HandoffPreparations.workerAdmitted(plan)
                ||!record.owner.equals(receipt.candidate().node())||!record.directory.equals(receipt.candidate().directory())
                ||!Objects.equals(record.activeConfigurationVersion,plan.writerCommit().configuration())
                ||!Objects.equals(record.configurationVersion,plan.writerCommit().configuration()))
            throw new TaskService.Conflict("HANDOFF_ADOPTED_SCOPE_CHANGED");
        return receipt;
    }
    private void verifyAdoptedEvidence(Connection c,Entry entry)throws Exception{
        if(entry.request.readsAdoptedEvidence()&&!Objects.equals(entry.request.adoptedEvidence,adoptedEvidence(c,tasks.record(c,entry.task))))
            throw new TaskService.Conflict("HANDOFF_ADOPTED_SCOPE_CHANGED");
    }
    private static void requireKafkaPhysicalScope(GlobalTaskStore.Record task,AccessControl.Principal actor,TaskRequest request){
        var predecessor=request.kafkaNoticePredecessor;
        if(predecessor==null||!task.id.equals(predecessor.task().taskId())||!task.revision.equals(predecessor.task().revision())
                ||!task.owner.equals(predecessor.task().ownerNodeId())||!task.directory.equals(predecessor.task().ownerDirectoryId())
                ||!Objects.equals(task.configurationVersion,predecessor.task().configurationVersion())
                ||!Objects.equals(task.activeConfigurationVersion,predecessor.task().configurationVersion()))
            throw new TaskService.Conflict("KAFKA_PHYSICAL_RECEIPT_SCOPE_CHANGED");
        if(!actor.id.equals(predecessor.administrator().actorId())
                ||!Objects.equals(actor.authorizationRevision,predecessor.administrator().authorizationRevision()))
            throw new SecurityException("FORBIDDEN");
    }
    private static void requireHandoffAgent(Entry entry,AgentDirectory.Binding target){
        if(!entry.owner.equals(target.node)||!entry.directory.equals(target.directory)||entry.epoch!=target.epoch
                ||!entry.incarnation.equals(target.incarnation)||!"HEALTHY".equals(target.state))
            throw new TaskService.Conflict("HANDOFF_AGENT_CHANGED");
    }
    private static AccessControl.Principal actor(AccountDirectory.State accounts,Entry entry){
        AccountDirectory.User user=accounts.users.get(entry.actor);if(user==null||!user.enabled||user.deleted||!Objects.equals(accounts.revision,entry.actorRevision))throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        AccessControl.Principal actor=new AccessControl.Principal(user.id,user.role,user.taskIds,accounts.revision,user.nodeIds);
        if(!actor.task(entry.task)||!actor.node(entry.owner))throw new SecurityException("FORBIDDEN");authorize(entry.request,actor);return actor;
    }
    List<String> pending(ControlStore.Lease lease,String directory,int limit)throws Exception{
        if(limit<1||limit>50)throw new IllegalArgumentException("OWNER_QUERY_PAGE_INVALID");return tasks.service((c,accounts)->{
            agents.require(c,lease,directory);List<String> result=new ArrayList<>();
            try(PreparedStatement p=db.prepare(c,"SELECT query_id FROM "+db.table("owner_query")+" WHERE cluster_id=? AND owner_id=? AND node_epoch=? AND phase='PENDING' AND expires_at>? ORDER BY expires_at,query_id LIMIT ?",cluster,lease.nodeId,lease.epoch,db.now(c),limit);ResultSet r=p.executeQuery()){while(r.next())result.add(r.getString(1));}return result;
        });
    }
    void serve(String id,ControlStore.Lease lease,String directory,TaskControlPlane local)throws Exception{
        Entry entry=tasks.service((c,accounts)->{
            agents.require(c,lease,directory);Entry e=read(c,id);
            if(!e.owner.equals(lease.nodeId)||!e.directory.equals(directory)||e.epoch!=lease.epoch||!e.incarnation.equals(lease.incarnation))throw new ControlStore.Fenced();
            if(e.expiresAt<=db.now(c))throw new TaskService.Conflict("OWNER_QUERY_EXPIRED");if(!"PENDING".equals(e.phase))return null;
            if(e.request.readAtRevision()&&!Objects.equals(e.request.revision,tasks.record(c,e.task).revision)){e.phase="REJECTED";e.code="REVISION_CHANGED";write(c,e);return null;}
            try{actor(accounts,e);}catch(SecurityException|TaskService.Conflict revoked){e.phase="REJECTED";e.code="AUTHORIZATION_CHANGED";write(c,e);return null;}
            e.phase="RUNNING";write(c,e);return e;
        });
        if(entry==null)return;Map<String,Object> response=null;String failure=null;
        try{
            Execution execution=tasks.service((c,accounts)->{
                agents.require(c,lease,directory);AccessControl.Principal actor=actor(accounts,entry);
                if(entry.request.placementProbe()){
                    verifyPlacementRoute(c,entry,actor);
                    return new Execution(entry.request,actor);
                }
                if(entry.request.kind==TaskRequest.Kind.KAFKA_CATALOG_PROBE){
                    if(!recoveryReview)throw new TaskService.Conflict("FAILURE_REVIEW_DISABLED");
                    return new Execution(entry.request,actor,KafkaNoticeCatalog.probeInput(tasks,c,entry.request));
                }
                if(entry.request.kind==TaskRequest.Kind.KAFKA_PHYSICAL_RECEIPT)
                    requireKafkaPhysicalScope(tasks.record(c,entry.task),actor,entry.request);
                if(!entry.request.handoffRpc()){
                    verifyAdoptedEvidence(c,entry);
                    return new Execution(entry.request,actor);
                }
                GlobalTaskStore.Record record=tasks.record(c,entry.task);
                TaskRequest scoped=entry.request.snapshot();
                if(scoped.preparedHandoffRpc())return preparedExecution(c,record,actor,entry);
                if(scoped.offlineReadyRpc()){
                    var route=verifyOfflineCandidate(c,record,actor,entry);
                    scoped.handoffPreparation=route.plan();
                    scoped.yaml=tasks.activeYaml(c,record);
                    return new Execution(scoped,actor);
                }
                if(scoped.originalFreezeRpc()){
                    var route=verifyFreezeOrigin(c,record,actor,entry);
                    scoped.handoffPreparation=route.plan();
                    scoped.handoffStaging=route.staging();
                    scoped.handoffClosedReceipts=route.receipts();
                    scoped.yaml=tasks.activeYaml(c,record);
                    return new Execution(scoped,actor);
                }
                ManagedHandoff.ProbeRoute route=verifyHandoff(c,record,actor,entry);
                if(scoped.kind==TaskRequest.Kind.HANDOFF_CANDIDATE_PROBE){
                    scoped.yaml=tasks.activeYaml(c,record);
                    scoped.handoffCut=route.cut();
                    scoped.ownerNodeId=route.original().node;
                }else{
                    scoped.handoffEvidenceScope=new SharedHandoffEvidence.Scope(cluster,record.id,scoped.reference,
                            route.revision(),route.activeVersion(),route.original().directory,route.candidate().directory);
                    if(scoped.kind==TaskRequest.Kind.HANDOFF_EVIDENCE_PROBE&&sharedHandoffEvidence)
                        scoped.handoffClosedReceipts=handoff.closedReceipts(c,record);
                }
                return new Execution(scoped,actor);
            });
            Map<String,Object> returned;
            if(execution.request.kind==TaskRequest.Kind.KAFKA_PHYSICAL_RECEIPT)
                returned=local.ownerKafkaPhysicalReceipt(execution.request,execution.actor,tasks.identity,cipher);
            else returned=execution.kafkaCatalog==null?local.ownerQuery(execution.request,execution.actor)
                    :local.ownerKafkaCatalog(execution.kafkaCatalog,execution.actor);
            response=entry.request.kind==TaskRequest.Kind.HANDOFF_READINESS_RPC||entry.request.offlineReadyRpc()?readinessResponse(entry,returned)
                    :entry.request.originalFreezeRpc()?freezeOriginResponse(entry,returned):returned;
            if(entry.request.placementProbe()&&!AutomaticPlacement.receipt(entry.request).equals(response))
                throw new TaskService.Conflict("PLACEMENT_PROBE_INVALID");
            if(response==null||Model.JSON.writeValueAsBytes(response).length>1024*1024){response=null;failure="OWNER_QUERY_RESULT_LIMIT";}
        }catch(TaskService.Conflict rejected){failure=rejected.getMessage()!=null&&rejected.getMessage().matches("[A-Z0-9_]{1,120}")?rejected.getMessage():"OWNER_QUERY_REJECTED";}
        catch(SecurityException denied){failure="AUTHORIZATION_CHANGED";}
        catch(NoSuchElementException absent){failure="OWNER_DATA_NOT_FOUND";}
        catch(Exception unavailable){failure="OWNER_QUERY_FAILED";}
        final Map<String,Object> body=response;final String code=failure;
        tasks.service((c,accounts)->{
            agents.require(c,lease,directory);Entry current=read(c,id);if(!"RUNNING".equals(current.phase))throw new TaskService.Conflict("OWNER_QUERY_PHASE_CHANGED");
            if(current.expiresAt<=db.now(c))throw new TaskService.Conflict("OWNER_QUERY_EXPIRED");
            if(current.request.readAtRevision()&&!Objects.equals(current.request.revision,tasks.record(c,current.task).revision)){current.phase="REJECTED";current.code="REVISION_CHANGED";current.response=null;write(c,current);return null;}
            try{
                AccessControl.Principal actor=actor(accounts,current);
                if(current.request.placementProbe())verifyPlacementRoute(c,current,actor);
                else if(current.request.preparedHandoffRpc())
                    verifyPreparedHandoff(c,tasks.record(c,current.task),actor,current);
                else if(current.request.originalFreezeRpc())verifyFreezeOrigin(c,tasks.record(c,current.task),actor,current);
                else if(current.request.offlineReadyRpc())verifyOfflineCandidate(c,tasks.record(c,current.task),actor,current);
                else if(current.request.handoffRpc())verifyHandoff(c,tasks.record(c,current.task),actor,current);
                else if(current.request.kind==TaskRequest.Kind.KAFKA_PHYSICAL_RECEIPT)
                    requireKafkaPhysicalScope(tasks.record(c,current.task),actor,current.request);
                else verifyAdoptedEvidence(c,current);
            }catch(SecurityException|TaskService.Conflict revoked){
                current.phase="REJECTED";current.code=revoked instanceof SecurityException?"AUTHORIZATION_CHANGED":revoked.getMessage();
                current.response=null;write(c,current);return null;
            }
            current.response=body;current.code=code;
            if(code!=null)current.phase="REJECTED";
            else if(current.request.kind==TaskRequest.Kind.HANDOFF_READINESS_RPC
                    &&body.get("rawCapture")==null&&body.get("failureRecords")==null)
                current.phase=HANDOFF_READY;
            else current.phase="SUCCEEDED";
            write(c,current);return null;
        });
    }
    private Execution preparedExecution(Connection c,GlobalTaskStore.Record record,AccessControl.Principal actor,
                                        Entry entry)throws Exception{
        var route=verifyPreparedHandoff(c,record,actor,entry);
        TaskRequest request=entry.request.snapshot();
        request.handoffStaging=route.binding();
        request.handoffRawStaging=route.rawCapture();
        if(request.kind==TaskRequest.Kind.HANDOFF_READINESS_RPC){
            request.handoffPreparation=route.plan();
            request.yaml=tasks.activeYaml(c,record);
        }
        return new Execution(request,actor);
    }
    String nextSplitHandoff(ControlStore.Lease lease,String directory,String after)throws Exception{
        return tasks.service((c,accounts)->{
            agents.require(c,lease,directory);
            return tasks.preparations().nextSplitTransfer(c,lease.nodeId,after);
        });
    }

    void resumeSplitHandoff(String id,ControlStore.Lease lease,String directory)throws Exception{
        tasks.service((c,accounts)->{
            agents.require(c,lease,directory);
            var plan=tasks.preparations().byRequest(c,id,db.now(c));
            if(plan==null||!"PREPARED".equals(plan.phase()))return null;
            if(!plan.evidence().candidate().node().equals(lease.nodeId)
                    ||!plan.evidence().candidate().directory().equals(directory))throw new ControlStore.Fenced();
            var transfer=tasks.preparations().splitTransfer(c,plan);
            if(transfer==null)return null;
            var user=accounts.users.get(plan.actor());
            if(user==null||!user.enabled||user.deleted||!Objects.equals(accounts.revision,plan.actorRevision()))
                throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
            var actor=new AccessControl.Principal(user.id,user.role,user.taskIds,accounts.revision,user.nodeIds);
            handoff.finishSplitTransfer(c,tasks.record(c,plan.task()),actor,plan,transfer.request());
            agents.require(c,lease,directory);
            return null;
        });
    }

    String nextHandoffResume(ControlStore.Lease lease,String directory,String after)throws Exception{
        return tasks.service((c,accounts)->{
            agents.require(c,lease,directory);
            try(var p=db.prepare(c,"SELECT query_id FROM "+db.table("owner_query")
                    +" WHERE cluster_id=? AND owner_id=? AND node_epoch=? AND phase=? AND expires_at>? AND query_id>?"
                    +" ORDER BY query_id LIMIT 1",cluster,lease.nodeId,lease.epoch,HANDOFF_READY,db.now(c),after);
                var rows=p.executeQuery()){
                return rows.next()?rows.getString(1):null;
            }
        });
    }
    void resumeHandoff(String id,ControlStore.Lease lease,String directory,PreparedRecheck recheck)throws Exception{
        PreparedResume expected=tasks.service((c,accounts)->preparedResume(c,accounts,id,lease,directory));
        if(expected==null)return;
        var cut=recheck.verify(expected.execution.request,expected.execution.actor,expected.admission);
        tasks.service((c,accounts)->{
            var current=preparedResume(c,accounts,id,lease,directory);
            if(current==null)return null;
            TaskRequest sealed=expected.execution.request,live=current.execution.request;
            if(!Objects.equals(sealed.handoffPreparation,live.handoffPreparation)||!Objects.equals(sealed.yaml,live.yaml)
                    ||!Objects.equals(sealed.handoffStaging,live.handoffStaging)||!expected.admission.equals(current.admission))
                throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
            handoff.resumePreparedWriter(c,tasks.record(c,live.taskId),current.execution.actor,current.admission,live.requestId,cut);
            agents.require(c,lease,directory);
            Entry complete=read(c,id);complete.phase="SUCCEEDED";write(c,complete);
            return null;
        });
    }
    private PreparedResume preparedResume(Connection c,AccountDirectory.State accounts,String id,ControlStore.Lease lease,
                                          String directory)throws Exception{
        agents.require(c,lease,directory);
        Entry entry=read(c,id);
        if(!entry.owner.equals(lease.nodeId)||!entry.directory.equals(directory)
                ||entry.epoch!=lease.epoch||!entry.incarnation.equals(lease.incarnation))throw new ControlStore.Fenced();
        long now=db.now(c);
        if(!HANDOFF_READY.equals(entry.phase)||entry.expiresAt<=now)return null;
        AccessControl.Principal actor=actor(accounts,entry);
        GlobalTaskStore.Record task=tasks.record(c,entry.task);
        var plan=tasks.preparations().latest(c,entry.task,now);
        if(plan==null||!plan.id().equals(entry.request.reference)||!"PREPARED".equals(plan.phase())){
            entry.phase="SUCCEEDED";write(c,entry);
            return null;
        }
        var admission=Model.JSON.convertValue(readinessResponse(entry,entry.response),ManagedHandoff.CandidateAdmission.class);
        return new PreparedResume(preparedExecution(c,task,actor,entry),admission);
    }

    private static Map<String,Object> freezeOriginResponse(Entry entry,Map<String,Object> returned){
        if(returned==null||!returned.keySet().equals(Set.of("cut","receiptPayloadSha256"))
                ||!(returned.get("receiptPayloadSha256") instanceof String digest)||!digest.matches("[a-f0-9]{64}"))
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        var cut=Model.JSON.convertValue(returned.get("cut"),JournalWriterAssignment.CutProof.class);
        if(cut==null||cut.managedWriter()==null||!entry.owner.equals(cut.managedWriter().originalNode()))
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CUT_CHANGED");
        var original=new HandoffPreparations.Binding(entry.owner,entry.directory,entry.incarnation,entry.epoch);
        return Model.JSON.convertValue(new ManagedHandoff.FreezeOriginProof(original,cut,digest),Map.class);
    }

    private static Map<String,Object> readinessResponse(Entry entry,Map<String,Object> response){
        Set<String> required=Set.of("preparation","binding","desired","configurationSha256","stagingSha256");
        Set<String> allowed=Set.of("preparation","binding","desired","configurationSha256","stagingSha256","rawCapture","failureRecords");
        if(response==null||!response.keySet().containsAll(required)||!allowed.containsAll(response.keySet())
                ||response.containsKey("rawCapture")&&response.get("rawCapture")==null
                ||response.containsKey("failureRecords")&&!(response.get("failureRecords") instanceof Integer count&&count>0))
            throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        ManagedHandoff.CandidateAdmission ready;
        try{ready=Model.JSON.convertValue(response,ManagedHandoff.CandidateAdmission.class);}
        catch(IllegalArgumentException invalid){throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED");}
        var expected=new HandoffPreparations.Binding(entry.owner,entry.directory,entry.incarnation,entry.epoch);
        if(!ready.valid()||!entry.request.reference.equals(ready.preparation())||!expected.equals(ready.binding())
                ||entry.request.offlineReadyRpc()&&(ready.rawCapture()!=null||ready.failureRecords()!=0))
            throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        return Model.JSON.convertValue(ready,Map.class);
    }
    Map<String,Object> poll(String id,AccessControl.Principal supplied)throws Exception{
        String task;try(Connection c=db.connect();PreparedStatement p=db.prepare(c,"SELECT task_id FROM "+db.table("owner_query")+" WHERE cluster_id=? AND query_id=?",cluster,id);ResultSet r=p.executeQuery()){if(!r.next())throw new NoSuchElementException("OWNER_QUERY_NOT_FOUND");task=r.getString(1);}catch(SQLException failure){throw new ControlStore.Unavailable();}
        return tasks.transaction(task,supplied,(c,record,current)->{
            Entry entry=read(c,id);
            if(entry.request.placementProbe())throw new IllegalArgumentException("PLACEMENT_PROBE_INVALID");
            if(!entry.actor.equals(current.id))throw new SecurityException("FORBIDDEN");if(!entry.actorRevision.equals(current.authorizationRevision))throw new TaskService.Conflict("AUTHORIZATION_CHANGED");authorize(entry.request,current);
            if(entry.request.readAtRevision()&&!Objects.equals(entry.request.revision,record.revision))throw new TaskService.Conflict("REVISION_CHANGED");
            if(entry.expiresAt<=db.now(c))throw new TaskService.Conflict("OWNER_QUERY_EXPIRED");
            if(entry.request.preparedHandoffRpc())verifyPreparedHandoff(c,record,current,entry);
            else if(entry.request.originalFreezeRpc())verifyFreezeOrigin(c,record,current,entry);
            else if(entry.request.offlineReadyRpc())verifyOfflineCandidate(c,record,current,entry);
            else if(entry.request.handoffRpc())verifyHandoff(c,record,current,entry);
            else{
                if(entry.request.kind==TaskRequest.Kind.KAFKA_PHYSICAL_RECEIPT)requireKafkaPhysicalScope(record,current,entry.request);
                else verifyAdoptedEvidence(c,entry);
                AgentDirectory.Binding owner=agents.owner(c,record.owner,record.directory);
                if(!entry.owner.equals(owner.node)||!entry.directory.equals(owner.directory)||entry.epoch!=owner.epoch||!entry.incarnation.equals(owner.incarnation)||!"HEALTHY".equals(owner.state))throw new TaskService.Conflict("OWNER_QUERY_UNAVAILABLE");
            }
            String phase=HANDOFF_READY.equals(entry.phase)?"SUCCEEDED":entry.phase;
            Map<String,Object> result=new LinkedHashMap<>();result.put("status",phase);result.put("code",entry.code);
            if("SUCCEEDED".equals(phase))result.put("result",entry.response);return result;
        });
    }
    Map<String,Object> await(String id,AccessControl.Principal actor)throws Exception{
        long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(lifetime);
        while(System.nanoTime()<deadline){Map<String,Object> result=poll(id,actor);if("SUCCEEDED".equals(result.get("status")))return Model.JSON.convertValue(result.get("result"),Map.class);if("REJECTED".equals(result.get("status")))throw new TaskService.Conflict((String)result.get("code"));Thread.sleep(50);}
        throw new TaskService.Conflict("OWNER_QUERY_TIMEOUT");
    }
}
