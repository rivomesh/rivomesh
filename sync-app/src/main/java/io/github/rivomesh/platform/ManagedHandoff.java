package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.JournalWriterAssignment;
import java.sql.*;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.*;

/** Records a bounded assessment of one control-plane handoff without granting authority to execute it. */
final class ManagedHandoff {
    private record Agent(String node,String directory,String incarnation,long epoch,String state){
        static Agent of(AgentDirectory.Binding b){return new Agent(b.node,b.directory,b.incarnation,b.epoch,b.state);}
        boolean matches(AgentDirectory.Binding b){return node.equals(b.node)&&directory.equals(b.directory)
                &&incarnation.equals(b.incarnation)&&epoch==b.epoch&&state.equals(b.state);}
    }
    private record OriginalCut(String leaseState,String journalEpoch,String sequence,String checkpointHash){}
    private record ProbeResult(long checkedAt,Map<String,Object> candidate,Map<String,Object> originalEvidence,
                               Map<String,Object> sharedReportCustody,
                               @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                               Map<String,Object> sharedRawCaptureCustody){}
    record ProbeRoute(String revision,String activeVersion,AgentDirectory.Binding original,AgentDirectory.Binding candidate,
                      JournalWriterAssignment.ExpectedCut cut){}
    record StageRoute(HandoffStaging.Binding binding,AgentDirectory.Binding candidate,HandoffPreparations.Plan plan){}
    private record Assessment(int version,String task,String request,String replaces,String revision,String activeVersion,
                              Agent original,Agent candidate,int taskSlots,int workerSlots,long assigned,
                              String actor,String actorRevision,long assessedAt,OriginalCut originalCut,List<String> blockers,
                              ProbeResult probe){}
    private static final String CANDIDATE_ACCESS="CANDIDATE_ACCESS_UNVERIFIED";
    private static final String RECOVERY_EVIDENCE="ORIGINAL_RECOVERY_EVIDENCE_UNVERIFIED";
    private static final ObjectMapper YAML=new ObjectMapper(new YAMLFactory());
    private final String cluster;
    private final long freshness;
    private final JdbcControlStore db;
    private final ControlCipher cipher;
    private final SharedAccessStore access;
    private final GlobalTaskStore tasks;
    private final DurableCommands commands;
    private final AgentDirectory agents;
    private final GlobalTaskViews views;

    ManagedHandoff(ClusterConfig config,SharedAccessStore access,GlobalTaskStore tasks,DurableCommands commands,
                   AgentDirectory agents,GlobalTaskViews views)throws Exception{
        if(!access.controls(config)||!tasks.accessIdentity().equals(access.identity))
            throw new IllegalArgumentException("HANDOFF_POLICY_INVALID");
        cluster=config.id;freshness=Math.min(300000,Math.max(15000,config.leaseMillis));
        this.access=access;this.tasks=tasks;this.commands=commands;this.agents=agents;this.views=views;
        db=tasks.database();cipher=tasks.commandCipher();
        try(Connection c=db.connect()){
            db.bootstrapLock(c);
            String suffix=db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"";
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("task_handoff")
                    +" (cluster_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,request_id VARCHAR(128) NOT NULL,"
                    +"envelope "+(db.mysql()?"LONGTEXT":"TEXT")+" NOT NULL,PRIMARY KEY(cluster_id,task_id))"+suffix);
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES"
                    +" WHERE TABLE_SCHEMA=? AND TABLE_NAME='rmc_task_handoff'",config.namespace);ResultSet r=p.executeQuery()){
                if(!r.next()||!"InnoDB".equalsIgnoreCase(r.getString(1))||!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))
                    throw new TaskService.Conflict("HANDOFF_STORE_INVALID");
            }
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }

    Map<String,Object> assess(TaskRequest request,AccessControl.Principal supplied)throws Exception{
        return tasks.transaction(request.taskId,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            requireGrants(actor,task.owner,request.ownerNodeId);
            Assessment prior=read(c,task.id);
            if(prior!=null){
                requireGrants(actor,prior.original.node,prior.candidate.node);
                if(prior.request.equals(request.requestId)){
                    if(!prior.actor.equals(actor.id)||!prior.revision.equals(request.revision)
                            ||!prior.candidate.node.equals(request.ownerNodeId)||!Objects.equals(prior.replaces,request.reference))
                        throw new TaskService.Conflict("HANDOFF_REQUEST_REUSED");
                    return view(c,task,actor,prior);
                }
            }
            tasks.preparations().requireUnreserved(c,task.id);
            if(!Objects.equals(prior==null?null:prior.request,request.reference))
                throw new TaskService.Conflict("HANDOFF_ASSESSMENT_CHANGED");
            if(!task.revision.equals(request.revision))throw new TaskService.Conflict("REVISION_CHANGED");
            if(task.owner.equals(request.ownerNodeId))throw new TaskService.Conflict("HANDOFF_SAME_AGENT");
            AgentDirectory.Binding original=agents.owner(c,task.owner,task.directory);
            AgentDirectory.HandoffCapacity candidate=agents.handoffCapacity(c,request.ownerNodeId);
            List<String> blockers=new ArrayList<>();
            if(!"HEALTHY".equals(candidate.binding().state))blockers.add("CANDIDATE_AGENT_UNAVAILABLE");
            if(!candidate.roomForPreparation())
                blockers.add("CANDIDATE_TASK_CAPACITY_UNAVAILABLE");
            if(!Objects.equals(task.configurationVersion,task.activeConfigurationVersion))
                blockers.add("TASK_CONFIGURATION_NOT_APPLIED");
            else if(!supported(tasks.activeYaml(c,task)))blockers.add("HANDOFF_JOURNAL_PROFILE_UNSUPPORTED");
            if(commands.hasUnresolved(c,task.id))blockers.add("TASK_COMMANDS_UNRESOLVED");
            long assessedAt=db.now(c);
            OriginalCut originalCut=inspectOriginal(views.read(c,task,actor),assessedAt,blockers);
            blockers.add(CANDIDATE_ACCESS);
            blockers.add(RECOVERY_EVIDENCE);
            Assessment next=new Assessment(1,task.id,request.requestId,request.reference,task.revision,task.activeConfigurationVersion,
                    Agent.of(original),Agent.of(candidate.binding()),candidate.taskSlots(),candidate.workerSlots(),
                    candidate.assignedTasks(),actor.id,actor.authorizationRevision,assessedAt,originalCut,List.copyOf(blockers),null);
            save(c,next,prior==null,actor,"MANAGED_HANDOFF_ASSESSED");
            return summary(next,false,assessedAt);
        });
    }

    private void save(Connection c,Assessment assessment,boolean insert,AccessControl.Principal actor,String kind)throws Exception{
        String encoded=Model.JSON.writeValueAsString(assessment);
        String envelope=cipher.seal(purpose(assessment.task,assessment.request),encoded);
        if(insert)db.update(c,"INSERT INTO "+db.table("task_handoff")
                +" (cluster_id,task_id,request_id,envelope) VALUES (?,?,?,?)",cluster,assessment.task,assessment.request,envelope);
        else if(db.update(c,"UPDATE "+db.table("task_handoff")+" SET request_id=?,envelope=? WHERE cluster_id=? AND task_id=?",
                assessment.request,envelope,cluster,assessment.task)!=1)throw new TaskService.Conflict("HANDOFF_ASSESSMENT_CHANGED");
        AuditStore.Event event=new AuditStore.Event();event.kind=kind;event.taskId=assessment.task;
        event.actorId=actor.id;event.actorRole=actor.role.name();event.subjectId=assessment.candidate.node;
        event.clientRequestHash=AuditStore.hash(assessment.request);event.operationHash=AuditStore.hash(encoded);
        event.status="COMMITTED";access.append(c,event);
    }

    Map<String,Object> probe(TaskRequest request,AccessControl.Principal actor,OwnerQueries transport)throws Exception{
        OwnerQueries.HandoffQueries calls=transport.submitHandoff(request,actor);
        Map<String,Object> candidate=transport.await(calls.candidate(),actor);
        Map<String,Object> evidence=transport.await(calls.originalEvidence(),actor);
        Map<String,Object> custody=verifySharedEvidence(request,actor,transport,evidence);
        Map<String,Object> rawCustody=verifyArchive(request,actor,transport,evidence.get("rawCaptureEvidence"),true);
        return tasks.transaction(request.taskId,actor,Connection.TRANSACTION_READ_COMMITTED,(c,task,current)->{
            Assessment a=probeAssessment(c,task,current,request.reference);
            long now=db.now(c);
            ProbeResult result=new ProbeResult(now,candidate,evidence,custody,rawCustody);
            Assessment proved=new Assessment(a.version,a.task,a.request,a.replaces,a.revision,a.activeVersion,a.original,
                    a.candidate,a.taskSlots,a.workerSlots,a.assigned,a.actor,a.actorRevision,a.assessedAt,a.originalCut,a.blockers,result);
            save(c,proved,false,current,"MANAGED_HANDOFF_PROBED");
            return summary(proved,false,now);
        });
    }

    private Map<String,Object> verifySharedEvidence(TaskRequest request,AccessControl.Principal actor,
                                                   OwnerQueries transport,Map<String,Object> evidence)throws Exception{
        for(String category:List.of("shareableReports","closedReceipts","historicalFailureEvidence")){
            Map<String,Object> proof=verifyArchive(request,actor,transport,evidence.get(category),false);
            if(proof!=null)return proof;
        }
        return null;
    }

    private Map<String,Object> verifyArchive(TaskRequest request,AccessControl.Principal actor,OwnerQueries transport,
                                            Object artifact,boolean rawCapture)throws Exception{
        if(!(artifact instanceof Map<?,?> response)||!"ARCHIVED".equals(response.get("state")))return null;
        SharedHandoffEvidence.Archive descriptor=Model.JSON.convertValue(response.get("archive"),SharedHandoffEvidence.Archive.class);
        if(descriptor==null||(descriptor.rawCaptures()>0)!=rawCapture)
            throw new TaskService.Conflict("HANDOFF_ARCHIVE_DESCRIPTOR_INVALID");
        return transport.await(transport.submitHandoffCustody(request,actor,descriptor),actor);
    }

    ProbeRoute probeRoute(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,String expected)throws Exception{
        Assessment a=probeAssessment(c,task,actor,expected);
        JournalWriterAssignment.ExpectedCut cut=new JournalWriterAssignment.ExpectedCut(
                Long.parseLong(a.originalCut.journalEpoch),Long.parseLong(a.originalCut.sequence),a.originalCut.checkpointHash);
        return new ProbeRoute(a.revision,a.activeVersion,agents.owner(c,a.original.node,a.original.directory),
                agents.owner(c,a.candidate.node,a.candidate.directory),cut);
    }
    SharedHandoffEvidence.ReceiptPlan closedReceipts(Connection c,GlobalTaskStore.Record task)throws Exception{
        return commands.closedHandoffReceipts(c,task);
    }

    private Assessment probeAssessment(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,String expected)throws Exception{
        Assessment a=read(c,task.id);
        if(a==null||!expected.equals(a.request))throw new TaskService.Conflict("HANDOFF_ASSESSMENT_CHANGED");
        requireGrants(actor,a.original.node,a.candidate.node);
        tasks.preparations().requireUnreserved(c,task.id);
        if(stale(c,task,actor,a))throw new TaskService.Conflict("HANDOFF_ASSESSMENT_CHANGED");
        if(a.blockers.size()!=2||a.originalCut==null)throw new TaskService.Conflict("HANDOFF_ASSESSMENT_BLOCKED");
        return a;
    }

    Map<String,Object> latest(String id,AccessControl.Principal supplied)throws Exception{
        return tasks.transaction(id,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            if(!actor.administers())throw new SecurityException("FORBIDDEN");
            Assessment assessment=read(c,id);
            if(assessment==null)throw new NoSuchElementException("HANDOFF_ASSESSMENT_NOT_FOUND");
            requireGrants(actor,assessment.original.node,assessment.candidate.node);
            return view(c,task,actor,assessment);
        });
    }

    Map<String,Object> prepare(TaskRequest request,AccessControl.Principal supplied)throws Exception{
        return tasks.transaction(request.taskId,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            HandoffPreparations plans=tasks.preparations();
            long now=db.now(c);
            HandoffPreparations.Plan existing=plans.byRequest(c,request.requestId,now);
            if(existing!=null){
                requirePlanGrants(actor,task,existing);
                HandoffPreparations.Evidence saved=existing.evidence();
                if(!existing.id().equals(request.requestId)||!existing.actor().equals(actor.id)
                        ||!saved.assessment().equals(request.reference)||!saved.revision().equals(request.revision)
                        ||!saved.proof().equals(request.identityHash))throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_REUSED");
                return HandoffPreparations.summary(existing);
            }
            Assessment selected=probeAssessment(c,task,actor,request.reference);
            if(!task.revision.equals(request.revision))throw new TaskService.Conflict("REVISION_CHANGED");
            HandoffPreparations.Evidence evidence=preparationEvidence(selected,now);
            if(evidence==null)throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_REQUIRED");
            if(!evidence.proof().equals(request.identityHash))throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
            if(!agents.handoffCapacity(c,selected.candidate.node).roomForPreparation())
                throw new TaskService.Conflict("CANDIDATE_TASK_CAPACITY_UNAVAILABLE");
            HandoffPreparations.WriterActivation activation=writerActivation(c,task,selected);
            HandoffPreparations.Plan plan=plans.prepare(c,task,request.requestId,actor,evidence,activation,now);
            auditPreparation(c,plan,actor,request.requestId,"MANAGED_HANDOFF_PREPARED");
            return HandoffPreparations.summary(plan);
        });
    }

    StageRoute stageRoute(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,TaskRequest request)throws Exception{
        if(!actor.administers())throw new SecurityException("FORBIDDEN");
        HandoffPreparations.Plan plan=tasks.preparations().latest(c,task.id,db.now(c));
        if(plan==null||!"PREPARED".equals(plan.phase())||!plan.id().equals(request.reference))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        requirePlanGrants(actor,task,plan);
        HandoffPreparations.Evidence evidence=plan.evidence();
        if(!evidence.revision().equals(request.revision)||!task.revision.equals(request.revision)
                ||!Objects.equals(task.activeConfigurationVersion,evidence.configuration())
                ||!evidence.proof().equals(request.identityHash))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
        Assessment assessment=read(c,task.id);
        if(assessment==null||!evidence.assessment().equals(assessment.request)||assessment.probe==null
                ||!evidence.proof().equals(AuditStore.hash(Model.JSON.writeValueAsString(assessment)))
                ||stale(c,task,actor,assessment))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
        if(request.kind==TaskRequest.Kind.HANDOFF_READINESS_RPC){
            requireCandidateReadiness(plan,actor,assessment,db.now(c));
            HandoffPreparations.requireExecution(plan,request.requestId);
        }
        Map<String,Object> custody=assessment.probe.sharedReportCustody;
        SharedHandoffEvidence.Archive archive=custody==null?null:
                Model.JSON.convertValue(custody.get("archive"),SharedHandoffEvidence.Archive.class);
        if(archive==null||!"VERIFIED".equals(custody.get("state"))||!evidence.custody().equals(archive.sha256()))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
        AgentDirectory.Binding candidate=agents.owner(c,evidence.candidate().node(),evidence.candidate().directory());
        if(!"HEALTHY".equals(candidate.state)||candidate.epoch!=evidence.candidate().epoch()
                ||!candidate.incarnation.equals(evidence.candidate().incarnation()))
            throw new TaskService.Conflict("HANDOFF_AGENT_CHANGED");
        SharedHandoffEvidence.Scope scope=new SharedHandoffEvidence.Scope(cluster,task.id,evidence.assessment(),
                evidence.revision(),evidence.configuration(),evidence.original().directory(),evidence.candidate().directory());
        return new StageRoute(new HandoffStaging.Binding(plan.id(),evidence.proof(),evidence.candidate(),scope,archive),candidate,plan);
    }

    Map<String,Object> latestPreparation(String id,AccessControl.Principal supplied)throws Exception{
        return tasks.transaction(id,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            if(!actor.administers())throw new SecurityException("FORBIDDEN");
            HandoffPreparations.Plan plan=tasks.preparations().latest(c,id,db.now(c));
            if(plan==null)throw new NoSuchElementException("HANDOFF_PREPARATION_NOT_FOUND");
            requirePlanGrants(actor,task,plan);
            return HandoffPreparations.summary(plan);
        });
    }

    Map<String,Object> releasePreparation(TaskRequest request,AccessControl.Principal supplied)throws Exception{
        return tasks.transaction(request.taskId,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            if(!actor.administers())throw new SecurityException("FORBIDDEN");
            HandoffPreparations plans=tasks.preparations();
            long now=db.now(c);
            HandoffPreparations.Plan existing=plans.byRequest(c,request.requestId,now);
            if(existing!=null){
                requirePlanGrants(actor,task,existing);
                if(!request.requestId.equals(existing.releaseId())||!request.reference.equals(existing.id())
                        ||!actor.id.equals(existing.releasedBy()))throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_REUSED");
                return HandoffPreparations.summary(existing);
            }
            HandoffPreparations.Plan plan=plans.latest(c,task.id,now);
            if(plan==null)throw new NoSuchElementException("HANDOFF_PREPARATION_NOT_FOUND");
            requirePlanGrants(actor,task,plan);
            if(!request.reference.equals(plan.id()))throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
            plan=plans.release(c,plan,request.requestId,actor,now);
            auditPreparation(c,plan,actor,request.requestId,"MANAGED_HANDOFF_PREPARATION_RELEASED");
            return HandoffPreparations.summary(plan);
        });
    }

    /** From the candidate's authenticated private admission proof, after durable staging. */
    record CandidateAdmission(String preparation,HandoffPreparations.Binding binding,String desired,
                              String configurationSha256,String stagingSha256){
        boolean valid(){return preparation!=null&&binding!=null&&"STOPPED".equals(desired)
                &&digest(configurationSha256)&&digest(stagingSha256);}
    }

    // Internal transaction only. Candidate staging is not an HTTP grant to call this method.
    Map<String,Object> commitAdmittedWriter(String taskId,CandidateAdmission candidate,
                                           String executionRequest,AccessControl.Principal supplied)throws Exception{
        if(candidate==null||!candidate.valid())
            throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_REQUIRED");
        return tasks.transaction(taskId,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            HandoffPreparations.Plan plan=tasks.preparations().byRequest(c,candidate.preparation(),db.now(c));
            if(plan==null||!plan.id().equals(candidate.preparation()))throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
            requirePlanGrants(actor,task,plan);
            if(!plan.evidence().candidate().equals(candidate.binding()))
                throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
            if(plan.writerCommit()!=null){
                requireExecutionReplay(plan,actor,executionRequest);
                if(!plan.writerCommit().stagingSha256().equals(candidate.stagingSha256())
                        ||!plan.writerCommit().configurationSha256().equals(candidate.configurationSha256()))
                    throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_REUSED");
                return HandoffPreparations.summary(plan);
            }
            Assessment current=read(c,task.id);
            requireCandidateReadiness(plan,actor,current,db.now(c));
            if(stale(c,task,actor,current))throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
            AgentDirectory.HandoffCapacity capacity=agents.handoffCapacity(c,candidate.binding().node());
            long total=capacity.assignedTasks()+capacity.reservedTasks();
            if(capacity.reservedTasks()<1||total>capacity.taskSlots()||total>capacity.workerSlots())
                throw new TaskService.Conflict("CANDIDATE_TASK_CAPACITY_UNAVAILABLE");
            String yaml=ManagedWriterDeclaration.successorYaml(tasks.activeYaml(c,task),task.owner,
                    candidate.binding().node(),plan.activation().nextAssignment());
            if(!AuditStore.hash(yaml).equals(candidate.configurationSha256()))
                throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
            JournalWriterAssignment.CutProof cut=Model.JSON.convertValue(current.probe.candidate.get("cut"),JournalWriterAssignment.CutProof.class);
            return HandoffPreparations.summary(tasks.commitPreparedWriter(c,task,plan,actor,cut,yaml,
                    candidate.stagingSha256(),executionRequest));
        });
    }

    private void requireCandidateReadiness(HandoffPreparations.Plan plan,AccessControl.Principal actor,
                                           Assessment assessment,long now)throws Exception{
        if(!plan.actor().equals(actor.id)||!plan.actorRevision().equals(actor.authorizationRevision))
            throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        if(!"PREPARED".equals(plan.phase())||plan.activation()==null||!plan.activation().checkedInControlTransaction())
            throw new TaskService.Conflict("HANDOFF_MANAGED_CONTROL_CUT_REQUIRED");
        HandoffPreparations.Evidence active=assessment==null?null:preparationEvidence(assessment,now);
        if(!plan.evidence().equals(active))throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
        if(!(assessment.probe.originalEvidence.get("originInventory") instanceof Map<?,?> inventory)
                ||!"ABSENT".equals(inventory.get("sourceEvidence"))||!"ABSENT".equals(inventory.get("failureEvidence")))
            throw new TaskService.Conflict("HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED");
    }

    private static boolean digest(String value){return value!=null&&value.matches("[a-f0-9]{64}");}

    Map<String,Object> orchestratePreparedWriter(TaskRequest supplied,AccessControl.Principal actor,OwnerQueries transport)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(request.kind!=TaskRequest.Kind.HANDOFF_STAGE||request.yaml!=null)
            throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_INVALID");
        Map<String,Object> replay=bindOrReplayExecution(request,actor);
        if(replay!=null)return replay;
        try{
            CandidateAdmission candidate=transport.candidateReadiness(request,actor);
            return commitAdmittedWriter(request.taskId,candidate,request.requestId,actor);
        }catch(TaskService.Conflict concurrent){
            replay=committedReplay(request,actor);
            if(replay!=null)return replay;
            throw concurrent;
        }
    }
    Map<String,Object> bindOrReplayExecution(TaskRequest supplied,AccessControl.Principal principal)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(request.kind!=TaskRequest.Kind.HANDOFF_STAGE||request.yaml!=null)
            throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_INVALID");
        return tasks.transaction(request.taskId,principal,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            HandoffPreparations.Plan plan=executionPlan(c,task,actor,request);
            if(plan.writerCommit()!=null){
                requireExecutionReplay(plan,actor,request.requestId);
                return HandoffPreparations.summary(plan);
            }
            Assessment assessment=read(c,task.id);
            requireCandidateReadiness(plan,actor,assessment,db.now(c));
            if(stale(c,task,actor,assessment))throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
            HandoffPreparations.Plan bound=tasks.preparations().bindExecution(c,plan,request.requestId);
            if(plan.executionRequest()==null)auditPreparation(c,bound,actor,request.requestId,"MANAGED_HANDOFF_EXECUTION_BOUND");
            return null;
        });
    }
    private Map<String,Object> committedReplay(TaskRequest request,AccessControl.Principal supplied)throws Exception{
        return tasks.transaction(request.taskId,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            HandoffPreparations.Plan plan=executionPlan(c,task,actor,request);
            if(plan.writerCommit()==null)return null;
            requireExecutionReplay(plan,actor,request.requestId);
            return HandoffPreparations.summary(plan);
        });
    }
    private HandoffPreparations.Plan executionPlan(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,
                                                  TaskRequest request)throws Exception{
        HandoffPreparations.Plan plan=tasks.preparations().byRequest(c,request.reference,db.now(c));
        if(plan==null||!plan.id().equals(request.reference))throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        requirePlanGrants(actor,task,plan);
        if(!plan.evidence().revision().equals(request.revision)||!plan.evidence().proof().equals(request.identityHash))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        return plan;
    }
    private static void requireExecutionReplay(HandoffPreparations.Plan plan,AccessControl.Principal actor,String executionRequest){
        if(!plan.actor().equals(actor.id)||!plan.writerCommit().actor().equals(actor.id)
                ||!plan.actorRevision().equals(actor.authorizationRevision))
            throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        HandoffPreparations.requireExecution(plan,executionRequest);
    }

    private HandoffPreparations.WriterActivation writerActivation(Connection c,GlobalTaskStore.Record task,Assessment assessment)throws Exception{
        ManagedWriterDeclaration declaration=ManagedWriterDeclaration.read(tasks.activeYaml(c,task));
        if(declaration==null)return null;
        declaration.requireOwner(task.owner);
        JournalWriterAssignment.CutProof proof=Model.JSON.convertValue(assessment.probe.candidate.get("cut"),JournalWriterAssignment.CutProof.class);
        JournalWriterAssignment.ManagedCut cut=proof==null?null:proof.managedWriter();
        if(cut==null||!proof.writerPlacementEnrolled()||!task.owner.equals(cut.originalNode())
                ||!Objects.toString(proof.journalGenerationSha256(),"").matches("[a-f0-9]{64}")
                ||!Objects.toString(proof.nativeEvidenceSha256(),"").matches("[a-f0-9]{64}")
                ||!AuditStore.hash(declaration.assignment()).equals(cut.originalAssignmentSha256())
                ||!assessment.originalCut.journalEpoch.equals(Long.toString(cut.epoch()))
                ||!assessment.originalCut.sequence.equals(Long.toString(cut.sequence()))
                ||!assessment.originalCut.checkpointHash.equals(cut.checkpointSha256()))
            throw new TaskService.Conflict("HANDOFF_MANAGED_WRITER_PROOF_REQUIRED");
        boolean shared=db.sharesPostgresUrl(declaration.journalUrl());
        if(shared)try{
            JournalWriterAssignment.verifyManagedControlCut(c,declaration.namespace(),task.id,declaration.assignment(),proof);
        }catch(IllegalStateException invalid){throw new TaskService.Conflict(invalid.getMessage());}
        return new HandoffPreparations.WriterActivation(cut.originalAssignmentSha256(),UUID.randomUUID().toString(),
                proof.journalGenerationSha256(),proof.nativeEvidenceSha256(),shared);
    }

    private void auditPreparation(Connection c,HandoffPreparations.Plan plan,AccessControl.Principal actor,String request,String kind)throws Exception{
        AuditStore.Event event=new AuditStore.Event();event.kind=kind;event.taskId=plan.task();event.actorId=actor.id;
        event.actorRole=actor.role.name();event.subjectId=plan.evidence().candidate().node();
        event.clientRequestHash=AuditStore.hash(request);event.operationHash=plan.evidence().proof();event.status="COMMITTED";
        access.append(c,event);
    }
    private static void requirePlanGrants(AccessControl.Principal actor,GlobalTaskStore.Record task,HandoffPreparations.Plan plan){
        if(!task.id.equals(plan.task()))throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_REUSED");
        requireGrants(actor,plan.evidence().original().node(),plan.evidence().candidate().node());
    }

    private boolean probeFresh(Assessment a,long now){
        return a.probe!=null&&a.probe.checkedAt>=now-freshness&&a.probe.checkedAt<=now;
    }
    private HandoffPreparations.Evidence preparationEvidence(Assessment a,long now)throws Exception{
        if(!probeFresh(a,now)||a.originalCut==null||!"VERIFIED".equals(a.probe.candidate.get("state"))
                ||!(a.probe.originalEvidence.get("closedReceipts") instanceof Map<?,?> originalReceipts)
                ||!"ARCHIVED".equals(originalReceipts.get("state"))||a.probe.sharedReportCustody==null
                ||!"VERIFIED".equals(a.probe.sharedReportCustody.get("state")))return null;
        SharedHandoffEvidence.Archive receipt=Model.JSON.convertValue(originalReceipts.get("archive"),SharedHandoffEvidence.Archive.class);
        SharedHandoffEvidence.Archive candidate=Model.JSON.convertValue(a.probe.sharedReportCustody.get("archive"),SharedHandoffEvidence.Archive.class);
        if(receipt==null||!receipt.equals(candidate)||candidate.closedReceipts()==0)return null;
        if(!(a.probe.originalEvidence.get("originInventory") instanceof Map<?,?> inventory))return null;
        if(!"ABSENT".equals(inventory.get("validationReports"))&&!("PRESENT".equals(inventory.get("validationReports"))&&candidate.reports()>0))
            return null;
        String proof=AuditStore.hash(Model.JSON.writeValueAsString(a));
        return new HandoffPreparations.Evidence(a.request,a.revision,a.activeVersion,
                new HandoffPreparations.Binding(a.original.node,a.original.directory,a.original.incarnation,a.original.epoch),
                new HandoffPreparations.Binding(a.candidate.node,a.candidate.directory,a.candidate.incarnation,a.candidate.epoch),
                new HandoffPreparations.JournalCut(a.originalCut.journalEpoch,a.originalCut.sequence,a.originalCut.checkpointHash),
                proof,candidate.sha256());
    }

    private static void requireGrants(AccessControl.Principal actor,String original,String candidate){
        if(!actor.administers()||!actor.node(original)||!actor.node(candidate))throw new SecurityException("FORBIDDEN");
    }

    private String purpose(String task,String request){return "managed-handoff/"+task+"/"+request;}

    private Assessment read(Connection c,String task)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT request_id,envelope FROM "+db.table("task_handoff")
                +" WHERE cluster_id=? AND task_id=?",cluster,task);ResultSet r=p.executeQuery()){
            if(!r.next())return null;
            String id=r.getString(1);
            Assessment value=Model.JSON.readValue(cipher.open(purpose(task,id),r.getString(2)),Assessment.class);
            if(value.version!=1||!task.equals(value.task)||!id.equals(value.request)
                    ||value.original==null||value.candidate==null||value.blockers==null||value.blockers.size()>12
                    ||!value.blockers.contains(CANDIDATE_ACCESS)||!value.blockers.contains(RECOVERY_EVIDENCE))
                throw new TaskService.Conflict("HANDOFF_ASSESSMENT_INVALID");
            return value;
        }
    }

    private Map<String,Object> view(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,Assessment a)throws Exception{
        return summary(a,stale(c,task,actor,a),db.now(c));
    }
    private boolean stale(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,Assessment a)throws Exception{
        boolean stale=!task.revision.equals(a.revision)||!Objects.equals(task.activeConfigurationVersion,a.activeVersion)
                ||!task.owner.equals(a.original.node)||!task.directory.equals(a.original.directory);
        if(!stale){
            AgentDirectory.Binding original=agents.owner(c,task.owner,task.directory);
            AgentDirectory.HandoffCapacity candidate=agents.handoffCapacity(c,a.candidate.node);
            stale=!a.original.matches(original)||!a.candidate.matches(candidate.binding())
                    ||a.assigned!=candidate.assignedTasks()||a.taskSlots!=candidate.taskSlots()||a.workerSlots!=candidate.workerSlots();
            if(!stale){
                List<String> blockers=new ArrayList<>();
                OriginalCut cut=inspectOriginal(views.read(c,task,actor),db.now(c),blockers);
                List<String> previous=a.blockers.stream().filter(code->code.startsWith("ORIGINAL_")&&!code.equals(RECOVERY_EVIDENCE)).toList();
                stale=!Objects.equals(cut,a.originalCut)||!blockers.equals(previous)
                        ||commands.hasUnresolved(c,task.id)!=a.blockers.contains("TASK_COMMANDS_UNRESOLVED");
            }
        }
        return stale;
    }

    private Map<String,Object> summary(Assessment a,boolean stale,long now)throws Exception{
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("id",a.request);result.put("taskId",a.task);result.put("taskRevision",a.revision);
        result.put("activeConfigurationVersion",a.activeVersion);
        result.put("originalNodeId",a.original.node);result.put("originalNodeEpoch",Long.toString(a.original.epoch));
        result.put("candidateNodeId",a.candidate.node);result.put("candidateNodeEpoch",Long.toString(a.candidate.epoch));
        result.put("candidateState",a.candidate.state);result.put("candidateAssignedTasks",a.assigned);
        result.put("candidateTaskCapacity",a.taskSlots);result.put("candidateWorkerCapacity",a.workerSlots);
        result.put("assessedAt",Instant.ofEpochMilli(a.assessedAt).toString());
        result.put("originalCut",a.originalCut);
        result.put("state",stale?"STALE":a.blockers.size()==2?"EVIDENCE_REQUIRED":"BLOCKED");
        List<String> blockers=new ArrayList<>(a.blockers);
        if(a.probe!=null){
            boolean applicable=!stale&&probeFresh(a,now);
            if(applicable&&"VERIFIED".equals(a.probe.candidate.get("state")))blockers.remove(CANDIDATE_ACCESS);
            Map<String,Object> probe=new LinkedHashMap<>(Map.of("checkedAt",Instant.ofEpochMilli(a.probe.checkedAt).toString(),
                    "current",applicable,"candidate",a.probe.candidate,"originalEvidence",a.probe.originalEvidence));
            if(a.probe.sharedReportCustody!=null){
                if(a.probe.originalEvidence.get("shareableReports") instanceof Map<?,?> reports&&"ARCHIVED".equals(reports.get("state")))
                    probe.put("sharedReportCustody",a.probe.sharedReportCustody);
                if(a.probe.originalEvidence.get("closedReceipts") instanceof Map<?,?> receipts&&"ARCHIVED".equals(receipts.get("state")))
                    probe.put("sharedReceiptCustody",a.probe.sharedReportCustody);
                if(a.probe.originalEvidence.get("historicalFailureEvidence") instanceof Map<?,?> failures&&"ARCHIVED".equals(failures.get("state")))
                    probe.put("sharedFailureEvidenceCustody",a.probe.sharedReportCustody);
            }
            if(a.probe.sharedRawCaptureCustody!=null)probe.put("sharedRawCaptureCustody",a.probe.sharedRawCaptureCustody);
            result.put("probe",probe);
            HandoffPreparations.Evidence prepared=applicable?preparationEvidence(a,now):null;
            if(prepared!=null)result.put("preparationEvidenceSha256",prepared.proof());
        }
        result.put("blockers",blockers);result.put("executionAuthorized",false);
        return result;
    }

    private static boolean supported(String yaml)throws Exception{
        JsonNode doc=YAML.readTree(yaml);
        JsonNode journal=doc.path("journal"),targets=doc.path("targets");
        if(doc.path("formatVersion").asInt()!=3||!"EXTERNAL".equals(journal.path("mode").asText())
                ||!"POSTGRESQL".equals(journal.path("product").asText("POSTGRESQL"))
                ||!"MYSQL".equals(doc.path("source").path("type").asText(doc.path("sourceType").asText("MYSQL")))
                ||!targets.isArray()||targets.size()<1||targets.size()>8)return false;
        for(JsonNode target:targets)if(!Set.of("POSTGRESQL","MYSQL").contains(target.path("product").asText()))return false;
        return true;
    }

    private boolean freshStatus(Object observed,long now){
        if(!(observed instanceof String at))return false;
        try{
            long timestamp=Instant.parse(at).toEpochMilli();
            return timestamp>=now-freshness&&timestamp<=now+freshness;
        }catch(DateTimeException|ArithmeticException invalid){return false;}
    }

    private OriginalCut inspectOriginal(Map<String,Object> view,long now,List<String> blockers)throws Exception{
        if(!"HEALTHY".equals(view.get("ownerState"))||!Boolean.TRUE.equals(view.get("statusAvailable"))
                ||!freshStatus(view.get("observedAt"),now)||!(view.get("status") instanceof Map<?,?> status)){
            blockers.add("ORIGINAL_TASK_STATE_UNVERIFIED");
            return null;
        }
        if(!"STOPPED".equals(view.get("desired"))||!"STOPPED".equals(view.get("workerState"))
                ||Boolean.TRUE.equals(view.get("configurationProjectionPending")))blockers.add("ORIGINAL_WORKER_NOT_DRAINED");
        String lease=Objects.toString(view.get("captureLeaseState"),"UNKNOWN");
        if(!Set.of("UNCLAIMED","EXPIRED").contains(lease))blockers.add("ORIGINAL_CAPTURE_LEASE_UNVERIFIED");
        String epoch=Objects.toString(status.get("epoch"),""),sequence=Objects.toString(status.get("captured_sequence"),"");
        String normalized=Objects.toString(status.get("normalized_sequence"),"");
        if(!"STREAMING".equals(status.get("phase"))||!"RUNNING".equals(status.get("run_state"))
                ||Boolean.TRUE.equals(status.get("hasOpenGaps"))||!(status.get("table_count") instanceof Number count)
                ||count.longValue()<1||count.longValue()>256||!epoch.matches("[1-9][0-9]{0,18}")
                ||!sequence.matches("0|[1-9][0-9]{0,18}")||!sequence.equals(normalized)||status.get("sourcePosition")==null){
            blockers.add("ORIGINAL_COMMON_CUT_UNVERIFIED");
            return null;
        }
        return new OriginalCut(lease,epoch,sequence,AuditStore.hash(Model.JSON.writeValueAsString(status.get("sourcePosition"))));
    }
}
