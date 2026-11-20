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
                               Object sharedReportCustody,
                               @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                               Object sharedRawCaptureCustody){
        Map<?,?> reportCustody(){return sharedReportCustody instanceof Map<?,?> custody?custody:null;}
        Map<?,?> rawCustody(){return sharedRawCaptureCustody instanceof Map<?,?> custody?custody:null;}
        void requireCustodyShape(){
            if(sharedReportCustody!=null&&reportCustody()==null||sharedRawCaptureCustody!=null&&rawCustody()==null)
                throw new TaskService.Conflict("HANDOFF_ARCHIVE_DESCRIPTOR_INVALID");
        }
    }
    record ProbeRoute(String revision,String activeVersion,AgentDirectory.Binding original,AgentDirectory.Binding candidate,
                      JournalWriterAssignment.ExpectedCut cut){}
    record StageRoute(HandoffStaging.Binding binding,AgentDirectory.Binding candidate,HandoffPreparations.Plan plan,
                      RawCaptureHandoffStaging.Binding rawCapture){}
    record OfflineRoute(HandoffPreparations.Plan plan,AgentDirectory.Binding candidate){}
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
            return summary(task,next,false,assessedAt,views.ownerControlState(c,original.node));
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
            return summary(task,proved,false,now,views.ownerControlState(c,proved.original.node));
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
        Map<?,?> custody=assessment.probe.reportCustody();
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
        RawCaptureHandoffStaging.Binding raw=rawCaptureStage(assessment,plan,scope);
        Object observed=assessment.probe.originalEvidence.get("offlineRecoveryInventory");
        HandoffRecoveryInventory.ReceiptOnly inventory=null;
        if(observed!=null&&plan.activation()!=null&&plan.activation().checkedInControlTransaction()){
            if(raw!=null)throw new TaskService.Conflict("HANDOFF_OFFLINE_INVENTORY_UNVERIFIED");
            inventory=HandoffRecoveryInventory.validate(observed,scope,archive,closedReceipts(c,task));
        }
        return new StageRoute(new HandoffStaging.Binding(plan.id(),evidence.proof(),evidence.candidate(),scope,archive,inventory),candidate,plan,raw);
    }

    private static RawCaptureHandoffStaging.Binding rawCaptureStage(Assessment assessment,HandoffPreparations.Plan plan,
                                                                   SharedHandoffEvidence.Scope scope){
        Object witness=assessment.probe.originalEvidence.get("rawCaptureEvidence");
        Map<?,?> candidate=assessment.probe.rawCustody();
        if(!(witness instanceof Map<?,?> original)||!"ARCHIVED".equals(original.get("state"))
                ||candidate==null||!"VERIFIED".equals(candidate.get("state")))return null;
        var published=Model.JSON.convertValue(original.get("archive"),SharedHandoffEvidence.Archive.class);
        var verified=Model.JSON.convertValue(candidate.get("archive"),SharedHandoffEvidence.Archive.class);
        if(published==null||!published.equals(verified)||published.rawCaptures()<1)
            throw new TaskService.Conflict("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        return new RawCaptureHandoffStaging.Binding(plan.id(),plan.evidence().proof(),plan.evidence().candidate(),scope,published);
    }

    Map<String,Object> latestPreparation(String id,AccessControl.Principal supplied)throws Exception{
        return tasks.transaction(id,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            if(!actor.administers())throw new SecurityException("FORBIDDEN");
            HandoffPreparations.Plan plan=tasks.preparations().latest(c,id,db.now(c));
            if(plan==null)throw new NoSuchElementException("HANDOFF_PREPARATION_NOT_FOUND");
            requirePlanGrants(actor,task,plan);
            Map<String,Object> receipt=HandoffPreparations.summary(plan);
            if("PREPARED".equals(plan.phase())&&tasks.preparations().splitTransfer(c,plan)!=null)
                receipt.put("splitTransferPending",true);
            if("FENCE_PENDING".equals(plan.phase())&&!plan.targetFence().actorRevision().equals(actor.authorizationRevision))
                receipt.put("targetFenceAvailable",true);
            if("START_PENDING".equals(plan.phase())&&!plan.workerStart().actorRevision().equals(actor.authorizationRevision))
                receipt.put("workerStartAvailable",true);
            return receipt;
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
                if(existing.writerFreeze()!=null)throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_REQUEST_REUSED");
                if(!request.requestId.equals(existing.releaseId())||!request.reference.equals(existing.id())
                        ||!actor.id.equals(existing.releasedBy()))throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_REUSED");
                return HandoffPreparations.summary(existing);
            }
            HandoffPreparations.Plan plan=plans.latest(c,task.id,now);
            if(plan==null)throw new NoSuchElementException("HANDOFF_PREPARATION_NOT_FOUND");
            requirePlanGrants(actor,task,plan);
            if(!request.reference.equals(plan.id()))throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
            if("ORIGINAL_FROZEN".equals(plan.phase()))throw new TaskService.Conflict("HANDOFF_ORIGINAL_FROZEN");
            plan=plans.release(c,plan,request.requestId,actor,now);
            auditPreparation(c,plan,actor,request.requestId,"MANAGED_HANDOFF_PREPARATION_RELEASED");
            return HandoffPreparations.summary(plan);
        });
    }

    record FreezeOriginRoute(HandoffPreparations.Plan plan,HandoffStaging.Binding staging,AgentDirectory.Binding original,
                             SharedHandoffEvidence.ReceiptPlan receipts){}
    record FreezeOriginProof(HandoffPreparations.Binding original,JournalWriterAssignment.CutProof cut,String receiptPayloadSha256){}

    Map<String,Object> changeOriginalFreeze(TaskRequest supplied,AccessControl.Principal principal,OwnerQueries transport)throws Exception{
        TaskRequest request=supplied.snapshot();
        boolean cancelling=request.kind==TaskRequest.Kind.HANDOFF_CANCEL_FREEZE;
        if(!cancelling&&request.kind!=TaskRequest.Kind.HANDOFF_FREEZE_ORIGINAL||request.yaml!=null||request.document!=null)
            throw new IllegalArgumentException("HANDOFF_ORIGINAL_FREEZE_REQUEST_INVALID");
        Map<String,Object> replay=readFreezeReplay(request,principal,cancelling);
        if(replay!=null)return replay;
        FreezeOriginProof checked;
        Map<String,Object> staged;
        try{
            if(!cancelling){
                TaskRequest stage=TaskRequest.of(TaskRequest.Kind.HANDOFF_STAGE,request.taskId);
                stage.reference=request.reference;stage.revision=request.revision;stage.identityHash=request.identityHash;
                stage.requestId=request.requestId;
                staged=transport.await(transport.submitHandoffStaging(stage,principal),principal);
                requireFrozenInventoryReceipt(request,staged,null);
            }else staged=null;
            checked=Model.JSON.convertValue(transport.await(transport.submitFreezeOrigin(request,principal),principal),
                    FreezeOriginProof.class);
        }catch(TaskService.Conflict concurrent){
            replay=readFreezeReplay(request,principal,cancelling);
            if(replay!=null)return replay;
            throw concurrent;
        }
        return tasks.transaction(request.taskId,principal,(c,task,actor)->{
            var plan=freezePlan(c,task,actor,request);
            Map<String,Object> repeated=freezeReplay(plan,request,actor,cancelling);
            if(repeated!=null)return repeated;
            FreezeOriginRoute route=freezeOriginRoute(c,task,actor,request,cancelling);
            var origin=route.original();
            var binding=new HandoffPreparations.Binding(origin.node,origin.directory,origin.incarnation,origin.epoch);
            var inventory=cancelling?plan.writerFreeze().inventory():route.staging().offlineInventory();
            if(checked==null||!binding.equals(checked.original())||!inventory.receiptPayloadSha256().equals(checked.receiptPayloadSha256()))
                throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
            ManagedWriterDeclaration declaration=ManagedWriterDeclaration.read(tasks.activeYaml(c,task));
            if(declaration==null||!db.sharesPostgresUrl(declaration.journalUrl()))
                throw new TaskService.Conflict("HANDOFF_MANAGED_CONTROL_CUT_REQUIRED");
            HandoffPreparations.Plan result;
            if(cancelling){
                var saved=plan.writerFreeze();
                if(!saved.cut().equals(checked.cut()))throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CUT_CHANGED");
                JournalWriterAssignment.cancelOriginalFreeze(c,declaration.namespace(),task.id,declaration.assignment(),saved.cut(),saved.fence());
                result=tasks.preparations().cancelOriginalFreeze(c,plan,request.requestId,actor,db.now(c));
            }else{
                requireFrozenInventoryReceipt(request,staged,route.staging());
                Assessment assessment=read(c,task.id);
                var reviewed=Model.JSON.convertValue(assessment.probe.candidate.get("cut"),JournalWriterAssignment.CutProof.class);
                if(reviewed==null||!reviewed.equals(checked.cut()))throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CUT_CHANGED");
                var fence=JournalWriterAssignment.freezeOriginal(c,declaration.namespace(),task.id,declaration.assignment(),reviewed);
                result=tasks.preparations().freezeOriginal(c,plan,new HandoffPreparations.WriterFreeze(
                        request.requestId,request.handoffInventorySha256,inventory,reviewed,fence,db.now(c)));
            }
            auditPreparation(c,result,actor,request.requestId,cancelling?"MANAGED_HANDOFF_ORIGINAL_FREEZE_CANCELLED":"MANAGED_HANDOFF_ORIGINAL_FROZEN");
            return HandoffPreparations.summary(result);
        });
    }

    private Map<String,Object> readFreezeReplay(TaskRequest request,AccessControl.Principal principal,
                                                boolean cancelling)throws Exception{
        return tasks.transaction(request.taskId,principal,(c,task,actor)->
                freezeReplay(freezePlan(c,task,actor,request),request,actor,cancelling));
    }

    private static void requireFrozenInventoryReceipt(TaskRequest request,Map<String,Object> staged,HandoffStaging.Binding expected){
        if(staged==null||!"STAGED".equals(staged.get("state"))||!Boolean.FALSE.equals(staged.get("executionAuthorized"))
                ||!request.reference.equals(staged.get("preparationId"))||!request.revision.equals(staged.get("taskRevision"))
                ||!request.identityHash.equals(staged.get("evidenceSha256"))
                ||!(staged.get("offlineInventory") instanceof Map<?,?> inventory)
                ||!"SEALED".equals(inventory.get("state"))||!HandoffRecoveryInventory.PROFILE.equals(inventory.get("profile"))
                ||!Boolean.FALSE.equals(inventory.get("executionAuthorized"))
                ||!request.handoffInventorySha256.equals(inventory.get("manifestSha256"))
                ||expected!=null&&(!expected.candidate().node().equals(staged.get("candidateNodeId"))
                   ||!expected.archive().sha256().equals(staged.get("archiveSha256"))
                   ||!Objects.equals(expected.offlineInventory().closedReceipts(),inventory.get("closedReceipts"))
                   ||!expected.offlineInventory().originInventory().equals(inventory.get("originInventory"))))
            throw new TaskService.Conflict("HANDOFF_OFFLINE_INVENTORY_UNVERIFIED");
    }

    FreezeOriginRoute freezeOriginRoute(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,
                                       TaskRequest request,boolean cancelling)throws Exception{
        HandoffPreparations.Plan plan=freezePlan(c,task,actor,request);
        var evidence=plan.evidence();
        if(!task.owner.equals(evidence.original().node())||!task.directory.equals(evidence.original().directory())
                ||!task.revision.equals(evidence.revision())||!Objects.equals(task.activeConfigurationVersion,evidence.configuration())
                ||!Objects.equals(task.configurationVersion,evidence.configuration())||commands.hasUnresolved(c,task.id))
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        AgentDirectory.Binding original=agents.owner(c,task.owner,task.directory);
        if(!"HEALTHY".equals(original.state))throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_OWNER_UNAVAILABLE");
        if(cancelling){
            if(!"ORIGINAL_FROZEN".equals(plan.phase())||plan.writerFreeze()==null
                    ||!request.handoffInventorySha256.equals(plan.writerFreeze().manifestSha256())
                    ||plan.writerFreeze().request().equals(request.requestId))
                throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
            var receipts=closedReceipts(c,task);
            plan.writerFreeze().inventory().requireReceipts(receipts);
            return new FreezeOriginRoute(plan,null,original,receipts);
        }
        if(!"PREPARED".equals(plan.phase())||plan.executionRequest()!=null||!plan.actor().equals(actor.id)
                ||!plan.actorRevision().equals(actor.authorizationRevision)
                ||!evidence.original().incarnation().equals(original.incarnation)||evidence.original().epoch()!=original.epoch)
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        StageRoute stage=stageRoute(c,task,actor,request);
        if(stage.binding().offlineInventory()==null||stage.rawCapture()!=null)
            throw new TaskService.Conflict("HANDOFF_OFFLINE_INVENTORY_UNVERIFIED");
        return new FreezeOriginRoute(plan,stage.binding(),original,closedReceipts(c,task));
    }

    private HandoffPreparations.Plan freezePlan(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,
                                                TaskRequest request)throws Exception{
        var plan=tasks.preparations().byRequest(c,request.reference,db.now(c));
        if(plan==null||!plan.id().equals(request.reference))throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        requirePlanGrants(actor,task,plan);
        if(!plan.evidence().revision().equals(request.revision)||!plan.evidence().proof().equals(request.identityHash))
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        return plan;
    }

    private static Map<String,Object> freezeReplay(HandoffPreparations.Plan plan,TaskRequest request,
                                                   AccessControl.Principal actor,boolean cancelling){
        var frozen=plan.writerFreeze();
        if(frozen==null)return null;
        boolean sameManifest=request.handoffInventorySha256.equals(frozen.manifestSha256());
        if(cancelling){
            if(frozen.offlineCommit()!=null)throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CONSUMED");
            if("ORIGINAL_FROZEN".equals(plan.phase()))return null;
            if(!sameManifest||!request.requestId.equals(plan.releaseId())||!actor.id.equals(plan.releasedBy()))
                throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_REQUEST_REUSED");
        }else if(!sameManifest||!request.requestId.equals(frozen.request())||!actor.id.equals(plan.actor())){
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_REQUEST_REUSED");
        }
        return HandoffPreparations.summary(plan);
    }

    Map<String,Object> orchestrateOfflineOwner(TaskRequest supplied,AccessControl.Principal principal,OwnerQueries transport)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(request.kind!=TaskRequest.Kind.HANDOFF_OFFLINE_OWNER_COMMIT||request.yaml!=null||request.document!=null)
            throw new IllegalArgumentException("HANDOFF_OFFLINE_COMMIT_REQUEST_INVALID");
        Map<String,Object> replay=readOfflineReplay(request,principal);
        if(replay!=null)return replay;
        CandidateAdmission ready;
        try{
            ready=transport.offlineCandidateReadiness(request,principal);
        }catch(TaskService.Conflict concurrent){
            replay=readOfflineReplay(request,principal);
            if(replay!=null)return replay;
            throw concurrent;
        }
        return tasks.transaction(request.taskId,principal,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            Map<String,Object> repeated=offlineReplay(offlinePlan(c,task,actor,request),request,actor);
            if(repeated!=null)return repeated;
            var route=offlineRoute(c,task,actor,request);
            return HandoffPreparations.summary(tasks.commitFrozenWriter(c,task,route.plan(),actor,ready,request.requestId));
        });
    }

    private Map<String,Object> readOfflineReplay(TaskRequest request,AccessControl.Principal principal)throws Exception{
        return tasks.transaction(request.taskId,principal,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->
                offlineReplay(offlinePlan(c,task,actor,request),request,actor));
    }

    private HandoffPreparations.Plan offlinePlan(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,
                                                 TaskRequest request)throws Exception{
        var plan=freezePlan(c,task,actor,request);
        var frozen=plan.writerFreeze();
        if(frozen==null||!Boolean.TRUE.equals(request.confirmOriginalUnavailable)
                ||!request.handoffInventorySha256.equals(frozen.manifestSha256())||request.requestId.equals(frozen.request()))
            throw new TaskService.Conflict("HANDOFF_OFFLINE_COMMIT_FREEZE_REQUIRED");
        if(!plan.actor().equals(actor.id))throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        return plan;
    }

    private static Map<String,Object> offlineReplay(HandoffPreparations.Plan plan,TaskRequest request,AccessControl.Principal actor){
        var consumed=plan.writerFreeze().offlineCommit();
        if(consumed==null)return null;
        if(!consumed.actor().equals(actor.id))throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        if(!consumed.request().equals(request.requestId))throw new TaskService.Conflict("HANDOFF_OFFLINE_COMMIT_REQUEST_REUSED");
        return HandoffPreparations.summary(plan);
    }

    OfflineRoute offlineRoute(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,TaskRequest request)throws Exception{
        var plan=offlinePlan(c,task,actor,request);
        var evidence=plan.evidence();
        if(!"ORIGINAL_FROZEN".equals(plan.phase())||plan.writerFreeze().offlineCommit()!=null
                ||plan.executionRequest()!=null||plan.activation()==null||!plan.activation().checkedInControlTransaction())
            throw new TaskService.Conflict("HANDOFF_OFFLINE_COMMIT_FREEZE_REQUIRED");
        if(!task.owner.equals(evidence.original().node())||!task.directory.equals(evidence.original().directory())
                ||!task.revision.equals(evidence.revision())||!Objects.equals(task.activeConfigurationVersion,evidence.configuration())
                ||!Objects.equals(task.configurationVersion,evidence.configuration())||commands.hasUnresolved(c,task.id))
            throw new TaskService.Conflict("HANDOFF_ORIGINAL_FREEZE_CHANGED");
        agents.requireExpiredOriginal(c,evidence.original());
        var capacity=agents.handoffCapacity(c,evidence.candidate().node());
        var candidate=capacity.binding();
        if(!"HEALTHY".equals(candidate.state)||!evidence.candidate().directory().equals(candidate.directory)
                ||!evidence.candidate().incarnation().equals(candidate.incarnation)||evidence.candidate().epoch()!=candidate.epoch)
            throw new TaskService.Conflict("HANDOFF_OFFLINE_CANDIDATE_CHANGED");
        if(!capacity.roomForPreparation())throw new TaskService.Conflict("HANDOFF_CANDIDATE_CAPACITY_CHANGED");
        plan.writerFreeze().inventory().requireReceipts(closedReceipts(c,task));
        return new OfflineRoute(plan,candidate);
    }

    /** From the candidate's authenticated private admission proof, after durable staging. */
    record CandidateAdmission(String preparation,HandoffPreparations.Binding binding,String desired,
                              String configurationSha256,String stagingSha256,
                              @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                              RawCaptureHandoffStaging.Proof rawCapture,
                 @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT)
                 int failureRecords){
        CandidateAdmission(String preparation,HandoffPreparations.Binding binding,String desired,String configurationSha256,String stagingSha256){
            this(preparation,binding,desired,configurationSha256,stagingSha256,null,0);
        }
        CandidateAdmission(String preparation,HandoffPreparations.Binding binding,String desired,String configurationSha256,String stagingSha256,
                 RawCaptureHandoffStaging.Proof rawCapture){
            this(preparation,binding,desired,configurationSha256,stagingSha256,rawCapture,0);
        }
        boolean valid(){return preparation!=null&&binding!=null&&"STOPPED".equals(desired)
                &&digest(configurationSha256)&&digest(stagingSha256)&&HandoffFailureHistory.bounded(failureRecords);}
    }

    // Commits only after the authenticated candidate has verified its private staged files.
    Map<String,Object> commitAdmittedWriter(String taskId,CandidateAdmission candidate,
                                           String executionRequest,AccessControl.Principal supplied)throws Exception{
        var result=tasks.transaction(taskId,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->
                commitCandidate(c,task,actor,candidate,executionRequest,null));
        if(Boolean.TRUE.equals(result.get("splitTransferPending")))
            return finishSplitTransfer(taskId,candidate.preparation(),executionRequest,supplied);
        return result;
    }

    private Map<String,Object> finishSplitTransfer(String taskId,String preparation,String request,
                                                    AccessControl.Principal supplied)throws Exception{
        return tasks.transaction(taskId,supplied,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            var plan=tasks.preparations().byRequest(c,preparation,db.now(c));
            if(plan==null||!plan.id().equals(preparation))throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
            requirePlanGrants(actor,task,plan);
            if(plan.writerCommit()!=null){
                requireExecutionReplay(plan,actor,request);
                return HandoffPreparations.summary(plan);
            }
            return finishSplitTransfer(c,task,actor,plan,request);
        });
    }

    Map<String,Object> finishSplitTransfer(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,
                                           HandoffPreparations.Plan plan,String request)throws Exception{
        requirePlanGrants(actor,task,plan);
        if(!actor.administers()||!actor.task(task.id))throw new SecurityException("FORBIDDEN");
        var sealed=tasks.preparations().splitTransfer(c,plan);
        if(sealed==null||!sealed.request().equals(request))throw new TaskService.Conflict("HANDOFF_SPLIT_TRANSFER_CHANGED");
        if(!plan.actor().equals(actor.id)||!plan.actorRevision().equals(actor.authorizationRevision))
            throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        if(commands.hasUnresolved(c,task.id))throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
        var candidate=sealed.admission();
        var capacity=agents.handoffCapacity(c,candidate.binding().node());
        var binding=capacity.binding();
        if(!"HEALTHY".equals(binding.state)||!candidate.binding().directory().equals(binding.directory)
                ||binding.epoch<candidate.binding().epoch()
                ||binding.epoch==candidate.binding().epoch()&&!binding.incarnation.equals(candidate.binding().incarnation()))
            throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        long total=capacity.assignedTasks()+capacity.reservedTasks();
        if(capacity.reservedTasks()<1||total>capacity.taskSlots()||total>capacity.workerSlots())
            throw new TaskService.Conflict("CANDIDATE_TASK_CAPACITY_UNAVAILABLE");
        String yaml=ManagedWriterDeclaration.successorYaml(tasks.activeYaml(c,task),task.owner,
                candidate.binding().node(),plan.activation().nextAssignment());
        if(!AuditStore.hash(yaml).equals(candidate.configurationSha256()))
            throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        return HandoffPreparations.summary(tasks.commitPreparedWriter(c,task,plan,actor,sealed.cut(),yaml,
                candidate.stagingSha256(),request,candidate.rawCapture(),candidate.failureRecords()));
    }

    void resumePreparedWriter(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,
                              CandidateAdmission candidate,String executionRequest,
                              JournalWriterAssignment.CutProof recoveredCut)throws Exception{
        Objects.requireNonNull(recoveredCut);
        commitCandidate(c,task,actor,candidate,executionRequest,recoveredCut);
    }

    private Map<String,Object> commitCandidate(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor,
                                              CandidateAdmission candidate,String executionRequest,
                                              JournalWriterAssignment.CutProof recoveredCut)throws Exception{
        if(candidate==null||!candidate.valid())
            throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_REQUIRED");
        HandoffPreparations.Plan plan=tasks.preparations().byRequest(c,candidate.preparation(),db.now(c));
        if(plan==null||!plan.id().equals(candidate.preparation()))throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        requirePlanGrants(actor,task,plan);
        if(!plan.evidence().candidate().equals(candidate.binding()))
            throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        if(plan.writerCommit()!=null){
            requireExecutionReplay(plan,actor,executionRequest);
            if(!plan.writerCommit().stagingSha256().equals(candidate.stagingSha256())
                    ||!plan.writerCommit().configurationSha256().equals(candidate.configurationSha256())
                    ||!Objects.equals(plan.writerCommit().rawCapture(),candidate.rawCapture())
                    ||plan.writerCommit().failureRecords()!=candidate.failureRecords())
                throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_REUSED");
            return HandoffPreparations.summary(plan);
        }
        var sealed=tasks.preparations().splitTransfer(c,plan);
        if(sealed!=null){
            if(!candidate.equals(sealed.admission()))throw new TaskService.Conflict("HANDOFF_SPLIT_TRANSFER_CHANGED");
            return finishSplitTransfer(c,task,actor,plan,executionRequest);
        }
        Assessment current=read(c,task.id);
        RecoveryCustody required=requireCandidateReadiness(plan,actor,current,db.now(c));
        if(recoveredCut!=null&&(required.raw()!=null||required.failures()>0))
            throw new TaskService.Conflict("HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED");
        if(!Objects.equals(required.raw(),candidate.rawCapture()==null?null:candidate.rawCapture().binding()))
            throw new TaskService.Conflict("HANDOFF_RAW_STAGING_REQUIRED");
        if(required.failures()!=candidate.failureRecords())
            throw new TaskService.Conflict("HANDOFF_FAILURE_HISTORY_STAGING_REQUIRED");
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
        if(recoveredCut!=null&&!cut.equals(recoveredCut))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
        if(plan.activation().externalJournal()){
            tasks.preparations().sealSplitTransfer(c,plan,new HandoffPreparations.SplitTransfer(executionRequest,candidate,cut),db.now(c));
            auditPreparation(c,plan,actor,executionRequest,"MANAGED_HANDOFF_SPLIT_TRANSFER_SEALED");
            return Map.of("splitTransferPending",true);
        }
        var committed=tasks.commitPreparedWriter(c,task,plan,actor,cut,yaml,candidate.stagingSha256(),
                executionRequest,candidate.rawCapture(),candidate.failureRecords());
        if(recoveredCut!=null)auditPreparation(c,committed,actor,executionRequest,"MANAGED_HANDOFF_EXECUTION_RECOVERED");
        return HandoffPreparations.summary(committed);
    }

    private record RecoveryCustody(RawCaptureHandoffStaging.Binding raw,int failures){}
    private RecoveryCustody requireCandidateReadiness(HandoffPreparations.Plan plan,AccessControl.Principal actor,
                                           Assessment assessment,long now)throws Exception{
        if(!plan.actor().equals(actor.id)||!plan.actorRevision().equals(actor.authorizationRevision))
            throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        if(!"PREPARED".equals(plan.phase())||plan.activation()==null||!plan.activation().canPlace())
            throw new TaskService.Conflict("HANDOFF_MANAGED_CONTROL_CUT_REQUIRED");
        HandoffPreparations.Evidence active=assessment==null?null:preparationEvidence(assessment,now);
        if(!plan.evidence().equals(active))throw new TaskService.Conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED");
        if(!(assessment.probe.originalEvidence.get("originInventory") instanceof Map<?,?> inventory))
            throw new TaskService.Conflict("HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED");
        int failures;
        if("PRESENT".equals(inventory.get("failureEvidence")))failures=verifiedFailureRecords(assessment,plan);
        else if("ABSENT".equals(inventory.get("failureEvidence")))failures=0;
        else throw new TaskService.Conflict("HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED");
        if("ABSENT".equals(inventory.get("sourceEvidence")))return new RecoveryCustody(null,failures);
        if(!"PRESENT".equals(inventory.get("sourceEvidence")))
            throw new TaskService.Conflict("HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED");
        var e=plan.evidence();
        var scope=new SharedHandoffEvidence.Scope(cluster,plan.task(),e.assessment(),e.revision(),e.configuration(),
                e.original().directory(),e.candidate().directory());
        RawCaptureHandoffStaging.Binding raw=rawCaptureStage(assessment,plan,scope);
        if(raw==null)throw new TaskService.Conflict("HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED");
        return new RecoveryCustody(raw,failures);
    }

    private static int verifiedFailureRecords(Assessment assessment,HandoffPreparations.Plan plan){
        Object original=assessment.probe.originalEvidence.get("historicalFailureEvidence");
        Map<?,?> candidate=assessment.probe.reportCustody();
        if(!(original instanceof Map<?,?> witnessed)||!"ARCHIVED".equals(witnessed.get("state"))
                ||candidate==null||!"VERIFIED".equals(candidate.get("state")))
            throw new TaskService.Conflict("HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED");
        var archive=Model.JSON.convertValue(witnessed.get("archive"),SharedHandoffEvidence.Archive.class);
        var verified=Model.JSON.convertValue(candidate.get("archive"),SharedHandoffEvidence.Archive.class);
        if(archive==null||!archive.equals(verified)||archive.failureRecords()<1
                ||!plan.evidence().custody().equals(archive.sha256()))
            throw new TaskService.Conflict("HANDOFF_FAILURE_HISTORY_SCOPE_CHANGED");
        return archive.failureRecords();
    }

    private static boolean digest(String value){return value!=null&&value.matches("[a-f0-9]{64}");}

    Map<String,Object> orchestratePreparedWriter(TaskRequest supplied,AccessControl.Principal actor,OwnerQueries transport)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(request.kind!=TaskRequest.Kind.HANDOFF_COMMIT_OWNER||request.yaml!=null)
            throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_INVALID");
        Map<String,Object> replay=bindOrReplayExecution(request,actor);
        if(replay!=null){
            if(Boolean.TRUE.equals(replay.get("splitTransferPending")))
                return finishSplitTransfer(request.taskId,request.reference,request.requestId,actor);
            return replay;
        }
        try{
            CandidateAdmission candidate=transport.candidateReadiness(request,actor);
            return commitAdmittedWriter(request.taskId,candidate,request.requestId,actor);
        }catch(TaskService.Conflict concurrent){
            replay=committedReplay(request,actor);
            if(replay!=null)return replay;
            throw concurrent;
        }
    }
    Map<String,Object> requestTargetFence(TaskRequest supplied,AccessControl.Principal principal)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(request.kind!=TaskRequest.Kind.HANDOFF_TARGET_FENCE)throw new IllegalArgumentException("HANDOFF_TARGET_FENCE_REQUEST_INVALID");
        return tasks.transaction(request.taskId,principal,(c,task,actor)->{
            var plan=tasks.preparations().latest(c,task.id,db.now(c));
            if(!HandoffPreparations.evidenceInstalled(plan)||!plan.id().equals(request.reference))
                throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
            requirePlanGrants(actor,task,plan);
            if(!task.revision.equals(request.revision)||!task.revision.equals(plan.writerCommit().revision())
                    ||!task.activeConfigurationVersion.equals(plan.writerCommit().configuration())
                    ||!task.configurationVersion.equals(task.activeConfigurationVersion)
                    ||!task.owner.equals(plan.evidence().candidate().node())||!task.directory.equals(plan.evidence().candidate().directory()))
                throw new TaskService.Conflict("REVISION_CHANGED");
            var approved=tasks.preparations().bindTargetFence(c,plan,request.requestId,actor);
            if(!approved.equals(plan))auditPreparation(c,approved,actor,request.requestId,"MANAGED_HANDOFF_TARGET_FENCE_APPROVED");
            return HandoffPreparations.summary(approved);
        });
    }
    Map<String,Object> requestWorkerStart(TaskRequest supplied,AccessControl.Principal principal)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(request.kind!=TaskRequest.Kind.HANDOFF_START_WORKER)throw new IllegalArgumentException("HANDOFF_WORKER_START_REQUEST_INVALID");
        return tasks.transaction(request.taskId,principal,(c,task,actor)->{
            var plan=tasks.preparations().latest(c,task.id,db.now(c));
            if(!HandoffPreparations.evidenceInstalled(plan)||!plan.id().equals(request.reference))
                throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
            requirePlanGrants(actor,task,plan);
            var commit=plan.writerCommit();
            var candidate=plan.evidence().candidate();
            if(!commit.revision().equals(request.revision)
                    ||!task.revision.equals(commit.revision())&&!HandoffPreparations.workerAdmitted(plan)
                    ||!commit.configuration().equals(task.activeConfigurationVersion)
                    ||!commit.configuration().equals(task.configurationVersion)
                    ||!task.owner.equals(candidate.node())||!task.directory.equals(candidate.directory()))
                throw new TaskService.Conflict("REVISION_CHANGED");
            var approved=tasks.preparations().bindWorkerStart(c,plan,request.requestId,actor);
            if(!approved.equals(plan))auditPreparation(c,approved,actor,request.requestId,"MANAGED_HANDOFF_WORKER_START_APPROVED");
            return HandoffPreparations.summary(approved);
        });
    }
    Map<String,Object> bindOrReplayExecution(TaskRequest supplied,AccessControl.Principal principal)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(request.kind!=TaskRequest.Kind.HANDOFF_COMMIT_OWNER||request.yaml!=null)
            throw new TaskService.Conflict("HANDOFF_PREPARATION_REQUEST_INVALID");
        return tasks.transaction(request.taskId,principal,Connection.TRANSACTION_READ_COMMITTED,(c,task,actor)->{
            HandoffPreparations.Plan plan=executionPlan(c,task,actor,request);
            if(plan.writerCommit()!=null){
                requireExecutionReplay(plan,actor,request.requestId);
                return HandoffPreparations.summary(plan);
            }
            if(tasks.preparations().splitTransfer(c,plan)!=null){
                HandoffPreparations.requireExecution(plan,request.requestId);
                if(!actor.administers())throw new SecurityException("FORBIDDEN");
                if(!plan.actorRevision().equals(actor.authorizationRevision)){
                    var approved=tasks.preparations().reapproveSplitTransfer(c,plan,actor);
                    auditPreparation(c,approved,actor,request.requestId,"MANAGED_HANDOFF_SPLIT_TRANSFER_REAPPROVED");
                }else if(!plan.actor().equals(actor.id))throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
                return Map.of("splitTransferPending",true);
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
        if("ORIGINAL_FROZEN".equals(plan.phase()))throw new TaskService.Conflict("HANDOFF_ORIGINAL_FROZEN");
        if(plan.writerFreeze()!=null&&plan.writerFreeze().offlineCommit()!=null)
            throw new TaskService.Conflict("HANDOFF_OFFLINE_COMMIT_REQUIRED");
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
                proof.journalGenerationSha256(),proof.nativeEvidenceSha256(),shared,!shared&&!db.mysql()&&declaration.splitStoreHandoff());
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
        if(!probeFresh(a,now)||a.originalCut==null||a.probe.candidate==null
                ||!"VERIFIED".equals(a.probe.candidate.get("state")))return null;
        a.probe.requireCustodyShape();
        var custody=reviewedReportReceiptCustody(a);
        if(custody==null)return null;
        String proof=AuditStore.hash(Model.JSON.writeValueAsString(a));
        return new HandoffPreparations.Evidence(a.request,a.revision,a.activeVersion,
                new HandoffPreparations.Binding(a.original.node,a.original.directory,a.original.incarnation,a.original.epoch),
                new HandoffPreparations.Binding(a.candidate.node,a.candidate.directory,a.candidate.incarnation,a.candidate.epoch),
                new HandoffPreparations.JournalCut(a.originalCut.journalEpoch,a.originalCut.sequence,a.originalCut.checkpointHash),
                proof,custody.sha256());
    }

    private static SharedHandoffEvidence.Archive reviewedReportReceiptCustody(Assessment a){
        if(a.probe==null||a.probe.originalEvidence==null
                ||!(a.probe.originalEvidence.get("closedReceipts") instanceof Map<?,?> originalReceipts)
                ||!"ARCHIVED".equals(originalReceipts.get("state")))return null;
        Map<?,?> shared=a.probe.reportCustody();
        if(shared==null||!"VERIFIED".equals(shared.get("state")))return null;
        SharedHandoffEvidence.Archive receipt=Model.JSON.convertValue(originalReceipts.get("archive"),SharedHandoffEvidence.Archive.class);
        SharedHandoffEvidence.Archive candidate=Model.JSON.convertValue(shared.get("archive"),SharedHandoffEvidence.Archive.class);
        if(receipt==null||!receipt.equals(candidate)||candidate.closedReceipts()==0)return null;
        if(!(a.probe.originalEvidence.get("originInventory") instanceof Map<?,?> inventory))return null;
        if(!"ABSENT".equals(inventory.get("validationReports"))&&!("PRESENT".equals(inventory.get("validationReports"))&&candidate.reports()>0))
            return null;
        return candidate;
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
        Map<String,Object> result=summary(task,a,stale(c,task,actor,a),db.now(c),views.ownerControlState(c,a.original.node));
        if(result.containsKey("offlinePreflight")&&tasks.preparations().hasOriginalFreeze(c,task.id))
            result.remove("offlinePreflight");
        return result;
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

    private Map<String,Object> summary(GlobalTaskStore.Record task,Assessment a,boolean stale,long now,
                                       String originalControlState)throws Exception{
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("id",a.request);result.put("taskId",a.task);result.put("taskRevision",a.revision);
        result.put("activeConfigurationVersion",a.activeVersion);
        result.put("originalNodeId",a.original.node);result.put("originalNodeEpoch",Long.toString(a.original.epoch));
        result.put("originalControlState",originalControlState);
        result.put("candidateNodeId",a.candidate.node);result.put("candidateNodeEpoch",Long.toString(a.candidate.epoch));
        result.put("candidateState",a.candidate.state);result.put("candidateAssignedTasks",a.assigned);
        result.put("candidateTaskCapacity",a.taskSlots);result.put("candidateWorkerCapacity",a.workerSlots);
        result.put("assessedAt",Instant.ofEpochMilli(a.assessedAt).toString());
        result.put("originalCut",a.originalCut);
        result.put("state",stale?"STALE":a.blockers.size()==2?"EVIDENCE_REQUIRED":"BLOCKED");
        List<String> blockers=new ArrayList<>(a.blockers);
        if(a.probe!=null){
            boolean applicable=!stale&&probeFresh(a,now);
            if(applicable&&a.probe.candidate!=null&&"VERIFIED".equals(a.probe.candidate.get("state")))blockers.remove(CANDIDATE_ACCESS);
            Map<String,Object> original=a.probe.originalEvidence==null?Map.of():a.probe.originalEvidence;
            if(!applicable){
                original=new LinkedHashMap<>(original);
                for(String name:List.of("shareableReports","closedReceipts","historicalFailureEvidence","rawCaptureEvidence")){
                    var retained=historicalCustody(original.get(name),"ARCHIVED");
                    if(retained==null)original.remove(name);else original.put(name,retained);
                }
            }
            Map<String,Object> probe=new LinkedHashMap<>(Map.of("checkedAt",Instant.ofEpochMilli(a.probe.checkedAt).toString(),
                    "current",applicable,"candidate",a.probe.candidate==null?Map.of():a.probe.candidate,"originalEvidence",original));
            Map<?,?> sharedReports=applicable?a.probe.reportCustody():historicalCustody(a.probe.sharedReportCustody,"VERIFIED");
            if(visibleCustody(original.get("shareableReports"),sharedReports,applicable))
                probe.put("sharedReportCustody",sharedReports);
            if(visibleCustody(original.get("closedReceipts"),sharedReports,applicable))
                probe.put("sharedReceiptCustody",sharedReports);
            if(visibleCustody(original.get("historicalFailureEvidence"),sharedReports,applicable))
                probe.put("sharedFailureEvidenceCustody",sharedReports);
            Map<?,?> sharedRaw=applicable?a.probe.rawCustody():historicalCustody(a.probe.sharedRawCaptureCustody,"VERIFIED");
            if(sharedRaw!=null&&(applicable||visibleCustody(original.get("rawCaptureEvidence"),sharedRaw,false)))
                probe.put("sharedRawCaptureCustody",sharedRaw);
            result.put("probe",probe);
            HandoffPreparations.Evidence prepared=applicable?preparationEvidence(a,now):null;
            if(prepared!=null)result.put("preparationEvidenceSha256",prepared.proof());
        }
        result.put("blockers",blockers);result.put("executionAuthorized",false);
        if(task.owner.equals(a.original.node)&&Set.of("STALE","STOPPED","UNKNOWN").contains(originalControlState))
            result.put("offlinePreflight",offlinePreflight(task,a));
        return result;
    }

    private Map<String,Object> offlinePreflight(GlobalTaskStore.Record task,Assessment a)throws Exception{
        boolean currentAuthority=task.directory.equals(a.original.directory)&&task.revision.equals(a.revision)
                &&Objects.equals(task.activeConfigurationVersion,a.activeVersion)
                &&Objects.equals(task.configurationVersion,a.activeVersion);
        boolean historicalCut=a.originalCut!=null&&a.blockers.stream()
                .noneMatch(code->code.startsWith("ORIGINAL_")&&!code.equals(RECOVERY_EVIDENCE));
        boolean historicalCustody;
        try{historicalCustody=reviewedReportReceiptCustody(a)!=null;}
        catch(IllegalArgumentException malformed){historicalCustody=false;}
        List<String> blockers=new ArrayList<>(List.of("HANDOFF_OFFLINE_DURABLE_FREEZE_REQUIRED",
                "HANDOFF_OFFLINE_COMPLETE_ARCHIVE_REQUIRED"));
        if(!currentAuthority)blockers.add("HANDOFF_OFFLINE_REVIEW_CHANGED");
        if(!historicalCut)blockers.add("HANDOFF_OFFLINE_STOPPED_CUT_UNRECORDED");
        if(!historicalCustody)blockers.add("HANDOFF_OFFLINE_REPORT_RECEIPT_CUSTODY_UNRECORDED");
        return Map.of("state","BLOCKED","authorityMatchesReview",currentAuthority,"historicalDrainedCut",historicalCut,
                "historicalReportsAndReceipts",historicalCustody,"blockers",List.copyOf(blockers));
    }

    private static boolean visibleCustody(Object witness,Map<?,?> shared,boolean current){
        return shared!=null&&witness instanceof Map<?,?> original&&"ARCHIVED".equals(original.get("state"))
                &&(current||"REFUSED".equals(shared.get("state"))||Objects.equals(original.get("archive"),shared.get("archive")));
    }

    private static Map<?,?> historicalCustody(Object stored,String acceptedState){
        if(!(stored instanceof Map<?,?> custody))return null;
        Object state=custody.get("state");
        if("REFUSED".equals(state))
            return custody.get("code") instanceof String code&&code.matches("[A-Z][A-Z0-9_]{2,119}")
                    ?Map.of("state","REFUSED","code",code):null;
        if(!acceptedState.equals(state))return null;
        try{
            var archive=Model.JSON.convertValue(custody.get("archive"),SharedHandoffEvidence.Archive.class);
            return archive==null?null:Map.of("state",state,"archive",archive);
        }catch(IllegalArgumentException malformed){return null;}
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
