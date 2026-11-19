package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.JournalWriterAssignment;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Durable review reservations. All callers hold the global task authority lock. */
final class HandoffPreparations {
    private static final long REVIEW_MILLIS=15*60*1000;
    record Binding(String node,String directory,String incarnation,long epoch){}
    record Evidence(String assessment,String revision,String configuration,Binding original,Binding candidate,
                    JournalCut cut,String proof,String custody){}
    record JournalCut(String epoch,String sequence,String checkpointSha256){}
    record WriterActivation(String originalAssignmentSha256,String nextAssignment,String journalGenerationSha256,
                            String nativeEvidenceSha256,boolean checkedInControlTransaction){}
    record WriterCommit(String actor,String stagingSha256,String configurationSha256,String revision,String configuration,long journalEpoch,
                        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                        RawCaptureHandoffStaging.Proof rawCapture,
                 @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT)
                 int failureRecords){
        WriterCommit(String actor,String stagingSha256,String configurationSha256,String revision,String configuration,long journalEpoch){
            this(actor,stagingSha256,configurationSha256,revision,configuration,journalEpoch,null,0);
        }
        WriterCommit(String actor,String stagingSha256,String configurationSha256,String revision,String configuration,long journalEpoch,
                 RawCaptureHandoffStaging.Proof rawCapture){
            this(actor,stagingSha256,configurationSha256,revision,configuration,journalEpoch,rawCapture,0);
        }
    }
    record PendingProblem(String code,long observedAt){}
    record TargetFenceRequest(String id,String actor,String actorRevision,JournalWriterAssignment.TargetFence result,
                              @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                              PendingProblem problem){
        TargetFenceRequest(String id,String actor,String actorRevision,JournalWriterAssignment.TargetFence result){
            this(id,actor,actorRevision,result,null);
        }
    }
    record WorkerStart(String id,String actor,String actorRevision,
                       @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                       PendingProblem problem){
        WorkerStart(String id,String actor,String actorRevision){this(id,actor,actorRevision,null);}
    }
    record Plan(String id,String task,int sequence,String phase,String actor,String actorRevision,Evidence evidence,
                long created,long expires,String releaseId,String releasedBy,long closedAt,
                @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                WriterActivation activation,
                @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                WriterCommit writerCommit,
                @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                String executionRequest,
                @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                HandoffEvidenceAdoption.Receipt evidenceAdoption,
                @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                TargetFenceRequest targetFence,
                @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                WorkerStart workerStart){
        Plan(String id,String task,int sequence,String phase,String actor,String actorRevision,Evidence evidence,
             long created,long expires,String releaseId,String releasedBy,long closedAt,WriterActivation activation,
             WriterCommit writerCommit,String executionRequest,HandoffEvidenceAdoption.Receipt evidenceAdoption){
            this(id,task,sequence,phase,actor,actorRevision,evidence,created,expires,releaseId,releasedBy,closedAt,
                    activation,writerCommit,executionRequest,evidenceAdoption,null,null);
        }
    }

    private final JdbcControlStore db;
    private final ControlCipher cipher;
    private final String cluster;

    HandoffPreparations(ClusterConfig config,JdbcControlStore db,ControlCipher cipher)throws Exception{
        this.db=db;this.cipher=cipher;cluster=config.id;
        try(Connection c=db.connect()){
            db.bootstrapLock(c);
            String suffix=db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"";
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("task_handoff_plan")
                    +" (cluster_id VARCHAR(128) NOT NULL,request_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,"
                    +"candidate_id VARCHAR(64) NOT NULL,ordinal INTEGER NOT NULL,phase VARCHAR(16) NOT NULL,"
                    +"release_id VARCHAR(128),envelope "+(db.mysql()?"LONGTEXT":"TEXT")+" NOT NULL,"
                    +"PRIMARY KEY(cluster_id,request_id),UNIQUE(cluster_id,release_id),UNIQUE(cluster_id,task_id,ordinal))"+suffix);
            db.index(c,"task_handoff_plan","rmc_handoff_active_candidate","cluster_id, phase, candidate_id, request_id");
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES"
                    +" WHERE TABLE_SCHEMA=? AND TABLE_NAME='rmc_task_handoff_plan'",config.namespace);ResultSet r=p.executeQuery()){
                if(!r.next()||!"InnoDB".equalsIgnoreCase(r.getString(1))||!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))
                    throw new TaskService.Conflict("HANDOFF_STORE_INVALID");
            }
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }

    private String purpose(String id){return "managed-handoff-plan/"+id;}
    private Plan read(ResultSet row)throws Exception{
        String id=row.getString("request_id");
        Plan p=Model.JSON.readValue(cipher.open(purpose(id),row.getString("envelope")),Plan.class);
        if(p.evidence==null||p.evidence.candidate==null||!id.equals(p.id)||!row.getString("task_id").equals(p.task)||!row.getString("candidate_id").equals(p.evidence.candidate.node)
                ||!row.getString("phase").equals(p.phase)||row.getInt("ordinal")!=p.sequence
                ||!Objects.equals(row.getString("release_id"),p.releaseId)
                ||p.sequence<1||p.created<=0||p.expires-p.created!=REVIEW_MILLIS
                ||!(switch(p.phase){
                    case "PREPARED"->p.releaseId==null&&p.releasedBy==null&&p.closedAt==0&&p.writerCommit==null;
                    case "RELEASED"->p.releaseId!=null&&p.releasedBy!=null&&p.closedAt>=p.created&&p.writerCommit==null;
                    case "EXPIRED"->p.releaseId==null&&p.releasedBy==null&&p.closedAt>=p.expires&&p.writerCommit==null;
                    case "COMMITTED","PROJECTED","EVIDENCE_ADOPTED","FENCE_PENDING","TARGETS_FENCED","START_PENDING","WORKER_ADMITTED","WORKER_PROJECTED"->p.releaseId==null&&p.releasedBy==null&&p.closedAt>=p.created&&p.closedAt<p.expires
                            &&p.activation!=null&&p.writerCommit!=null&&p.actor.equals(p.writerCommit.actor)
                            &&HandoffFailureHistory.bounded(p.writerCommit.failureRecords())
                            &&(p.writerCommit.rawCapture()==null||p.writerCommit.rawCapture().matches(p)
                                &&cluster.equals(p.writerCommit.rawCapture().binding().scope().cluster()));
                    default->false;
                })||p.executionRequest!=null&&!validExecutionId(p,p.executionRequest)
                ||(evidenceInstalled(p)?!validAdoption(p,p.evidenceAdoption):p.evidenceAdoption!=null)
                ||!validTargetFence(p)||!validWorkerStart(p))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_INVALID");
        return p;
    }
    private String seal(Plan p)throws Exception{return cipher.seal(purpose(p.id),Model.JSON.writeValueAsString(p));}
    private Plan select(Connection c,String predicate,Object... arguments)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("task_handoff_plan")+" WHERE "+predicate+" FOR UPDATE",arguments);
            ResultSet r=p.executeQuery()){
            return r.next()?read(r):null;
        }
    }
    private Plan close(Connection c,Plan p,String phase,String release,String actor,long now)throws Exception{
        Plan closed=new Plan(p.id,p.task,p.sequence,phase,p.actor,p.actorRevision,p.evidence,p.created,p.expires,
                release,actor,now,p.activation,null,p.executionRequest,null);
        if(db.update(c,"UPDATE "+db.table("task_handoff_plan")+" SET phase=?,release_id=?,envelope=?"
                        +" WHERE cluster_id=? AND request_id=? AND phase='PREPARED'",phase,release,seal(closed),cluster,p.id)!=1)
            throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        return closed;
    }
    private Plan expire(Connection c,Plan p,long now)throws Exception{
        return p!=null&&"PREPARED".equals(p.phase)&&p.expires<=now?close(c,p,"EXPIRED",null,null,now):p;
    }
    Plan byRequest(Connection c,String request,long now)throws Exception{
        return expire(c,select(c,"cluster_id=? AND (request_id=? OR release_id=?)",cluster,request,request),now);
    }
    Plan latest(Connection c,String task,long now)throws Exception{
        return expire(c,select(c,"cluster_id=? AND task_id=? ORDER BY ordinal DESC LIMIT 1",cluster,task),now);
    }
    void requireUnreserved(Connection c,String task)throws Exception{
        Plan plan=latest(c,task,db.now(c));
        if(plan!=null&&"PREPARED".equals(plan.phase))throw new TaskService.Conflict("HANDOFF_PREPARATION_ACTIVE");
        if(plan!=null&&plan.writerCommit!=null)throw new TaskService.Conflict("HANDOFF_RECIPIENT_IMPORT_REQUIRED");
    }
    void requireCommandAllowed(Connection c,String task)throws Exception{
        Plan plan=latest(c,task,db.now(c));
        if(plan!=null&&"WORKER_PROJECTED".equals(plan.phase()))return;
        requireUnreserved(c,task);
    }
    long reserved(Connection c,String candidate)throws Exception{
        return reservations(c,candidate).getOrDefault(candidate,0L);
    }
    Map<String,Long> reservations(Connection c)throws Exception{return reservations(c,null);}
    private Map<String,Long> reservations(Connection c,String candidate)throws Exception{
        List<Plan> open=new ArrayList<>();
        String sql="SELECT * FROM "+db.table("task_handoff_plan")+" WHERE cluster_id=? AND phase='PREPARED'"
                +(candidate==null?"":" AND candidate_id=?")+" FOR UPDATE";
        try(PreparedStatement p=candidate==null?db.prepare(c,sql,cluster):db.prepare(c,sql,cluster,candidate);
            ResultSet r=p.executeQuery()){
            while(r.next())open.add(read(r));
        }
        Map<String,Long> counts=new HashMap<>();long now=db.now(c);
        for(Plan p:open)if("PREPARED".equals(expire(c,p,now).phase))counts.merge(p.evidence.candidate.node,1L,Long::sum);
        return counts;
    }
    Plan prepare(Connection c,GlobalTaskStore.Record task,String request,AccessControl.Principal actor,Evidence evidence,
                 WriterActivation activation,long now)throws Exception{
        Plan latest=latest(c,task.id,now);
        if(latest!=null&&"PREPARED".equals(latest.phase))throw new TaskService.Conflict("HANDOFF_PREPARATION_ACTIVE");
        int next=latest==null?1:Math.addExact(latest.sequence,1);
        Plan p=new Plan(request,task.id,next,"PREPARED",actor.id,actor.authorizationRevision,evidence,now,now+REVIEW_MILLIS,null,null,0,activation,null,null,null);
        db.update(c,"INSERT INTO "+db.table("task_handoff_plan")
                        +" (cluster_id,request_id,task_id,candidate_id,ordinal,phase,envelope) VALUES (?,?,?,?,?,?,?)",
                cluster,p.id,p.task,p.evidence.candidate.node,p.sequence,p.phase,seal(p));
        return p;
    }
    Plan release(Connection c,Plan p,String request,AccessControl.Principal actor,long now)throws Exception{
        if(!"PREPARED".equals(p.phase))throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        return close(c,p,"RELEASED",request,actor.id,Math.max(p.created,now));
    }
    private static boolean validExecutionId(Plan plan,String request){
        return request!=null&&request.matches("[A-Za-z0-9_-]{8,128}")&&!plan.id.equals(request);
    }
    Plan bindExecution(Connection c,Plan plan,String request)throws Exception{
        if(!"PREPARED".equals(plan.phase)||!validExecutionId(plan,request))
            throw new TaskService.Conflict("HANDOFF_EXECUTION_REQUEST_INVALID");
        if(plan.executionRequest!=null){
            requireExecution(plan,request);
            return plan;
        }
        Plan bound=new Plan(plan.id,plan.task,plan.sequence,plan.phase,plan.actor,plan.actorRevision,plan.evidence,
                plan.created,plan.expires,null,null,0,plan.activation,null,request,null);
        if(db.update(c,"UPDATE "+db.table("task_handoff_plan")+" SET envelope=?"
                +" WHERE cluster_id=? AND request_id=? AND phase='PREPARED'",seal(bound),cluster,plan.id)!=1)
            throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        return bound;
    }
    static void requireExecution(Plan plan,String request){
        if(plan.executionRequest==null||!plan.executionRequest.equals(request))
            throw new TaskService.Conflict("HANDOFF_EXECUTION_REQUEST_CHANGED");
    }

    Plan commitWriter(Connection c,Plan p,WriterCommit receipt,long now)throws Exception{
        if(!"PREPARED".equals(p.phase)||p.activation==null||now>=p.expires
                ||!HandoffFailureHistory.bounded(receipt.failureRecords())||receipt.rawCapture()!=null&&!receipt.rawCapture().matches(p))
            throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        Plan complete=new Plan(p.id,p.task,p.sequence,"COMMITTED",p.actor,p.actorRevision,p.evidence,p.created,p.expires,
                null,null,Math.max(now,p.created),p.activation,receipt,p.executionRequest,null);
        if(db.update(c,"UPDATE "+db.table("task_handoff_plan")+" SET phase=?,envelope=?"
                +" WHERE cluster_id=? AND request_id=? AND phase='PREPARED'",complete.phase,seal(complete),cluster,p.id)!=1)
            throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        return complete;
    }

    record PendingRecipient(String task,String preparation,String phase,String approval){}
    List<PendingRecipient> pendingRecipient(Connection c,String candidate,String directory,String after,int limit)throws Exception{
        List<PendingRecipient> pending=new ArrayList<>();
        try(PreparedStatement statement=db.prepare(c,"SELECT * FROM "+db.table("task_handoff_plan")
                +" WHERE cluster_id=? AND candidate_id=? AND phase IN ('COMMITTED','PROJECTED','FENCE_PENDING','START_PENDING','WORKER_ADMITTED') AND request_id>? ORDER BY request_id LIMIT ?",
                cluster,candidate,after,limit);ResultSet rows=statement.executeQuery()){
            while(rows.next()){
                Plan p=read(rows);
                if(!directory.equals(p.evidence.candidate.directory))throw new ControlStore.Fenced();
                String approval=switch(p.phase){
                    case "FENCE_PENDING" -> p.targetFence.id;
                    case "START_PENDING" -> p.workerStart.id;
                    default -> null;
                };
                pending.add(new PendingRecipient(p.task,p.id,p.phase,approval));
            }
        }
        return pending;
    }
    void recordProjected(Connection c,Plan plan,ManagedRecipientImport.Receipt imported)throws Exception{
        if(plan==null||plan.writerCommit==null||!plan.id.equals(imported.preparation())
                ||!plan.writerCommit.configurationSha256.equals(imported.configurationSha256())
                ||!plan.writerCommit.stagingSha256.equals(imported.stagingSha256()))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_PROJECTION_CHANGED");
        if(evidenceEligible(plan))return;
        if(!"COMMITTED".equals(plan.phase))throw new TaskService.Conflict("HANDOFF_RECIPIENT_COMMIT_REQUIRED");
        Plan projected=new Plan(plan.id,plan.task,plan.sequence,"PROJECTED",plan.actor,plan.actorRevision,plan.evidence,
                plan.created,plan.expires,null,null,plan.closedAt,plan.activation,plan.writerCommit,plan.executionRequest,null);
        if(db.update(c,"UPDATE "+db.table("task_handoff_plan")+" SET phase=?,envelope=?"
                +" WHERE cluster_id=? AND request_id=? AND phase='COMMITTED'",projected.phase,seal(projected),cluster,plan.id)!=1)
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_COMMIT_REQUIRED");
    }

    static boolean evidenceEligible(Plan plan){
        return plan!=null&&("PROJECTED".equals(plan.phase)||evidenceInstalled(plan));
    }

    private static boolean validAdoption(Plan plan,HandoffEvidenceAdoption.Receipt receipt){
        return receipt!=null&&plan.writerCommit!=null&&plan.id.equals(receipt.preparation())
                &&plan.task.equals(receipt.task())&&plan.evidence.candidate.equals(receipt.candidate())
                &&plan.writerCommit.revision.equals(receipt.committedRevision())
                &&plan.writerCommit.configurationSha256.equals(receipt.configurationSha256())
                &&plan.writerCommit.stagingSha256.equals(receipt.stagingSha256())
                &&Objects.equals(plan.writerCommit.rawCapture(),receipt.rawCapture())
                &&plan.writerCommit.failureRecords()==receipt.failureRecords()
                &&receipt.adoptionSha256()!=null&&receipt.adoptionSha256().matches("[a-f0-9]{64}")
                &&receipt.reports()>=0&&receipt.closedReceipts()>0&&receipt.closedReceipts()<=SharedHandoffEvidence.MAX_RECEIPTS;
    }
    void recordEvidenceAdoption(Connection c,Plan plan,HandoffEvidenceAdoption.Receipt receipt)throws Exception{
        if(!evidenceEligible(plan)||!validAdoption(plan,receipt))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
        if(plan.evidenceAdoption!=null){
            if(!plan.evidenceAdoption.equals(receipt))throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
            return;
        }
        Plan adopted=new Plan(plan.id,plan.task,plan.sequence,"EVIDENCE_ADOPTED",plan.actor,plan.actorRevision,plan.evidence,
                plan.created,plan.expires,null,null,plan.closedAt,plan.activation,plan.writerCommit,plan.executionRequest,receipt);
        if(db.update(c,"UPDATE "+db.table("task_handoff_plan")+" SET phase=?,envelope=?"
                +" WHERE cluster_id=? AND request_id=? AND phase='PROJECTED'",adopted.phase,seal(adopted),cluster,plan.id)!=1)
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
    }

    static boolean evidenceInstalled(Plan plan){
        return plan!=null&&Set.of("EVIDENCE_ADOPTED","FENCE_PENDING","TARGETS_FENCED","START_PENDING","WORKER_ADMITTED","WORKER_PROJECTED").contains(plan.phase());
    }
    private static boolean validTargetFence(Plan plan){
        if(!Set.of("FENCE_PENDING","TARGETS_FENCED","START_PENDING","WORKER_ADMITTED","WORKER_PROJECTED").contains(plan.phase()))return plan.targetFence==null;
        TargetFenceRequest request=plan.targetFence;
        if(request==null||!validExecutionId(plan,request.id)||request.id.equals(plan.executionRequest)
                ||request.actor==null||request.actorRevision==null)return false;
        JournalWriterAssignment.TargetFence result=request.result;
        if(!validProblem(request.problem,"FENCE_PENDING".equals(plan.phase())))return false;
        return "FENCE_PENDING".equals(plan.phase())?result==null:result!=null&&result.epoch()==plan.writerCommit.journalEpoch()
                &&plan.activation.nativeEvidenceSha256().equals(result.cutSha256())&&result.destinations()!=null
                &&!result.destinations().isEmpty()&&result.destinations().size()<=8
                &&result.destinations().values().stream().allMatch(hash->hash!=null&&hash.matches("[a-f0-9]{64}"));
    }
    Plan bindTargetFence(Connection c,Plan plan,String request,AccessControl.Principal actor)throws Exception{
        TargetFenceRequest bound=plan.targetFence();
        if(bound!=null){
            if("FENCE_PENDING".equals(plan.phase)&&!bound.id.equals(request)
                    &&!bound.actorRevision.equals(actor.authorizationRevision))
                return updateTargetFence(c,plan,"FENCE_PENDING",new TargetFenceRequest(request,actor.id,actor.authorizationRevision,null));
            if(!bound.id.equals(request))throw new TaskService.Conflict("HANDOFF_TARGET_FENCE_REQUEST_CHANGED");
            if(!bound.actor.equals(actor.id)||!bound.actorRevision.equals(actor.authorizationRevision))
                throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
            return plan;
        }
        if(!"EVIDENCE_ADOPTED".equals(plan.phase))throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
        return updateTargetFence(c,plan,"FENCE_PENDING",new TargetFenceRequest(request,actor.id,actor.authorizationRevision,null));
    }
    Plan recordTargetFence(Connection c,Plan plan,JournalWriterAssignment.TargetFence result)throws Exception{
        if(!"FENCE_PENDING".equals(plan.phase)||plan.targetFence==null)
            throw new TaskService.Conflict("HANDOFF_TARGET_FENCE_REQUEST_CHANGED");
        TargetFenceRequest pending=plan.targetFence;
        return updateTargetFence(c,plan,"TARGETS_FENCED",new TargetFenceRequest(pending.id,pending.actor,pending.actorRevision,result));
    }
    private Plan updateTargetFence(Connection c,Plan plan,String phase,TargetFenceRequest request)throws Exception{
        Plan next=new Plan(plan.id,plan.task,plan.sequence,phase,plan.actor,plan.actorRevision,plan.evidence,plan.created,
                plan.expires,plan.releaseId,plan.releasedBy,plan.closedAt,plan.activation,plan.writerCommit,
                plan.executionRequest,plan.evidenceAdoption,request,null);
        if(!validTargetFence(next)||db.update(c,"UPDATE "+db.table("task_handoff_plan")+" SET phase=?,envelope=?"
                +" WHERE cluster_id=? AND request_id=? AND phase=?",phase,seal(next),cluster,plan.id,plan.phase)!=1)
            throw new TaskService.Conflict("HANDOFF_TARGET_FENCE_REQUEST_CHANGED");
        return next;
    }

    static boolean workerAdmitted(Plan plan){
        return plan!=null&&("WORKER_ADMITTED".equals(plan.phase)||"WORKER_PROJECTED".equals(plan.phase));
    }
    private static boolean validWorkerStart(Plan plan){
        boolean expected=workerAdmitted(plan)||"START_PENDING".equals(plan.phase);
        WorkerStart start=plan.workerStart;
        if(!expected)return start==null;
        return start!=null&&validExecutionId(plan,start.id)&&!start.id.equals(plan.executionRequest)
                &&plan.targetFence!=null&&!start.id.equals(plan.targetFence.id)
                &&start.actor!=null&&start.actorRevision!=null&&validProblem(start.problem,"START_PENDING".equals(plan.phase));
    }
    private static boolean validProblem(PendingProblem problem,boolean pending){
        return problem==null||pending&&problem.observedAt>0&&Objects.equals(problem.code,safeProblem(problem.code));
    }
    static String safeProblem(String code){
        return code!=null&&code.matches("(?:HANDOFF|WRITER_ASSIGNMENT|JOURNAL|SOURCE|SINK|PIPELINE)_[A-Z0-9_]{1,90}"
                +"|AUTHORIZATION_CHANGED|FORBIDDEN|OWNER_RUNTIME_UNAVAILABLE")?code:"OWNER_RUNTIME_UNAVAILABLE";
    }
    void recordPendingProblem(Connection c,Plan plan,PendingRecipient attempt,String code,long now)throws Exception{
        if(plan==null||!plan.id.equals(attempt.preparation)||!plan.phase.equals(attempt.phase)||attempt.approval==null)return;
        String sanitized=safeProblem(code);
        var problem=new PendingProblem(sanitized,now);
        if("FENCE_PENDING".equals(plan.phase)){
            var request=plan.targetFence;
            if(!attempt.approval.equals(request.id)||request.problem!=null&&sanitized.equals(request.problem.code))return;
            updateTargetFence(c,plan,plan.phase,new TargetFenceRequest(request.id,request.actor,request.actorRevision,null,problem));
        }else if("START_PENDING".equals(plan.phase)){
            var request=plan.workerStart;
            if(!attempt.approval.equals(request.id)||request.problem!=null&&sanitized.equals(request.problem.code))return;
            updateWorkerStart(c,plan,plan.phase,new WorkerStart(request.id,request.actor,request.actorRevision,problem));
        }
    }
    Plan bindWorkerStart(Connection c,Plan plan,String request,AccessControl.Principal actor)throws Exception{
        WorkerStart bound=plan.workerStart;
        if(bound!=null){
            if("START_PENDING".equals(plan.phase)&&!bound.id.equals(request)
                    &&!bound.actorRevision.equals(actor.authorizationRevision))
                return updateWorkerStart(c,plan,"START_PENDING",new WorkerStart(request,actor.id,actor.authorizationRevision));
            if(!bound.id.equals(request))throw new TaskService.Conflict("HANDOFF_WORKER_START_REQUEST_CHANGED");
            if(!bound.actor.equals(actor.id)||!bound.actorRevision.equals(actor.authorizationRevision))
                throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
            return plan;
        }
        if(!"TARGETS_FENCED".equals(plan.phase))throw new TaskService.Conflict("HANDOFF_TARGET_FENCE_REQUIRED");
        return updateWorkerStart(c,plan,"START_PENDING",new WorkerStart(request,actor.id,actor.authorizationRevision));
    }
    Plan recordWorkerAdmitted(Connection c,Plan plan)throws Exception{
        if(!"START_PENDING".equals(plan.phase))throw new TaskService.Conflict("HANDOFF_WORKER_START_REQUEST_CHANGED");
        var start=plan.workerStart;
        return updateWorkerStart(c,plan,"WORKER_ADMITTED",new WorkerStart(start.id,start.actor,start.actorRevision));
    }
    Plan recordWorkerProjected(Connection c,Plan plan)throws Exception{
        if(!"WORKER_ADMITTED".equals(plan.phase))throw new TaskService.Conflict("HANDOFF_WORKER_START_REQUEST_CHANGED");
        return updateWorkerStart(c,plan,"WORKER_PROJECTED",plan.workerStart);
    }
    private Plan updateWorkerStart(Connection c,Plan plan,String phase,WorkerStart request)throws Exception{
        Plan next=new Plan(plan.id,plan.task,plan.sequence,phase,plan.actor,plan.actorRevision,plan.evidence,plan.created,
                plan.expires,plan.releaseId,plan.releasedBy,plan.closedAt,plan.activation,plan.writerCommit,
                plan.executionRequest,plan.evidenceAdoption,plan.targetFence,request);
        if(!validWorkerStart(next)||!validTargetFence(next)||db.update(c,"UPDATE "+db.table("task_handoff_plan")+" SET phase=?,envelope=?"
                +" WHERE cluster_id=? AND request_id=? AND phase=?",phase,seal(next),cluster,plan.id,plan.phase)!=1)
            throw new TaskService.Conflict("HANDOFF_WORKER_START_REQUEST_CHANGED");
        return next;
    }

    Plan retirement(Connection c,String task,String original,String directory)throws Exception{
        try(PreparedStatement statement=db.prepare(c,"SELECT * FROM "+db.table("task_handoff_plan")
                +" WHERE cluster_id=? AND task_id=? AND phase IN ('COMMITTED','PROJECTED','EVIDENCE_ADOPTED','FENCE_PENDING','TARGETS_FENCED','START_PENDING','WORKER_ADMITTED','WORKER_PROJECTED') ORDER BY ordinal DESC",cluster,task);
            ResultSet rows=statement.executeQuery()){
            while(rows.next()){
                Plan p=read(rows);
                if(p.evidence.original.node.equals(original)&&p.evidence.original.directory.equals(directory))return p;
            }
            return null;
        }
    }

    static Map<String,Object> summary(Plan p){
        Evidence e=p.evidence;
        Map<String,Object> view=new LinkedHashMap<>();
        view.put("id",p.id);view.put("taskId",p.task);view.put("state",p.phase);view.put("assessmentId",e.assessment);
        view.put("taskRevision",e.revision);view.put("activeConfigurationVersion",e.configuration);
        view.put("originalNodeId",e.original.node);view.put("candidateNodeId",e.candidate.node);
        view.put("originalNodeEpoch",Long.toString(e.original.epoch));view.put("candidateNodeEpoch",Long.toString(e.candidate.epoch));
        view.put("cut",e.cut);view.put("custodySha256",e.custody);view.put("evidenceSha256",e.proof);
        view.put("createdAt",Instant.ofEpochMilli(p.created).toString());view.put("expiresAt",Instant.ofEpochMilli(p.expires).toString());
        view.put("executionAuthorized",workerAdmitted(p));view.put("reservedTaskSlots","PREPARED".equals(p.phase)?1:0);
        view.put("reservedWorkerSlots","PREPARED".equals(p.phase)?1:0);
        if(p.releaseId!=null)view.put("releaseId",p.releaseId);
        if(p.executionRequest!=null&&(p.writerCommit!=null||"PREPARED".equals(p.phase)))
            view.put("commitRequestId",p.executionRequest);
        if(p.writerCommit!=null){
            view.put("activatedJournalEpoch",Long.toString(p.writerCommit.journalEpoch));
            view.put("committedRevision",p.writerCommit.revision);
            view.put("committedConfigurationVersion",p.writerCommit.configuration);
        }
        if(p.evidenceAdoption!=null){
            var adopted=new LinkedHashMap<String,Object>();
            adopted.put("validationReports",p.evidenceAdoption.reports());
            adopted.put("closedReceipts",p.evidenceAdoption.closedReceipts());
            if(p.evidenceAdoption.rawCapture()!=null)
                adopted.put("rawCaptures",p.evidenceAdoption.rawCapture().binding().archive().rawCaptures());
            if(p.evidenceAdoption.failureRecords()>0)adopted.put("failureRecords",p.evidenceAdoption.failureRecords());
            view.put("adoptedEvidence",adopted);
        }
        if("EVIDENCE_ADOPTED".equals(p.phase))view.put("targetFenceAvailable",true);
        if("TARGETS_FENCED".equals(p.phase))view.put("workerStartAvailable",true);
        if(p.workerStart!=null)view.put("workerStart",Map.of("requestId",p.workerStart.id()));
        if(p.targetFence!=null)view.put("targetFence",Map.of("requestId",p.targetFence.id(),
                "destinationsFenced",p.targetFence.result()==null?0:p.targetFence.result().destinations().size()));
        PendingProblem problem=switch(p.phase){
            case "FENCE_PENDING" -> p.targetFence.problem;
            case "START_PENDING" -> p.workerStart.problem;
            default -> null;
        };
        if(problem!=null)view.put("pendingProblem",Map.of("code",problem.code,"observedAt",Instant.ofEpochMilli(problem.observedAt).toString()));
        if(p.activation!=null)view.put("writerActivation",Map.of(
                "checkedInControlTransaction",p.activation.checkedInControlTransaction,
                "activationSha256",AuditStore.hash(p.id+"\n"+e.proof+"\n"+p.activation.nextAssignment),
                "originalAssignmentSha256",p.activation.originalAssignmentSha256,
                "journalGenerationSha256",p.activation.journalGenerationSha256,
                "nativeEvidenceSha256",p.activation.nativeEvidenceSha256));
        return view;
    }
}
