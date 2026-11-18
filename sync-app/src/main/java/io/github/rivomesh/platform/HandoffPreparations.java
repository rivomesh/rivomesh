package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
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
    record Plan(String id,String task,int sequence,String phase,String actor,String actorRevision,Evidence evidence,
                long created,long expires,String releaseId,String releasedBy,long closedAt,
                @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                WriterActivation activation){}

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
                    case "PREPARED"->p.releaseId==null&&p.releasedBy==null&&p.closedAt==0;
                    case "RELEASED"->p.releaseId!=null&&p.releasedBy!=null&&p.closedAt>=p.created;
                    case "EXPIRED"->p.releaseId==null&&p.releasedBy==null&&p.closedAt>=p.expires;
                    default->false;
                }))throw new TaskService.Conflict("HANDOFF_PREPARATION_INVALID");
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
                release,actor,now,p.activation);
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
        Plan p=new Plan(request,task.id,next,"PREPARED",actor.id,actor.authorizationRevision,evidence,now,now+REVIEW_MILLIS,null,null,0,activation);
        db.update(c,"INSERT INTO "+db.table("task_handoff_plan")
                        +" (cluster_id,request_id,task_id,candidate_id,ordinal,phase,envelope) VALUES (?,?,?,?,?,?,?)",
                cluster,p.id,p.task,p.evidence.candidate.node,p.sequence,p.phase,seal(p));
        return p;
    }
    Plan release(Connection c,Plan p,String request,AccessControl.Principal actor,long now)throws Exception{
        if(!"PREPARED".equals(p.phase))throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        return close(c,p,"RELEASED",request,actor.id,Math.max(p.created,now));
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
        view.put("executionAuthorized",false);view.put("reservedTaskSlots","PREPARED".equals(p.phase)?1:0);
        view.put("reservedWorkerSlots","PREPARED".equals(p.phase)?1:0);
        if(p.releaseId!=null)view.put("releaseId",p.releaseId);
        if(p.activation!=null)view.put("writerActivation",Map.of(
                "checkedInControlTransaction",p.activation.checkedInControlTransaction,
                "activationSha256",AuditStore.hash(p.id+"\n"+e.proof+"\n"+p.activation.nextAssignment),
                "originalAssignmentSha256",p.activation.originalAssignmentSha256,
                "journalGenerationSha256",p.activation.journalGenerationSha256,
                "nativeEvidenceSha256",p.activation.nativeEvidenceSha256));
        return view;
    }
}
