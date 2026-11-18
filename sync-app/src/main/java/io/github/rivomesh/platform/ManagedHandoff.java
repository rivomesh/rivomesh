package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.Model;
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
    private record Assessment(int version,String task,String request,String replaces,String revision,String activeVersion,
                              Agent original,Agent candidate,int taskSlots,int workerSlots,long assigned,
                              String actor,String actorRevision,long assessedAt,OriginalCut originalCut,List<String> blockers){}
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
            if(!Objects.equals(prior==null?null:prior.request,request.reference))
                throw new TaskService.Conflict("HANDOFF_ASSESSMENT_CHANGED");
            if(!task.revision.equals(request.revision))throw new TaskService.Conflict("REVISION_CHANGED");
            if(task.owner.equals(request.ownerNodeId))throw new TaskService.Conflict("HANDOFF_SAME_AGENT");
            AgentDirectory.Binding original=agents.owner(c,task.owner,task.directory);
            AgentDirectory.HandoffCapacity candidate=agents.handoffCapacity(c,request.ownerNodeId);
            List<String> blockers=new ArrayList<>();
            if(!"HEALTHY".equals(candidate.binding().state))blockers.add("CANDIDATE_AGENT_UNAVAILABLE");
            if(candidate.assignedTasks()>=candidate.taskSlots()||candidate.workerSlots()<1)
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
                    candidate.assignedTasks(),actor.id,actor.authorizationRevision,assessedAt,originalCut,List.copyOf(blockers));
            String encoded=Model.JSON.writeValueAsString(next);
            String envelope=cipher.seal(purpose(task.id,next.request),encoded);
            if(prior==null)db.update(c,"INSERT INTO "+db.table("task_handoff")
                    +" (cluster_id,task_id,request_id,envelope) VALUES (?,?,?,?)",cluster,task.id,next.request,envelope);
            else db.update(c,"UPDATE "+db.table("task_handoff")+" SET request_id=?,envelope=? WHERE cluster_id=? AND task_id=?",
                    next.request,envelope,cluster,task.id);
            AuditStore.Event event=new AuditStore.Event();event.kind="MANAGED_HANDOFF_ASSESSED";event.taskId=task.id;
            event.actorId=actor.id;event.actorRole=actor.role.name();event.subjectId=candidate.binding().node;
            event.clientRequestHash=AuditStore.hash(request.requestId);event.operationHash=AuditStore.hash(encoded);
            event.status="COMMITTED";access.append(c,event);
            return summary(next,false);
        });
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
        return summary(a,stale);
    }

    private static Map<String,Object> summary(Assessment a,boolean stale){
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
        result.put("blockers",a.blockers);result.put("executionAuthorized",false);
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
