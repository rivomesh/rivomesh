package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Durable admission and authenticated receipts. Owner dispatch is a separate, fenced step. */
final class DurableCommands {
    static final class Stored {
        public int version=1,priority=1;
        public String id,task,owner,directory,actor,actorRole,actorRevision,inputHash,acceptedRevision,configurationVersion;
        public String phase="PENDING",code,parent;
        public long createdAt;
        public TaskRequest request;
        public long dispatchEpoch;
        public String dispatchIncarnation;
        public TaskRequest localRequest;
        public TaskRequest executionTemplate;
    }
    static final class Dispatch {
        final Stored command;
        final AccessControl.Principal actor;
        Dispatch(Stored command,AccessControl.Principal actor){this.command=command;this.actor=actor;}
    }
    private final GlobalTaskStore tasks;
    private final SharedAccessStore access;
    private final JdbcControlStore db;
    private final ControlCipher cipher;
    private final String cluster,anchor;
    private final int maximum;
    private final GlobalResources resources;
    private final boolean executionRoles;
    private final boolean recoveryReview,recoveryCommit,recoverySkip;
    DurableCommands(ClusterConfig config,SharedAccessStore access,GlobalTaskStore tasks,int maximum)throws Exception{
        if(!access.controls(config)||!tasks.accessIdentity().equals(access.identity)||maximum<1||maximum>100000)throw new IllegalArgumentException("COMMAND_POLICY_INVALID");
        this.tasks=tasks;this.access=access;this.maximum=maximum;db=tasks.database();cipher=tasks.commandCipher();cluster=config.id;executionRoles=config.separateExecution;recoveryReview=config.recoveryReview;recoveryCommit=config.recoveryCommit;recoverySkip=config.recoverySkip;
        anchor=UUID.nameUUIDFromBytes(("rivomesh-command-authority/"+tasks.identity).getBytes(StandardCharsets.UTF_8)).toString();
        resources=new GlobalResources(config,tasks,access,2000000);
        try(Connection c=db.connect()){
            db.bootstrapLock(c);String suffix=db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"";
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("command_root")+" (cluster_id VARCHAR(128) PRIMARY KEY,store_id VARCHAR(36) NOT NULL,maximum_pending INTEGER NOT NULL,policy_hash VARCHAR(43) NOT NULL,accepted_count BIGINT NOT NULL,state_hash VARCHAR(43) NOT NULL)"+suffix);
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("command")+" (cluster_id VARCHAR(128) NOT NULL,request_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,owner_id VARCHAR(64) NOT NULL,phase VARCHAR(16) NOT NULL,priority INTEGER NOT NULL,created_at BIGINT NOT NULL,envelope "+(db.mysql()?"LONGTEXT":"TEXT")+" NOT NULL,PRIMARY KEY(cluster_id,request_id))"+suffix);
            db.index(c,"command","rmc_command_owner_pending","cluster_id, owner_id, phase, priority, created_at, request_id");
            db.index(c,"command","rmc_command_task_history","cluster_id, task_id, created_at, request_id");
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rmc_command_root','rmc_command')",config.namespace);ResultSet r=p.executeQuery()){
                int count=0;while(r.next()){count++;if(!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new TaskService.Conflict("CONTROL_STORE_ENGINE_INVALID");if(!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))throw new TaskService.Conflict("CONTROL_STORE_COLLATION_INVALID");}if(count!=2)throw new TaskService.Conflict("CONTROL_STORE_TABLES_MISSING");
            }
            c.setAutoCommit(false);tasks.service(c,(transaction,directory)->{
                boolean exists;try(PreparedStatement p=db.prepare(c,"SELECT store_id FROM "+db.table("command_root")+" WHERE cluster_id=? FOR UPDATE",cluster);ResultSet r=p.executeQuery()){exists=r.next();}
                boolean recorded=access.containsReceipt(c,anchor);
                if(!exists){
                    if(recorded)throw new TaskService.Conflict("COMMAND_STATE_REQUIRED");
                    try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("command")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)>0)throw new TaskService.Conflict("COMMAND_STATE_REQUIRED");}
                    db.update(c,"INSERT INTO "+db.table("command_root")+" (cluster_id,store_id,maximum_pending,policy_hash,accepted_count,state_hash) VALUES (?,?,?,?,0,?)",cluster,tasks.identity,maximum,policy(),stateHash(0));
                    AuditStore.Event event=new AuditStore.Event();event.id=anchor;event.kind="GLOBAL_COMMAND_AUTHORITY_CREATED";event.subjectId=tasks.identity;event.operationHash=AuditStore.hash(policy());event.status="COMMITTED";access.append(c,event);
                }else if(!recorded)throw new TaskService.Conflict("COMMAND_ANCHOR_REQUIRED");long accepted=root(c);
                try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("command")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)!=accepted)throw new TaskService.Conflict("COMMAND_CATALOG_INCOMPLETE");}return null;
            });c.commit();
        }
    }
    private String policy(){return cipher.signature("command-policy\n"+tasks.identity+"\n"+maximum);}
    private String stateHash(long count){return cipher.signature("command-state\n"+tasks.identity+"\n"+maximum+"\n"+count);}
    private long root(Connection c)throws Exception{return root(c,true);}
    private long root(Connection c,boolean lock)throws Exception{
        long count;
        try(PreparedStatement p=db.prepare(c,"SELECT store_id,maximum_pending,policy_hash,accepted_count,state_hash FROM "+db.table("command_root")+" WHERE cluster_id=?"+(lock?" FOR UPDATE":""),cluster);ResultSet r=p.executeQuery()){
            if(!r.next()||!tasks.identity.equals(r.getString(1)))throw new TaskService.Conflict("COMMAND_STATE_REQUIRED");
            if(maximum!=r.getInt(2)||!cipher.matches("command-policy\n"+tasks.identity+"\n"+maximum,r.getString(3)))throw new TaskService.Conflict("COMMAND_POLICY_MISMATCH");
            count=r.getLong(4);if(count<0||!stateHash(count).equals(r.getString(5)))throw new TaskService.Conflict("COMMAND_ROOT_INVALID");
        }
        if(!access.containsReceipt(c,anchor))throw new TaskService.Conflict("COMMAND_ANCHOR_REQUIRED");return count;
    }
    private String signature(TaskRequest request,AccessControl.Principal actor)throws Exception{return cipher.signature("command-input\n"+actor.id+"\n"+Model.JSON.writeValueAsString(request));}
    private Stored read(Connection c,String id)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("command")+" WHERE cluster_id=? AND request_id=?",cluster,id);ResultSet r=p.executeQuery()){
            if(!r.next())return null;Stored command=Model.JSON.readValue(cipher.open("command/"+id,r.getString("envelope")),Stored.class);
            if(command.version!=1||!id.equals(command.id)||!Objects.equals(command.task,r.getString("task_id"))||!Objects.equals(command.owner,r.getString("owner_id"))||!Objects.equals(command.phase,r.getString("phase"))||command.priority!=r.getInt("priority")||command.createdAt!=r.getLong("created_at")||command.request==null||!id.equals(command.request.requestId)||!Objects.equals(command.task,command.request.taskId))throw new TaskService.Conflict("COMMAND_RECORD_INVALID");return command;
        }
    }
    private static void authorize(TaskRequest request,AccessControl.Principal actor){if(request.admin()&&!actor.administers()||request.kind==TaskRequest.Kind.ACTION&&!actor.operates())throw new SecurityException("FORBIDDEN");}
    private Stored replay(Connection c,TaskRequest request,AccessControl.Principal actor)throws Exception{
        Stored previous=read(c,request.requestId);if(previous==null)return null;if(!actor.id.equals(previous.actor))throw new TaskService.Conflict("REQUEST_IDENTITY_CHANGED");if(!signature(request,actor).equals(previous.inputHash))throw new TaskService.Conflict("REQUEST_ID_REUSED");return previous;
    }
    private Stored insert(Connection c,GlobalTaskStore.Record task,TaskRequest request,AccessControl.Principal actor)throws Exception{return insert(c,task,request,actor,null);}
    private Stored insert(Connection c,GlobalTaskStore.Record task,TaskRequest request,AccessControl.Principal actor,TaskRequest template)throws Exception{
        // Recheck inside the committing transaction: a previously read Agent binding may already be stale.
        if(executionRoles)db.requireExecutionRole(c,task.owner);
        long acceptedCount=root(c);authorize(request,actor);if(read(c,request.requestId)!=null)throw new TaskService.Conflict("REQUEST_ID_REUSED");
        if(request.kind==TaskRequest.Kind.ACTION)try(PreparedStatement p=db.prepare(c,"SELECT request_id FROM "+db.table("task_request")+" WHERE cluster_id=? AND request_id=?",cluster,request.requestId);ResultSet r=p.executeQuery()){if(r.next())throw new TaskService.Conflict("REQUEST_ID_REUSED");}
        boolean cancellation=request.recoveryCancel()||request.kind==TaskRequest.Kind.ACTION&&Arrays.asList("VALIDATE_CANCEL","REPAIR_CANCEL","GAP_VERIFY_CANCEL").contains(request.action);Stored parent=null;
        try(PreparedStatement p=db.prepare(c,"SELECT request_id FROM "+db.table("command")+" WHERE cluster_id=? AND task_id=? AND phase IN ('PENDING','DISPATCHED','ACCEPTED','UNKNOWN') ORDER BY priority,created_at,request_id",cluster,task.id);ResultSet rows=p.executeQuery()){
            while(rows.next()){
                Stored candidate=read(c,rows.getString(1));if(!cancellation)throw new TaskService.Conflict("OPERATION_PENDING");
                if(candidate.priority==0)throw new TaskService.Conflict("CANCEL_ALREADY_REQUESTED");
                if(candidate.request.kind==TaskRequest.Kind.ACTION&&matchesCancellation(request,candidate))parent=candidate;
            }
        }
        if(cancellation&&parent==null)throw new TaskService.Conflict("CANCEL_SCOPE_CHANGED");
        if(!cancellation)try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("command")+" WHERE cluster_id=? AND priority>0 AND phase IN ('PENDING','DISPATCHED','ACCEPTED','UNKNOWN')",cluster);ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)>=maximum)throw new TaskService.Conflict("COMMAND_CAPACITY");}
        Stored command=new Stored();command.id=request.requestId;command.task=task.id;command.owner=task.owner;command.directory=task.directory;command.actor=actor.id;command.actorRole=actor.role.name();command.actorRevision=actor.authorizationRevision;command.inputHash=signature(request,actor);command.acceptedRevision=task.revision;command.configurationVersion=request.kind==TaskRequest.Kind.ACTION&&task.activeConfigurationVersion!=null?task.activeConfigurationVersion:task.configurationVersion;command.request=request;command.executionTemplate=template;command.createdAt=db.now(c);command.priority=cancellation?0:1;command.parent=parent==null?null:parent.id;
        db.update(c,"INSERT INTO "+db.table("command")+" (cluster_id,request_id,task_id,owner_id,phase,priority,created_at,envelope) VALUES (?,?,?,?,?,?,?,?)",cluster,command.id,command.task,command.owner,command.phase,command.priority,command.createdAt,cipher.seal("command/"+command.id,Model.JSON.writeValueAsString(command)));
        long nextCount=Math.addExact(acceptedCount,1);db.update(c,"UPDATE "+db.table("command_root")+" SET accepted_count=?,state_hash=? WHERE cluster_id=?",nextCount,stateHash(nextCount),cluster);
        AuditStore.Event event=new AuditStore.Event();event.kind="GLOBAL_COMMAND_ACCEPTED";event.taskId=task.id;event.action=request.action==null?request.kind.name():request.action;event.actorId=actor.id;event.actorRole=actor.role.name();event.clientRequestHash=AuditStore.hash(command.id);event.status="PENDING";access.append(c,event);return command;
    }
    private static boolean matchesCancellation(TaskRequest request,Stored parent){
        String action=parent.request.action;
        switch(request.action){
            case "VALIDATE_CANCEL":return Arrays.asList("VALIDATE","VALIDATE_RETRY").contains(action);
            case "REPAIR_CANCEL":return Arrays.asList("REPAIR_PLAN","REPAIR_EXECUTE").contains(action);
            case "GAP_VERIFY_CANCEL":return Arrays.asList("GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE").contains(action);
            case "RECOVERY_CANCEL":return request.reference.equals(parent.id)&&TaskRequest.cancellableRecovery(action);
            default:return false;
        }
    }
    Map<String,Object> enqueue(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();if(request.kind!=TaskRequest.Kind.ACTION)throw new IllegalArgumentException("COMMAND_ACTION_REQUIRED");
        requireRecovery(request);
        return tasks.transaction(request.taskId,actor,(c,task,current)->{
            authorize(request,current);root(c);Stored previous=replay(c,request,current);if(previous!=null)return view(previous);
            if(!Objects.equals(task.revision,request.revision))throw new TaskService.Conflict("REVISION_CHANGED");tasks.advance(c,task);return view(insert(c,task,request,current));
        });
    }
    void requireRecovery(TaskRequest request){if(request.skipRecovery()&&!recoverySkip)throw new TaskService.Conflict("SKIP_DISABLED");if(request.commitRecovery()&&!recoveryCommit)throw new TaskService.Conflict("FAILURE_ACK_DISABLED");if(request.failureReview()&&!recoveryReview)throw new TaskService.Conflict("FAILURE_REVIEW_DISABLED");}
    private static void definition(String id,String yaml)throws Exception{
        com.fasterxml.jackson.databind.ObjectMapper mapper=new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.yaml.YAMLFactory()).enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        com.fasterxml.jackson.databind.JsonNode document=mapper.readTree(yaml);
        if(document==null||!document.isObject()||!id.equals(document.path(document.path("formatVersion").asInt()==3?"pipelineId":"jobId").asText()))throw new TaskService.Conflict("COMMAND_TASK_ID_CHANGED");ConfigurationDocuments.validate(document);
    }
    private void write(Connection c,Stored command)throws Exception{
        if(db.update(c,"UPDATE "+db.table("command")+" SET phase=?,envelope=? WHERE cluster_id=? AND request_id=?",command.phase,cipher.seal("command/"+command.id,Model.JSON.writeValueAsString(command)),cluster,command.id)!=1)throw new TaskService.Conflict("COMMAND_STATE_REQUIRED");
    }
    private static boolean terminal(String phase){return Arrays.asList("SUCCEEDED","FAILED","REJECTED","CANCELLED").contains(phase);}
    private void outcome(Connection c,Stored command,String phase,String code)throws Exception{
        command.phase=phase;command.code=code;write(c,command);
        AuditStore.Event event=new AuditStore.Event();event.kind="GLOBAL_COMMAND_OUTCOME";event.taskId=command.task;event.subjectId=command.owner;event.actorId=command.actor;event.clientRequestHash=AuditStore.hash(command.id);event.action=command.request.action==null?command.request.kind.name():command.request.action;event.status=phase;event.code=code;access.append(c,event);
    }
    private static AccessControl.Principal actor(AccountDirectory.State state,Stored command){
        AccountDirectory.User user=state.users.get(command.actor);
        if(user==null||!user.enabled||user.deleted||!Objects.equals(command.actorRevision,state.revision))return null;
        AccessControl.Principal actor=new AccessControl.Principal(user.id,user.role,user.taskIds,state.revision,user.nodeIds);
        if(!actor.task(command.task)||!actor.node(command.owner))return null;
        try{authorize(command.request,actor);return actor;}catch(SecurityException denied){return null;}
    }
    List<String> pending(AgentDirectory agents,ControlStore.Lease lease,String directory,int limit)throws Exception{
        if(limit<1||limit>50)throw new IllegalArgumentException("COMMAND_PAGE_INVALID");
        return tasks.service((c,accounts)->{
            agents.require(c,lease,directory);root(c);List<String> ids=new ArrayList<>();
            try(PreparedStatement p=db.prepare(c,"SELECT request_id FROM "+db.table("command")+" WHERE cluster_id=? AND owner_id=? AND phase IN ('PENDING','DISPATCHED','ACCEPTED','UNKNOWN') ORDER BY priority,created_at,request_id LIMIT ?",cluster,lease.nodeId,limit);ResultSet r=p.executeQuery()){
                while(r.next()){Stored command=read(c,r.getString(1));if(!directory.equals(command.directory))throw new TaskService.Conflict("AGENT_DIRECTORY_CHANGED");ids.add(command.id);}
            }return ids;
        });
    }
    List<Stored> recoverable(AgentDirectory agents,ControlStore.Lease lease,String directory,String after)throws Exception{
        return outstanding(agents,lease,directory,after,false);
    }
    List<Stored> outstanding(AgentDirectory agents,ControlStore.Lease lease,String directory,String after,boolean includePending)throws Exception{
        if(after==null||!after.isEmpty()&&!after.matches("[A-Za-z0-9_-]{8,128}"))throw new IllegalArgumentException("COMMAND_CURSOR_INVALID");
        return tasks.service((c,accounts)->{
            agents.require(c,lease,directory);root(c);List<Stored> result=new ArrayList<>();
            String phases=includePending?"('PENDING','DISPATCHED','ACCEPTED','UNKNOWN')":"('DISPATCHED','ACCEPTED','UNKNOWN')";
            try(PreparedStatement p=db.prepare(c,"SELECT request_id FROM "+db.table("command")+" WHERE cluster_id=? AND owner_id=? AND phase IN "+phases+" AND request_id>? ORDER BY request_id LIMIT 50",cluster,lease.nodeId,after);ResultSet r=p.executeQuery()){
                while(r.next()){Stored command=read(c,r.getString(1));if(!directory.equals(command.directory))throw new TaskService.Conflict("AGENT_DIRECTORY_CHANGED");result.add(command);}
            }return result;
        });
    }
    Dispatch claim(String requestId,AgentDirectory agents,ControlStore.Lease lease,String directory)throws Exception{
        return tasks.service((c,accounts)->{
            agents.require(c,lease,directory);root(c);Stored command=read(c,requestId);if(command==null)throw new NoSuchElementException("COMMAND_NOT_FOUND");
            if(!lease.nodeId.equals(command.owner)||!directory.equals(command.directory))throw new ControlStore.Fenced();
            GlobalTaskStore.Record task=tasks.record(c,command.task);
            if(!command.owner.equals(task.owner)||!command.directory.equals(task.directory))throw new TaskService.Conflict("COMMAND_OWNER_CHANGED");
            if(terminal(command.phase))return null;
            AccessControl.Principal current=actor(accounts,command);
            if("PENDING".equals(command.phase)){
                if(current==null){outcome(c,command,"REJECTED","AUTHORIZATION_CHANGED");return null;}
                if(command.parent!=null){
                    Stored parent=read(c,command.parent);if(parent==null||!command.task.equals(parent.task))throw new TaskService.Conflict("COMMAND_PARENT_REQUIRED");
                    if("PENDING".equals(parent.phase)){
                        outcome(c,parent,"CANCELLED","CANCELLED_BEFORE_DISPATCH");outcome(c,command,"SUCCEEDED","CANCELLED_BEFORE_DISPATCH");return null;
                    }
                    if(terminal(parent.phase)){outcome(c,command,"SUCCEEDED","PARENT_ALREADY_TERMINAL");return null;}
                    // A lost owner reply is resolved from its exact local receipt before cancellation.
                    if(!"ACCEPTED".equals(parent.phase))return null;
                }
                command.phase="DISPATCHED";
            }
            command.dispatchEpoch=lease.epoch;command.dispatchIncarnation=lease.incarnation;write(c,command);return new Dispatch(command,current);
        });
    }
    TaskRequest bindLocal(Dispatch dispatch,TaskRequest supplied,AgentDirectory agents,ControlStore.Lease lease,String directory)throws Exception{
        TaskRequest request=supplied.snapshot();
        return tasks.service((c,accounts)->{
            agents.require(c,lease,directory);root(c);Stored command=dispatched(c,dispatch,lease,directory);
            if(!"DISPATCHED".equals(command.phase))throw new TaskService.Conflict("COMMAND_PHASE_CHANGED");
            TaskRequest template=command.executionTemplate==null?command.request:command.executionTemplate;TaskRequest comparison=request.snapshot();comparison.revision=template.revision;
            if(!Model.JSON.writeValueAsString(comparison).equals(Model.JSON.writeValueAsString(template)))throw new TaskService.Conflict("COMMAND_LOCAL_PAYLOAD_CHANGED");
            if(command.localRequest!=null){
                if(!Model.JSON.writeValueAsString(command.localRequest).equals(Model.JSON.writeValueAsString(request)))throw new TaskService.Conflict("COMMAND_LOCAL_REVISION_CHANGED");return command.localRequest;
            }
            if(actor(accounts,command)==null)throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
            command.localRequest=request;write(c,command);return request;
        });
    }
    private Stored dispatched(Connection c,Dispatch dispatch,ControlStore.Lease lease,String directory)throws Exception{
        Stored command=read(c,dispatch.command.id);
        if(command==null)throw new TaskService.Conflict("COMMAND_STATE_REQUIRED");
        if(!lease.nodeId.equals(command.owner)||!directory.equals(command.directory)||command.dispatchEpoch!=lease.epoch||!Objects.equals(command.dispatchIncarnation,lease.incarnation)||dispatch.command.dispatchEpoch!=lease.epoch||!Objects.equals(dispatch.command.dispatchIncarnation,lease.incarnation))throw new ControlStore.Fenced();return command;
    }
    void complete(Dispatch dispatch,String phase,String code,AgentDirectory agents,ControlStore.Lease lease,String directory)throws Exception{
        if(!Arrays.asList("DISPATCHED","ACCEPTED","UNKNOWN","SUCCEEDED","FAILED","REJECTED","CANCELLED").contains(phase)||code==null||!code.matches("[A-Z0-9_]{1,120}"))throw new IllegalArgumentException("COMMAND_OUTCOME_INVALID");
        tasks.service((c,accounts)->{
            agents.require(c,lease,directory);root(c);Stored command=dispatched(c,dispatch,lease,directory);
            if(terminal(command.phase)){if(!command.phase.equals(phase)||!Objects.equals(command.code,code))throw new TaskService.Conflict("COMMAND_OUTCOME_CHANGED");return null;}
            if(("ACCEPTED".equals(command.phase)||"UNKNOWN".equals(command.phase))&&Arrays.asList("DISPATCHED","REJECTED").contains(phase))throw new TaskService.Conflict("COMMAND_PHASE_CHANGED");
            if(command.localRequest==null&&!Arrays.asList("REJECTED","UNKNOWN").contains(phase))throw new TaskService.Conflict("COMMAND_LOCAL_REQUEST_REQUIRED");
            if(command.phase.equals(phase)&&Objects.equals(command.code,code))return null;
            if("SUCCEEDED".equals(phase)&&Arrays.asList(TaskRequest.Kind.CREATE,TaskRequest.Kind.CONFIG_UPDATE,TaskRequest.Kind.RESTORE).contains(command.request.kind))tasks.applied(c,tasks.record(c,command.task),command.configurationVersion);
            outcome(c,command,phase,code);agents.require(c,lease,directory);return null;
        });
    }
    void verifyDispatch(Dispatch dispatch,AgentDirectory agents,ControlStore.Lease lease,String directory)throws Exception{
        tasks.service((c,accounts)->{agents.require(c,lease,directory);root(c);Stored command=dispatched(c,dispatch,lease,directory);if(terminal(command.phase))throw new TaskService.Conflict("COMMAND_PHASE_CHANGED");return null;});
    }
    void reserveResources(Dispatch dispatch,TaskDefinition definition,AgentDirectory agents,ControlStore.Lease lease,String directory)throws Exception{
        if(definition==null)return;
        tasks.service((c,accounts)->{agents.require(c,lease,directory);root(c);Stored command=dispatched(c,dispatch,lease,directory);if(actor(accounts,command)==null)throw new TaskService.Conflict("AUTHORIZATION_CHANGED");resources.reserve(c,tasks.record(c,command.task),definition);agents.require(c,lease,directory);return null;});
    }
    void reserveExisting(Connection c,GlobalTaskStore.Record record,TaskDefinition definition)throws Exception{resources.reserve(c,record,definition);}
    GlobalTaskStore.Record create(String id,String owner,String directory,TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();if(request.kind!=TaskRequest.Kind.CREATE)throw new IllegalArgumentException("COMMAND_CREATE_REQUIRED");request.taskId=id;definition(id,request.yaml);
        return tasks.create(id,owner,directory,request.yaml,request.requestId,actor,(c,task,current)->insert(c,task,request,current));
    }
    GlobalTaskStore.Record allocate(String id,TaskRequest supplied,AccessControl.Principal actor,AgentDirectory agents)throws Exception{
        TaskRequest request=supplied.snapshot();
        if(request.kind!=TaskRequest.Kind.CREATE||!"AUTO".equals(request.placement))
            throw new IllegalArgumentException("TASK_PLACEMENT_INVALID");
        request.taskId=id;
        definition(id,request.yaml);
        TaskRequest template=request.snapshot();
        template.placement=null;
        return tasks.allocate(id,request.yaml,request.requestId,actor,agents,(c,task,current)->insert(c,task,request,current,template));
    }
    GlobalTaskStore.Record update(String id,String yaml,TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();if((request.kind!=TaskRequest.Kind.CONFIG_UPDATE&&request.kind!=TaskRequest.Kind.RESTORE)||!id.equals(request.taskId))throw new IllegalArgumentException("COMMAND_CONFIGURATION_REQUIRED");definition(id,yaml);
        TaskRequest template=request.snapshot();template.kind=TaskRequest.Kind.CONFIG_UPDATE;template.reference=null;template.document=new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.yaml.YAMLFactory()).readTree(yaml);template.validate();
        return tasks.update(id,request.revision,yaml,request.requestId,actor,(c,task,current)->insert(c,task,request,current,template),request.kind==TaskRequest.Kind.RESTORE?"RESTORE_CONFIG":"EDIT_CONFIG");
    }
    Map<String,Object> existing(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();if(!request.mutation()||request.taskId==null)throw new IllegalArgumentException("COMMAND_TASK_REQUIRED");
        return tasks.transaction(request.taskId,actor,(c,task,current)->{authorize(request,current);root(c);Stored previous=replay(c,request,current);return previous==null?null:view(previous);});
    }
    Map<String,Object> view(String requestId,AccessControl.Principal actor)throws Exception{
        String task;try(Connection c=db.connect();PreparedStatement p=db.prepare(c,"SELECT task_id FROM "+db.table("command")+" WHERE cluster_id=? AND request_id=?",cluster,requestId);ResultSet r=p.executeQuery()){if(!r.next())throw new NoSuchElementException("COMMAND_NOT_FOUND");task=r.getString(1);}
        return tasks.transaction(task,actor,(c,record,current)->{root(c);Stored command=read(c,requestId);if(command==null||!current.node(command.owner))throw new NoSuchElementException("COMMAND_NOT_FOUND");return view(command);});
    }
    List<Map<String,Object>> operations(Connection c,String task,AccessControl.Principal actor)throws Exception{
        root(c,false);List<Map<String,Object>> result=new ArrayList<>();
        try(PreparedStatement p=db.prepare(c,"SELECT request_id FROM "+db.table("command")+" WHERE cluster_id=? AND task_id=? ORDER BY created_at DESC,request_id DESC LIMIT 20",cluster,task);ResultSet r=p.executeQuery()){
            while(r.next()){
                Stored command=read(c,r.getString(1));if(!actor.task(command.task)||!actor.node(command.owner))throw new SecurityException("FORBIDDEN");Map<String,Object> row=view(command);row.put("actorRole",command.actorRole);row.put("deliveryStatus",command.phase);if(!terminal(command.phase))row.put("status","PENDING");result.add(row);
            }
        }Collections.reverse(result);return result;
    }
    boolean hasUnresolved(Connection c,String task)throws Exception{
        root(c,false);
        try(PreparedStatement p=db.prepare(c,"SELECT request_id FROM "+db.table("command")
                +" WHERE cluster_id=? AND task_id=? AND phase NOT IN ('SUCCEEDED','FAILED','REJECTED','CANCELLED')"
                +" ORDER BY created_at,request_id LIMIT 1",cluster,task);ResultSet r=p.executeQuery()){
            if(!r.next())return false;
            Stored command=read(c,r.getString(1));
            if(command==null||!task.equals(command.task)||terminal(command.phase))
                throw new TaskService.Conflict("COMMAND_STATE_INVALID");
            return true;
        }
    }
    private static Map<String,Object> view(Stored command){Map<String,Object> view=new LinkedHashMap<>();view.put("id",command.id);view.put("taskId",command.task);view.put("ownerNodeId",command.owner);view.put("action",command.request.action==null?command.request.kind.name():command.request.action);view.put("status",command.phase);view.put("code",command.code);view.put("actorId",command.actor);view.put("revision",command.acceptedRevision);view.put("createdAt",Instant.ofEpochMilli(command.createdAt).toString());view.put("parentRequestId",command.parent);return view;}
}
