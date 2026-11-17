package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.sql.*;
import java.util.*;

/** Expiring encrypted read-only RPC. Task mutations use DurableCommands instead. */
final class OwnerQueries {
    static final class Entry {
        public int version=1;
        public String id,task,owner,directory,incarnation,actor,actorRevision,phase="PENDING",code;
        public long epoch,expiresAt;
        public TaskRequest request;
        public Map<String,Object> response;
    }
    private final GlobalTaskStore tasks;
    private final JdbcControlStore db;
    private final AgentDirectory agents;
    private final ControlCipher cipher;
    private final String cluster;
    private final int maximum;
    private final long lifetime;
    private final boolean recoveryReview,recoverySkip;

    OwnerQueries(ClusterConfig config,GlobalTaskStore tasks,AgentDirectory agents,int maximum,long lifetime)throws Exception{
        if(maximum<1||maximum>10000||lifetime<1000||lifetime>60000)throw new IllegalArgumentException("OWNER_QUERY_POLICY_INVALID");
        this.tasks=tasks;this.agents=agents;this.maximum=maximum;this.lifetime=lifetime;db=tasks.database();cipher=tasks.commandCipher();cluster=config.id;recoveryReview=config.recoveryReview;recoverySkip=config.recoverySkip;
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
            if(entry.version!=1||!id.equals(entry.id)||!Objects.equals(entry.task,r.getString("task_id"))||!Objects.equals(entry.owner,r.getString("owner_id"))||entry.epoch!=r.getLong("node_epoch")||!Objects.equals(entry.phase,r.getString("phase"))||entry.expiresAt!=r.getLong("expires_at")||entry.request==null||!entry.task.equals(entry.request.taskId))throw new TaskService.Conflict("OWNER_QUERY_RECORD_INVALID");readonly(entry.request);return entry;
        }
    }
    private void write(Connection c,Entry entry)throws Exception{
        if(db.update(c,"UPDATE "+db.table("owner_query")+" SET phase=?,envelope=? WHERE cluster_id=? AND query_id=?",entry.phase,cipher.seal("owner-query/"+entry.id,Model.JSON.writeValueAsString(entry)),cluster,entry.id)!=1)throw new TaskService.Conflict("OWNER_QUERY_REQUIRED");
    }
    String submit(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        TaskRequest request=supplied.snapshot();readonly(request);if(Model.JSON.writeValueAsBytes(request).length>512*1024)throw new IllegalArgumentException("OWNER_QUERY_REQUEST_LIMIT");
        if(request.skipRecovery()&&!recoverySkip)throw new TaskService.Conflict("SKIP_DISABLED");
        if(request.failureReview()&&!recoveryReview)throw new TaskService.Conflict("FAILURE_REVIEW_DISABLED");
        return tasks.transaction(request.taskId,actor,(c,task,current)->{
            authorize(request,current);if(request.kind==TaskRequest.Kind.CONFIG_PREVIEW&&!Objects.equals(request.revision,task.revision))throw new TaskService.Conflict("REVISION_CHANGED");AgentDirectory.Binding owner=agents.owner(c,task.owner,task.directory);
            if(!"HEALTHY".equals(owner.state))throw new TaskService.Conflict("OWNER_QUERY_UNAVAILABLE");long now=db.now(c);
            db.update(c,"DELETE FROM "+db.table("owner_query")+" WHERE cluster_id=? AND expires_at<=?",cluster,now);
            try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("owner_query")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)>=maximum)throw new TaskService.Conflict("OWNER_QUERY_CAPACITY");}
            Entry entry=new Entry();entry.id=UUID.randomUUID().toString();entry.task=task.id;entry.owner=task.owner;entry.directory=task.directory;entry.epoch=owner.epoch;entry.incarnation=owner.incarnation;entry.actor=current.id;entry.actorRevision=current.authorizationRevision;entry.request=request;entry.expiresAt=now+lifetime;
            db.update(c,"INSERT INTO "+db.table("owner_query")+" (cluster_id,query_id,task_id,owner_id,node_epoch,phase,expires_at,envelope) VALUES (?,?,?,?,?,?,?,?)",cluster,entry.id,entry.task,entry.owner,entry.epoch,entry.phase,entry.expiresAt,cipher.seal("owner-query/"+entry.id,Model.JSON.writeValueAsString(entry)));return entry.id;
        });
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
            if(e.request.kind==TaskRequest.Kind.CONFIG_PREVIEW&&!Objects.equals(e.request.revision,tasks.record(c,e.task).revision)){e.phase="REJECTED";e.code="REVISION_CHANGED";write(c,e);return null;}
            try{actor(accounts,e);}catch(SecurityException|TaskService.Conflict revoked){e.phase="REJECTED";e.code="AUTHORIZATION_CHANGED";write(c,e);return null;}
            e.phase="RUNNING";write(c,e);return e;
        });
        if(entry==null)return;Map<String,Object> response=null;String failure=null;
        try{
            AccessControl.Principal actor=tasks.service((c,accounts)->{agents.require(c,lease,directory);return actor(accounts,entry);});
            response=local.ownerQuery(entry.request,actor);if(response==null||Model.JSON.writeValueAsBytes(response).length>1024*1024){response=null;failure="OWNER_QUERY_RESULT_LIMIT";}
        }catch(TaskService.Conflict rejected){failure=rejected.getMessage()!=null&&rejected.getMessage().matches("[A-Z0-9_]{1,120}")?rejected.getMessage():"OWNER_QUERY_REJECTED";}
        catch(SecurityException denied){failure="AUTHORIZATION_CHANGED";}
        catch(NoSuchElementException absent){failure="OWNER_DATA_NOT_FOUND";}
        catch(Exception unavailable){failure="OWNER_QUERY_FAILED";}
        final Map<String,Object> body=response;final String code=failure;
        tasks.service((c,accounts)->{
            agents.require(c,lease,directory);Entry current=read(c,id);if(!"RUNNING".equals(current.phase))throw new TaskService.Conflict("OWNER_QUERY_PHASE_CHANGED");
            if(current.expiresAt<=db.now(c))throw new TaskService.Conflict("OWNER_QUERY_EXPIRED");
            if(current.request.kind==TaskRequest.Kind.CONFIG_PREVIEW&&!Objects.equals(current.request.revision,tasks.record(c,current.task).revision)){current.phase="REJECTED";current.code="REVISION_CHANGED";current.response=null;write(c,current);return null;}
            try{actor(accounts,current);}catch(SecurityException|TaskService.Conflict revoked){current.phase="REJECTED";current.code="AUTHORIZATION_CHANGED";current.response=null;write(c,current);return null;}
            current.response=body;current.code=code;current.phase=code==null?"SUCCEEDED":"REJECTED";write(c,current);return null;
        });
    }
    Map<String,Object> poll(String id,AccessControl.Principal supplied)throws Exception{
        String task;try(Connection c=db.connect();PreparedStatement p=db.prepare(c,"SELECT task_id FROM "+db.table("owner_query")+" WHERE cluster_id=? AND query_id=?",cluster,id);ResultSet r=p.executeQuery()){if(!r.next())throw new NoSuchElementException("OWNER_QUERY_NOT_FOUND");task=r.getString(1);}catch(SQLException failure){throw new ControlStore.Unavailable();}
        return tasks.transaction(task,supplied,(c,record,current)->{
            Entry entry=read(c,id);if(!entry.actor.equals(current.id))throw new SecurityException("FORBIDDEN");if(!entry.actorRevision.equals(current.authorizationRevision))throw new TaskService.Conflict("AUTHORIZATION_CHANGED");authorize(entry.request,current);
            if(entry.request.kind==TaskRequest.Kind.CONFIG_PREVIEW&&!Objects.equals(entry.request.revision,record.revision))throw new TaskService.Conflict("REVISION_CHANGED");
            if(entry.expiresAt<=db.now(c))throw new TaskService.Conflict("OWNER_QUERY_EXPIRED");AgentDirectory.Binding owner=agents.owner(c,record.owner,record.directory);
            if(!entry.owner.equals(owner.node)||!entry.directory.equals(owner.directory)||entry.epoch!=owner.epoch||!entry.incarnation.equals(owner.incarnation)||!"HEALTHY".equals(owner.state))throw new TaskService.Conflict("OWNER_QUERY_UNAVAILABLE");
            Map<String,Object> result=new LinkedHashMap<>();result.put("status",entry.phase);result.put("code",entry.code);if("SUCCEEDED".equals(entry.phase))result.put("result",entry.response);return result;
        });
    }
    Map<String,Object> await(String id,AccessControl.Principal actor)throws Exception{
        long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(lifetime);
        while(System.nanoTime()<deadline){Map<String,Object> result=poll(id,actor);if("SUCCEEDED".equals(result.get("status")))return Model.JSON.convertValue(result.get("result"),Map.class);if("REJECTED".equals(result.get("status")))throw new TaskService.Conflict((String)result.get("code"));Thread.sleep(50);}
        throw new TaskService.Conflict("OWNER_QUERY_TIMEOUT");
    }
}
