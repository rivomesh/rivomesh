package io.github.rivomesh.platform;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/** Private execution admission: a registry lease alone never authorizes a new data directory. */
final class AgentDirectory {
    static final class Binding {
        String node,directory,incarnation,state;
        long epoch,boundAt;
    }
    private record Capacity(String node,int maximum,long assigned) {
        double utilization(){return (double)assigned/maximum;}
    }
    record HandoffCapacity(Binding binding,int taskSlots,int workerSlots,long assignedTasks){}
    private final GlobalTaskStore tasks;
    private final SharedAccessStore access;
    private final JdbcControlStore db;
    private final ControlCipher cipher;
    private final String cluster,anchor;
    private final int maximum;

    AgentDirectory(ClusterConfig config,SharedAccessStore access,GlobalTaskStore tasks)throws Exception{
        if(!access.controls(config)||!tasks.accessIdentity().equals(access.identity))throw new IllegalArgumentException("AGENT_POLICY_INVALID");
        this.tasks=tasks;this.access=access;cluster=config.id;maximum=config.maximumNodes;db=new JdbcControlStore(config);cipher=tasks.commandCipher();
        anchor=UUID.nameUUIDFromBytes(("rivomesh-agent-authority/"+tasks.identity).getBytes(StandardCharsets.UTF_8)).toString();
        try(Connection c=db.connect()){
            db.bootstrapLock(c);String suffix=db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"";
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("agent_root")+" (cluster_id VARCHAR(128) PRIMARY KEY,store_id VARCHAR(36) NOT NULL,maximum_nodes INTEGER NOT NULL,registered_count BIGINT NOT NULL,state_hash VARCHAR(43) NOT NULL)"+suffix);
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("agent")+" (cluster_id VARCHAR(128) NOT NULL,node_id VARCHAR(64) NOT NULL,directory_id VARCHAR(36) NOT NULL,incarnation VARCHAR(36) NOT NULL,node_epoch BIGINT NOT NULL,bound_at BIGINT NOT NULL,record_hash VARCHAR(43) NOT NULL,PRIMARY KEY(cluster_id,node_id))"+suffix);
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rmc_agent_root','rmc_agent')",config.namespace);ResultSet r=p.executeQuery()){
                int count=0;while(r.next()){count++;if(!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new TaskService.Conflict("CONTROL_STORE_ENGINE_INVALID");if(!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))throw new TaskService.Conflict("CONTROL_STORE_COLLATION_INVALID");}if(count!=2)throw new TaskService.Conflict("CONTROL_STORE_TABLES_MISSING");
            }
            c.setAutoCommit(false);tasks.service(c,(transaction,directory)->{
                boolean exists;try(PreparedStatement p=db.prepare(c,"SELECT store_id FROM "+db.table("agent_root")+" WHERE cluster_id=? FOR UPDATE",cluster);ResultSet r=p.executeQuery()){exists=r.next();}
                boolean recorded=access.containsReceipt(c,anchor);
                if(!exists){
                    if(recorded||count(c)>0)throw new TaskService.Conflict("AGENT_STATE_REQUIRED");
                    db.update(c,"INSERT INTO "+db.table("agent_root")+" (cluster_id,store_id,maximum_nodes,registered_count,state_hash) VALUES (?,?,?,0,?)",cluster,tasks.identity,maximum,stateHash(0));
                    AuditStore.Event event=new AuditStore.Event();event.id=anchor;event.kind="GLOBAL_AGENT_AUTHORITY_CREATED";event.subjectId=tasks.identity;event.status="COMMITTED";access.append(c,event);
                }else if(!recorded)throw new TaskService.Conflict("AGENT_ANCHOR_REQUIRED");
                if(root(c)!=count(c))throw new TaskService.Conflict("AGENT_CATALOG_INCOMPLETE");return null;
            });c.commit();
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    private long count(Connection c)throws SQLException{
        try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("agent")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();return r.getLong(1);}
    }
    private String stateHash(long count){return cipher.signature("agent-root\n"+tasks.identity+"\n"+access.identity+"\n"+maximum+"\n"+count);}
    private long root(Connection c)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT store_id,maximum_nodes,registered_count,state_hash FROM "+db.table("agent_root")+" WHERE cluster_id=? FOR UPDATE",cluster);ResultSet r=p.executeQuery()){
            if(!r.next()||!tasks.identity.equals(r.getString(1)))throw new TaskService.Conflict("AGENT_STATE_REQUIRED");
            if(maximum!=r.getInt(2))throw new TaskService.Conflict("AGENT_POLICY_MISMATCH");long count=r.getLong(3);
            if(count<0||count>maximum||!stateHash(count).equals(r.getString(4)))throw new TaskService.Conflict("AGENT_ROOT_INVALID");
            if(!access.containsReceipt(c,anchor))throw new TaskService.Conflict("AGENT_ANCHOR_REQUIRED");return count;
        }
    }
    private String signature(Binding b){return cipher.signature("agent\n"+tasks.identity+"\n"+b.node+"\n"+b.directory+"\n"+b.incarnation+"\n"+b.epoch+"\n"+b.boundAt);}
    private Binding read(Connection c,String node)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("agent")+" WHERE cluster_id=? AND node_id=?",cluster,node);ResultSet r=p.executeQuery()){
            if(!r.next())return null;Binding b=new Binding();b.node=node;b.directory=r.getString("directory_id");b.incarnation=r.getString("incarnation");b.epoch=r.getLong("node_epoch");b.boundAt=r.getLong("bound_at");
            if(!signature(b).equals(r.getString("record_hash")))throw new TaskService.Conflict("AGENT_RECORD_INVALID");return b;
        }
    }
    private static void directory(String value){
        try{if(value==null||!UUID.fromString(value).toString().equals(value))throw new IllegalArgumentException();}
        catch(IllegalArgumentException invalid){throw new IllegalArgumentException("AGENT_DIRECTORY_INVALID");}
    }
    Binding register(ControlStore.Lease lease,String directory)throws Exception{
        directory(directory);
        return tasks.service((c,accountDirectory)->{
            // Lock order is IAM -> task authority -> node lease -> agent root.
            db.requireLiveOwnLease(c,lease);db.requireExecutionRole(c,lease.nodeId);long count=root(c);if(count!=count(c))throw new TaskService.Conflict("AGENT_CATALOG_INCOMPLETE");Binding old=read(c,lease.nodeId);
            if(old!=null){
                if(!directory.equals(old.directory))throw new TaskService.Conflict("AGENT_DIRECTORY_CHANGED");
                if(lease.epoch<old.epoch||lease.epoch==old.epoch&&!lease.incarnation.equals(old.incarnation))throw new ControlStore.Fenced();
                if(lease.epoch==old.epoch){old.state="HEALTHY";return old;}
            }else if(count>=maximum)throw new TaskService.Conflict("AGENT_CAPACITY_REACHED");
            Binding b=new Binding();b.node=lease.nodeId;b.directory=directory;b.incarnation=lease.incarnation;b.epoch=lease.epoch;b.boundAt=db.now(c);b.state="HEALTHY";
            if(old==null){
                db.update(c,"INSERT INTO "+db.table("agent")+" (cluster_id,node_id,directory_id,incarnation,node_epoch,bound_at,record_hash) VALUES (?,?,?,?,?,?,?)",cluster,b.node,b.directory,b.incarnation,b.epoch,b.boundAt,signature(b));
                db.update(c,"UPDATE "+db.table("agent_root")+" SET registered_count=?,state_hash=? WHERE cluster_id=?",count+1,stateHash(count+1),cluster);
            }else db.update(c,"UPDATE "+db.table("agent")+" SET incarnation=?,node_epoch=?,bound_at=?,record_hash=? WHERE cluster_id=? AND node_id=?",b.incarnation,b.epoch,b.boundAt,signature(b),cluster,b.node);
            AuditStore.Event event=new AuditStore.Event();event.kind="GLOBAL_AGENT_BOUND";event.subjectId=b.node;event.operationHash=AuditStore.hash(b.directory);event.status="COMMITTED";access.append(c,event);return b;
        });
    }
    Binding owner(String node,String directory)throws Exception{
        directory(directory);return tasks.service((c,accountDirectory)->owner(c,node,directory));
    }
    Binding owner(String node)throws Exception{return tasks.service((c,accountDirectory)->owner(c,node,null));}
    /** The caller holds the task authority lock through selection and creation. */
    Binding allocate(Connection c,AccessControl.Principal actor)throws Exception{
        List<Capacity> candidates=new ArrayList<>();
        String sql="SELECT n.node_id,n.metadata,(SELECT COUNT(*) FROM "+db.table("task_authority")
                +" t WHERE t.cluster_id=n.cluster_id AND t.owner_id=n.node_id) AS assigned FROM "+db.table("node")
                +" n JOIN "+db.table("agent")+" a ON a.cluster_id=n.cluster_id AND a.node_id=n.node_id"
                +" AND a.node_epoch=n.epoch AND a.incarnation=n.incarnation"
                +" WHERE n.cluster_id=? AND n.lifecycle='RUNNING' AND n.lease_until>?";
        try(PreparedStatement p=db.prepare(c,sql,cluster,db.now(c));ResultSet rows=p.executeQuery()){
            while(rows.next()){
                String node=rows.getString(1);
                if(!actor.node(node))continue;
                ControlStore.Node metadata=io.github.rivomesh.Model.JSON.readValue(rows.getString(2),ControlStore.Node.class);
                long assigned=rows.getLong(3);
                if(node.equals(metadata.id)&&metadata.executes()&&metadata.taskCapacity>assigned&&metadata.runningCapacity>0)
                    candidates.add(new Capacity(node,metadata.taskCapacity,assigned));
            }
        }
        candidates.sort(Comparator.comparingDouble(Capacity::utilization).thenComparing(Capacity::node));
        for(Capacity capacity:candidates){
            Binding ready=owner(c,capacity.node,null);
            if("HEALTHY".equals(ready.state))return ready;
        }
        throw new TaskService.Conflict("AGENT_PLACEMENT_UNAVAILABLE");
    }
    Binding owner(Connection c,String node,String directory)throws Exception{
        if(node==null||!node.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}"))throw new IllegalArgumentException("AGENT_NODE_INVALID");
        String incarnation=null,lifecycle=null;long epoch=0,until=0;
        try(PreparedStatement p=db.prepare(c,"SELECT incarnation,epoch,lease_until,lifecycle,metadata FROM "+db.table("node")+" WHERE cluster_id=? AND node_id=? FOR UPDATE",cluster,node);ResultSet r=p.executeQuery()){
            if(r.next()){incarnation=r.getString(1);epoch=r.getLong(2);until=r.getLong(3);lifecycle=r.getString(4);if(!io.github.rivomesh.Model.JSON.readValue(r.getString(5),ControlStore.Node.class).executes())throw new TaskService.Conflict("OWNER_ROLE_REQUIRED");}
        }
        root(c);Binding b=read(c,node);if(b==null)throw new TaskService.Conflict("AGENT_NOT_REGISTERED");
        if(directory!=null&&!Objects.equals(directory,b.directory))throw new TaskService.Conflict("AGENT_DIRECTORY_CHANGED");
        b.state=incarnation==null?"UNKNOWN":!"RUNNING".equals(lifecycle)?"STOPPED":until<=db.now(c)?"STALE":epoch!=b.epoch||!incarnation.equals(b.incarnation)?"REGISTERING":"HEALTHY";return b;
    }
    HandoffCapacity handoffCapacity(Connection c,String node)throws Exception{
        Binding binding=owner(c,node,null);
        String sql="SELECT n.metadata,(SELECT COUNT(*) FROM "+db.table("task_authority")
                +" t WHERE t.cluster_id=n.cluster_id AND t.owner_id=n.node_id) FROM "+db.table("node")
                +" n WHERE n.cluster_id=? AND n.node_id=?";
        try(PreparedStatement p=db.prepare(c,sql,cluster,node);ResultSet r=p.executeQuery()){
            if(!r.next())return new HandoffCapacity(binding,0,0,0);
            ControlStore.Node declared=io.github.rivomesh.Model.JSON.readValue(r.getString(1),ControlStore.Node.class);
            if(!node.equals(declared.id)||!declared.executes())throw new TaskService.Conflict("AGENT_RECORD_INVALID");
            return new HandoffCapacity(binding,declared.taskCapacity,declared.runningCapacity,r.getLong(2));
        }
    }
    void require(Connection c,ControlStore.Lease lease,String directory)throws Exception{
        directory(directory);db.requireLiveOwnLease(c,lease);db.requireExecutionRole(c,lease.nodeId);root(c);Binding b=read(c,lease.nodeId);
        if(b==null)throw new TaskService.Conflict("AGENT_NOT_REGISTERED");
        if(!directory.equals(b.directory))throw new TaskService.Conflict("AGENT_DIRECTORY_CHANGED");
        if(lease.epoch!=b.epoch||!lease.incarnation.equals(b.incarnation))throw new ControlStore.Fenced();
    }
}
