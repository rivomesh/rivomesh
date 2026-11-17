package io.github.rivomesh.platform;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.Model;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Row-lock/epoch based registry, isolated from per-pipeline data journals. */
final class JdbcControlStore implements ControlStore {
    private final ClusterConfig config;
    private final boolean mysql,h2;
    JdbcControlStore(ClusterConfig config)throws Exception{
        config.validate();this.config=config;mysql=config.driver.equals("com.mysql.cj.jdbc.Driver");h2=config.driver.equals("org.h2.Driver");
        initialize();
    }
    private String q(String name){return (mysql?"`":"\"")+name+(mysql?"`":"\"");}
    String table(String name){return q(config.namespace)+"."+q("rmc_"+name);}
    Connection connect()throws SQLException{
        String url=config.url;
        if(!h2){String separator=url.contains("?")?"&":"?";url+=separator+(mysql?"connectTimeout=5000&socketTimeout=5000":"connectTimeout=5&socketTimeout=5");}
        Connection connection=JdbcConnections.open(config.driver,url,config.username,config.password);
        try{if(mysql)execute(connection,"SET SESSION innodb_lock_wait_timeout=5");else if(!h2)execute(connection,"SET lock_timeout='5s'");return connection;}
        catch(SQLException failure){connection.close();throw failure;}
    }
    static void execute(Connection c,String sql)throws SQLException{try(Statement s=c.createStatement()){s.setQueryTimeout(5);s.execute(sql);}}
    PreparedStatement prepare(Connection c,String sql,Object...args)throws SQLException{
        PreparedStatement p=c.prepareStatement(sql);p.setQueryTimeout(5);for(int i=0;i<args.length;i++)p.setObject(i+1,args[i]);return p;
    }
    int update(Connection c,String sql,Object...args)throws SQLException{try(PreparedStatement p=prepare(c,sql,args)){return p.executeUpdate();}}
    long now(Connection c)throws SQLException{
        String sql=mysql?"SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)":h2?"SELECT CURRENT_TIMESTAMP":"SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT)";
        try(PreparedStatement p=prepare(c,sql);ResultSet rs=p.executeQuery()){rs.next();return h2?rs.getTimestamp(1).getTime():rs.getLong(1);}
    }
    void bootstrapLock(Connection c)throws SQLException{
        if(!h2){
            try(PreparedStatement p=prepare(c,mysql?"SELECT GET_LOCK(?,5)":"SELECT pg_advisory_lock(?)",mysql?"rmc-"+AccountDirectory.hash(config.namespace).substring(0,32):(Object)(long)config.namespace.hashCode());ResultSet rs=p.executeQuery()){
                if(mysql&&(!rs.next()||rs.getInt(1)!=1))throw new Unavailable();
            }
        }
    }
    boolean mysql(){return mysql;}
    void index(Connection c,String table,String name,String columns)throws SQLException{
        if(!table.matches("[a-z_]+")||!name.matches("rmc_[a-z_]+")||!columns.matches("[a-z_, ]+"))throw new IllegalArgumentException("CONTROL_INDEX_INVALID");
        if(mysql){try(PreparedStatement p=prepare(c,"SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME=?",config.namespace,"rmc_"+table,name);ResultSet r=p.executeQuery()){r.next();if(r.getInt(1)>0)return;}}
        execute(c,"CREATE INDEX "+(mysql?"":"IF NOT EXISTS ")+q(name)+" ON "+table(table)+" ("+columns+")");
    }
    private void initialize()throws Exception{
        try(Connection c=connect()){
            bootstrapLock(c);
            execute(c,(mysql?"CREATE DATABASE IF NOT EXISTS ":"CREATE SCHEMA IF NOT EXISTS ")+q(config.namespace));
            String suffix=mysql?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"";
            execute(c,"CREATE TABLE IF NOT EXISTS "+table("cluster")+" (cluster_id VARCHAR(128) PRIMARY KEY,format_version INTEGER NOT NULL,lease_millis BIGINT NOT NULL,maximum_nodes INTEGER NOT NULL)"+suffix);
            execute(c,"CREATE TABLE IF NOT EXISTS "+table("node")+" (cluster_id VARCHAR(128) NOT NULL,node_id VARCHAR(64) NOT NULL,incarnation VARCHAR(36) NOT NULL,epoch BIGINT NOT NULL,seen_at BIGINT NOT NULL,lease_until BIGINT NOT NULL,lifecycle VARCHAR(16) NOT NULL,metadata TEXT NOT NULL,PRIMARY KEY(cluster_id,node_id))"+suffix);
            execute(c,"CREATE TABLE IF NOT EXISTS "+table("node_task")+" (cluster_id VARCHAR(128) NOT NULL,node_id VARCHAR(64) NOT NULL,node_epoch BIGINT NOT NULL,task_id VARCHAR(128) NOT NULL,observation TEXT NOT NULL,PRIMARY KEY(cluster_id,node_id,task_id))"+suffix);
            if(mysql)try(PreparedStatement p=prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rmc_cluster','rmc_node','rmc_node_task')",config.namespace);ResultSet rs=p.executeQuery()){
                int count=0;while(rs.next()){count++;if(!"InnoDB".equalsIgnoreCase(rs.getString(1)))throw new TaskService.Conflict("CONTROL_STORE_ENGINE_INVALID");if(!"utf8mb4_bin".equalsIgnoreCase(rs.getString(2)))throw new TaskService.Conflict("CONTROL_STORE_COLLATION_INVALID");}if(count!=3)throw new TaskService.Conflict("CONTROL_STORE_TABLES_MISSING");
            }
            try{update(c,"INSERT INTO "+table("cluster")+" (cluster_id,format_version,lease_millis,maximum_nodes) VALUES (?,1,?,?)",config.id,config.leaseMillis,config.maximumNodes);}
            catch(SQLException duplicate){if(duplicate.getSQLState()==null||!duplicate.getSQLState().startsWith("23"))throw duplicate;}
            c.setAutoCommit(false);cluster(c);c.commit();
        }catch(SQLException failure){throw new Unavailable();}
    }
    private void cluster(Connection c)throws SQLException{
        try(PreparedStatement p=prepare(c,"SELECT format_version,lease_millis,maximum_nodes FROM "+table("cluster")+" WHERE cluster_id=? FOR UPDATE",config.id);ResultSet rs=p.executeQuery()){
            if(!rs.next()||rs.getLong(2)!=config.leaseMillis||rs.getInt(3)!=config.maximumNodes)throw new TaskService.Conflict("CONTROL_STORE_PROTOCOL_MISMATCH");
            int format=rs.getInt(1);
            int required=config.recoverySkip?7:config.recoveryCommit?6:config.recoveryReview?5:config.separateExecution?4:config.sharedTasks?3:config.sharedAccess?2:1;
            if(config.sharedAccess&&format<required){
                try(PreparedStatement members=prepare(c,"SELECT COUNT(*) FROM "+table("node")+" WHERE cluster_id=? AND lifecycle<>'STOPPED'",config.id);ResultSet nodes=members.executeQuery()){
                    nodes.next();if(nodes.getLong(1)>0)throw new TaskService.Conflict(config.sharedTasks?"GLOBAL_TASK_QUIESCENCE_REQUIRED":"SHARED_ACCESS_QUIESCENCE_REQUIRED");
                }
                update(c,"UPDATE "+table("cluster")+" SET format_version=? WHERE cluster_id=? AND format_version=?",required,config.id,format);format=required;
            }
            if(format!=required)throw new TaskService.Conflict("CONTROL_STORE_PROTOCOL_MISMATCH");
        }
    }
    private void validate(Node node,List<Task> tasks){
        if(node==null||!Objects.equals(node.id,config.nodeId)||node.incarnation==null||!node.incarnation.matches("[a-f0-9-]{36}")||tasks==null||tasks.size()>200)throw new IllegalArgumentException("NODE_OBSERVATION_INVALID");
        if(!Objects.equals(node.role,config.separateExecution?config.roleName():null)||!config.executes()&&(!tasks.isEmpty()||node.taskCapacity!=0||node.runningCapacity!=0))throw new IllegalArgumentException("NODE_ROLE_INVALID");
        Set<String> ids=new HashSet<>();for(Task task:tasks)if(task==null||task.id==null||!task.id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||!ids.add(task.id))throw new IllegalArgumentException("NODE_OBSERVATION_INVALID");
    }
    private void publish(Connection c,Lease lease,Node node,List<Task> tasks,long now)throws Exception{
        update(c,"UPDATE "+table("node")+" SET seen_at=?,lease_until=?,lifecycle='RUNNING',metadata=? WHERE cluster_id=? AND node_id=? AND incarnation=? AND epoch=?",now,now+config.leaseMillis,Model.JSON.writeValueAsString(node),config.id,lease.nodeId,lease.incarnation,lease.epoch);
        update(c,"DELETE FROM "+table("node_task")+" WHERE cluster_id=? AND node_id=?",config.id,lease.nodeId);
        try(PreparedStatement p=prepare(c,"INSERT INTO "+table("node_task")+" (cluster_id,node_id,node_epoch,task_id,observation) VALUES (?,?,?,?,?)")){
            for(Task task:tasks){p.setString(1,config.id);p.setString(2,lease.nodeId);p.setLong(3,lease.epoch);p.setString(4,task.id);p.setString(5,Model.JSON.writeValueAsString(task));p.addBatch();}if(!tasks.isEmpty())p.executeBatch();
        }
    }
    public Lease register(Node node,List<Task> tasks)throws Exception{
        validate(node,tasks);
        try(Connection c=connect()){
            c.setAutoCommit(false);cluster(c);long now=now(c),epoch=1;boolean existing=false;
            try(PreparedStatement p=prepare(c,"SELECT incarnation,epoch,lease_until,lifecycle FROM "+table("node")+" WHERE cluster_id=? AND node_id=? FOR UPDATE",config.id,node.id);ResultSet rs=p.executeQuery()){
                if(rs.next()){
                    existing=true;now=now(c);boolean live=rs.getLong(3)>now&&"RUNNING".equals(rs.getString(4));
                    if(live&&!node.incarnation.equals(rs.getString(1)))throw new TaskService.Conflict("NODE_ID_IN_USE");
                    epoch=rs.getLong(2)+(live?0:1);
                }
            }
            if(config.separateExecution&&!config.executes())try(PreparedStatement p=prepare(c,"SELECT COUNT(*) FROM "+table("task_authority")+" WHERE cluster_id=? AND owner_id=?",config.id,node.id);ResultSet tasksOwned=p.executeQuery()){
                tasksOwned.next();if(tasksOwned.getLong(1)>0)throw new TaskService.Conflict("CONTROLLER_OWNS_TASKS");
            }
            if(!existing){
                try(PreparedStatement p=prepare(c,"SELECT COUNT(*) FROM "+table("node")+" WHERE cluster_id=?",config.id);ResultSet rs=p.executeQuery()){rs.next();if(rs.getInt(1)>=config.maximumNodes)throw new TaskService.Conflict("NODE_CAPACITY_REACHED");}
                update(c,"INSERT INTO "+table("node")+" (cluster_id,node_id,incarnation,epoch,seen_at,lease_until,lifecycle,metadata) VALUES (?,?,?,?,?,?,'RUNNING',?)",config.id,node.id,node.incarnation,epoch,now,now+config.leaseMillis,Model.JSON.writeValueAsString(node));
            }else update(c,"UPDATE "+table("node")+" SET incarnation=?,epoch=? WHERE cluster_id=? AND node_id=?",node.incarnation,epoch,config.id,node.id);
            Lease lease=new Lease(node.id,node.incarnation,epoch);publish(c,lease,node,tasks,now(c));c.commit();return lease;
        }catch(SQLException failure){throw new Unavailable();}
    }
    private void fence(Connection c,Lease lease,long now,boolean requireLive)throws SQLException{
        try(PreparedStatement p=prepare(c,"SELECT incarnation,epoch,lease_until,lifecycle FROM "+table("node")+" WHERE cluster_id=? AND node_id=? FOR UPDATE",config.id,lease.nodeId);ResultSet rs=p.executeQuery()){
            if(!rs.next()||!lease.incarnation.equals(rs.getString(1))||lease.epoch!=rs.getLong(2)||!"RUNNING".equals(rs.getString(4)))throw new Fenced();
            if(requireLive&&rs.getLong(3)<=now(c))throw new Expired();
        }
    }
    void requireLiveOwnLease(Connection c,Lease lease)throws SQLException{
        if(lease==null||!config.nodeId.equals(lease.nodeId))throw new Fenced();
        fence(c,lease,now(c),true);
    }
    void requireExecutionRole(Connection c,String node)throws Exception{
        try(PreparedStatement p=prepare(c,"SELECT metadata FROM "+table("node")+" WHERE cluster_id=? AND node_id=? FOR UPDATE",config.id,node);ResultSet r=p.executeQuery()){
            if(!r.next()||!Model.JSON.readValue(r.getString(1),Node.class).executes())throw new TaskService.Conflict("OWNER_ROLE_REQUIRED");
        }
    }
    public void heartbeat(Lease lease,Node node,List<Task> tasks)throws Exception{
        validate(node,tasks);if(!node.id.equals(lease.nodeId)||!node.incarnation.equals(lease.incarnation))throw new Fenced();
        try(Connection c=connect()){c.setAutoCommit(false);fence(c,lease,now(c),true);publish(c,lease,node,tasks,now(c));c.commit();}catch(SQLException failure){throw new Unavailable();}
    }
    public void stopped(Lease lease)throws Exception{
        try(Connection c=connect()){c.setAutoCommit(false);long now=now(c);fence(c,lease,now,false);update(c,"UPDATE "+table("node")+" SET lifecycle='STOPPED',seen_at=?,lease_until=? WHERE cluster_id=? AND node_id=?",now,now,config.id,lease.nodeId);c.commit();}catch(SQLException failure){throw new Unavailable();}
    }
    public Map<String,Object> page(String after,int limit)throws Exception{
        if(after==null||!after.isEmpty()&&!after.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")||limit<1||limit>50)throw new IllegalArgumentException("NODE_CURSOR_INVALID");
        try(Connection c=connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);long now=now(c);List<Map<String,Object>> nodes=new ArrayList<>();
            try(PreparedStatement p=prepare(c,"SELECT node_id,epoch,seen_at,lease_until,lifecycle,metadata FROM "+table("node")+" WHERE cluster_id=? AND node_id>? ORDER BY node_id LIMIT ?",config.id,after,limit+1);ResultSet rs=p.executeQuery()){
                while(rs.next()){
                    Node node=Model.JSON.readValue(rs.getString(6),Node.class);Map<String,Object> row=new LinkedHashMap<>();row.put("id",rs.getString(1));row.put("epoch",rs.getLong(2));row.put("lastHeartbeat",Instant.ofEpochMilli(rs.getLong(3)).toString());row.put("leaseExpiresAt",Instant.ofEpochMilli(rs.getLong(4)).toString());
                    row.put("state","STOPPED".equals(rs.getString(5))?"STOPPED":rs.getLong(4)<=now?"STALE":"HEALTHY");row.put("version",node.version);row.put("artifactSha256",node.artifactSha256);row.put("jvm",node.jvm);row.put("architecture",node.architecture);row.put("processors",node.processors);row.put("heapBytes",node.heapBytes);row.put("taskCapacity",node.taskCapacity);row.put("runningCapacity",node.runningCapacity);row.put("role",node.roleName());row.put("advertiseUrl",node.advertiseUrl);nodes.add(row);
                }
            }
            boolean more=nodes.size()>limit;if(more)nodes.remove(nodes.size()-1);
            for(Map<String,Object> node:nodes){List<Task> tasks=new ArrayList<>();try(PreparedStatement p=prepare(c,"SELECT observation FROM "+table("node_task")+" WHERE cluster_id=? AND node_id=? AND node_epoch=? ORDER BY task_id LIMIT 201",config.id,node.get("id"),node.get("epoch"));ResultSet rs=p.executeQuery()){while(rs.next())tasks.add(Model.JSON.readValue(rs.getString(1),Task.class));}if(tasks.size()>200)throw new IllegalStateException("NODE_OBSERVATION_INVALID");node.put("tasks",tasks);}
            Map<String,Object> result=new LinkedHashMap<>();result.put("clusterId",config.id);result.put("mode","SHARED_REGISTRY");result.put("observedAt",Instant.ofEpochMilli(now).toString());result.put("nodes",nodes);result.put("next",more?nodes.get(nodes.size()-1).get("id"):null);result.put("taskControlScope","LOCAL");c.commit();return result;
        }catch(SQLException failure){throw new Unavailable();}
    }
    public void close(){}
}
