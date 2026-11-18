package io.github.rivomesh.platform;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/** Conservative logical reservations. Native target ownership remains the physical alias guard. */
final class GlobalResources {
    private static final int BUSINESS=1,METADATA=2,SOURCE=4;
    private final GlobalTaskStore tasks;
    private final SharedAccessStore access;
    private final JdbcControlStore db;
    private final ControlCipher cipher;
    private final String cluster,anchor;
    private final int maximum;
    GlobalResources(ClusterConfig config,GlobalTaskStore tasks,SharedAccessStore access,int maximum)throws Exception{
        if(!access.controls(config)||!tasks.accessIdentity().equals(access.identity)||maximum<1||maximum>2000000)throw new IllegalArgumentException("RESOURCE_POLICY_INVALID");
        this.tasks=tasks;this.access=access;this.maximum=maximum;db=tasks.database();cipher=tasks.commandCipher();cluster=config.id;anchor=UUID.nameUUIDFromBytes(("rivomesh-resource-authority/"+tasks.identity).getBytes(StandardCharsets.UTF_8)).toString();
        try(Connection c=db.connect()){
            db.bootstrapLock(c);String suffix=db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"";
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("resource_root")+" (cluster_id VARCHAR(128) PRIMARY KEY,store_id VARCHAR(36) NOT NULL,maximum_claims INTEGER NOT NULL,claim_count BIGINT NOT NULL,state_hash VARCHAR(43) NOT NULL)"+suffix);
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("task_resource")+" (cluster_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,resource_key VARCHAR(43) NOT NULL,resource_kind INTEGER NOT NULL,record_hash VARCHAR(43) NOT NULL,PRIMARY KEY(cluster_id,task_id,resource_key))"+suffix);
            db.index(c,"task_resource","rmc_resource_conflict","cluster_id, resource_key");
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rmc_resource_root','rmc_task_resource')",config.namespace);ResultSet r=p.executeQuery()){
                int count=0;while(r.next()){count++;if(!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new TaskService.Conflict("CONTROL_STORE_ENGINE_INVALID");if(!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))throw new TaskService.Conflict("CONTROL_STORE_COLLATION_INVALID");}if(count!=2)throw new TaskService.Conflict("CONTROL_STORE_TABLES_MISSING");
            }
            c.setAutoCommit(false);tasks.service(c,(transaction,accounts)->{
                boolean exists;try(PreparedStatement p=db.prepare(c,"SELECT store_id FROM "+db.table("resource_root")+" WHERE cluster_id=? FOR UPDATE",cluster);ResultSet r=p.executeQuery()){exists=r.next();}
                boolean recorded=access.containsReceipt(c,anchor);
                if(!exists){
                    if(recorded||count(c)>0)throw new TaskService.Conflict("RESOURCE_STATE_REQUIRED");db.update(c,"INSERT INTO "+db.table("resource_root")+" (cluster_id,store_id,maximum_claims,claim_count,state_hash) VALUES (?,?,?,0,?)",cluster,tasks.identity,maximum,stateHash(0));
                    AuditStore.Event event=new AuditStore.Event();event.id=anchor;event.kind="GLOBAL_RESOURCE_AUTHORITY_CREATED";event.subjectId=tasks.identity;event.status="COMMITTED";access.append(c,event);
                }else if(!recorded)throw new TaskService.Conflict("RESOURCE_ANCHOR_REQUIRED");
                if(root(c)!=count(c))throw new TaskService.Conflict("RESOURCE_CATALOG_INCOMPLETE");return null;
            });c.commit();
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    private String stateHash(long count){return cipher.signature("resource-root\n"+tasks.identity+"\n"+access.identity+"\n"+maximum+"\n"+count);}
    private long count(Connection c)throws SQLException{try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("task_resource")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();return r.getLong(1);}}
    private long root(Connection c)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT store_id,maximum_claims,claim_count,state_hash FROM "+db.table("resource_root")+" WHERE cluster_id=? FOR UPDATE",cluster);ResultSet r=p.executeQuery()){
            if(!r.next()||!tasks.identity.equals(r.getString(1)))throw new TaskService.Conflict("RESOURCE_STATE_REQUIRED");if(maximum!=r.getInt(2))throw new TaskService.Conflict("RESOURCE_POLICY_MISMATCH");long count=r.getLong(3);if(count<0||count>maximum||!stateHash(count).equals(r.getString(4)))throw new TaskService.Conflict("RESOURCE_ROOT_INVALID");if(!access.containsReceipt(c,anchor))throw new TaskService.Conflict("RESOURCE_ANCHOR_REQUIRED");return count;
        }
    }
    private String rowHash(String task,String key,int kind){return cipher.signature("resource\n"+tasks.identity+"\n"+task+"\n"+key+"\n"+kind);}
    private void add(Map<String,Integer> claims,String purpose,int kind){String key=cipher.signature(purpose);claims.put(key,claims.getOrDefault(key,0)|kind);}
    private Map<String,Integer> plan(TaskDefinition task){
        Map<String,Integer> claims=new TreeMap<>();namespaces(claims,task.spaces(false),BUSINESS);namespaces(claims,task.metadataSpaces(),METADATA);
        // Explicit logical cluster identities also catch failover endpoints that do not overlap.
        if("MYSQL".equals(task.sourceType())){
            if(!"default".equals(task.source.clusterId))add(claims,"mysql-source\ncluster/"+task.source.clusterId+"\n"+task.source.serverId,SOURCE);
            for(io.github.rivomesh.Config.Endpoint endpoint:task.endpoints())add(claims,"mysql-source\nendpoint/"+endpoint.host.toLowerCase(Locale.ROOT)+":"+endpoint.port+"\n"+task.source.serverId,SOURCE);
        }
        if(claims.size()>20000)throw new IllegalArgumentException("RESOURCE_PLAN_LIMIT");return claims;
    }
    private void namespaces(Map<String,Integer> claims,Map<String,Set<String>> destinations,int kind){for(Map.Entry<String,Set<String>> destination:destinations.entrySet())for(String namespace:destination.getValue()){if(namespace==null)throw new IllegalArgumentException("RESOURCE_NAMESPACE_REQUIRED");add(claims,"target\n"+destination.getKey()+"\n"+namespace.toLowerCase(Locale.ROOT),kind);}}
    private static boolean collision(int mine,int other){return (mine&SOURCE)!=0&&(other&SOURCE)!=0||(mine&BUSINESS)!=0&&(other&(BUSINESS|METADATA))!=0||(mine&METADATA)!=0&&(other&BUSINESS)!=0;}
    void reserve(Connection c,GlobalTaskStore.Record task,TaskDefinition definition)throws Exception{
        if(!task.id.equals(definition.jobId))throw new TaskService.Conflict("COMMAND_TASK_ID_CHANGED");Map<String,Integer> wanted=plan(definition);long count=root(c);if(count!=count(c))throw new TaskService.Conflict("RESOURCE_CATALOG_INCOMPLETE");Map<String,Integer> existing=new HashMap<>();List<String> keys=new ArrayList<>(wanted.keySet());
        for(int from=0;from<keys.size();from+=200){
            List<String> page=keys.subList(from,Math.min(keys.size(),from+200));List<Object> parameters=new ArrayList<>();parameters.add(cluster);parameters.addAll(page);
            try(PreparedStatement p=db.prepare(c,"SELECT task_id,resource_key,resource_kind,record_hash FROM "+db.table("task_resource")+" WHERE cluster_id=? AND resource_key IN ("+String.join(",",Collections.nCopies(page.size(),"?"))+")",parameters.toArray());ResultSet r=p.executeQuery()){
                while(r.next()){
                    String owner=r.getString(1),key=r.getString(2);int kind=r.getInt(3);if(kind<1||kind>7||!rowHash(owner,key,kind).equals(r.getString(4)))throw new TaskService.Conflict("RESOURCE_RECORD_INVALID");
                    if(owner.equals(task.id))existing.put(key,kind);else if(collision(wanted.get(key),kind))throw new TaskService.Conflict((wanted.get(key)&SOURCE)!=0?"REPLICATION_SERVER_ID_COLLISION":"DESTINATION_OVERLAP");
                }
            }
        }
        long additions=0;for(String key:keys)if(!existing.containsKey(key))additions++;if(count+additions>maximum)throw new TaskService.Conflict("RESOURCE_CAPACITY");
        for(String key:keys){int previous=existing.getOrDefault(key,0),kind=previous|wanted.get(key);if(previous==0)db.update(c,"INSERT INTO "+db.table("task_resource")+" (cluster_id,task_id,resource_key,resource_kind,record_hash) VALUES (?,?,?,?,?)",cluster,task.id,key,kind,rowHash(task.id,key,kind));else if(kind!=previous)db.update(c,"UPDATE "+db.table("task_resource")+" SET resource_kind=?,record_hash=? WHERE cluster_id=? AND task_id=? AND resource_key=?",kind,rowHash(task.id,key,kind),cluster,task.id,key);}
        if(additions>0)db.update(c,"UPDATE "+db.table("resource_root")+" SET claim_count=?,state_hash=? WHERE cluster_id=?",count+additions,stateHash(count+additions),cluster);
    }
}
