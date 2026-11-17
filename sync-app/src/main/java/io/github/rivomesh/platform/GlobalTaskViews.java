package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Encrypted owner observations; loss/staleness never creates execution authority. */
final class GlobalTaskViews {
    static final class Observation {
        public int version=1;
        public String task,owner,directory,incarnation;
        public long epoch,observedAt;
        public Map<String,Object> view;
    }
    private final GlobalTaskStore tasks;
    private final JdbcControlStore db;
    private final ControlCipher cipher;
    private final AgentDirectory agents;
    private final String cluster;
    private final long freshness;
    GlobalTaskViews(ClusterConfig config,GlobalTaskStore tasks,AgentDirectory agents,long freshness)throws Exception{
        if(freshness<1000||freshness>300000)throw new IllegalArgumentException("TASK_VIEW_POLICY_INVALID");
        this.tasks=tasks;this.agents=agents;this.freshness=freshness;db=tasks.database();cipher=tasks.commandCipher();cluster=config.id;
        try(Connection c=db.connect()){
            db.bootstrapLock(c);JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("task_observation")+" (cluster_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,envelope "+(db.mysql()?"LONGTEXT":"TEXT")+" NOT NULL,PRIMARY KEY(cluster_id,task_id))"+(db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":""));
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME='rmc_task_observation'",config.namespace);ResultSet r=p.executeQuery()){
                if(!r.next()||!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new TaskService.Conflict("CONTROL_STORE_ENGINE_INVALID");if(!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))throw new TaskService.Conflict("CONTROL_STORE_COLLATION_INVALID");
            }
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    void publish(ControlStore.Lease lease,String directory,List<Map<String,Object>> views)throws Exception{
        if(views==null||views.size()>20)throw new IllegalArgumentException("TASK_VIEW_BATCH_INVALID");
        tasks.service((c,accounts)->{
            agents.require(c,lease,directory);Set<String> ids=new HashSet<>();
            for(Map<String,Object> view:views){
                Object id=view.get("id");if(!(id instanceof String)||!ids.add((String)id)||Model.JSON.writeValueAsBytes(view).length>1024*1024)throw new IllegalArgumentException("TASK_VIEW_INVALID");GlobalTaskStore.Record task=tasks.record(c,(String)id);
                if(!lease.nodeId.equals(task.owner)||!directory.equals(task.directory))throw new ControlStore.Fenced();
                Observation observation=new Observation();observation.task=task.id;observation.owner=task.owner;observation.directory=directory;observation.incarnation=lease.incarnation;observation.epoch=lease.epoch;observation.observedAt=db.now(c);observation.view=view;
                String envelope=cipher.seal("task-view/"+task.id,Model.JSON.writeValueAsString(observation));
                if(db.update(c,"UPDATE "+db.table("task_observation")+" SET envelope=? WHERE cluster_id=? AND task_id=?",envelope,cluster,task.id)==0)db.update(c,"INSERT INTO "+db.table("task_observation")+" (cluster_id,task_id,envelope) VALUES (?,?,?)",cluster,task.id,envelope);
            }agents.require(c,lease,directory);return null;
        });
    }
    private Map<String,Object> unavailable(GlobalTaskStore.Record task,String state){
        Map<String,Object> view=new LinkedHashMap<>();view.put("id",task.id);view.put("ownerNodeId",task.owner);view.put("revision",task.revision);view.put("configurationVersion",task.configurationVersion);view.put("workerState","UNKNOWN");view.put("desired","UNKNOWN");view.put("status",Collections.emptyMap());view.put("operations",Collections.emptyList());view.put("destinations",Collections.emptyList());view.put("statusAvailable",false);view.put("ownerState",state);view.put("sourceDatabase","—");view.put("sourceCluster","");view.put("targetSchema","—");return view;
    }
    Map<String,Object> read(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor)throws Exception{
        Observation observation;
        try(PreparedStatement p=db.prepare(c,"SELECT envelope FROM "+db.table("task_observation")+" WHERE cluster_id=? AND task_id=?",cluster,task.id);ResultSet r=p.executeQuery()){
            if(!r.next())return unavailable(task,"UNOBSERVED");observation=Model.JSON.readValue(cipher.open("task-view/"+task.id,r.getString(1)),Observation.class);
        }
        if(observation.version!=1||!task.id.equals(observation.task)||!task.owner.equals(observation.owner)||!task.directory.equals(observation.directory)||observation.view==null||!task.id.equals(observation.view.get("id")))throw new TaskService.Conflict("TASK_VIEW_INVALID");
        String state="UNKNOWN";long now=db.now(c);
        try(PreparedStatement p=db.prepare(c,"SELECT incarnation,epoch,lease_until,lifecycle FROM "+db.table("node")+" WHERE cluster_id=? AND node_id=?",cluster,task.owner);ResultSet r=p.executeQuery()){
            if(r.next())state=!"RUNNING".equals(r.getString(4))?"STOPPED":r.getLong(3)<=now?"STALE":r.getLong(2)!=observation.epoch||!observation.incarnation.equals(r.getString(1))?"REGISTERING":"HEALTHY";
        }
        if(now<observation.observedAt||now-observation.observedAt>freshness)state="OBSERVATION_STALE";
        if(!"HEALTHY".equals(state)){Map<String,Object> view=unavailable(task,state);view.put("ownerObservedAt",Instant.ofEpochMilli(observation.observedAt).toString());view.put("observedAt",observation.view.get("observedAt"));return view;}
        Map<String,Object> result=new LinkedHashMap<>(observation.view);result.put("ownerRevision",result.get("revision"));result.put("ownerConfigurationVersion",result.get("configurationVersion"));result.put("id",task.id);result.put("revision",task.revision);result.put("configurationVersion",task.configurationVersion);result.put("ownerNodeId",task.owner);result.put("ownerState",state);result.put("ownerObservedAt",Instant.ofEpochMilli(observation.observedAt).toString());return result;
    }
    Map<String,Object> get(String id,AccessControl.Principal actor)throws Exception{return tasks.transaction(id,actor,(c,record,current)->read(c,record,current));}
}
