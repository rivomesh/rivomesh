package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.JournalWriterAssignment;
import java.sql.*;
import java.util.*;

/** SQL configuration authority. Commands and owner agents build on these versioned records. */
final class GlobalTaskStore {
    static final class Record {
        String id,owner,directory,revision,configurationVersion,activeConfigurationVersion,previousConfigurationVersion,yaml,configurationAction;
        long sequence,configurationIndex;
    }
    static final class Version {
        public int version=1;
        public String task,id,parent,yaml,actorId,actorRole,action;
        public long index,createdAt;
    }
    interface AuthorityTransaction<T>{T run(Connection connection,AccountDirectory.State directory)throws Exception;}
    interface CommitHook{void commit(Connection connection,Record record,AccessControl.Principal actor)throws Exception;}
    interface Transaction<T>{T run(Connection connection,Record record,AccessControl.Principal actor)throws Exception;}
    interface RowView{Map<String,Object> read(Connection connection,Record record,AccessControl.Principal actor)throws Exception;}
    private final JdbcControlStore db;
    private final SharedAccessStore access;
    private final String cluster;
    final String identity;
    private final ControlCipher cipher;
    private final int maximum;
    private final HandoffPreparations preparations;

    GlobalTaskStore(ClusterConfig config,SharedAccessStore access,String key,String expected,int maximum)throws Exception{
        if(!access.controls(config)||maximum<1||maximum>200000)throw new IllegalArgumentException("GLOBAL_TASK_POLICY_INVALID");this.access=access;this.maximum=maximum;cluster=config.id;db=new JdbcControlStore(config);
        String selected;ControlCipher selectedCipher;
        try(Connection c=db.connect()){
            db.bootstrapLock(c);String suffix=db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"",text=db.mysql()?"LONGTEXT":"TEXT";
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("task_root")+" (cluster_id VARCHAR(128) PRIMARY KEY,store_id VARCHAR(36) NOT NULL,key_proof VARCHAR(43) NOT NULL,access_identity VARCHAR(36) NOT NULL,state_hash VARCHAR(43) NOT NULL,maximum_tasks INTEGER NOT NULL,next_sequence BIGINT NOT NULL)"+suffix);
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("task_authority")+" (cluster_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,owner_id VARCHAR(64) NOT NULL,directory_id VARCHAR(36) NOT NULL,revision VARCHAR(36) NOT NULL,configuration_version VARCHAR(36) NOT NULL,active_version VARCHAR(36),configuration_index BIGINT NOT NULL,record_hash VARCHAR(43) NOT NULL,sequence_no BIGINT NOT NULL,PRIMARY KEY(cluster_id,task_id))"+suffix);
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("task_version")+" (cluster_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,version_id VARCHAR(36) NOT NULL,parent_version VARCHAR(36),sealed_yaml "+text+" NOT NULL,content_hash VARCHAR(43) NOT NULL,actor_id VARCHAR(64) NOT NULL,created_at BIGINT NOT NULL,PRIMARY KEY(cluster_id,task_id,version_id))"+suffix);
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("task_request")+" (cluster_id VARCHAR(128) NOT NULL,request_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,actor_id VARCHAR(64) NOT NULL,signature VARCHAR(43) NOT NULL,PRIMARY KEY(cluster_id,request_id))"+suffix);
            db.index(c,"task_authority","rmc_task_owner_page","cluster_id, owner_id, task_id");
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rmc_task_root','rmc_task_authority','rmc_task_version','rmc_task_request')",config.namespace);ResultSet r=p.executeQuery()){
                int count=0;while(r.next()){count++;if(!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new TaskService.Conflict("CONTROL_STORE_ENGINE_INVALID");if(!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))throw new TaskService.Conflict("CONTROL_STORE_COLLATION_INVALID");}if(count!=4)throw new TaskService.Conflict("CONTROL_STORE_TABLES_MISSING");
            }
            c.setAutoCommit(false);String proof=null,stateHash=null;int configuredMaximum=maximum;long sequence=0;
            try(PreparedStatement p=db.prepare(c,"SELECT store_id,key_proof,maximum_tasks,access_identity,next_sequence,state_hash FROM "+db.table("task_root")+" WHERE cluster_id=? FOR UPDATE",cluster);ResultSet r=p.executeQuery()){
                if(r.next()){selected=r.getString(1);proof=r.getString(2);configuredMaximum=r.getInt(3);if(!access.identity.equals(r.getString(4)))throw new TaskService.Conflict("GLOBAL_TASK_ACCESS_IDENTITY_CHANGED");sequence=r.getLong(5);stateHash=r.getString(6);}else selected=null;
            }
            if(selected==null){
                if(expected!=null)throw new TaskService.Conflict("GLOBAL_TASK_STATE_REQUIRED");
                for(String table:Arrays.asList("task_authority","task_version","task_request"))try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table(table)+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)>0)throw new TaskService.Conflict("GLOBAL_TASK_STATE_REQUIRED");}
                selected=UUID.randomUUID().toString();
            }
            if(expected!=null&&!expected.equals(selected))throw new TaskService.Conflict("GLOBAL_TASK_IDENTITY_CHANGED");
            selectedCipher=new ControlCipher(key,cluster,selected);
            if(proof==null)db.update(c,"INSERT INTO "+db.table("task_root")+" (cluster_id,store_id,key_proof,access_identity,state_hash,maximum_tasks,next_sequence) VALUES (?,?,?,?,?,?,0)",cluster,selected,selectedCipher.signature("key-proof"),access.identity,selectedCipher.signature("root\n"+access.identity+"\n"+maximum+"\n0"),maximum);
            else if(!selectedCipher.matches("key-proof",proof))throw new TaskService.Conflict("CONTROL_KEY_MISMATCH");
            if(configuredMaximum!=maximum)throw new TaskService.Conflict("GLOBAL_TASK_POLICY_MISMATCH");
            if(proof!=null&&!selectedCipher.matches("root\n"+access.identity+"\n"+maximum+"\n"+sequence,stateHash))throw new TaskService.Conflict("GLOBAL_TASK_ROOT_INVALID");
            try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("task_authority")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)!=sequence)throw new TaskService.Conflict("GLOBAL_TASK_CATALOG_INCOMPLETE");}c.commit();
        }
        identity=selected;cipher=selectedCipher;preparations=new HandoffPreparations(config,db,cipher);
    }
    private void root(Connection c)throws Exception{root(c,true);}
    private void root(Connection c,boolean lock)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT store_id,key_proof,maximum_tasks,access_identity,next_sequence,state_hash FROM "+db.table("task_root")+" WHERE cluster_id=?"+(lock?" FOR UPDATE":""),cluster);ResultSet r=p.executeQuery()){
            if(!r.next()||!identity.equals(r.getString(1)))throw new TaskService.Conflict("GLOBAL_TASK_IDENTITY_CHANGED");
            if(!cipher.matches("key-proof",r.getString(2)))throw new TaskService.Conflict("CONTROL_KEY_MISMATCH");
            if(maximum!=r.getInt(3))throw new TaskService.Conflict("GLOBAL_TASK_POLICY_MISMATCH");
            if(!access.identity.equals(r.getString(4)))throw new TaskService.Conflict("GLOBAL_TASK_ACCESS_IDENTITY_CHANGED");
            if(!cipher.matches("root\n"+access.identity+"\n"+maximum+"\n"+r.getLong(5),r.getString(6)))throw new TaskService.Conflict("GLOBAL_TASK_ROOT_INVALID");
        }
    }
    private AccessControl.Principal authorize(Connection c,AccessControl.Principal actor)throws Exception{return access.authorize(c,actor,true);}
    private static void valid(String value,String pattern){if(value==null||!value.matches(pattern))throw new IllegalArgumentException("GLOBAL_TASK_REQUEST_INVALID");}
    private static void scope(AccessControl.Principal actor,String task,String owner,boolean write){if(!actor.task(task)||!actor.node(owner)||write&&!actor.administers())throw new SecurityException("FORBIDDEN");}
    private String recordPurpose(Record record){return "record\n"+record.id+"\n"+record.owner+"\n"+record.directory+"\n"+record.revision+"\n"+record.configurationVersion+"\n"+record.sequence+"\n"+record.configurationIndex+"\n"+record.activeConfigurationVersion;}
    private String recordSignature(Record record){return cipher.signature(recordPurpose(record));}
    private Record read(Connection c,String id)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("task_authority")+" WHERE cluster_id=? AND task_id=?",cluster,id);ResultSet r=p.executeQuery()){
            if(!r.next())throw new NoSuchElementException("TASK_NOT_FOUND");Record record=row(r);
            if(!cipher.matches(recordPurpose(record),r.getString("record_hash")))throw new TaskService.Conflict("GLOBAL_TASK_RECORD_INVALID");return record;
        }
    }
    private void configuration(Connection c,Record record)throws Exception{
        Version version=version(c,record.id,record.configurationVersion);if(version.index!=record.configurationIndex)throw new TaskService.Conflict("GLOBAL_CONFIG_CHAIN_INVALID");record.yaml=version.yaml;
    }
    private boolean replay(Connection c,String request,String signature,String task,AccessControl.Principal actor)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT task_id,actor_id,signature FROM "+db.table("task_request")+" WHERE cluster_id=? AND request_id=?",cluster,request);ResultSet r=p.executeQuery()){
            if(!r.next())return false;if(!task.equals(r.getString(1))||!cipher.matches(signature,r.getString(3)))throw new TaskService.Conflict("REQUEST_ID_REUSED");if(!actor.id.equals(r.getString(2)))throw new TaskService.Conflict("REQUEST_IDENTITY_CHANGED");return true;
        }
    }
    private void version(Connection c,Record record,AccessControl.Principal actor)throws Exception{
        Version version=new Version();version.task=record.id;version.id=record.configurationVersion;version.parent=record.previousConfigurationVersion;version.index=record.configurationIndex;version.yaml=record.yaml;version.actorId=actor.id;version.actorRole=actor.role.name();version.action=record.configurationAction!=null?record.configurationAction:record.configurationIndex==1?"CREATE":"EDIT_CONFIG";version.createdAt=db.now(c);String encoded=Model.JSON.writeValueAsString(version);
        db.update(c,"INSERT INTO "+db.table("task_version")+" (cluster_id,task_id,version_id,parent_version,sealed_yaml,content_hash,actor_id,created_at) VALUES (?,?,?,?,?,?,?,?)",cluster,record.id,record.configurationVersion,version.parent,cipher.seal("config/"+record.id+"/"+record.configurationVersion,encoded),cipher.signature("config-content\n"+encoded),actor.id,version.createdAt);
    }
    private Version version(Connection c,String task,String id)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("task_version")+" WHERE cluster_id=? AND task_id=? AND version_id=?",cluster,task,id);ResultSet r=p.executeQuery()){
            if(!r.next())throw new TaskService.Conflict("GLOBAL_CONFIG_REQUIRED");String encoded=cipher.open("config/"+task+"/"+id,r.getString("sealed_yaml"));
            if(!cipher.matches("config-content\n"+encoded,r.getString("content_hash")))throw new TaskService.Conflict("GLOBAL_CONFIG_INVALID");Version v;
            try{v=Model.JSON.readValue(encoded,Version.class);}catch(com.fasterxml.jackson.core.JsonProcessingException invalid){throw new TaskService.Conflict("GLOBAL_CONFIG_INVALID");}
            if(v.version!=1||!task.equals(v.task)||!id.equals(v.id)||v.index<1||v.yaml==null||!Objects.equals(v.parent,r.getString("parent_version"))||!Objects.equals(v.actorId,r.getString("actor_id"))||v.createdAt!=r.getLong("created_at"))throw new TaskService.Conflict("GLOBAL_CONFIG_INVALID");return v;
        }
    }
    private static Record row(ResultSet r)throws SQLException{
        Record record=new Record();record.id=r.getString("task_id");record.owner=r.getString("owner_id");record.directory=r.getString("directory_id");record.revision=r.getString("revision");record.configurationVersion=r.getString("configuration_version");record.activeConfigurationVersion=r.getString("active_version");record.configurationIndex=r.getLong("configuration_index");record.sequence=r.getLong("sequence_no");return record;
    }
    Record create(String id,String owner,String directory,String yaml,String request,AccessControl.Principal supplied)throws Exception{return create(id,owner,directory,yaml,request,supplied,null);}
    Record create(String id,String owner,String directory,String yaml,String request,AccessControl.Principal supplied,CommitHook hook)throws Exception{
        if(owner==null||directory==null)throw new IllegalArgumentException("GLOBAL_TASK_REQUEST_INVALID");
        return create(id,owner,directory,yaml,request,supplied,hook,null);
    }
    Record allocate(String id,String yaml,String request,AccessControl.Principal supplied,
                    AgentDirectory agents,CommitHook hook)throws Exception{
        return create(id,null,null,yaml,request,supplied,hook,Objects.requireNonNull(agents));
    }
    private Record create(String id,String owner,String directory,String yaml,String request,AccessControl.Principal supplied,
                          CommitHook hook,AgentDirectory agents)throws Exception{
        valid(id,"[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
        valid(request,"[A-Za-z0-9_-]{8,128}");
        boolean automatic=agents!=null;
        if(!automatic){
            valid(owner,"[A-Za-z0-9][A-Za-z0-9._-]{0,63}");
            valid(directory,"[a-f0-9-]{36}");
        }
        ManagedWriterDeclaration declaration=ManagedWriterDeclaration.read(yaml);
        if(declaration!=null)declaration.requireOwner(owner);
        String signature=automatic?"CREATE_AUTO\n"+id+"\n"+yaml
                :"CREATE\n"+id+"\n"+owner+"\n"+directory+"\n"+yaml;
        try(Connection c=db.connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.setAutoCommit(false);AccessControl.Principal actor=authorize(c,supplied);
            if(!actor.task(id)||!actor.administers())throw new SecurityException("FORBIDDEN");
            if(!automatic)scope(actor,id,owner,true);
            root(c);
            if(replay(c,request,signature,id,actor)){Record old=read(c,id);scope(actor,id,old.owner,true);configuration(c,old);c.commit();return old;}
            long registered;try(PreparedStatement p=db.prepare(c,"SELECT next_sequence FROM "+db.table("task_root")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();registered=r.getLong(1);if(registered>=maximum)throw new TaskService.Conflict("GLOBAL_TASK_CAPACITY");}
            try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("task_authority")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)!=registered)throw new TaskService.Conflict("GLOBAL_TASK_CATALOG_INCOMPLETE");}
            try{read(c,id);throw new TaskService.Conflict("TASK_ID_EXISTS");}catch(NoSuchElementException absent){/* checked under the authority root lock */}
            if(automatic){
                AgentDirectory.Binding chosen=agents.allocate(c,actor);
                owner=chosen.node;directory=chosen.directory;
                scope(actor,id,owner,true);
            }else requireReservedCapacity(c,owner);
            Record record=new Record();record.id=id;record.owner=owner;record.directory=directory;record.yaml=yaml;record.revision=UUID.randomUUID().toString();record.configurationVersion=UUID.randomUUID().toString();record.configurationIndex=1;
            try(PreparedStatement p=db.prepare(c,"SELECT next_sequence FROM "+db.table("task_root")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();record.sequence=r.getLong(1)+1;}
            version(c,record,actor);db.update(c,"INSERT INTO "+db.table("task_authority")+" (cluster_id,task_id,owner_id,directory_id,revision,configuration_version,active_version,configuration_index,record_hash,sequence_no) VALUES (?,?,?,?,?,?,?,?,?,?)",cluster,id,owner,directory,record.revision,record.configurationVersion,null,record.configurationIndex,recordSignature(record),record.sequence);
            db.update(c,"UPDATE "+db.table("task_root")+" SET next_sequence=?,state_hash=? WHERE cluster_id=?",record.sequence,cipher.signature("root\n"+access.identity+"\n"+maximum+"\n"+record.sequence),cluster);db.update(c,"INSERT INTO "+db.table("task_request")+" (cluster_id,request_id,task_id,actor_id,signature) VALUES (?,?,?,?,?)",cluster,request,id,actor.id,cipher.signature(signature));
            if(hook!=null)hook.commit(c,record,actor);access.append(c,event(record,request,actor,"GLOBAL_TASK_CREATED"));c.commit();return record;
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    private void requireReservedCapacity(Connection c,String owner)throws Exception{
        long reserved=preparations.reserved(c,owner);
        if(reserved==0)return;
        String sql="SELECT n.metadata,(SELECT COUNT(*) FROM "+db.table("task_authority")
                +" t WHERE t.cluster_id=n.cluster_id AND t.owner_id=n.node_id) FROM "+db.table("node")
                +" n WHERE n.cluster_id=? AND n.node_id=? FOR UPDATE";
        try(PreparedStatement p=db.prepare(c,sql,cluster,owner);ResultSet row=p.executeQuery()){
            if(row.next()){
                ControlStore.Node declared=Model.JSON.readValue(row.getString(1),ControlStore.Node.class);
                long used=row.getLong(2)+reserved;
                if(owner.equals(declared.id)&&declared.executes()&&used<declared.taskCapacity&&used<declared.runningCapacity)return;
            }
        }
        throw new TaskService.Conflict("AGENT_HANDOFF_CAPACITY_RESERVED");
    }
    Record update(String id,String revision,String yaml,String request,AccessControl.Principal supplied)throws Exception{return update(id,revision,yaml,request,supplied,null);}
    Record update(String id,String revision,String yaml,String request,AccessControl.Principal supplied,CommitHook hook)throws Exception{
        return update(id,revision,yaml,request,supplied,hook,"EDIT_CONFIG");
    }
    Record update(String id,String revision,String yaml,String request,AccessControl.Principal supplied,CommitHook hook,String action)throws Exception{
        if(!Arrays.asList("EDIT_CONFIG","RESTORE_CONFIG").contains(action))throw new IllegalArgumentException("GLOBAL_CONFIG_ACTION_INVALID");
        valid(request,"[A-Za-z0-9_-]{8,128}");String signature="UPDATE\n"+id+"\n"+revision+"\n"+yaml;
        try(Connection c=db.connect()){
            c.setAutoCommit(false);AccessControl.Principal actor=authorize(c,supplied);root(c);Record record=read(c,id);scope(actor,id,record.owner,true);
            if(replay(c,request,signature,id,actor)){configuration(c,record);c.commit();return record;}
            if(!record.revision.equals(revision))throw new TaskService.Conflict("REVISION_CHANGED");
            preparations.requireUnreserved(c,id);
            ManagedWriterDeclaration.requireUnchanged(version(c,id,record.configurationVersion).yaml,yaml);
            record.revision=UUID.randomUUID().toString();record.previousConfigurationVersion=record.configurationVersion;record.configurationVersion=UUID.randomUUID().toString();record.configurationIndex=Math.addExact(record.configurationIndex,1);record.configurationAction=action;record.yaml=yaml;version(c,record,actor);
            db.update(c,"UPDATE "+db.table("task_authority")+" SET revision=?,configuration_version=?,configuration_index=?,record_hash=? WHERE cluster_id=? AND task_id=?",record.revision,record.configurationVersion,record.configurationIndex,recordSignature(record),cluster,id);
            db.update(c,"INSERT INTO "+db.table("task_request")+" (cluster_id,request_id,task_id,actor_id,signature) VALUES (?,?,?,?,?)",cluster,request,id,actor.id,cipher.signature(signature));if(hook!=null)hook.commit(c,record,actor);access.append(c,event(record,request,actor,"GLOBAL_CONFIG_COMMITTED"));c.commit();return record;
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    <T>T service(Connection c,AuthorityTransaction<T> work)throws Exception{AccountDirectory.State directory=access.serviceState(c,true);root(c);return work.run(c,directory);}
    <T>T service(AuthorityTransaction<T> work)throws Exception{
        try(Connection c=db.connect()){c.setAutoCommit(false);T result=service(c,work);c.commit();return result;}catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    String accessIdentity(){return access.identity;}
    AccessControl.Principal principal(AccessControl.Principal supplied)throws Exception{
        try(Connection c=db.connect()){c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);AccessControl.Principal actor=access.authorize(c,supplied,false);root(c,false);c.commit();return actor;}catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    <T>T transaction(String id,AccessControl.Principal supplied,Transaction<T> work)throws Exception{
        return transaction(id,supplied,Connection.TRANSACTION_NONE,work);
    }
    <T>T transaction(String id,AccessControl.Principal supplied,int isolation,Transaction<T> work)throws Exception{
        try(Connection c=db.connect()){
            if(isolation!=Connection.TRANSACTION_NONE)c.setTransactionIsolation(isolation);
            c.setAutoCommit(false);AccessControl.Principal actor=authorize(c,supplied);root(c);Record record=read(c,id);scope(actor,id,record.owner,false);T result=work.run(c,record,actor);c.commit();return result;
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    /** Called inside transaction(), with the validated authority root held for the entire check. */
    List<Record> kafkaCatalogRecords(Connection c,int maximumTasks)throws Exception {
        long registered;
        try(PreparedStatement p=db.prepare(c,"SELECT next_sequence FROM "+db.table("task_root")+" WHERE cluster_id=?",cluster);
            ResultSet r=p.executeQuery()){
            if(!r.next())throw new TaskService.Conflict("GLOBAL_TASK_CATALOG_INCOMPLETE");
            registered=r.getLong(1);
        }
        if(registered>maximumTasks)throw new KafkaNoticeCatalog.Limit();
        List<String> ids=new ArrayList<>();
        try(PreparedStatement p=db.prepare(c,"SELECT task_id FROM "+db.table("task_authority")+" WHERE cluster_id=? ORDER BY task_id LIMIT ?",
                cluster,maximumTasks+1);ResultSet r=p.executeQuery()){
            while(r.next())ids.add(r.getString(1));
        }
        if(ids.size()!=registered)throw new TaskService.Conflict("GLOBAL_TASK_CATALOG_INCOMPLETE");
        List<Record> rows=new ArrayList<>();
        Set<Long> sequences=new HashSet<>();
        for(String id:ids){
            Record item=read(c,id);
            if(item.sequence<1||item.sequence>registered||!sequences.add(item.sequence))
                throw new TaskService.Conflict("GLOBAL_TASK_CATALOG_INCOMPLETE");
            rows.add(item);
        }
        return rows;
    }

    List<Version> kafkaCatalogVersions(Connection c,Record record,int maximumVersions)throws Exception {
        if(record.configurationIndex>maximumVersions)throw new KafkaNoticeCatalog.Limit();
        List<Version> chain=new ArrayList<>();
        boolean active=record.activeConfigurationVersion==null;
        String next=record.configurationVersion;
        for(long index=record.configurationIndex;index>0;index--){
            if(next==null)throw new TaskService.Conflict("GLOBAL_CONFIG_CHAIN_INVALID");
            Version item=version(c,record.id,next);
            if(item.index!=index)throw new TaskService.Conflict("GLOBAL_CONFIG_CHAIN_INVALID");
            active|=item.id.equals(record.activeConfigurationVersion);
            chain.add(item);
            next=item.parent;
        }
        if(next!=null||!active||chain.isEmpty())throw new TaskService.Conflict("GLOBAL_CONFIG_CHAIN_INVALID");
        return chain;
    }
    HandoffPreparations preparations(){return preparations;}
    void advance(Connection c,Record record)throws Exception{preparations.requireCommandAllowed(c,record.id);record.revision=UUID.randomUUID().toString();db.update(c,"UPDATE "+db.table("task_authority")+" SET revision=?,record_hash=? WHERE cluster_id=? AND task_id=?",record.revision,recordSignature(record),cluster,record.id);}
    void applied(Connection c,Record record,String version)throws Exception{
        if(!record.configurationVersion.equals(version))throw new TaskService.Conflict("CONFIGURATION_VERSION_CHANGED");configuration(c,record);record.activeConfigurationVersion=version;
        db.update(c,"UPDATE "+db.table("task_authority")+" SET active_version=?,record_hash=? WHERE cluster_id=? AND task_id=?",version,recordSignature(record),cluster,record.id);
    }
    HandoffPreparations.Plan commitPreparedWriter(Connection c,Record record,HandoffPreparations.Plan plan,
            AccessControl.Principal actor,JournalWriterAssignment.CutProof cut,String successorYaml,
            String candidateStagingSha256,String executionRequest)throws Exception{
        return commitPreparedWriter(c,record,plan,actor,cut,successorYaml,candidateStagingSha256,executionRequest,null,0);
    }
    HandoffPreparations.Plan commitPreparedWriter(Connection c,Record record,HandoffPreparations.Plan plan,
            AccessControl.Principal actor,JournalWriterAssignment.CutProof cut,String successorYaml,
            String candidateStagingSha256,String executionRequest,RawCaptureHandoffStaging.Proof rawCapture,int failureRecords)throws Exception{
        HandoffPreparations.requireExecution(plan,executionRequest);
        if(!HandoffFailureHistory.bounded(failureRecords))
            throw new TaskService.Conflict("HANDOFF_FAILURE_HISTORY_SCOPE_CHANGED");
        if(rawCapture!=null&&!rawCapture.matches(plan))throw new TaskService.Conflict("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        if(candidateStagingSha256==null||!candidateStagingSha256.matches("[a-f0-9]{64}"))
            throw new TaskService.Conflict("HANDOFF_CANDIDATE_ADMISSION_REQUIRED");
        HandoffPreparations.Evidence evidence=plan.evidence();
        HandoffPreparations.WriterActivation activation=plan.activation();
        if(!actor.administers()||!actor.node(evidence.original().node())||!actor.node(evidence.candidate().node()))
            throw new SecurityException("FORBIDDEN");
        if(!plan.actor().equals(actor.id)||!plan.actorRevision().equals(actor.authorizationRevision))
            throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        if(!record.id.equals(plan.task())||!record.owner.equals(evidence.original().node())
                ||!record.directory.equals(evidence.original().directory())||!record.revision.equals(evidence.revision())
                ||!evidence.configuration().equals(record.activeConfigurationVersion)
                ||!evidence.configuration().equals(record.configurationVersion)||!"PREPARED".equals(plan.phase())
                ||activation==null||!activation.checkedInControlTransaction()||db.now(c)>=plan.expires())
            throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        String originalYaml=activeYaml(c,record);
        ManagedWriterDeclaration declaration=ManagedWriterDeclaration.read(originalYaml);
        if(declaration==null||!db.sharesPostgresUrl(declaration.journalUrl())||cut==null||cut.managedWriter()==null
                ||!activation.originalAssignmentSha256().equals(cut.managedWriter().originalAssignmentSha256())
                ||!activation.journalGenerationSha256().equals(cut.journalGenerationSha256())
                ||!activation.nativeEvidenceSha256().equals(cut.nativeEvidenceSha256())
                ||!evidence.cut().epoch().equals(Long.toString(cut.managedWriter().epoch()))
                ||!evidence.cut().sequence().equals(Long.toString(cut.managedWriter().sequence()))
                ||!evidence.cut().checkpointSha256().equals(cut.managedWriter().checkpointSha256())
                ||!successorYaml.equals(ManagedWriterDeclaration.successorYaml(originalYaml,record.owner,
                    evidence.candidate().node(),activation.nextAssignment())))
            throw new TaskService.Conflict("HANDOFF_MANAGED_PLACEMENT_INVALID");
        JournalWriterAssignment.Placement placement=JournalWriterAssignment.writeManagedControlPlacement(c,
                declaration.namespace(),record.id,declaration.assignment(),cut,activation.nextAssignment(),evidence.candidate().node());
        var commit=installWriterProjection(c,record,plan,actor,successorYaml,candidateStagingSha256,placement.epoch(),rawCapture,failureRecords);
        HandoffPreparations.Plan committed=preparations.commitWriter(c,plan,commit,db.now(c));
        access.append(c,event(record,plan.id(),actor,"MANAGED_HANDOFF_WRITER_COMMITTED"));
        return committed;
    }

    HandoffPreparations.Plan commitFrozenWriter(Connection c,Record record,HandoffPreparations.Plan plan,
            AccessControl.Principal actor,ManagedHandoff.CandidateAdmission candidate,String offlineRequest)throws Exception{
        var evidence=plan.evidence();var freeze=plan.writerFreeze();
        if(!actor.administers()||!actor.node(evidence.original().node())||!actor.node(evidence.candidate().node()))
            throw new SecurityException("FORBIDDEN");
        if(!"ORIGINAL_FROZEN".equals(plan.phase())||freeze==null||freeze.offlineCommit()!=null
                ||plan.activation()==null||!plan.activation().checkedInControlTransaction()
                ||!record.id.equals(plan.task())||!record.owner.equals(evidence.original().node())
                ||!record.directory.equals(evidence.original().directory())||!record.revision.equals(evidence.revision())
                ||!evidence.configuration().equals(record.activeConfigurationVersion)||!evidence.configuration().equals(record.configurationVersion))
            throw new TaskService.Conflict("HANDOFF_OFFLINE_COMMIT_FREEZE_REQUIRED");
        if(candidate==null||!candidate.valid()||!plan.id().equals(candidate.preparation())||!evidence.candidate().equals(candidate.binding())
                ||candidate.rawCapture()!=null||candidate.failureRecords()!=0||!freeze.manifestSha256().equals(candidate.stagingSha256()))
            throw new TaskService.Conflict("HANDOFF_OFFLINE_CANDIDATE_CHANGED");
        String originalYaml=activeYaml(c,record);
        ManagedWriterDeclaration original=ManagedWriterDeclaration.read(originalYaml);
        if(original==null||!db.sharesPostgresUrl(original.journalUrl())
                ||!plan.activation().originalAssignmentSha256().equals(AuditStore.hash(original.assignment())))
            throw new TaskService.Conflict("HANDOFF_MANAGED_CONTROL_CUT_REQUIRED");
        String successor=ManagedWriterDeclaration.successorYaml(originalYaml,record.owner,candidate.binding().node(),plan.activation().nextAssignment());
        if(!AuditStore.hash(successor).equals(candidate.configurationSha256()))
            throw new TaskService.Conflict("HANDOFF_OFFLINE_CANDIDATE_CHANGED");
        var placement=JournalWriterAssignment.placeFrozenRecipient(c,original.namespace(),record.id,original.assignment(),
                freeze.cut(),freeze.fence(),plan.activation().nextAssignment(),candidate.binding().node());
        var receipt=installWriterProjection(c,record,plan,actor,successor,candidate.stagingSha256(),placement.epoch(),null,0);
        HandoffPreparations.Plan committed=preparations.consumeFrozenWriter(c,plan,receipt,offlineRequest,actor,db.now(c));
        access.append(c,event(record,offlineRequest,actor,"MANAGED_HANDOFF_OFFLINE_OWNER_COMMITTED"));
        return committed;
    }

    private HandoffPreparations.WriterCommit installWriterProjection(Connection c,Record record,HandoffPreparations.Plan plan,
            AccessControl.Principal actor,String successorYaml,String candidateStagingSha256,long epoch,
            RawCaptureHandoffStaging.Proof rawCapture,int failureRecords)throws Exception{
        var evidence=plan.evidence();
        record.owner=evidence.candidate().node();record.directory=evidence.candidate().directory();record.revision=UUID.randomUUID().toString();
        record.previousConfigurationVersion=record.configurationVersion;record.configurationVersion=UUID.randomUUID().toString();
        record.activeConfigurationVersion=record.configurationVersion;record.configurationIndex=Math.addExact(record.configurationIndex,1);
        record.configurationAction="AGENT_HANDOFF";record.yaml=successorYaml;
        version(c,record,actor);
        if(db.update(c,"UPDATE "+db.table("task_authority")+" SET owner_id=?,directory_id=?,revision=?,configuration_version=?,"
                +"active_version=?,configuration_index=?,record_hash=? WHERE cluster_id=? AND task_id=? AND owner_id=? AND directory_id=?"
                +" AND revision=? AND active_version=?",record.owner,record.directory,record.revision,record.configurationVersion,
                record.activeConfigurationVersion,record.configurationIndex,recordSignature(record),cluster,record.id,
                evidence.original().node(),evidence.original().directory(),evidence.revision(),evidence.configuration())!=1)
            throw new TaskService.Conflict("HANDOFF_PREPARATION_CHANGED");
        db.update(c,"DELETE FROM "+db.table("task_observation")+" WHERE cluster_id=? AND task_id=?",cluster,record.id);
        return new HandoffPreparations.WriterCommit(actor.id,candidateStagingSha256,AuditStore.hash(successorYaml),
                record.revision,record.configurationVersion,epoch,rawCapture,failureRecords);
    }
    ManagedRecipientImport.Grant verifiedStoppedRecipient(Connection c,String task,ControlStore.Lease lease,String directory)throws Exception{
        Record current=read(c,task);
        HandoffPreparations.Plan plan=preparations.latest(c,task,db.now(c));
        if(plan==null||!"COMMITTED".equals(plan.phase()))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_COMMIT_REQUIRED");
        return verifiedStoppedRecipient(c,current,plan,lease,directory);
    }
    ManagedRecipientImport.Grant verifiedEvidenceRecipient(Connection c,String task,String preparation,
                                                   ControlStore.Lease lease,String directory)throws Exception{
        Record current=read(c,task);
        HandoffPreparations.Plan plan=preparations.latest(c,task,db.now(c));
        if(!HandoffPreparations.evidenceEligible(plan)||!plan.id().equals(preparation))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_EVIDENCE_CHANGED");
        return verifiedStoppedRecipient(c,current,plan,lease,directory);
    }
    private ManagedRecipientImport.Grant verifiedStoppedRecipient(Connection c,Record current,HandoffPreparations.Plan plan,
                                                            ControlStore.Lease lease,String directory)throws Exception{
        if(plan.writerCommit()==null||plan.activation()==null)
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_COMMIT_REQUIRED");
        String task=current.id;
        HandoffPreparations.Binding candidate=plan.evidence().candidate();
        if(!current.owner.equals(lease.nodeId)||!current.directory.equals(directory)||!current.owner.equals(candidate.node())
                ||!current.directory.equals(candidate.directory())||lease.epoch<candidate.epoch()
                ||lease.epoch==candidate.epoch()&&!lease.incarnation.equals(candidate.incarnation()))throw new ControlStore.Fenced();
        HandoffPreparations.WriterCommit commit=plan.writerCommit();
        HandoffPreparations.Binding former=plan.evidence().original();
        HandoffPreparations.Plan retired=preparations.retirement(c,task,former.node(),former.directory());
        if(retired==null||!plan.id().equals(retired.id())||!commit.revision().equals(current.revision)
                ||!commit.configuration().equals(current.activeConfigurationVersion)||!commit.configuration().equals(current.configurationVersion))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_AUTHORITY_CHANGED");
        String historical=version(c,task,plan.evidence().configuration()).yaml;
        String successor=activeYaml(c,current);
        ManagedWriterDeclaration previous=ManagedWriterDeclaration.read(historical),newWriter=ManagedWriterDeclaration.read(successor);
        if(previous==null||newWriter==null||!db.sharesPostgresUrl(previous.journalUrl())||!plan.activation().checkedInControlTransaction()
                ||!AuditStore.hash(previous.assignment()).equals(plan.activation().originalAssignmentSha256())
                ||!AuditStore.hash(successor).equals(commit.configurationSha256())
                ||!successor.equals(ManagedWriterDeclaration.successorYaml(historical,former.node(),candidate.node(),plan.activation().nextAssignment())))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_CONFIGURATION_CHANGED");
        JournalWriterAssignment.CutProof proof=recipientCut(plan,newWriter,successor);
        JournalWriterAssignment.verifyRecipientControlCut(c,newWriter.namespace(),task,previous.assignment(),newWriter.assignment(),proof);
        return new ManagedRecipientImport.Grant(identity,plan,successor);
    }
    private static JournalWriterAssignment.CutProof recipientCut(HandoffPreparations.Plan plan,ManagedWriterDeclaration newWriter,
                                                                 String successor)throws Exception{
        HandoffPreparations.WriterCommit commit=plan.writerCommit();
        HandoffPreparations.Binding candidate=plan.evidence().candidate();
        HandoffPreparations.JournalCut prior=plan.evidence().cut();
        long previousEpoch=Long.parseLong(prior.epoch());
        if(previousEpoch<1||previousEpoch==Long.MAX_VALUE||commit.journalEpoch()!=previousEpoch+1)
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_NATIVE_HISTORY_CHANGED");
        return new JournalWriterAssignment.CutProof(plan.activation().journalGenerationSha256(),
                plan.activation().nativeEvidenceSha256(),ManagedWriterDeclaration.subscriptions(successor),true,
                new JournalWriterAssignment.ManagedCut(candidate.node(),AuditStore.hash(newWriter.assignment()),commit.journalEpoch(),
                        Long.parseLong(prior.sequence()),prior.checkpointSha256()));
    }
    void fenceParkedTargets(Connection c,ManagedRecipientImport.Grant grant,io.github.rivomesh.PipelineConfig config,
                            JournalWriterAssignment.FenceAuthorization authorize)throws Exception{
        HandoffPreparations.Plan plan=grant.committed();
        if(!"FENCE_PENDING".equals(plan.phase()))throw new TaskService.Conflict("HANDOFF_TARGET_FENCE_REQUEST_CHANGED");
        ManagedWriterDeclaration previous=ManagedWriterDeclaration.read(version(c,plan.task(),plan.evidence().configuration()).yaml);
        ManagedWriterDeclaration successor=ManagedWriterDeclaration.read(grant.successorYaml());
        requireRecipientConfiguration(plan,config,previous,successor);
        var proof=recipientCut(plan,successor,grant.successorYaml());
        var fenced=config.fenceParkedHandoff(c,previous.assignment(),proof,authorize);
        if(!fenced.destinations().keySet().equals(new TreeSet<>(proof.destinations())))
            throw new TaskService.Conflict("HANDOFF_TARGET_FENCE_REQUEST_CHANGED");
        preparations.recordTargetFence(c,plan,fenced);
        AuditStore.Event event=new AuditStore.Event();event.kind="HANDOFF_TARGETS_FENCED";event.taskId=plan.task();
        event.actorId=plan.targetFence().actor();event.actorRole="ADMIN";event.subjectId=plan.evidence().candidate().node();
        event.clientRequestHash=AuditStore.hash(plan.targetFence().id());event.operationHash=fenced.cutSha256();event.status="STOPPED";
        access.append(c,event);
    }
    private static void requireRecipientConfiguration(HandoffPreparations.Plan plan,io.github.rivomesh.PipelineConfig config,
                                                      ManagedWriterDeclaration previous,ManagedWriterDeclaration successor){
        if(previous==null||successor==null||!plan.task().equals(config.pipelineId)||!successor.assignment().equals(config.journal.writerAssignment)
                ||!successor.node().equals(config.nodeId)||!successor.namespace().equals(config.journal.namespace)
                ||!successor.journalUrl().equals(config.journal.url))throw new TaskService.Conflict("HANDOFF_RECIPIENT_CONFIGURATION_CHANGED");
    }
    void admitParkedWorker(Connection c,ManagedRecipientImport.Grant grant,io.github.rivomesh.PipelineConfig config,
                           JournalWriterAssignment.FenceAuthorization authorize)throws Exception{
        HandoffPreparations.Plan plan=grant.committed();
        if(!"START_PENDING".equals(plan.phase()))throw new TaskService.Conflict("HANDOFF_WORKER_START_REQUEST_CHANGED");
        ManagedWriterDeclaration previous=ManagedWriterDeclaration.read(version(c,plan.task(),plan.evidence().configuration()).yaml);
        ManagedWriterDeclaration successor=ManagedWriterDeclaration.read(grant.successorYaml());
        requireRecipientConfiguration(plan,config,previous,successor);
        var proof=recipientCut(plan,successor,grant.successorYaml());
        config.admitParkedHandoff(c,previous.assignment(),proof,authorize,plan.targetFence().result());
        preparations.recordWorkerAdmitted(c,plan);
        AuditStore.Event event=new AuditStore.Event();event.kind="HANDOFF_WORKER_ADMITTED";event.taskId=plan.task();
        event.actorId=plan.workerStart().actor();event.actorRole="ADMIN";event.subjectId=plan.evidence().candidate().node();
        event.clientRequestHash=AuditStore.hash(plan.workerStart().id());event.operationHash=plan.targetFence().result().cutSha256();
        event.status="COMMITTED";access.append(c,event);
    }
    ManagedRecipientImport.Grant admittedRecipient(Connection c,Record current,ControlStore.Lease lease,String directory)throws Exception{
        var plan=preparations.latest(c,current.id,db.now(c));
        if(!HandoffPreparations.workerAdmitted(plan))return null;
        var candidate=plan.evidence().candidate();
        if(!lease.nodeId.equals(candidate.node())||!directory.equals(candidate.directory())
                ||!current.owner.equals(candidate.node())||!current.directory.equals(candidate.directory())
                ||lease.epoch<candidate.epoch()||lease.epoch==candidate.epoch()&&!lease.incarnation.equals(candidate.incarnation()))
            throw new ControlStore.Fenced();
        String version=plan.writerCommit().configuration();
        if(!version.equals(current.configurationVersion)||!version.equals(current.activeConfigurationVersion))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_CONFIGURATION_CHANGED");
        String active=activeYaml(c,current);
        ManagedWriterDeclaration successor=ManagedWriterDeclaration.read(active);
        if(successor==null||!db.sharesPostgresUrl(successor.journalUrl())||!candidate.node().equals(successor.node())
                ||!AuditStore.hash(active).equals(plan.writerCommit().configurationSha256()))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_CONFIGURATION_CHANGED");
        var proof=recipientCut(plan,successor,active);
        JournalWriterAssignment.verifyAdmittedPlacement(c,successor.namespace(),current.id,successor.assignment(),proof);
        return new ManagedRecipientImport.Grant(identity,plan,active);
    }
    void recordStartedRecipient(Connection c,String task,ControlStore.Lease lease,String directory,String request)throws Exception{
        var grant=admittedRecipient(c,read(c,task),lease,directory);
        if(grant==null||!grant.committed().workerStart().id().equals(request))throw new TaskService.Conflict("HANDOFF_WORKER_START_REQUEST_CHANGED");
        if("WORKER_PROJECTED".equals(grant.committed().phase()))return;
        preparations.recordWorkerProjected(c,grant.committed());
        AuditStore.Event event=new AuditStore.Event();event.kind="HANDOFF_WORKER_INTENT_INSTALLED";event.taskId=task;
        event.subjectId=lease.nodeId;event.clientRequestHash=AuditStore.hash(request);event.status="RUNNING_REQUESTED";
        access.append(c,event);
    }

    void recordRecipientProjection(Connection c,String task,ControlStore.Lease lease,String directory,
                                   ManagedRecipientImport.Receipt receipt)throws Exception{
        Record current=read(c,task);
        HandoffPreparations.Plan plan=preparations.latest(c,task,db.now(c));
        if(plan==null||plan.writerCommit()==null||!plan.id().equals(receipt.preparation()))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_COMMIT_REQUIRED");
        if(!current.owner.equals(lease.nodeId)||!current.directory.equals(directory)
                ||!current.owner.equals(plan.evidence().candidate().node())||!current.directory.equals(plan.evidence().candidate().directory())
                ||!current.revision.equals(plan.writerCommit().revision())
                ||!Objects.equals(current.activeConfigurationVersion,plan.writerCommit().configuration()))throw new ControlStore.Fenced();
        if(HandoffPreparations.evidenceEligible(plan)){preparations.recordProjected(c,plan,receipt);return;}
        verifiedStoppedRecipient(c,task,lease,directory);
        preparations.recordProjected(c,plan,receipt);
        AuditStore.Event event=new AuditStore.Event();event.kind="HANDOFF_RECIPIENT_CONFIG_PROJECTED";event.taskId=task;
        event.subjectId=lease.nodeId;event.clientRequestHash=AuditStore.hash(plan.id());
        event.operationHash=plan.writerCommit().configurationSha256();event.status="STOPPED";
        access.append(c,event);
    }
    void recordRecipientEvidence(Connection c,String task,ControlStore.Lease lease,String directory,
                                 HandoffEvidenceAdoption.Receipt receipt)throws Exception{
        HandoffPreparations.Plan plan=verifiedEvidenceRecipient(c,task,receipt.preparation(),lease,directory).committed();
        preparations.recordEvidenceAdoption(c,plan,receipt);
        if(plan.evidenceAdoption()!=null)return;
        AuditStore.Event event=new AuditStore.Event();event.kind="HANDOFF_RECIPIENT_EVIDENCE_ADOPTED";event.taskId=plan.task();
        event.subjectId=plan.evidence().candidate().node();event.clientRequestHash=AuditStore.hash(plan.id());
        event.operationHash=receipt.adoptionSha256();event.status="STOPPED";
        access.append(c,event);
    }
    String retiredConfiguration(Connection c,Record current,String original,String directory)throws Exception{
        if(original.equals(current.owner)&&directory.equals(current.directory))return null;
        HandoffPreparations.Plan retired=preparations.retirement(c,current.id,original,directory);
        if(retired==null)throw new TaskService.Conflict("COMMAND_OWNER_CHANGED");
        return version(c,current.id,retired.evidence().configuration()).yaml;
    }
    private String reference(Record record,Version version){return version.id+"."+cipher.signature("config-history\n"+record.id+"\n"+record.configurationVersion+"\n"+version.id);}
    private Version selected(Connection c,Record record,String reference)throws Exception{
        if(reference==null||reference.length()!=80||reference.charAt(36)!='.')throw new TaskService.Conflict("CONFIG_HISTORY_CHANGED");String id=reference.substring(0,36);Version selected=version(c,record.id,id);
        if(!java.security.MessageDigest.isEqual(reference.getBytes(java.nio.charset.StandardCharsets.UTF_8),reference(record,selected).getBytes(java.nio.charset.StandardCharsets.US_ASCII)))throw new TaskService.Conflict("CONFIG_HISTORY_CHANGED");return selected;
    }
    Version selected(String id,String reference,String revision,AccessControl.Principal supplied)throws Exception{
        return transaction(id,supplied,(c,record,actor)->{if(!actor.administers())throw new SecurityException("FORBIDDEN");if(!Objects.equals(record.revision,revision))throw new TaskService.Conflict("REVISION_CHANGED");return selected(c,record,reference);});
    }
    Map<String,Object> history(String id,String cursor,AccessControl.Principal supplied)throws Exception{
        return transaction(id,supplied,(c,record,actor)->{
            if(!actor.administers())throw new SecurityException("FORBIDDEN");Version version=cursor==null?version(c,id,record.configurationVersion):selected(c,record,cursor);if(cursor==null&&version.index!=record.configurationIndex)throw new TaskService.Conflict("GLOBAL_CONFIG_CHAIN_INVALID");List<Map<String,Object>> versions=new ArrayList<>();Set<String> seen=new HashSet<>();
            for(int n=0;version!=null&&n<20;n++){
                if(!seen.add(version.id))throw new TaskService.Conflict("GLOBAL_CONFIG_CHAIN_INVALID");Map<String,Object> row=new LinkedHashMap<>();row.put("version",version.id);row.put("reference",reference(record,version));row.put("index",version.index);row.put("action",version.action==null?(version.index==1?"CREATE":"EDIT_CONFIG"):version.action);row.put("active",version.id.equals(record.activeConfigurationVersion));row.put("desired",version.id.equals(record.configurationVersion));row.put("createdAt",java.time.Instant.ofEpochMilli(version.createdAt).toString());row.put("actorId",version.actorId);row.put("actorRole",version.actorRole);versions.add(row);
                Version previous=version.parent==null?null:version(c,id,version.parent);if(previous==null&&version.index!=1||previous!=null&&previous.index!=version.index-1)throw new TaskService.Conflict("GLOBAL_CONFIG_CHAIN_INVALID");version=previous;
            }
            Map<String,Object> result=new LinkedHashMap<>();result.put("versions",versions);result.put("configurationVersion",record.configurationVersion);result.put("activeConfigurationVersion",record.activeConfigurationVersion);if(version!=null)result.put("nextCursor",reference(record,version));return result;
        });
    }
    JdbcControlStore database(){return db;}
    void requireControllerWithoutTasks(String node)throws Exception{
        service((c,accounts)->{try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("task_authority")+" WHERE cluster_id=? AND owner_id=?",cluster,node);ResultSet rows=p.executeQuery()){rows.next();if(rows.getLong(1)>0)throw new TaskService.Conflict("CONTROLLER_OWNS_TASKS");}return null;});
    }
    Record record(Connection c,String id)throws Exception{return read(c,id);}
    String activeYaml(Connection c,Record record)throws Exception{
        if(record.activeConfigurationVersion==null)throw new TaskService.Conflict("OWNER_ACTIVE_CONFIGURATION_REQUIRED");Version version=version(c,record.id,record.activeConfigurationVersion);if(version.index>record.configurationIndex)throw new TaskService.Conflict("GLOBAL_CONFIG_CHAIN_INVALID");return version.yaml;
    }
    ControlCipher commandCipher(){return cipher;}
    Record get(String id,AccessControl.Principal supplied)throws Exception{
        try(Connection c=db.connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);AccessControl.Principal actor=access.authorize(c,supplied,false);root(c,false);Record record=read(c,id);scope(actor,id,record.owner,false);if(actor.administers())configuration(c,record);c.commit();return record;
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    Map<String,Object> page(String after,int limit,AccessControl.Principal supplied)throws Exception{return page(after,limit,null,null,supplied,null);}
    Map<String,Object> page(String after,int limit,String owner,String task,AccessControl.Principal supplied,RowView view)throws Exception{
        if(after==null||!after.isEmpty()&&!after.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||limit<1||limit>50)throw new IllegalArgumentException("TASK_CURSOR_INVALID");
        try(Connection c=db.connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);AccessControl.Principal actor=access.authorize(c,supplied,false);root(c,false);
            StringBuilder filter=new StringBuilder("cluster_id=?");List<Object> parameters=new ArrayList<>();parameters.add(cluster);
            grants(filter,parameters,"task_id",actor.taskIds);grants(filter,parameters,"owner_id",actor.nodeIds);
            if(owner!=null){filter.append(" AND owner_id=?");parameters.add(owner);}
            if(task!=null){filter.append(" AND task_id=?");parameters.add(task);}
            long total;try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("task_authority")+" WHERE "+filter,parameters.toArray());ResultSet r=p.executeQuery()){r.next();total=r.getLong(1);}
            List<Object> selection=new ArrayList<>(parameters);selection.add(after);selection.add(limit+1);List<Map<String,Object>> rows=new ArrayList<>();
            try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("task_authority")+" WHERE "+filter+" AND task_id>? ORDER BY task_id LIMIT ?",selection.toArray());ResultSet r=p.executeQuery()){
                while(r.next()){
                    Record record=row(r);
                    if(!cipher.matches(recordPurpose(record),r.getString("record_hash")))throw new TaskService.Conflict("GLOBAL_TASK_RECORD_INVALID");
                    Map<String,Object> row=view==null?new LinkedHashMap<>():view.read(c,record,actor);row.put("id",record.id);row.put("ownerNodeId",record.owner);row.put("revision",record.revision);row.put("configurationVersion",record.configurationVersion);row.put("activeConfigurationVersion",record.activeConfigurationVersion);rows.add(row);
                }
            }
            boolean more=rows.size()>limit;if(more)rows.remove(rows.size()-1);Map<String,Object> result=new LinkedHashMap<>();result.put("tasks",rows);result.put("total",total);result.put("next",more?rows.get(rows.size()-1).get("id"):null);result.put("authority","SHARED");result.put("observedAt",java.time.Instant.ofEpochMilli(db.now(c)).toString());c.commit();return result;
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    private static void grants(StringBuilder query,List<Object> parameters,String field,Set<String> grants){
        if(grants==null)return;if(grants.isEmpty()){query.append(" AND 1=0");return;}query.append(" AND ").append(field).append(" IN (").append(String.join(",",Collections.nCopies(grants.size(),"?"))).append(")");parameters.addAll(grants);
    }
    private static AuditStore.Event event(Record record,String request,AccessControl.Principal actor,String kind){AuditStore.Event event=new AuditStore.Event();event.kind=kind;event.taskId=record.id;event.actorId=actor.id;event.actorRole=actor.role.name();event.subjectId=record.configurationVersion;event.clientRequestHash=AuditStore.hash(request);event.status="COMMITTED";return event;}
}
