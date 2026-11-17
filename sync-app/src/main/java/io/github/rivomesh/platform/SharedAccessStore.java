package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** SQL identity/session authority. No plaintext API token or cookie ID is durable. */
final class SharedAccessStore {
    private final PlatformConfig config;
    private final JdbcControlStore db;
    private final SharedAudit audit;
    private final String cluster,policy;
    final String identity;
    private volatile boolean available=true;
    boolean ready(){return available;}
    private static final int MAX_DIRECTORY=4*1024*1024;
    SharedAccessStore(PlatformConfig config,AccountDirectory.State seed,String expected,Map<String,Object> migrationAnchor)throws Exception{
        this.config=config;cluster=config.cluster.id;db=new JdbcControlStore(config.cluster);audit=new SharedAudit(db,cluster,config.auditMaxEvents);
        policy=AuditStore.hash(config.sessions.enabled+":"+config.sessions.secureCookie+":"+config.sessions.rememberBrowser+":"+config.sessions.idleSeconds+":"+config.sessions.absoluteSeconds+":"+config.sessions.maximum+":"+config.auditMaxEvents);
        String chosen;
        try(Connection c=db.connect()){
            db.bootstrapLock(c);String suffix=db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"";
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("access_root")+" (cluster_id VARCHAR(128) PRIMARY KEY,store_id VARCHAR(36) NOT NULL,policy_hash VARCHAR(64) NOT NULL,state_payload "+(db.mysql()?"LONGTEXT":"TEXT")+" NOT NULL,state_hash VARCHAR(64) NOT NULL)"+suffix);
            JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("session")+" (cluster_id VARCHAR(128) NOT NULL,session_hash VARCHAR(64) NOT NULL,user_id VARCHAR(64) NOT NULL,authorization_revision VARCHAR(64) NOT NULL,csrf VARCHAR(43) NOT NULL,issued_at BIGINT NOT NULL,last_seen BIGINT NOT NULL,expires_at BIGINT NOT NULL,PRIMARY KEY(cluster_id,session_hash))"+suffix);
            audit.initialize(c);
            if(db.mysql())try(PreparedStatement p=db.prepare(c,"SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rmc_access_root','rmc_session','rmc_audit_head','rmc_audit_event','rmc_audit_receipt')",config.cluster.namespace);ResultSet r=p.executeQuery()){
                int count=0;while(r.next()){count++;if(!"InnoDB".equalsIgnoreCase(r.getString(1)))throw new TaskService.Conflict("CONTROL_STORE_ENGINE_INVALID");if(!"utf8mb4_bin".equalsIgnoreCase(r.getString(2)))throw new TaskService.Conflict("CONTROL_STORE_COLLATION_INVALID");}if(count!=5)throw new TaskService.Conflict("CONTROL_STORE_TABLES_MISSING");
            }
            c.setAutoCommit(false);
            try(PreparedStatement p=db.prepare(c,"SELECT store_id FROM "+db.table("access_root")+" WHERE cluster_id=? FOR UPDATE",cluster);ResultSet r=p.executeQuery()){
                chosen=r.next()?r.getString(1):null;
            }
            if(chosen==null){
                if(expected!=null||audit.exists(c))throw new IOException("SHARED_ACCESS_STATE_REQUIRED");
                AccountDirectory.State state=copy(seed);if(!state.managed){state.managed=true;state.identity=UUID.randomUUID().toString();state.version=1;state.revision=UUID.randomUUID().toString();}
                if(seed.managed){state.version++;state.revision=UUID.randomUUID().toString();}
                for(AccountDirectory.User user:state.users.values())if(user.role!=PlatformConfig.Role.ADMIN&&user.nodeIds==null)user.nodeIds=new ArrayList<>(Collections.singletonList(config.cluster.nodeId));
                AccountDirectory.validate(state);chosen=UUID.randomUUID().toString();String payload=encode(state);
                db.update(c,"INSERT INTO "+db.table("access_root")+" (cluster_id,store_id,policy_hash,state_payload,state_hash) VALUES (?,?,?,?,?)",cluster,chosen,policy,payload,AuditStore.hash(payload));
                audit.seed(c,chosen,state);AuditStore.Event imported=AccountDirectory.event(state,seed.managed?"IMPORT_LOCAL":"ACTIVATE",null,null);imported.migrationAnchor=migrationAnchor;audit.append(c,imported);
            }else if(expected!=null&&!expected.equals(chosen))throw new IOException("SHARED_ACCESS_IDENTITY_CHANGED");
            AccountDirectory.State state=root(c,true,chosen);
            if(seed.managed&&!Objects.equals(seed.identity,state.identity))throw new IOException("SHARED_ACCESS_DIRECTORY_CONFLICT");c.commit();
        }
        identity=chosen;audit.verifyStartup();
    }
    private static AccountDirectory.State copy(AccountDirectory.State value)throws Exception{return Model.JSON.readValue(Model.JSON.writeValueAsBytes(value),AccountDirectory.State.class);}
    private static String encode(AccountDirectory.State state)throws Exception{String text=Model.JSON.writeValueAsString(state);if(text.length()>MAX_DIRECTORY)throw new TaskService.Conflict("ACCOUNT_DIRECTORY_CAPACITY");return text;}
    private static String bounded(Reader input)throws IOException{
        try(Reader reader=input){StringBuilder text=new StringBuilder();char[] buffer=new char[8192];int n;while((n=reader.read(buffer))!=-1){if(text.length()+n>MAX_DIRECTORY)throw new IOException("ACCOUNT_DIRECTORY_CAPACITY");text.append(buffer,0,n);}return text.toString();}
    }
    private static final class Verified {
        final String hash;final AccountDirectory.State state;
        Verified(String hash,AccountDirectory.State state){this.hash=hash;this.state=state;}
    }
    private volatile Verified verified;
    private AccountDirectory.State root(Connection c,boolean lock,String expected)throws Exception{
        String hash;
        try(PreparedStatement p=db.prepare(c,"SELECT store_id,policy_hash,state_hash FROM "+db.table("access_root")+" WHERE cluster_id=?"+(lock?" FOR UPDATE":""),cluster);ResultSet r=p.executeQuery()){
            if(!r.next()||!Objects.equals(r.getString("store_id"),expected))throw new IOException("SHARED_ACCESS_IDENTITY_CHANGED");
            if(!policy.equals(r.getString("policy_hash")))throw new TaskService.Conflict("SHARED_ACCESS_POLICY_MISMATCH");hash=r.getString("state_hash");
        }
        Verified current=verified;
        if(current!=null&&current.hash.equals(hash)){audit.checkAccess(c,current.state,lock);return current.state;}
        // Every request reads the SQL marker. Only immutable verified payloads are reused;
        // a failed SQL read never authenticates from memory. Serialize large cache misses.
        synchronized(this){
            current=verified;if(current!=null&&current.hash.equals(hash)){audit.checkAccess(c,current.state,lock);return current.state;}
            String payload;try(PreparedStatement p=db.prepare(c,"SELECT state_payload FROM "+db.table("access_root")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){
                if(!r.next())throw new IOException("SHARED_ACCESS_STATE_REQUIRED");payload=bounded(r.getCharacterStream(1));
            }
            if(!AuditStore.hash(payload).equals(hash))throw new IOException("ACCOUNT_STATE_INVALID");AccountDirectory.State state=Model.JSON.readValue(payload,AccountDirectory.State.class);AccountDirectory.validate(state);audit.checkAccess(c,state,lock);verified=new Verified(hash,state);return state;
        }
    }
    AccountDirectory.State serviceState(Connection c,boolean lock)throws Exception{return root(c,lock,identity);}
    boolean containsReceipt(Connection c,String eventId)throws SQLException{return audit.containsReceipt(c,eventId);}
    boolean controls(ClusterConfig c){return cluster.equals(c.id)&&config.cluster.driver.equals(c.driver)&&config.cluster.url.equals(c.url)&&config.cluster.namespace.equals(c.namespace);}
    AccessControl.Principal authorize(Connection c,AccessControl.Principal actor,boolean lock)throws Exception{
        if(actor==null||actor.authorizationRevision==null)throw new SecurityException("FORBIDDEN");
        AccountDirectory.State state=root(c,lock,identity);AccountDirectory.current(state,actor);AccountDirectory.User user=state.users.get(actor.id);
        if(user==null||!user.enabled||user.deleted)throw new SecurityException("FORBIDDEN");
        return new AccessControl.Principal(user.id,user.role,user.taskIds,state.revision,user.nodeIds);
    }
    void append(Connection c,AuditStore.Event event)throws Exception{audit.append(c,event);}
    AccountDirectory.State accounts(){
        try(Connection c=db.connect()){c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);AccountDirectory.State state=root(c,false,identity);c.commit();available=true;return state;}catch(TaskService.Conflict conflict){available=false;throw conflict;}catch(Exception failure){available=false;throw new ControlStore.Unavailable();}
    }
    Map<String,Object> mutate(String action,String id,JsonNode body,String request,AccessControl.Principal actor)throws Exception{
        try(Connection c=db.connect()){
            c.setAutoCommit(false);AccountDirectory.State current=root(c,true,identity);AccountDirectory.Change change=AccountDirectory.mutate(current,action,id,body,request,actor);
            if(!change.replayed){save(c,change.state);db.update(c,"DELETE FROM "+db.table("session")+" WHERE cluster_id=?",cluster);audit.append(c,change.audit);}
            Map<String,Object> result=AccountDirectory.view(change.state);result.put("replayed",change.replayed);result.put("subjectId",change.userId);result.put("authority","SHARED");c.commit();return result;
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    private void save(Connection c,AccountDirectory.State state)throws Exception{String payload=encode(state);if(db.update(c,"UPDATE "+db.table("access_root")+" SET state_payload=?,state_hash=? WHERE cluster_id=? AND store_id=?",payload,AuditStore.hash(payload),cluster,identity)!=1)throw new IOException("SHARED_ACCESS_IDENTITY_CHANGED");}
    void recover(String id,AccountDirectory.User seed)throws Exception{
        if(seed==null||seed.role!=PlatformConfig.Role.ADMIN)throw new IllegalArgumentException("ACCOUNT_RECOVERY_ADMIN_REQUIRED");
        try(Connection c=db.connect()){
            c.setAutoCommit(false);AccountDirectory.State state=copy(root(c,true,identity));AccountDirectory.User restored=Model.JSON.readValue(Model.JSON.writeValueAsBytes(seed),AccountDirectory.User.class);restored.enabled=true;restored.deleted=false;restored.updatedAt=Instant.ofEpochMilli(db.now(c)).toString();state.users.put(id,restored);state.version++;state.revision=UUID.randomUUID().toString();AccountDirectory.validate(state);save(c,state);db.update(c,"DELETE FROM "+db.table("session")+" WHERE cluster_id=?",cluster);AuditStore.Event event=AccountDirectory.event(state,"RECOVER",id,null);event.actorId="offline-recovery";audit.append(c,event);c.commit();
        }catch(SQLException failure){throw new ControlStore.Unavailable();}
    }
    void append(AuditStore.Event event)throws AuditStore.Unavailable{audit.append(event);}
    Map<String,Object> auditPage(long after,long through,int limit)throws AuditStore.Unavailable{return audit.page(after,through,limit);}
    BrowserSessions.Store sessions(){return new Sessions();}
    private final class Sessions implements BrowserSessions.Store {
        public BrowserSessions.Session insert(String hash,BrowserSessions.Session proposed){
            try(Connection c=db.connect()){
                c.setAutoCommit(false);AccountDirectory.State state=root(c,true,identity);AccountDirectory.current(state,proposed.principal);AccountDirectory.User user=state.users.get(proposed.principal.id);
                if(user==null||!user.enabled||user.deleted)throw new TaskService.Conflict("AUTHORIZATION_CHANGED");long now=db.now(c),idle=config.sessions.idleSeconds*1000L;
                db.update(c,"DELETE FROM "+db.table("session")+" WHERE cluster_id=? AND (expires_at<=? OR last_seen<=?)",cluster,now,now-idle);
                try(PreparedStatement p=db.prepare(c,"SELECT COUNT(*) FROM "+db.table("session")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){r.next();if(r.getInt(1)>=config.sessions.maximum)throw new TaskService.Conflict("SESSION_CAPACITY_REACHED");}
                BrowserSessions.Session saved=new BrowserSessions.Session(proposed.principal,proposed.csrf,now,config.sessions.absoluteSeconds*1000L,idle);
                db.update(c,"INSERT INTO "+db.table("session")+" (cluster_id,session_hash,user_id,authorization_revision,csrf,issued_at,last_seen,expires_at) VALUES (?,?,?,?,?,?,?,?)",cluster,hash,user.id,state.revision,saved.csrf,now,now,saved.expiresAt);c.commit();return saved;
            }catch(TaskService.Conflict conflict){throw conflict;}catch(Exception failure){throw new ControlStore.Unavailable();}
        }
        public BrowserSessions.Session find(String hash,long ignoredNow,long ignoredIdle){
            try(Connection c=db.connect()){
                c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);AccountDirectory.State state=root(c,false,identity);long now=db.now(c);BrowserSessions.Session found=null;
                try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("session")+" WHERE cluster_id=? AND session_hash=?",cluster,hash);ResultSet r=p.executeQuery()){
                    if(r.next()&&r.getLong("expires_at")>now&&r.getLong("last_seen")>now-config.sessions.idleSeconds*1000L&&state.revision.equals(r.getString("authorization_revision"))){
                        AccountDirectory.User user=state.users.get(r.getString("user_id"));if(user!=null&&user.enabled&&!user.deleted){AccessControl.Principal principal=new AccessControl.Principal(user.id,user.role,user.taskIds,state.revision,user.nodeIds);found=new BrowserSessions.Session(principal,r.getString("csrf"),r.getLong("issued_at"),r.getLong("expires_at")-r.getLong("issued_at"),config.sessions.idleSeconds*1000L);found.lastSeen=r.getLong("last_seen");}
                    }
                }c.commit();return found;
            }catch(Exception failure){throw new ControlStore.Unavailable();}
        }
        public void remove(String hash){try(Connection c=db.connect()){db.update(c,"DELETE FROM "+db.table("session")+" WHERE cluster_id=? AND session_hash=?",cluster,hash);}catch(Exception failure){throw new ControlStore.Unavailable();}}
        public void touch(String hash,long ignoredNow){try(Connection c=db.connect()){long now=db.now(c);db.update(c,"UPDATE "+db.table("session")+" SET last_seen=CASE WHEN last_seen<? THEN ? ELSE last_seen END WHERE cluster_id=? AND session_hash=? AND expires_at>? AND last_seen>?",now,now,cluster,hash,now,now-config.sessions.idleSeconds*1000L);}catch(Exception failure){throw new ControlStore.Unavailable();}}
        public void close(){/* Shared sessions survive a controller's shutdown. */}
    }
}
