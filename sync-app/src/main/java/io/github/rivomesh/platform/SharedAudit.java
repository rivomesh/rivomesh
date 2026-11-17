package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/** Shared hash-chain audit. Caller-owned transactions couple identity changes and receipts. */
final class SharedAudit {
    private final JdbcControlStore db;
    private final String cluster;
    private final int retention;
    SharedAudit(JdbcControlStore db,String cluster,int retention){this.db=db;this.cluster=cluster;this.retention=retention;}
    void initialize(Connection c)throws SQLException{
        String suffix=db.mysql()?" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin":"";
        JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("audit_head")+" (cluster_id VARCHAR(128) PRIMARY KEY,store_id VARCHAR(36) NOT NULL,anchor_seq BIGINT NOT NULL,anchor_hash VARCHAR(64) NOT NULL,last_seq BIGINT NOT NULL,last_hash VARCHAR(64) NOT NULL,access_identity VARCHAR(36),access_version BIGINT NOT NULL,access_revision VARCHAR(36))"+suffix);
        JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("audit_event")+" (cluster_id VARCHAR(128) NOT NULL,event_seq BIGINT NOT NULL,event_key VARCHAR(36) NOT NULL,payload TEXT NOT NULL,previous_hash VARCHAR(64) NOT NULL,event_hash VARCHAR(64) NOT NULL,PRIMARY KEY(cluster_id,event_seq),UNIQUE(cluster_id,event_key))"+suffix);
        JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table("audit_receipt")+" (cluster_id VARCHAR(128) NOT NULL,event_key VARCHAR(36) NOT NULL,payload_hash VARCHAR(64) NOT NULL,PRIMARY KEY(cluster_id,event_key))"+suffix);
    }
    boolean exists(Connection c)throws SQLException{try(PreparedStatement p=db.prepare(c,"SELECT store_id FROM "+db.table("audit_head")+" WHERE cluster_id=?",cluster);ResultSet r=p.executeQuery()){return r.next();}}
    void seed(Connection c,String store,AccountDirectory.State state)throws SQLException{
        db.update(c,"INSERT INTO "+db.table("audit_head")+" (cluster_id,store_id,anchor_seq,anchor_hash,last_seq,last_hash,access_identity,access_version,access_revision) VALUES (?,?,0,?,0,?,?,?,NULL)",cluster,store,AuditStore.ZERO,AuditStore.ZERO,state.identity,state.version-1);
    }
    private static final class Head {String store,anchorHash,lastHash,accessIdentity,accessRevision;long anchor,last,accessVersion;}
    private Head head(Connection c,boolean lock)throws Exception{
        try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("audit_head")+" WHERE cluster_id=?"+(lock?" FOR UPDATE":""),cluster);ResultSet r=p.executeQuery()){
            if(!r.next())throw new IOException("SHARED_AUDIT_REQUIRED");Head h=new Head();h.store=r.getString("store_id");h.anchor=r.getLong("anchor_seq");h.anchorHash=r.getString("anchor_hash");h.last=r.getLong("last_seq");h.lastHash=r.getString("last_hash");h.accessIdentity=r.getString("access_identity");h.accessVersion=r.getLong("access_version");h.accessRevision=r.getString("access_revision");
            if(h.store==null||!h.store.matches("[a-f0-9-]{36}")||h.anchor<0||h.last<h.anchor||h.anchorHash==null||!h.anchorHash.matches("[a-f0-9]{64}")||h.lastHash==null||!h.lastHash.matches("[a-f0-9]{64}"))throw new IOException("AUDIT_CHAIN_INVALID");return h;
        }
    }
    private static String chain(long seq,String previous,String payload){return AuditStore.hash(seq+"\n"+previous+"\n"+payload);}
    private AuditStore.Event verify(ResultSet r,long seq,String previous)throws Exception{
        String payload=r.getString("payload");if(r.getLong("event_seq")!=seq||!previous.equals(r.getString("previous_hash"))||!chain(seq,previous,payload).equals(r.getString("event_hash")))throw new IOException("AUDIT_CHAIN_INVALID");
        AuditStore.Event e=Model.JSON.readValue(payload,AuditStore.Event.class);if(e.version!=1||e.sequence!=seq||!e.id.equals(r.getString("event_key")))throw new IOException("AUDIT_CHAIN_INVALID");return e;
    }
    private String hashAt(Connection c,long seq,Head h)throws Exception{
        if(seq==h.anchor)return h.anchorHash;try(PreparedStatement p=db.prepare(c,"SELECT event_hash FROM "+db.table("audit_event")+" WHERE cluster_id=? AND event_seq=?",cluster,seq);ResultSet r=p.executeQuery()){if(!r.next())throw new IOException("AUDIT_CHAIN_INVALID");return r.getString(1);}
    }
    void checkAccess(Connection c,AccountDirectory.State state,boolean lock)throws Exception{
        Head h=head(c,lock);if(!Objects.equals(state.identity,h.accessIdentity)||state.version!=h.accessVersion||!Objects.equals(state.revision,h.accessRevision))throw new IOException("ACCOUNT_AUDIT_ANCHOR_MISMATCH");verifyTail(c,h,false,lock);
    }
    private void verifyTail(Connection c,Head h,boolean bootstrap,boolean lock)throws Exception{
        if(h.last<=h.anchor){if(bootstrap&&h.last==0&&h.anchor==0)return;throw new IOException("AUDIT_CHAIN_INVALID");}
        try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("audit_event")+" WHERE cluster_id=? AND event_seq=?"+(lock?" FOR UPDATE":""),cluster,h.last);ResultSet r=p.executeQuery()){
            if(!r.next()||!h.lastHash.equals(r.getString("event_hash")))throw new IOException("AUDIT_CHAIN_INVALID");verify(r,h.last,r.getString("previous_hash"));
        }
    }
    void verifyStartup()throws Exception{
        try(Connection c=db.connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);Head h=head(c,false);long cursor=h.anchor;String previous=h.anchorHash;
            while(cursor<h.last){int count=0;try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("audit_event")+" WHERE cluster_id=? AND event_seq>? AND event_seq<=? ORDER BY event_seq LIMIT 1000",cluster,cursor,h.last);ResultSet r=p.executeQuery()){
                while(r.next()){verify(r,++cursor,previous);previous=r.getString("event_hash");count++;}
            }if(count==0)throw new IOException("AUDIT_CHAIN_INVALID");}
            if(!previous.equals(h.lastHash))throw new IOException("AUDIT_CHAIN_INVALID");c.commit();
        }
    }
    void append(Connection c,AuditStore.Event input)throws Exception{
        if(input==null||input.id==null||!input.id.matches("[a-f0-9-]{36}")||input.sequence!=0)throw new IOException("AUDIT_EVENT_INVALID");Head h=head(c,true);verifyTail(c,h,true,true);
        String signature=AuditStore.hash(Model.JSON.writeValueAsString(input));boolean durable=!"HTTP_BEGIN".equals(input.kind)&&!"HTTP_END".equals(input.kind);
        if(durable)try(PreparedStatement p=db.prepare(c,"SELECT payload_hash FROM "+db.table("audit_receipt")+" WHERE cluster_id=? AND event_key=?",cluster,input.id);ResultSet r=p.executeQuery()){
            if(r.next()){if(!signature.equals(r.getString(1)))throw new IOException("AUDIT_EVENT_COLLISION");return;}
        }
        AuditStore.Event value=Model.JSON.readValue(Model.JSON.writeValueAsBytes(input),AuditStore.Event.class);value.sequence=h.last+1;String payload=Model.JSON.writeValueAsString(value);if(payload.length()>8192)throw new IOException("AUDIT_EVENT_TOO_LARGE");String hash=chain(value.sequence,h.lastHash,payload);
        if(value.accessVersion>0){if(value.accessVersion!=h.accessVersion+1||h.accessVersion>0&&!Objects.equals(h.accessIdentity,value.accessIdentity)||value.accessIdentity==null||value.accessRevision==null)throw new IOException("ACCOUNT_AUDIT_ANCHOR_MISMATCH");h.accessIdentity=value.accessIdentity;h.accessVersion=value.accessVersion;h.accessRevision=value.accessRevision;}
        db.update(c,"INSERT INTO "+db.table("audit_event")+" (cluster_id,event_seq,event_key,payload,previous_hash,event_hash) VALUES (?,?,?,?,?,?)",cluster,value.sequence,value.id,payload,h.lastHash,hash);
        if(durable)db.update(c,"INSERT INTO "+db.table("audit_receipt")+" (cluster_id,event_key,payload_hash) VALUES (?,?,?)",cluster,value.id,signature);
        long cut=value.sequence-retention;if(cut>h.anchor){h.anchorHash=hashAt(c,cut,h);h.anchor=cut;db.update(c,"DELETE FROM "+db.table("audit_event")+" WHERE cluster_id=? AND event_seq<=?",cluster,cut);}
        db.update(c,"UPDATE "+db.table("audit_head")+" SET anchor_seq=?,anchor_hash=?,last_seq=?,last_hash=?,access_identity=?,access_version=?,access_revision=? WHERE cluster_id=?",h.anchor,h.anchorHash,value.sequence,hash,h.accessIdentity,h.accessVersion,h.accessRevision,cluster);
    }
    boolean containsReceipt(Connection c,String eventId)throws SQLException{
        try(PreparedStatement p=db.prepare(c,"SELECT payload_hash FROM "+db.table("audit_receipt")+" WHERE cluster_id=? AND event_key=?",cluster,eventId);ResultSet r=p.executeQuery()){return r.next();}
    }
    void append(AuditStore.Event event)throws AuditStore.Unavailable{
        try(Connection c=db.connect()){c.setAutoCommit(false);append(c,event);c.commit();}catch(Exception failure){throw new AuditStore.Unavailable(new IOException("SHARED_AUDIT_UNAVAILABLE"));}
    }
    Map<String,Object> page(long after,long through,int limit)throws AuditStore.Unavailable{
        if(after<0||through<0||limit<1||limit>1000)throw new IllegalArgumentException("AUDIT_QUERY_INVALID");
        try(Connection c=db.connect()){
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);Head h=head(c,false);
            if(after>0&&after<h.anchor)throw new TaskService.Conflict("AUDIT_HISTORY_EXPIRED");long begin=Math.max(after,h.anchor),end=through==0?h.last:through;if(end>h.last||begin>end)throw new TaskService.Conflict("AUDIT_HISTORY_CHANGED");
            String previous=hashAt(c,begin,h),firstPrevious=previous,throughHash=hashAt(c,end,h);long next=begin;List<Map<String,Object>> events=new ArrayList<>();
            try(PreparedStatement p=db.prepare(c,"SELECT * FROM "+db.table("audit_event")+" WHERE cluster_id=? AND event_seq>? AND event_seq<=? ORDER BY event_seq LIMIT ?",cluster,begin,end,limit);ResultSet r=p.executeQuery()){
                while(r.next()){AuditStore.Event event=verify(r,next+1,previous);next=event.sequence;previous=r.getString("event_hash");Map<String,Object> row=Model.JSON.readValue(r.getString("payload"),LinkedHashMap.class);row.put("previousHash",r.getString("previous_hash"));row.put("hash",previous);events.add(row);}
            }
            if(next<end&&events.size()<limit)throw new IOException("AUDIT_CHAIN_INVALID");Map<String,Object> result=new LinkedHashMap<>();result.put("storeId",h.store);result.put("events",events);result.put("after",begin);result.put("through",end);result.put("throughHash",throughHash);result.put("next",next<end?next:null);result.put("anchorSequence",h.anchor);result.put("anchorHash",h.anchorHash);result.put("previousHash",firstPrevious);result.put("lastSequence",h.last);result.put("lastHash",h.lastHash);result.put("retentionEvents",retention);result.put("authority","SHARED");c.commit();return result;
        }catch(TaskService.Conflict|IllegalArgumentException failure){throw failure;}catch(Exception failure){throw new AuditStore.Unavailable(new IOException("SHARED_AUDIT_UNAVAILABLE"));}
    }
}
