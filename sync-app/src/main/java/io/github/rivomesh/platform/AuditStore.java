package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Local durable security events. Retained rows form a verified chain rooted in an explicit retention anchor. */
final class AuditStore implements AutoCloseable {
    static final String ZERO=String.join("",Collections.nCopies(64,"0"));
    static final class Unavailable extends IOException {Unavailable(Throwable cause){super("AUDIT_UNAVAILABLE",cause);}}
    public static final class Event {
        public int version=1;
        public long sequence;
        public String id=UUID.randomUUID().toString(),at=Instant.now().toString(),kind,correlation,clientRequestHash;
        public String actorId,actorRole,action,taskId,operationHash,peer,status,code;
        public Integer httpStatus;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String subjectId,accessIdentity,accessRevision;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public Map<String,Object> migrationAnchor;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT)
        public long accessVersion;
    }
    private final Connection connection;
    private final int retention;
    private String identity;
    String identity(){return identity;}
    private boolean closed;
    AuditStore(Path data,int retention)throws IOException{
        this.retention=retention;Connection opened=null;
        try{
            Path directory=data.resolve("audit");PrivateFiles.directory(directory);
            try{Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"));}catch(UnsupportedOperationException ignored){}
            Path file=directory.resolve("events").toAbsolutePath().normalize();
            if(file.toString().contains(";")||Files.isSymbolicLink(directory.resolve("events.mv.db")))throw new IOException("AUDIT_PATH_INVALID");
            opened=DriverManager.getConnection("jdbc:h2:file:"+file+";DB_CLOSE_ON_EXIT=FALSE;WRITE_DELAY=0","sa","");
            connection=opened;PrivateFiles.ownerOnly(directory.resolve("events.mv.db"));
            try(Statement sql=connection.createStatement()){
                sql.execute("CREATE TABLE IF NOT EXISTS rm_audit_state (id INT PRIMARY KEY, anchor_seq BIGINT NOT NULL, anchor_hash VARCHAR(64) NOT NULL, last_seq BIGINT NOT NULL, last_hash VARCHAR(64) NOT NULL, store_id VARCHAR(36) NOT NULL)");
                sql.execute("CREATE TABLE IF NOT EXISTS rm_audit_event (event_seq BIGINT PRIMARY KEY, event_key VARCHAR(64) NOT NULL UNIQUE, payload VARCHAR(8192) NOT NULL, previous_hash VARCHAR(64) NOT NULL, event_hash VARCHAR(64) NOT NULL)");
                sql.execute("CREATE TABLE IF NOT EXISTS rm_access_anchor (id INT PRIMARY KEY,directory_id VARCHAR(36),directory_version BIGINT NOT NULL,directory_revision VARCHAR(36))");
                sql.execute("INSERT INTO rm_access_anchor SELECT 1,NULL,0,NULL WHERE NOT EXISTS (SELECT 1 FROM rm_access_anchor)");
                sql.execute("INSERT INTO rm_audit_state SELECT 1,0,'"+ZERO+"',0,'"+ZERO+"','"+UUID.randomUUID().toString()+"' WHERE NOT EXISTS (SELECT 1 FROM rm_audit_state)");
            }
            verify();
        }catch(Exception failure){if(opened!=null)try{opened.close();}catch(SQLException ignored){}throw new Unavailable(failure);}
    }
    static String hash(String value){try{byte[] bytes=MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));char[] result=new char[bytes.length*2];final char[] hex="0123456789abcdef".toCharArray();for(int i=0;i<bytes.length;i++){int b=bytes[i]&255;result[i*2]=hex[b>>>4];result[i*2+1]=hex[b&15];}return new String(result);}catch(java.security.NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}}
    private long anchor,last;private String anchorHash,lastHash;
    private void head()throws SQLException,IOException{
        try(Statement sql=connection.createStatement();ResultSet row=sql.executeQuery("SELECT anchor_seq,anchor_hash,last_seq,last_hash,store_id FROM rm_audit_state WHERE id=1")){
            if(!row.next())throw new IOException("AUDIT_CHAIN_INVALID");anchor=row.getLong(1);anchorHash=row.getString(2);last=row.getLong(3);lastHash=row.getString(4);identity=row.getString(5);
            if(identity==null||!identity.matches("[a-f0-9-]{36}")||anchor<0||last<anchor||!anchorHash.matches("[a-f0-9]{64}")||!lastHash.matches("[a-f0-9]{64}"))throw new IOException("AUDIT_CHAIN_INVALID");
        }
    }
    private static String chain(long sequence,String previous,String payload){return hash(sequence+"\n"+previous+"\n"+payload);}
    private Event event(ResultSet row,long expected,String previous)throws Exception{
        long sequence=row.getLong("event_seq");String payload=row.getString("payload"),rowHash=row.getString("event_hash");
        if(sequence!=expected||!previous.equals(row.getString("previous_hash"))||!chain(sequence,previous,payload).equals(rowHash))throw new IOException("AUDIT_CHAIN_INVALID");
        Event value=Model.JSON.readValue(payload,Event.class);if(value.version!=1||value.sequence!=sequence||!value.id.equals(row.getString("event_key")))throw new IOException("AUDIT_CHAIN_INVALID");return value;
    }
    private void verify()throws Exception{
        head();long expected=anchor+1;String previous=anchorHash;
        try(Statement sql=connection.createStatement();ResultSet rows=sql.executeQuery("SELECT * FROM rm_audit_event ORDER BY event_seq")){
            while(rows.next()){event(rows,expected++,previous);previous=rows.getString("event_hash");}
        }
        if(expected!=last+1||!previous.equals(lastHash))throw new IOException("AUDIT_CHAIN_INVALID");
    }
    synchronized void append(Event input)throws Unavailable{
        try{
            if(closed)throw new IOException("AUDIT_CLOSED");
            if(input==null||input.id==null||!input.id.matches("[a-f0-9-]{36}")||input.sequence!=0)throw new IOException("AUDIT_EVENT_INVALID");
            connection.setAutoCommit(false);head();
            try(PreparedStatement find=connection.prepareStatement("SELECT payload FROM rm_audit_event WHERE event_key=?")){
                find.setString(1,input.id);try(ResultSet row=find.executeQuery()){if(row.next()){Event saved=Model.JSON.readValue(row.getString(1),Event.class),requested=Model.JSON.readValue(Model.JSON.writeValueAsBytes(input),Event.class);requested.sequence=saved.sequence;if(!Model.JSON.valueToTree(saved).equals(Model.JSON.valueToTree(requested)))throw new IOException("AUDIT_EVENT_COLLISION");connection.commit();return;}}
            }
            Event value=Model.JSON.readValue(Model.JSON.writeValueAsBytes(input),Event.class);value.sequence=last+1;
            String payload=Model.JSON.writeValueAsString(value);if(payload.length()>8192)throw new IOException("AUDIT_EVENT_TOO_LARGE");String next=chain(value.sequence,lastHash,payload);
            try(PreparedStatement insert=connection.prepareStatement("INSERT INTO rm_audit_event VALUES(?,?,?,?,?)")){
                insert.setLong(1,value.sequence);insert.setString(2,value.id);insert.setString(3,payload);insert.setString(4,lastHash);insert.setString(5,next);insert.executeUpdate();
            }
            if(value.accessVersion>0){
                Map<String,Object> anchor=accessAnchor();long version=((Number)anchor.get("version")).longValue();
                if(value.accessVersion!=version+1||version>0&&!Objects.equals(value.accessIdentity,anchor.get("identity"))||value.accessIdentity==null||!value.accessIdentity.matches("[a-f0-9-]{36}")||value.accessRevision==null||!value.accessRevision.matches("[a-f0-9-]{36}"))throw new IOException("ACCOUNT_AUDIT_ANCHOR_MISMATCH");
                try(PreparedStatement update=connection.prepareStatement("UPDATE rm_access_anchor SET directory_id=?,directory_version=?,directory_revision=? WHERE id=1")){update.setString(1,value.accessIdentity);update.setLong(2,value.accessVersion);update.setString(3,value.accessRevision);if(update.executeUpdate()!=1)throw new IOException("ACCOUNT_AUDIT_ANCHOR_MISMATCH");}
            }
            long cut=value.sequence-retention;
            if(cut>anchor){
                try(PreparedStatement find=connection.prepareStatement("SELECT event_hash FROM rm_audit_event WHERE event_seq=?")){find.setLong(1,cut);try(ResultSet row=find.executeQuery()){if(!row.next())throw new IOException("AUDIT_CHAIN_INVALID");anchorHash=row.getString(1);anchor=cut;}}
                try(PreparedStatement delete=connection.prepareStatement("DELETE FROM rm_audit_event WHERE event_seq<=?")){delete.setLong(1,cut);delete.executeUpdate();}
            }
            try(PreparedStatement update=connection.prepareStatement("UPDATE rm_audit_state SET anchor_seq=?,anchor_hash=?,last_seq=?,last_hash=? WHERE id=1")){
                update.setLong(1,anchor);update.setString(2,anchorHash);update.setLong(3,value.sequence);update.setString(4,next);if(update.executeUpdate()!=1)throw new IOException("AUDIT_CHAIN_INVALID");
            }
            connection.commit();
        }catch(Exception failure){try{connection.rollback();}catch(SQLException ignored){}throw new Unavailable(failure);}
        finally{try{connection.setAutoCommit(true);}catch(SQLException ignored){}}
    }
    synchronized Map<String,Object> accessAnchor()throws SQLException,IOException{
        try(Statement statement=connection.createStatement();ResultSet row=statement.executeQuery("SELECT directory_id,directory_version,directory_revision FROM rm_access_anchor WHERE id=1")){
            if(!row.next())throw new IOException("ACCOUNT_AUDIT_ANCHOR_MISSING");Map<String,Object> result=new LinkedHashMap<>();result.put("identity",row.getString(1));result.put("version",row.getLong(2));result.put("revision",row.getString(3));return result;
        }
    }
    synchronized Map<String,Object> page(long after,long through,int limit)throws Unavailable{
        try{
            if(closed)throw new IOException("AUDIT_CLOSED");head();
            if(after<0||through<0||limit<1||limit>1000)throw new IllegalArgumentException("AUDIT_QUERY_INVALID");
            if(after>0&&after<anchor)throw new TaskService.Conflict("AUDIT_HISTORY_EXPIRED");
            long begin=Math.max(after,anchor),end=through==0?last:through;if(end>last||begin>end)throw new TaskService.Conflict("AUDIT_HISTORY_CHANGED");
            String previous=anchorHash;if(begin>anchor)try(PreparedStatement find=connection.prepareStatement("SELECT event_hash FROM rm_audit_event WHERE event_seq=?")){find.setLong(1,begin);try(ResultSet row=find.executeQuery()){if(!row.next())throw new IOException("AUDIT_CHAIN_INVALID");previous=row.getString(1);}}
            String throughHash=end==last?lastHash:anchorHash;
            if(end>anchor&&end!=last)try(PreparedStatement find=connection.prepareStatement("SELECT event_hash FROM rm_audit_event WHERE event_seq=?")){find.setLong(1,end);try(ResultSet row=find.executeQuery()){if(!row.next())throw new IOException("AUDIT_CHAIN_INVALID");throughHash=row.getString(1);}}
            String firstPrevious=previous;List<Map<String,Object>> events=new ArrayList<>();long next=begin;
            try(PreparedStatement query=connection.prepareStatement("SELECT * FROM rm_audit_event WHERE event_seq>? AND event_seq<=? ORDER BY event_seq LIMIT ?")){
                query.setLong(1,begin);query.setLong(2,end);query.setInt(3,limit);try(ResultSet rows=query.executeQuery()){while(rows.next()){Event item=event(rows,next+1,previous);next=item.sequence;previous=rows.getString("event_hash");Map<String,Object> row=Model.JSON.readValue(rows.getString("payload"),LinkedHashMap.class);row.put("previousHash",rows.getString("previous_hash"));row.put("hash",previous);events.add(row);}}
            }
            if(next<end&&events.size()<limit)throw new IOException("AUDIT_CHAIN_INVALID");
            Map<String,Object> result=new LinkedHashMap<>();result.put("storeId",identity);result.put("events",events);result.put("after",begin);result.put("through",end);result.put("throughHash",throughHash);result.put("next",next<end?next:null);result.put("anchorSequence",anchor);result.put("anchorHash",anchorHash);result.put("previousHash",firstPrevious);result.put("lastSequence",last);result.put("lastHash",lastHash);result.put("retentionEvents",retention);return result;
        }catch(TaskService.Conflict|IllegalArgumentException failure){throw failure;}catch(Exception failure){throw new Unavailable(failure);}
    }
    @Override public synchronized void close()throws IOException{if(!closed){closed=true;try{connection.close();}catch(SQLException failure){throw new Unavailable(failure);}}}
}
