package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.Model;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/** Versioned local identities. Only token digests are durable; callers retain newly issued tokens. */
final class AccountDirectory {
    static final int MAX_USERS=200,MAX_RECEIPTS=500;
    public static final class User {
        public String id,tokenHash,updatedAt;
        public PlatformConfig.Role role;
        public boolean enabled=true,deleted;
        public List<String> taskIds;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public List<String> nodeIds;
    }
    public static final class Receipt {public String signature,actorId,userId,action;}
    public static final class State {
        public int formatVersion=1;
        public String identity,revision;
        public long version;
        public boolean managed;
        public Map<String,User> users=new LinkedHashMap<>();
        public Map<String,Receipt> requests=new LinkedHashMap<>();
    }
    static final class Change {State state;AuditStore.Event audit;String userId;boolean replayed;}
    private AccountDirectory(){}
    static String hash(String value){return AuditStore.hash(value);}
    static String tokenHash(String token,boolean managed){
        if(token==null||token.length()>(4096)||token.length()<(managed?32:24)||!token.chars().allMatch(c->c>=33&&c<=126)||token.contains("${"))throw new IllegalArgumentException("ACCOUNT_TOKEN_INVALID");return hash(token);
    }
    static State configured(PlatformConfig config){
        State s=new State();
        if(config.authToken!=null)add(s,"legacy-admin",PlatformConfig.Role.ADMIN,config.authToken,null);
        for(PlatformConfig.User user:config.users)add(s,user.id,user.role,user.token,user.taskIds);
        s.revision=hash(encode(s.users));return s;
    }
    private static void add(State s,String id,PlatformConfig.Role role,String token,List<String> tasks){User u=new User();u.id=id;u.role=role;u.tokenHash=tokenHash(token,false);u.taskIds=tasks==null?null:new ArrayList<>(tasks);s.users.put(id,u);}
    private static String encode(Object value){try{return Model.JSON.writeValueAsString(value);}catch(Exception invalid){throw new IllegalArgumentException("ACCOUNT_STATE_INVALID");}}
    static AccessControl.Principal authenticate(State s,String header){
        if(header==null||!header.startsWith("Bearer ")||header.length()>4103)return null;
        byte[] requested=hash(header.substring(7)).getBytes(StandardCharsets.US_ASCII);AccessControl.Principal found=null;
        for(User u:s.users.values())if(MessageDigest.isEqual(u.tokenHash.getBytes(StandardCharsets.US_ASCII),requested)&&u.enabled&&!u.deleted)found=new AccessControl.Principal(u.id,u.role,u.taskIds,s.revision,u.nodeIds);
        return found;
    }
    static void current(State state,AccessControl.Principal actor){
        if(actor!=null&&actor.authorizationRevision!=null&&!Objects.equals(actor.authorizationRevision,state.revision))throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
    }
    static void validate(State state){
        if(state==null||state.formatVersion!=1||!state.managed||state.identity==null||!state.identity.matches("[a-f0-9-]{36}")||state.revision==null||!state.revision.matches("[a-f0-9-]{36}")||state.version<1||state.users==null||state.users.size()>MAX_USERS||state.requests==null||state.requests.size()>MAX_RECEIPTS)throw new IllegalArgumentException("ACCOUNT_STATE_INVALID");
        Set<String> hashes=new HashSet<>();boolean admin=false;
        for(Map.Entry<String,User> entry:state.users.entrySet()){
            User u=entry.getValue();if(u==null||!entry.getKey().equals(u.id)||!id(u.id)||u.role==null||u.tokenHash==null||!u.tokenHash.matches("[a-f0-9]{64}")||!hashes.add(u.tokenHash)||u.deleted&&u.enabled)throw new IllegalArgumentException("ACCOUNT_STATE_INVALID");
            scope(u.role,u.taskIds);nodeScope(u.role,u.nodeIds);if(u.enabled&&!u.deleted&&u.role==PlatformConfig.Role.ADMIN)admin=true;
        }
        if(!admin)throw new TaskService.Conflict("ACCOUNT_LAST_ADMIN");
        for(Map.Entry<String,Receipt> entry:state.requests.entrySet()){Receipt r=entry.getValue();if(!requestId(entry.getKey())||r==null||r.signature==null||!r.signature.matches("[a-f0-9]{64}")||!id(r.actorId)||r.userId!=null&&!id(r.userId)||!Arrays.asList("ACTIVATE","CREATE","UPDATE","TOKEN","DELETE").contains(r.action))throw new IllegalArgumentException("ACCOUNT_STATE_INVALID");}
    }
    private static boolean id(String id){return id!=null&&id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");}
    private static boolean requestId(String id){return id!=null&&id.matches("[A-Za-z0-9_-]{8,128}");}
    private static void scope(PlatformConfig.Role role,List<String> ids){
        if(ids==null)return;if(role==PlatformConfig.Role.ADMIN||ids.size()>10000)throw new IllegalArgumentException("ACCOUNT_SCOPE_INVALID");Set<String> unique=new HashSet<>();for(String id:ids)if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||!unique.add(id))throw new IllegalArgumentException("ACCOUNT_SCOPE_INVALID");
    }
    private static void nodeScope(PlatformConfig.Role role,List<String> ids){
        if(ids==null)return;if(role==PlatformConfig.Role.ADMIN||ids.size()>10000)throw new IllegalArgumentException("ACCOUNT_NODE_SCOPE_INVALID");Set<String> unique=new HashSet<>();for(String id:ids)if(!id(id)||!unique.add(id))throw new IllegalArgumentException("ACCOUNT_NODE_SCOPE_INVALID");
    }
    private static User user(String id,JsonNode body,User previous){
        if(!id(id))throw new IllegalArgumentException("ACCOUNT_ID_INVALID");User u=new User();u.id=id;
        try{u.role=PlatformConfig.Role.valueOf(body.path("role").asText(""));}catch(Exception invalid){throw new IllegalArgumentException("ACCOUNT_ROLE_INVALID");}
        if(!body.path("enabled").isBoolean())throw new IllegalArgumentException("ACCOUNT_ENABLED_REQUIRED");u.enabled=body.path("enabled").asBoolean();
        JsonNode tasks=body.get("taskIds");if(tasks!=null&&!tasks.isNull()){if(!tasks.isArray())throw new IllegalArgumentException("ACCOUNT_SCOPE_INVALID");u.taskIds=new ArrayList<>();for(JsonNode task:tasks){if(!task.isTextual())throw new IllegalArgumentException("ACCOUNT_SCOPE_INVALID");u.taskIds.add(task.asText());}scope(u.role,u.taskIds);Collections.sort(u.taskIds);}
        JsonNode nodes=body.get("nodeIds");if(nodes==null){u.nodeIds=u.role==PlatformConfig.Role.ADMIN||previous==null||previous.nodeIds==null?null:new ArrayList<>(previous.nodeIds);}
        else if(!nodes.isNull()){if(!nodes.isArray())throw new IllegalArgumentException("ACCOUNT_NODE_SCOPE_INVALID");u.nodeIds=new ArrayList<>();for(JsonNode node:nodes){if(!node.isTextual())throw new IllegalArgumentException("ACCOUNT_NODE_SCOPE_INVALID");u.nodeIds.add(node.asText());}nodeScope(u.role,u.nodeIds);Collections.sort(u.nodeIds);}
        if(previous==null){if(!body.path("token").isTextual())throw new IllegalArgumentException("ACCOUNT_TOKEN_INVALID");u.tokenHash=tokenHash(body.path("token").asText(),true);}else{if(body.has("token"))throw new IllegalArgumentException("ACCOUNT_TOKEN_ROUTE_REQUIRED");u.tokenHash=previous.tokenHash;}
        u.updatedAt=Instant.now().toString();return u;
    }
    static Change mutate(State current,String action,String pathId,JsonNode body,String request,AccessControl.Principal actor)throws Exception{
        if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");current(current,actor);
        if(!requestId(request)||body==null||!body.isObject())throw new IllegalArgumentException("ACCOUNT_REQUEST_INVALID");
        Set<String> allowed=new HashSet<>(Arrays.asList("expectedRevision"));if(action.equals("CREATE"))allowed.addAll(Arrays.asList("id","role","enabled","taskIds","nodeIds","token"));if(action.equals("UPDATE"))allowed.addAll(Arrays.asList("role","enabled","taskIds","nodeIds"));if(action.equals("TOKEN"))allowed.add("token");Iterator<String> names=body.fieldNames();while(names.hasNext())if(!allowed.contains(names.next()))throw new IllegalArgumentException("ACCOUNT_REQUEST_INVALID");
        String signature=hash(actor.id+":"+actor.role+":"+action+":"+pathId+":"+encode(body));Receipt old=current.requests.get(request);
        if(old!=null){if(!actor.id.equals(old.actorId)||!signature.equals(old.signature))throw new TaskService.Conflict("REQUEST_ID_REUSED");Change result=new Change();result.state=current;result.userId=old.userId;result.replayed=true;return result;}
        if(!body.path("expectedRevision").isTextual()||!Objects.equals(body.path("expectedRevision").asText(),current.revision))throw new TaskService.Conflict("ACCOUNTS_REVISION_CHANGED");
        if(!action.equals("ACTIVATE")&&!current.managed)throw new TaskService.Conflict("ACCOUNT_MANAGEMENT_NOT_ENABLED");
        State next=Model.JSON.readValue(Model.JSON.writeValueAsBytes(current),State.class);String subject=pathId;
        if(action.equals("ACTIVATE")){
            if(current.managed)throw new TaskService.Conflict("ACCOUNT_MANAGEMENT_ALREADY_ENABLED");next.managed=true;next.identity=UUID.randomUUID().toString();
        }else if(action.equals("CREATE")){
            subject=body.path("id").asText(null);if(next.users.containsKey(subject))throw new TaskService.Conflict("ACCOUNT_EXISTS");if(next.users.size()>=MAX_USERS)throw new TaskService.Conflict("ACCOUNT_LIMIT");next.users.put(subject,user(subject,body,null));
        }else{
            User previous=next.users.get(subject);if(previous==null)throw new NoSuchElementException("ACCOUNT_NOT_FOUND");if(previous.deleted)throw new TaskService.Conflict("ACCOUNT_DELETED");
            if(action.equals("UPDATE"))next.users.put(subject,user(subject,body,previous));
            else if(action.equals("TOKEN")){if(!body.path("token").isTextual())throw new IllegalArgumentException("ACCOUNT_TOKEN_INVALID");previous.tokenHash=tokenHash(body.path("token").asText(),true);previous.updatedAt=Instant.now().toString();}
            else if(action.equals("DELETE")){previous.enabled=false;previous.deleted=true;previous.updatedAt=Instant.now().toString();}
            else throw new IllegalArgumentException("ACCOUNT_ACTION_INVALID");
        }
        if(subject!=null){User changed=next.users.get(subject);if(changed!=null)for(User other:next.users.values())if(!other.id.equals(subject)&&other.tokenHash.equals(changed.tokenHash))throw new TaskService.Conflict("ACCOUNT_TOKEN_COLLISION");}
        next.version=current.version+1;next.revision=UUID.randomUUID().toString();Receipt receipt=new Receipt();receipt.actorId=actor.id;receipt.signature=signature;receipt.userId=subject;receipt.action=action;next.requests.put(request,receipt);while(next.requests.size()>MAX_RECEIPTS)next.requests.remove(next.requests.keySet().iterator().next());validate(next);
        AuditStore.Event event=event(next,action,subject,actor);event.clientRequestHash=hash(request);event.operationHash=signature;
        Change result=new Change();result.state=next;result.audit=event;result.userId=subject;return result;
    }
    static AuditStore.Event event(State state,String action,String subject,AccessControl.Principal actor){AuditStore.Event e=new AuditStore.Event();e.kind="ACCOUNT_COMMITTED";e.action="ACCOUNT_"+action;e.subjectId=subject;e.actorId=actor==null?"bootstrap":actor.id;e.actorRole=actor==null?"ADMIN":actor.role.name();e.accessIdentity=state.identity;e.accessRevision=state.revision;e.accessVersion=state.version;e.status="COMMITTED";return e;}
    static Map<String,Object> view(State state){Map<String,Object> result=new LinkedHashMap<>();result.put("mode",state.managed?"MANAGED":"CONFIG");result.put("revision",state.revision);result.put("version",state.version);List<Map<String,Object>> users=new ArrayList<>();for(User u:state.users.values()){Map<String,Object> row=new LinkedHashMap<>();row.put("id",u.id);row.put("role",u.role.name());row.put("enabled",u.enabled);row.put("deleted",u.deleted);row.put("taskIds",u.taskIds==null?null:new ArrayList<>(u.taskIds));row.put("nodeIds",u.nodeIds==null?null:new ArrayList<>(u.nodeIds));row.put("updatedAt",u.updatedAt);users.add(row);}result.put("users",users);return result;}
}
