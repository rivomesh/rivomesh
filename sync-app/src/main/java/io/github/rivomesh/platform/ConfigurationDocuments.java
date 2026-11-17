package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import io.github.rivomesh.Model;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** No secret values leave the controller; retained fields carry scoped, opaque references. */
final class ConfigurationDocuments {
    private static final String KEEP="$rivomeshSecret";
    private static final Pattern SECRET=Pattern.compile("(?i).*(password|passwd|pwd|token|secret|credential|authorization|jaas|private.?key|api.?key).*");
    private static final Pattern URL_SECRET=Pattern.compile("(?i)(?:^|[?&;])[a-z0-9_.-]*(?:password|passwd|pwd|token|secret|credential|authorization|jaas|private.?key|api.?key)[a-z0-9_.-]*\\s*=");
    private static final Pattern USER_INFO=Pattern.compile("(?i)^(?:jdbc:)?[a-z][a-z0-9+.-]*://[^/@\\s]+@");
    private final byte[] key=new byte[32];
    ConfigurationDocuments(){new SecureRandom().nextBytes(key);}
    ConfigurationDocuments(byte[] sharedKey){if(sharedKey==null||sharedKey.length!=key.length)throw new IllegalArgumentException("CONFIG_REFERENCE_KEY_INVALID");System.arraycopy(sharedKey,0,key,0,key.length);}
    static String hash(byte[] bytes){try{byte[] digest=MessageDigest.getInstance("SHA-256").digest(bytes);StringBuilder value=new StringBuilder();for(byte b:digest)value.append(String.format(Locale.ROOT,"%02x",b&255));return value.toString();}catch(NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}}
    String signature(String value){try{Mac mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(key,"HmacSHA256"));byte[] digest=mac.doFinal(value.getBytes(StandardCharsets.UTF_8));StringBuilder out=new StringBuilder();for(byte b:digest)out.append(String.format(Locale.ROOT,"%02x",b&255));return out.toString();}catch(GeneralSecurityException impossible){throw new IllegalStateException(impossible);}}
    static void validate(JsonNode node){int[] count={0};validate(node,0,count);}
    private static void validate(JsonNode node,int depth,int[] count){if(node==null||depth>64||++count[0]>100000)throw new IllegalArgumentException("CONFIG_DOCUMENT_LIMIT");if(node.isContainerNode())for(JsonNode child:node)validate(child,depth+1,count);}
    static boolean sensitive(String name,JsonNode value){if(SECRET.matcher(name).matches())return true;if(!value.isTextual())return false;String text=value.asText();for(int i=0;i<3;i++){if(URL_SECRET.matcher(text).find()||USER_INFO.matcher(text).find())return true;try{String decoded=java.net.URLDecoder.decode(text,"UTF-8");if(decoded.equals(text))break;text=decoded;}catch(Exception invalid){return name.toLowerCase(Locale.ROOT).contains("url")||name.toLowerCase(Locale.ROOT).contains("options");}}return false;}
    private static String pointer(String parent,String name){return parent+"/"+name.replace("~","~0").replace("/","~1");}
    private static String scope(JsonNode root,String path){if(path.startsWith("/targets/")){String[] parts=path.split("/",4);try{return root.path("targets").path(Integer.parseInt(parts[2])).path("id").asText();}catch(RuntimeException invalid){return "";}}return "";}
    JsonNode redact(JsonNode root,String task,String revision){validate(root);JsonNode copy=root.deepCopy();redact(copy,copy,"",task,revision);return copy;}
    private void redact(JsonNode root,JsonNode node,String path,String task,String revision){
        if(node.isObject()){ObjectNode object=(ObjectNode)node;List<String> names=new ArrayList<>();object.fieldNames().forEachRemaining(names::add);for(String name:names){JsonNode value=object.get(name);String next=pointer(path,name);if(!value.isNull()&&sensitive(name,value)){ObjectNode reference=Model.JSON.createObjectNode();reference.put(KEEP,signature("secret\n"+task+"\n"+revision+"\n"+next+"\n"+scope(root,next)));object.set(name,reference);}else redact(root,value,next,task,revision);}}
        else if(node.isArray())for(int i=0;i<node.size();i++)redact(root,node.get(i),path+"/"+i,task,revision);
    }
    JsonNode restore(JsonNode submitted,JsonNode original,String task,String revision){validate(submitted);JsonNode copy=submitted.deepCopy();return restore(copy,copy,original,"",task,revision);}
    private JsonNode restore(JsonNode root,JsonNode node,JsonNode original,String path,String task,String revision){
        if(node.isObject()&&node.has(KEEP)){String expected=signature("secret\n"+task+"\n"+revision+"\n"+path+"\n"+scope(root,path));JsonNode old=original.at(path);if(node.size()!=1||!node.get(KEEP).isTextual()||!MessageDigest.isEqual(expected.getBytes(StandardCharsets.US_ASCII),node.get(KEEP).asText().getBytes(StandardCharsets.UTF_8))||old.isMissingNode()||!scope(root,path).equals(scope(original,path)))throw new TaskService.Conflict("CONFIG_SECRET_REFERENCE_CHANGED");return old.deepCopy();}
        if(node.isObject()){ObjectNode object=(ObjectNode)node;List<String> names=new ArrayList<>();object.fieldNames().forEachRemaining(names::add);for(String name:names)object.set(name,restore(root,object.get(name),original,pointer(path,name),task,revision));}
        else if(node.isArray()){ArrayNode array=(ArrayNode)node;for(int i=0;i<array.size();i++)array.set(i,restore(root,array.get(i),original,path+"/"+i,task,revision));}return node;
    }
    static List<String> changes(JsonNode before,JsonNode after){List<String> result=new ArrayList<>();changes(before,after,"",result);return result;}
    private static void changes(JsonNode a,JsonNode b,String path,List<String> result){if(Objects.equals(a,b))return;if(a!=null&&b!=null&&a.isObject()&&b.isObject()){Set<String> names=new TreeSet<>();a.fieldNames().forEachRemaining(names::add);b.fieldNames().forEachRemaining(names::add);for(String name:names)changes(a.get(name),b.get(name),pointer(path,name),result);}else if(a!=null&&b!=null&&a.isArray()&&b.isArray()&&a.size()==b.size()){for(int i=0;i<a.size();i++)changes(a.get(i),b.get(i),path+"/"+i,result);}else if(result.size()<200)result.add(path);}
}
