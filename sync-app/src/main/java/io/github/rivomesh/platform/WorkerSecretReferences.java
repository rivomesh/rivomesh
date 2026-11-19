package io.github.rivomesh.platform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Optional Agent-local deployment grants, applied to unresolved task configuration. */
final class WorkerSecretReferences {
    private static final Pattern TASK=Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    private static final Pattern NAME=Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern REFERENCE=Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");
    private static final Set<String> JVM_OPTIONS=Set.of("JAVA_TOOL_OPTIONS","JDK_JAVA_OPTIONS","_JAVA_OPTIONS","CLASSPATH");
    private static final Set<String> LEGACY=Set.of("/source/password","/target/password");
    private static final Set<String> V3=Set.of("/source/password","/journal/password","/targets/*/password",
            "/targets/*/messaging/token","/source/oracle/root/password","/source/oracle/pdb/password");
    private static final ObjectMapper YAML=new ObjectMapper(new YAMLFactory())
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private WorkerSecretReferences(){}

    static void validate(Map<String,List<String>> grants,Set<String> protectedNames){
        if(grants==null)return;
        for(Map.Entry<String,List<String>> grant:grants.entrySet()){
            String task=grant.getKey();
            if(task==null||!TASK.matcher(task).matches()||grant.getValue()==null)
                throw new IllegalArgumentException("WORKER_SECRET_POLICY_INVALID");
            Set<String> names=new HashSet<>();
            for(String name:grant.getValue())
                if(name==null||!NAME.matcher(name).matches()||!names.add(name)
                        ||protectedName(name,protectedNames)||JVM_OPTIONS.contains(name)
                        ||name.startsWith("LD_")||name.startsWith("DYLD_"))
                    throw new IllegalArgumentException("WORKER_SECRET_POLICY_INVALID");
        }
    }

    static boolean protectedName(String name,Set<String> protectedNames){
        return protectedNames.contains(name)||name.equals("RIVOMESH_TOKEN")||name.equals("RIVOMESH_PREVIEW_TOKEN")
                ||name.equals(io.github.rivomesh.cdc.KafkaPhysicalReceipt.GRANT_ENV)
                ||name.startsWith("RIVOMESH_CONTROL_");
    }

    static Set<String> permitted(String yaml,Map<String,List<String>> grants)throws IOException{
        Set<String> references=new LinkedHashSet<>();
        if(grants==null){
            Matcher names=REFERENCE.matcher(yaml);
            while(names.find())references.add(names.group(1));
            return references;
        }
        JsonNode document;
        try{document=YAML.readTree(yaml);}
        catch(IOException malformed){throw new IllegalArgumentException("WORKER_SECRET_DOCUMENT_INVALID");}
        ConfigurationDocuments.validate(document);
        if(!document.isObject())throw new IllegalArgumentException("WORKER_SECRET_DOCUMENT_INVALID");
        boolean pipeline=document.path("formatVersion").asInt()==3;
        String task=document.path(pipeline?"pipelineId":"jobId").textValue();
        if(task==null||!TASK.matcher(task).matches())throw new IllegalArgumentException("WORKER_SECRET_DOCUMENT_INVALID");
        visit(document,"",pipeline?V3:LEGACY,new HashSet<>(grants.getOrDefault(task,List.of())),references);
        return references;
    }

    private static void visit(JsonNode item,String path,Set<String> credentialFields,Set<String> allowed,Set<String> found){
        if(item.isTextual()){
            Matcher reference=REFERENCE.matcher(item.textValue());
            while(reference.find()){
                String name=reference.group(1);
                if(!credentialFields.contains(path)||!allowed.contains(name))
                    throw new IllegalArgumentException("WORKER_SECRET_REFERENCE_DENIED");
                found.add(name);
            }
        }else if(item.isObject()){
            item.fields().forEachRemaining(field->visit(field.getValue(),path+"/"+
                    field.getKey().replace("~","~0").replace("/","~1").replace("*","~2"),
                    credentialFields,allowed,found));
        }else if(item.isArray()){
            for(JsonNode child:item)visit(child,path+"/*",credentialFields,allowed,found);
        }
    }
}
