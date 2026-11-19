package io.github.rivomesh.platform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.Model;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.KafkaNoticePlanning.Prepared;
import io.github.rivomesh.cdc.KafkaRecoveryReview;
import io.github.rivomesh.recovery.FailureEvidence;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** A bounded current observation of every managed Kafka route eligible for configuration restore. */
final class KafkaNoticeCatalog {
    private static final int MAX_TASKS=4,MAX_VERSIONS=8,MAX_ROUTES=12;
    private static final ObjectMapper YAML=new ObjectMapper(new YAMLFactory())
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Set<String> READ_UNAVAILABLE=Set.of("OWNER_QUERY_REJECTED","OWNER_QUERY_FAILED","OWNER_QUERY_UNAVAILABLE",
            "OWNER_QUERY_TIMEOUT","OWNER_QUERY_EXPIRED","OWNER_QUERY_NOT_FOUND","OWNER_QUERY_CAPACITY");

    static final class Limit extends IllegalStateException {
        Limit(){super("KAFKA_NOTICE_CATALOG_REVIEW_LIMIT");}
    }

    record Revision(String id,String yaml,List<String> kafkaTargets) {}
    @FunctionalInterface
    interface VersionParser { PipelineConfig parse(String yaml)throws Exception; }
    record Selection(String task,String revision,String owner,String directory,String active,List<Revision> versions) {
        String digest(){return Codec.sha256(Codec.encode(this));}
        int routes(){return versions.stream().mapToInt(item->item.kafkaTargets().size()).sum();}
    }
    private record Snapshot(List<Selection> selections) {
        String digest(){return Codec.sha256(Codec.encode(selections.stream().map(Selection::digest).toList()));}
    }

    private KafkaNoticeCatalog(){}

    static Map<String,Object> coverage(GlobalTaskStore tasks,OwnerQueries queries,Prepared plan,AccessControl.Principal actor)throws Exception{
        Snapshot before;
        try{before=snapshot(tasks,plan,actor);}
        catch(Limit exceeded){return incomplete(exceeded.getMessage());}
        var selected=plan.task();
        var identity=plan.catalogIdentity();
        String target=plan.controlTopic().targetId();
        String selectedKey=key(selected.configurationVersion(),target);
        Map<String,String> nativeRoutes=new TreeMap<>();
        nativeRoutes.put(selected.taskId()+"/"+selectedKey,identity.businessTopicSha256());
        Map<Selection,String> probes=new LinkedHashMap<>();
        boolean liveMissing=false,retainedMissing=false,collision=false;
        for(Selection selection:before.selections()){
            boolean selectedTask=selection.task().equals(selected.taskId());
            if(selection.routes()==0||selectedTask&&selection.routes()==1)continue;
            TaskRequest request=TaskRequest.of(TaskRequest.Kind.KAFKA_CATALOG_PROBE,selection.task());
            request.revision=selection.revision();request.identityHash=selection.digest();
            try{probes.put(selection,queries.submit(request,actor));}
            catch(TaskService.Conflict blocked){
                if(!READ_UNAVAILABLE.contains(blocked.getMessage()))throw blocked;
                probes.put(selection,null);
            }catch(ControlStore.Fenced unavailable){probes.put(selection,null);}
        }
        for(var probe:probes.entrySet()){
            Selection selection=probe.getKey();
            Map<String,String> resolved=Map.of();
            try{
                if(probe.getValue()!=null)resolved=verified(queries.await(probe.getValue(),actor),selection);
            }catch(TaskService.Conflict blocked){
                if(!READ_UNAVAILABLE.contains(blocked.getMessage()))throw blocked;
            }catch(ControlStore.Fenced|java.util.NoSuchElementException unavailable){/* Evidence remains missing. */}
            for(Revision version:selection.versions())for(String configured:version.kafkaTargets()){
                String ref=key(version.id(),configured),resolvedIdentity=resolved.get(ref);
                boolean selectedRoute=selection.task().equals(selected.taskId())&&ref.equals(selectedKey);
                if(selectedRoute&&resolvedIdentity!=null&&!identity.businessTopicSha256().equals(resolvedIdentity))
                    throw new TaskService.Conflict("KAFKA_NOTICE_CATALOG_IDENTITY_CHANGED");
                if(selectedRoute)continue; // The paused predecessor supplied this exact route.
                if(resolvedIdentity==null){
                    if(version.id().equals(selection.active()))liveMissing=true;
                    else retainedMissing=true;
                }else if(identity.controlTopicSha256().equals(resolvedIdentity))collision=true;
                else nativeRoutes.put(selection.task()+"/"+ref,resolvedIdentity);
            }
        }
        try{
            if(!before.digest().equals(snapshot(tasks,plan,actor).digest()))throw changed();
        }catch(Limit exceeded){throw changed();}
        if(collision)return Map.of("status","CONFLICT","code","KAFKA_NOTICE_CATALOG_PHYSICAL_TOPIC_COLLISION");
        if(retainedMissing)return incomplete("KAFKA_NOTICE_CATALOG_RETAINED_REVISION_MISSING");
        if(liveMissing)return incomplete("KAFKA_NOTICE_CATALOG_MANAGED_TARGETS_MISSING");
        int versions=before.selections().stream().mapToInt(item->item.versions().size()).sum();
        int routes=before.selections().stream().mapToInt(Selection::routes).sum();
        if(routes!=nativeRoutes.size())throw changed();
        String cut=configurationCut(tasks,before);
        String routesProof=Codec.sha256(Codec.encode(List.of("kafka-notice-routes-v1",cut,identity.controlTopicSha256(),nativeRoutes)));
        if(versions==1&&routes==1)return Map.of("status","VERIFIED_SINGLETON","code","KAFKA_NOTICE_CATALOG_SINGLE_TASK_VERIFIED",
                "configurationCutSha256",cut,"physicalRoutesSha256",routesProof);
        return Map.of("status","VERIFIED_AT_REVIEW","code","KAFKA_NOTICE_CATALOG_RESTORABLE_ROUTES_VERIFIED",
                "configurationVersions",versions,"kafkaRoutes",routes,"configurationCutSha256",cut,"physicalRoutesSha256",routesProof);
    }

    /** Uses the same locked shared root as the approval insert. */
    static void requireConfigurationCut(GlobalTaskStore tasks,Connection c,GlobalTaskStore.Record selected,
                                        Prepared plan,String expected)throws Exception{
        try{
            if(!configurationCut(tasks,snapshot(tasks,c,selected,plan)).equals(expected))throw changed();
        }catch(Limit exceeded){throw changed();}
    }

    private static String configurationCut(GlobalTaskStore tasks,Snapshot snapshot){
        return Codec.sha256(tasks.commandCipher().signature("kafka-notice-catalog-cut-v1\n"+snapshot.digest()));
    }

    private static Snapshot snapshot(GlobalTaskStore tasks,Prepared plan,AccessControl.Principal actor)throws Exception{
        return tasks.transaction(plan.task().taskId(),actor,(c,selected,current)->snapshot(tasks,c,selected,plan));
    }

    private static Snapshot snapshot(GlobalTaskStore tasks,Connection c,GlobalTaskStore.Record selected,Prepared plan)throws Exception{
            var expected=plan.task();
            if(!selected.revision.equals(expected.revision())||!selected.owner.equals(expected.ownerNodeId())
                    ||!selected.directory.equals(expected.ownerDirectoryId())||!selected.configurationVersion.equals(expected.configurationVersion())
                    ||!selected.configurationVersion.equals(selected.activeConfigurationVersion))
                throw new TaskService.Conflict("REVISION_CHANGED");
            List<Selection> catalog=new ArrayList<>();
            int versions=0,routes=0;
            for(GlobalTaskStore.Record task:tasks.kafkaCatalogRecords(c,MAX_TASKS)){
                Selection selection=selection(tasks,c,task,MAX_VERSIONS-versions);
                versions+=selection.versions().size();routes+=selection.routes();
                if(routes>MAX_ROUTES)throw new Limit();
                if(task.id.equals(selected.id)&&selection.versions().stream().noneMatch(v->v.id().equals(selected.activeConfigurationVersion)
                        &&v.kafkaTargets().contains(plan.controlTopic().targetId())))throw changed();
                catalog.add(selection);
            }
            return new Snapshot(List.copyOf(catalog));
    }

    static Selection selection(GlobalTaskStore tasks,Connection c,GlobalTaskStore.Record task,int remaining)throws Exception{
        List<Revision> revisions=new ArrayList<>();
        for(GlobalTaskStore.Version version:tasks.kafkaCatalogVersions(c,task,remaining)){
            if(version.yaml.length()>512*1024)throw new Limit();
            JsonNode config=YAML.readTree(version.yaml);
            if(config==null||!config.isObject())throw changed();
            List<String> targetIds=new ArrayList<>();
            if(config.path("formatVersion").asInt()==3){
                if(!task.id.equals(config.path("pipelineId").asText())||!config.path("targets").isArray())throw changed();
                for(JsonNode target:config.path("targets")){
                    if(!"KAFKA".equalsIgnoreCase(target.path("product").asText()))continue;
                    String id=target.path("id").asText();
                    if(!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||targetIds.contains(id))throw changed();
                    targetIds.add(id);
                    if(targetIds.size()>MAX_ROUTES)throw new Limit();
                }
            }else if(config.has("formatVersion")||!task.id.equals(config.path("jobId").asText())||config.has("targets")){
                throw changed();
            }
            revisions.add(new Revision(version.id,version.yaml,List.copyOf(targetIds)));
        }
        return new Selection(task.id,task.revision,task.owner,task.directory,task.activeConfigurationVersion,List.copyOf(revisions));
    }

    static Selection probeInput(GlobalTaskStore tasks,Connection c,TaskRequest request)throws Exception{
        Selection selection=selection(tasks,c,tasks.record(c,request.taskId),MAX_VERSIONS);
        if(!selection.revision().equals(request.revision)||!selection.digest().equals(request.identityHash)
                ||selection.routes()<1||selection.routes()>MAX_ROUTES)throw changed();
        return selection;
    }

    static Map<String,Object> observe(Selection selection,VersionParser parser)throws InterruptedException{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(4);
        Map<String,String> seen=new TreeMap<>();
        for(Revision version:selection.versions()){
            if(version.kafkaTargets().isEmpty())continue;
            PipelineConfig pipeline;
            try{pipeline=parser.parse(version.yaml());}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw interrupted;}
            catch(Exception unavailable){continue;}
            for(var target:pipeline.targets){
                if(!version.kafkaTargets().contains(target.id)||System.nanoTime()>=deadline)continue;
                try{seen.put(key(version.id(),target.id),KafkaRecoveryReview.inspectTopicIdentity(target,deadline));}
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw interrupted;}
                catch(Exception unavailable){/* No result is evidence of an unresolved route. */}
            }
        }
        return Map.of("scopeSha256",selection.digest(),"topicIdentities",seen);
    }

    private static Map<String,String> verified(Map<String,Object> response,Selection selection){
        if(!selection.digest().equals(response.get("scopeSha256"))||!(response.get("topicIdentities") instanceof Map<?,?> input))throw changed();
        Map<String,String> result=new HashMap<>();
        for(Revision version:selection.versions())for(String target:version.kafkaTargets()){
            String key=key(version.id(),target);
            if(input.containsKey(key)){
                Object hash=input.get(key);
                if(!(hash instanceof String digest)||!FailureEvidence.hash(digest))throw changed();
                result.put(key,digest);
            }
        }
        if(result.size()!=input.size())throw changed();
        return result;
    }

    private static String key(String version,String target){return version+"/"+target;}
    private static TaskService.Conflict changed(){return new TaskService.Conflict("KAFKA_NOTICE_CATALOG_CHANGED");}
    private static Map<String,Object> incomplete(String code){return Map.of("status","INCOMPLETE","code",code);}
}
