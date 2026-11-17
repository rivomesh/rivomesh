package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.sql.Connection;
import java.util.*;

/** Shared task entry. Enabled only by the global runtime after migration/admission gates. */
final class GlobalTaskPlane implements TaskControlPlane {
    private final GlobalTaskStore tasks;
    private final DurableCommands commands;
    private final AgentDirectory agents;
    private final GlobalTaskViews views;
    private final OwnerQueries queries;
    private final String localNode;
    private final ConfigurationDocuments documents;
    private final boolean localExecution;
    GlobalTaskPlane(GlobalTaskStore tasks,DurableCommands commands,AgentDirectory agents,GlobalTaskViews views,OwnerQueries queries,String localNode){this(tasks,commands,agents,views,queries,localNode,true);}
    GlobalTaskPlane(GlobalTaskStore tasks,DurableCommands commands,AgentDirectory agents,GlobalTaskViews views,OwnerQueries queries,String localNode,boolean localExecution){this.tasks=tasks;this.commands=commands;this.agents=agents;this.views=views;this.queries=queries;this.localNode=localNode;this.localExecution=localExecution;documents=new ConfigurationDocuments(Base64.getUrlDecoder().decode(tasks.commandCipher().signature("shared-config-references")));}
    public boolean localOnly(){return false;}
    public Map<String,Object> invoke(TaskRequest supplied,AccessControl.Principal proposed)throws Exception{
        TaskRequest request=supplied.snapshot();AccessControl.Principal actor=tasks.principal(proposed);
        if(request.admin()&&!actor.administers()||request.kind==TaskRequest.Kind.ACTION&&!actor.operates())throw new SecurityException("FORBIDDEN");
        commands.requireRecovery(request);
        switch(request.kind){
            case LIST:{Map<String,Object> page=tasks.page(request.cursor==null?"":request.cursor,request.limit,actor,this::view);page.put("invalidConfigs",Collections.emptyList());return page;}
            case VIEW:return get(request.taskId,actor);
            case CREATE:{
                ObjectMapper yaml=new ObjectMapper(new YAMLFactory()).enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);JsonNode document=yaml.readTree(request.yaml);ConfigurationDocuments.validate(document);
                if(document==null||!document.isObject())throw new IllegalArgumentException("CONFIG_REQUIRED");String id=document.path(document.path("formatVersion").asInt()==3?"pipelineId":"jobId").asText();
                if(!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw new IllegalArgumentException("TASK_REQUEST_ID_INVALID");if(request.ownerNodeId==null&&!localExecution)throw new TaskService.Conflict("TASK_OWNER_REQUIRED");String owner=request.ownerNodeId==null?localNode:request.ownerNodeId;
                if(!actor.node(owner)||!actor.task(id))throw new SecurityException("FORBIDDEN");AgentDirectory.Binding binding=agents.owner(owner);commands.create(id,owner,binding.directory,request,actor);return get(id,actor);
            }
            case ACTION:
                if(tasks.get(request.taskId,actor).activeConfigurationVersion==null&&!"STOP".equals(request.action))throw new TaskService.Conflict("TASK_CONFIGURATION_NOT_APPLIED");commands.enqueue(request,actor);return get(request.taskId,actor);
            case CONFIGURATION:{
                GlobalTaskStore.Record record=tasks.get(request.taskId,actor);Map<String,Object> result=new LinkedHashMap<>();result.put("revision",record.revision);result.put("configurationVersion",record.configurationVersion);result.put("activeConfigurationVersion",record.activeConfigurationVersion);result.put("config",documents.redact(document(record.yaml),record.id,record.revision));return result;
            }
            case CONFIG_HISTORY:return tasks.history(request.taskId,request.cursor,actor);
            case CONFIG_PREVIEW:case CONFIG_UPDATE:{
                if(request.kind==TaskRequest.Kind.CONFIG_UPDATE&&commands.existing(request,actor)!=null)return get(request.taskId,actor);
                GlobalTaskStore.Record record=tasks.get(request.taskId,actor);if(!Objects.equals(record.revision,request.revision))throw new TaskService.Conflict("REVISION_CHANGED");JsonNode restored=documents.restore(request.document,document(record.yaml),record.id,record.revision);
                if(request.kind==TaskRequest.Kind.CONFIG_PREVIEW)return preview(request.taskId,restored,request.revision,actor);
                commands.update(request.taskId,new ObjectMapper(new YAMLFactory()).writeValueAsString(restored),request,actor);return get(request.taskId,actor);
            }
            case RESTORE_PREVIEW:case RESTORE:{
                if(request.kind==TaskRequest.Kind.RESTORE&&commands.existing(request,actor)!=null)return get(request.taskId,actor);
                GlobalTaskStore.Version selected=tasks.selected(request.taskId,request.reference,request.revision,actor);
                if(request.kind==TaskRequest.Kind.RESTORE_PREVIEW)return preview(request.taskId,document(selected.yaml),request.revision,actor);
                commands.update(request.taskId,selected.yaml,request,actor);return get(request.taskId,actor);
            }
            case VALIDATION_REPORT:case VALIDATION_DIFFERENCES:case REPAIR_PLAN:case FAILURE_EVIDENCE_SUMMARY:case FAILURE_EVIDENCE_EVENTS:case SKIP_PLAN:case SKIP_PLANS:case GAPS:case GAP_VERIFICATION:return queries.await(queries.submit(request,actor),actor);
            default:throw new TaskService.Conflict("GLOBAL_CONFIGURATION_WORKFLOW_PENDING");
        }
    }
    private static JsonNode document(String yaml)throws Exception{return new ObjectMapper(new YAMLFactory()).readTree(yaml);}
    private Map<String,Object> preview(String id,JsonNode document,String revision,AccessControl.Principal actor)throws Exception{
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.CONFIG_PREVIEW,id);request.document=document;request.revision=revision;return queries.await(queries.submit(request,actor),actor);
    }
    private Map<String,Object> get(String id,AccessControl.Principal actor)throws Exception{return tasks.transaction(id,actor,(c,record,current)->view(c,record,current));}
    private Map<String,Object> view(Connection c,GlobalTaskStore.Record task,AccessControl.Principal actor)throws Exception{
        Map<String,Object> result=views.read(c,task,actor);List<Map<String,Object>> operations=commands.operations(c,task.id,actor);Map<String,Map<?,?>> observed=new HashMap<>();
        Object local=result.get("operations");if(local instanceof List)for(Object value:(List<?>)local)if(value instanceof Map){Map<?,?> operation=(Map<?,?>)value;observed.put(String.valueOf(operation.get("id")),operation);}
        for(Map<String,Object> operation:operations){
            Map<?,?> actual=observed.get(operation.get("id"));
            // Keep durable routing/authorization fields authoritative while retaining the owner's public workflow inputs.
            if(actual!=null)for(String field:Arrays.asList("result","validation","repair","failureEvidenceId","skipPlanId","skip","gapPlanId","gap","cancelRequested","finishedAt","errorCount","subscriptionId"))if(actual.containsKey(field))operation.put(field,actual.get(field));
        }
        result.put("operations",operations);result.put("authority","SHARED");result.put("activeConfigurationVersion",task.activeConfigurationVersion);result.put("configurationPending",!Objects.equals(task.configurationVersion,task.activeConfigurationVersion));return result;
    }
}
