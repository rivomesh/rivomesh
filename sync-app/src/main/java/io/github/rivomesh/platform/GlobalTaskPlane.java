package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.KafkaNoticePlanning;
import java.sql.Connection;
import java.util.*;

/** Shared task entry. Enabled only by the global runtime after migration/admission gates. */
final class GlobalTaskPlane implements TaskControlPlane {
    private final GlobalTaskStore tasks;
    private final DurableCommands commands;
    private final AgentDirectory agents;
    private final GlobalTaskViews views;
    private final OwnerQueries queries;
    private final ManagedHandoff handoff;
    private final String localNode;
    private final ConfigurationDocuments documents;
    private final boolean localExecution;
    GlobalTaskPlane(GlobalTaskStore tasks,DurableCommands commands,AgentDirectory agents,GlobalTaskViews views,OwnerQueries queries,String localNode){this(tasks,commands,agents,views,queries,localNode,true);}
    GlobalTaskPlane(GlobalTaskStore tasks,DurableCommands commands,AgentDirectory agents,GlobalTaskViews views,OwnerQueries queries,String localNode,boolean localExecution){this(tasks,commands,agents,views,queries,localNode,localExecution,null);}
    GlobalTaskPlane(GlobalTaskStore tasks,DurableCommands commands,AgentDirectory agents,GlobalTaskViews views,OwnerQueries queries,String localNode,boolean localExecution,ManagedHandoff handoff){this.tasks=tasks;this.commands=commands;this.agents=agents;this.views=views;this.queries=queries;this.localNode=localNode;this.localExecution=localExecution;this.handoff=handoff;documents=new ConfigurationDocuments(Base64.getUrlDecoder().decode(tasks.commandCipher().signature("shared-config-references")));}
    public boolean localOnly(){return false;}
    public Map<String,Object> invoke(TaskRequest supplied,AccessControl.Principal proposed)throws Exception{
        TaskRequest request=supplied.snapshot();AccessControl.Principal actor=tasks.principal(proposed);
        if(request.admin()&&!actor.administers()||request.kind==TaskRequest.Kind.ACTION&&!actor.operates())throw new SecurityException("FORBIDDEN");
        commands.requireRecovery(request);
        switch(request.kind){
            case LIST:{Map<String,Object> page=tasks.page(request.cursor==null?"":request.cursor,request.limit,request.ownerNodeId,request.taskId,actor,this::view);page.put("invalidConfigs",Collections.emptyList());return page;}
            case VIEW:return get(request.taskId,actor);
            case RECOVERY_RECEIPT:return recoveryReceipt(request,actor);
            case HANDOFF_RAW_HISTORY:
                try{return queries.await(queries.submit(request,actor),actor);}
                catch(TaskService.Conflict rejected){
                    if(!"OWNER_DATA_NOT_FOUND".equals(rejected.getMessage()))throw rejected;
                    throw new NoSuchElementException("HANDOFF_RAW_HISTORY_NOT_FOUND");
                }
            case KAFKA_NOTICE_PLAN:
                if(request.kafkaNoticePredecessor!=null)throw new IllegalArgumentException("KAFKA_NOTICE_REQUEST_INVALID");
                request.kafkaNoticePredecessor=commands.kafkaNoticePredecessor(request.taskId,request.reference,request.revision,actor);
                return kafkaNoticePlan(request,actor);
            case HANDOFF_ASSESS:case HANDOFF_LATEST:
                if(handoff==null)throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
                return request.kind==TaskRequest.Kind.HANDOFF_ASSESS?handoff.assess(request,actor):handoff.latest(request.taskId,actor);
            case HANDOFF_PROBE:
                if(handoff==null)throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
                return handoff.probe(request,actor,queries);
            case HANDOFF_STAGE:
                if(handoff==null)throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
                return queries.await(queries.submitHandoffStaging(request,actor),actor);
            case HANDOFF_COMMIT_OWNER:
                if(handoff==null)throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
                return handoff.orchestratePreparedWriter(request,actor,queries);
            case HANDOFF_TARGET_FENCE:
                if(handoff==null)throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
                return handoff.requestTargetFence(request,actor);
            case HANDOFF_START_WORKER:
                if(handoff==null)throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
                return handoff.requestWorkerStart(request,actor);
            case HANDOFF_PREPARE:case HANDOFF_PREPARATION:case HANDOFF_RELEASE:
                if(handoff==null)throw new TaskService.Conflict("GLOBAL_HANDOFF_REQUIRED");
                return switch(request.kind){
                    case HANDOFF_PREPARE->handoff.prepare(request,actor);
                    case HANDOFF_RELEASE->handoff.releasePreparation(request,actor);
                    default->handoff.latestPreparation(request.taskId,actor);
                };
            case CREATE:{
                ObjectMapper yaml=new ObjectMapper(new YAMLFactory()).enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);JsonNode document=yaml.readTree(request.yaml);ConfigurationDocuments.validate(document);
                if(document==null||!document.isObject())throw new IllegalArgumentException("CONFIG_REQUIRED");String id=document.path(document.path("formatVersion").asInt()==3?"pipelineId":"jobId").asText();
                if(!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw new IllegalArgumentException("TASK_REQUEST_ID_INVALID");
                if("AUTO".equals(request.placement)){
                    commands.allocate(id,request,actor,agents);
                    return get(id,actor);
                }
                if(request.ownerNodeId==null&&!localExecution)throw new TaskService.Conflict("TASK_OWNER_REQUIRED");String owner=request.ownerNodeId==null?localNode:request.ownerNodeId;
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
            case VALIDATION_REPORTS:case VALIDATION_REPORT:case VALIDATION_DIFFERENCES:case VALIDATION_STORAGE:case REPAIR_PLAN:case CAPTURE_RECOVERY:case CAPTURE_EXCLUSIONS:case DIAGNOSTICS:
            case FAILURE_EVIDENCE_SUMMARY:case FAILURE_EVIDENCE_EVENTS:case SKIP_PLAN:case SKIP_PLANS:case GAPS:case GAP_VERIFICATION:case CORRECTION_SCHEMA:case CORRECTION_PREVIEW:return queries.await(queries.submit(request,actor),actor);
            default:throw new TaskService.Conflict("GLOBAL_CONFIGURATION_WORKFLOW_PENDING");
        }
    }
    private Map<String,Object> recoveryReceipt(TaskRequest request,AccessControl.Principal actor)throws Exception{
        Map<String,Object> retained=commands.recoveryReceipt(request.taskId,request.reference,actor);
        boolean adopted=tasks.transaction(request.taskId,actor,(c,task,current)->{
            HandoffPreparations.Plan plan=tasks.preparations().latest(c,task.id,tasks.database().now(c));
            return HandoffPreparations.evidenceInstalled(plan)
                    &&(task.revision.equals(plan.writerCommit().revision())||HandoffPreparations.workerAdmitted(plan))
                    &&plan.writerCommit().configuration().equals(task.activeConfigurationVersion)
                    &&plan.writerCommit().configuration().equals(task.configurationVersion)
                    &&task.owner.equals(plan.evidence().candidate().node())&&task.directory.equals(plan.evidence().candidate().directory())
                    &&Objects.equals(retained.get("ownerNodeId"),plan.evidence().original().node());
        });
        if(!adopted)return retained;
        try{
            Map<String,Object> candidate=queries.await(queries.submit(request,actor),actor);
            for(String field:List.of("id","status","code","actorRole"))
                if(!Objects.equals(retained.get(field),candidate.get(field)))throw new TaskService.Conflict("HANDOFF_ADOPTED_RECEIPT_CHANGED");
            retained.put("candidateCustody","VERIFIED");
        }catch(ControlStore.Fenced|ControlStore.Unavailable deferred){
            // The authenticated historical result remains available independently of the candidate lease.
        }catch(TaskService.Conflict deferred){
            if(!Set.of("OWNER_DATA_NOT_FOUND","OWNER_QUERY_UNAVAILABLE","OWNER_QUERY_TIMEOUT","OWNER_QUERY_EXPIRED",
                    "OWNER_QUERY_CAPACITY").contains(deferred.getMessage()))throw deferred;
        }
        return retained;
    }

    private Map<String,Object> kafkaNoticePlan(TaskRequest request,AccessControl.Principal actor)throws Exception{
        Map<String,Object> response=queries.await(queries.submit(request,actor),actor);
        // The owner-query envelope already authenticates this response and fences the producing owner.
        var plan=Model.JSON.convertValue(response.get("plan"),KafkaNoticePlanning.Prepared.class);
        var predecessor=request.kafkaNoticePredecessor;
        if(plan==null||!predecessor.task().equals(plan.task())||!predecessor.administrator().equals(plan.administrator())
                ||!predecessor.receipt().requestId().equals(plan.predecessorRequestId())
                ||!Codec.sha256(Codec.encode(predecessor.receipt())).equals(plan.predecessorReceiptSha256())
                ||!plan.approvalSha256().equals(response.get("approvalSha256"))
                ||!Boolean.FALSE.equals(response.get("executionAuthorized")))
            throw new TaskService.Conflict("KAFKA_NOTICE_PLAN_CHANGED");
        Map<String,Object> coverage=tasks.transaction(request.taskId,actor,(c,task,current)->{
            if(!task.revision.equals(plan.task().revision())||!task.owner.equals(plan.task().ownerNodeId())
                    ||!task.directory.equals(plan.task().ownerDirectoryId())
                    ||!task.configurationVersion.equals(plan.task().configurationVersion())
                    ||!task.configurationVersion.equals(task.activeConfigurationVersion))
                throw new TaskService.Conflict("REVISION_CHANGED");
            var identity=plan.catalogIdentity();
            if(identity==null)return incompleteCatalog("KAFKA_NOTICE_CATALOG_NATIVE_IDENTITY_MISSING");
            if(!predecessor.receipt().observation().observedTopicIdentitySha256().equals(identity.businessTopicSha256()))
                throw new TaskService.Conflict("KAFKA_NOTICE_CATALOG_IDENTITY_CHANGED");
            verifyKafkaNoticeReservation(tasks.activeYaml(c,task),plan.controlTopic());
            if(identity.businessTopicSha256().equals(identity.controlTopicSha256()))
                return Map.of("status","CONFLICT","code","KAFKA_NOTICE_CATALOG_PHYSICAL_TOPIC_COLLISION");
            return null;
        });
        if(coverage==null)coverage=KafkaNoticeCatalog.coverage(tasks,queries,plan,actor);
        response.put("catalogCoverage",coverage);
        return response;
    }

    private static Map<String,Object> incompleteCatalog(String code){return Map.of("status","INCOMPLETE","code",code);}

    private static void verifyKafkaNoticeReservation(String yaml,KafkaNoticePlanning.TopicReservation reservation)throws Exception{
        JsonNode config=document(yaml),targets=config.path("targets");
        if(config.path("formatVersion").asInt()!=3||!reservation.taskId().equals(config.path("pipelineId").asText())||!targets.isArray())
            throw new TaskService.Conflict("KAFKA_NOTICE_CATALOG_IDENTITY_CHANGED");
        boolean declared=false;
        for(JsonNode target:targets){
            if(!"KAFKA".equalsIgnoreCase(target.path("product").asText()))continue;
            if(!reservation.targetId().equals(target.path("id").asText()))continue;
            JsonNode notice=target.path("messaging").path("recoveryNotice");
            declared=reservation.reservationId().equals(notice.path("reservationId").asText())
                    &&reservation.topicName().equals(notice.path("controlTopic").asText())
                    &&reservation.kafkaClusterSha256().equals(notice.path("clusterSha256").asText())
                    &&reservation.topicId().equals(notice.path("topicId").asText());
        }
        if(!declared)throw new TaskService.Conflict("KAFKA_NOTICE_CATALOG_IDENTITY_CHANGED");
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
            if(actual!=null)for(String field:Arrays.asList("result","validation","repair","failureEvidenceId","skipPlanId","skip","gapPlanId","gap","compensation","cancelRequested","finishedAt","errorCount","subscriptionId"))if(actual.containsKey(field))operation.put(field,actual.get(field));
        }
        result.put("operations",operations);result.put("authority","SHARED");result.put("activeConfigurationVersion",task.activeConfigurationVersion);result.put("configurationPending",!Objects.equals(task.configurationVersion,task.activeConfigurationVersion));return result;
    }
}
