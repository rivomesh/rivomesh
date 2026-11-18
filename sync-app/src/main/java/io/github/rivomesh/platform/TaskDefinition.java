package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.*;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.TargetProduct;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

/** One controller task can retain its legacy core or select the versioned fan-out runner. */
public final class TaskDefinition {
    final Config legacy;
    final PipelineConfig pipeline;
    final String jobId;
    final Config.Source source;
    final Config.Target target;
    final String workerYaml;
    private JdbcJournal localKeeper;
    private TaskDefinition(Config legacy,PipelineConfig pipeline,String yaml){
        this.legacy=legacy;this.pipeline=pipeline;workerYaml=yaml;
        Config common=legacy==null?pipeline.legacy():legacy;jobId=common.jobId;source=common.source;target=common.target;
        if(pipeline!=null){PipelineConfig.Target first=pipeline.targets.get(0);target.url=first.url;target.type=pipeline.targets.size()>1?"MULTIPLE":first.product.name();target.schema=pipeline.targets.size()>1?pipeline.targets.size()+" targets":String.join(",",first.mappings.values());target.metadataSchema=first.metadataSchema;}
    }
    static TaskDefinition parse(String value,Path directory,Path platformData)throws Exception{
        ObjectMapper mapper=new ObjectMapper(new YAMLFactory());JsonNode tree=mapper.readTree(value);
        if(tree!=null&&tree.path("formatVersion").asInt()==3){
            PipelineConfig pipeline=PipelineConfig.parse(value,directory);
            if("LOCAL".equals(pipeline.journal.mode)){
                Path managed=platformData.toAbsolutePath().normalize().resolve("journals").resolve(pipeline.pipelineId);
                if(tree.path("journal").has("path")&&!managed.equals(pipeline.localDirectory()))throw new TaskService.Conflict("LOCAL_JOURNAL_MIGRATION_REQUIRED");
                pipeline.journal.path=managed.toString();
                com.fasterxml.jackson.databind.node.ObjectNode object=(com.fasterxml.jackson.databind.node.ObjectNode)tree;
                if(!object.has("journal"))object.set("journal",mapper.createObjectNode());((com.fasterxml.jackson.databind.node.ObjectNode)object.get("journal")).put("path",managed.toString());
            }
            return new TaskDefinition(null,pipeline,mapper.writeValueAsString(tree));
        }
        return new TaskDefinition(Config.parse(value),null,value);
    }
    void validate(){if(pipeline==null)legacy.validate();else pipeline.validate();}
    String fingerprint()throws Exception{return pipeline==null?legacy.fingerprint():pipeline.fingerprint();}
    String stateIdentity(){if(pipeline==null)return "V2";JdbcJournal.Settings settings=pipeline.journalSettings();return pipeline.journal.mode+"\n"+settings.driver+"\n"+settings.url+"\n"+settings.namespace;}
    String sourceType(){return pipeline==null?"MYSQL":pipeline.effectiveSourceType();}
    List<Config.Endpoint> endpoints(){return pipeline==null?legacy.endpoints():pipeline.legacy().endpoints();}
    String databases(){return pipeline==null?source.database:String.join(", ",pipeline.databases);}
    /** Explicit allowlist for consumer setup; never return connection URLs or credentials. */
    List<Map<String,Object>> destinations(){return destinations(null);}
    List<Map<String,Object>> destinations(String generation){
        List<Map<String,Object>> result=new ArrayList<>();if(pipeline==null)return result;
        for(PipelineConfig.Target destination:pipeline.targets){
            Map<String,Object> row=new LinkedHashMap<>();row.put("id",destination.id);row.put("product",destination.product.name());row.put("mappings",new TreeMap<>(destination.mappings));row.put("consistency",destination.product.messaging()?"AT_LEAST_ONCE_EVENTS":destination.product.materialized()?"VERSIONED_MATERIALIZATION":"TRANSACTIONAL");
            if(destination.product==TargetProduct.KAFKA||destination.product==TargetProduct.PULSAR)row.put("resource",destination.messaging.topic);
            if(destination.product==TargetProduct.REDIS_STREAMS||destination.product==TargetProduct.NATS_JETSTREAM)row.put("resource",destination.messaging.stream);
            if(destination.product==TargetProduct.ROCKETMQ){row.put("topicPrefix",destination.messaging.topic);if(generation!=null)row.put("resource",io.github.rivomesh.transport.rocketmq.RocketTransport.physicalTopic(destination.messaging.topic,io.github.rivomesh.cdc.MessagePublishers.binding(pipeline.pipelineId,destination.id,generation)));}
            if(destination.product==TargetProduct.ACTIVEMQ_ARTEMIS){row.put("queuePrefix",destination.messaging.stream);if(generation!=null)row.put("resource",io.github.rivomesh.transport.artemis.ArtemisTransport.physicalQueue(destination.messaging.stream,io.github.rivomesh.cdc.MessagePublishers.binding(pipeline.pipelineId,destination.id,generation)));}
            if(destination.product==TargetProduct.ACTIVEMQ_CLASSIC){row.put("queuePrefix",destination.messaging.stream);if(generation!=null)row.put("resource",io.github.rivomesh.transport.classic.ClassicTransport.physicalQueue(destination.messaging.stream,io.github.rivomesh.cdc.MessagePublishers.binding(pipeline.pipelineId,destination.id,generation)));}
            result.add(row);
        }return result;
    }
    synchronized void holdLocalJournal()throws Exception{
        if(pipeline!=null&&"LOCAL".equals(pipeline.journal.mode)&&localKeeper==null){pipeline.prepareLocalDirectory();localKeeper=new JdbcJournal(pipeline.journalSettings());}
    }
    synchronized void holdExistingLocalJournal()throws Exception{
        if(pipeline!=null&&"LOCAL".equals(pipeline.journal.mode)&&localKeeper==null&&Files.isRegularFile(pipeline.localDirectory().resolve("rivomesh.mv.db")))localKeeper=new JdbcJournal(pipeline.existingJournalSettings());
    }
    synchronized void closeLocalJournal()throws Exception{if(localKeeper!=null)try{localKeeper.close();}finally{localKeeper=null;}}
    private static String destination(String url){
        try{URI uri=URI.create(url.startsWith("jdbc:")?url.substring("jdbc:".length()):url);if(uri.getHost()!=null&&(uri.getScheme().equals("redis")||uri.getScheme().equals("rediss"))){String path=uri.getPath();return uri.getScheme()+"://"+uri.getHost().toLowerCase(Locale.ROOT)+":"+(uri.getPort()<0?6379:uri.getPort())+"/"+(path==null||path.isEmpty()?0:Integer.parseInt(path.substring(1)));}if(uri.getHost()!=null)return uri.getScheme()+"://"+uri.getHost().toLowerCase(Locale.ROOT)+":"+(uri.getPort()<0?(uri.getScheme().equals("http")?80:uri.getScheme().equals("https")?443:uri.getScheme().equals("mysql")?3306:uri.getScheme().equals("postgresql")?5432:uri.getScheme().equals("amqp")?5672:uri.getScheme().equals("amqps")?5671:uri.getScheme().equals("kafka")?9092:uri.getScheme().equals("pulsar")?6650:uri.getScheme().equals("pulsar+ssl")?6651:uri.getScheme().equals("rocketmq")?9876:uri.getScheme().equals("activemq")?61616:uri.getScheme().equals("activemqs")?61617:uri.getScheme().equals("artemis")||uri.getScheme().equals("artemiss")?61616:4222):uri.getPort())+((uri.getScheme().equals("http")||uri.getScheme().equals("https"))&&("/".equals(uri.getPath())||uri.getPath().isEmpty())?"":uri.getPath());}
        catch(RuntimeException ignored){/* exact conservative URL comparison */}return url.split("\\?",2)[0];
    }
    Map<String,Set<String>> spaces(boolean metadata){
        Map<String,Set<String>> result=new HashMap<>();
        if(pipeline==null){Set<String> names=new HashSet<>();names.add(target.schema);if(metadata)names.add(target.metadataSchema);result.put(destination(target.url),names);}
        else for(PipelineConfig.Target t:pipeline.targets){Set<String> names=result.computeIfAbsent(destination(t.url),key->new HashSet<>());if(t.product==TargetProduct.KAFKA)names.add("kafka-topic:"+t.messaging.topic);else if(t.product==TargetProduct.REDIS_STREAMS)names.add("redis-stream:"+t.messaging.stream);else if(t.product==TargetProduct.PULSAR)names.add("pulsar-topic:"+t.messaging.topic);else if(t.product==TargetProduct.ROCKETMQ)names.add("rocketmq-publication:"+pipeline.pipelineId+":"+t.id);else if(t.product==TargetProduct.ACTIVEMQ_CLASSIC)names.add("classic-publication:"+pipeline.pipelineId+":"+t.id);else if(t.product==TargetProduct.ACTIVEMQ_ARTEMIS)names.add("artemis-publication:"+pipeline.pipelineId+":"+t.id);else names.addAll(t.mappings.values());if(metadata&&!t.product.messaging())names.add(t.metadataSchema);}
        return result;
    }
    Map<String,Set<String>> metadataSpaces(){
        Map<String,Set<String>> result=new HashMap<>();if(pipeline==null)result.computeIfAbsent(destination(target.url),key->new HashSet<>()).add(target.metadataSchema);
        else for(PipelineConfig.Target target:pipeline.targets)if(!target.product.messaging())result.computeIfAbsent(destination(target.url),key->new HashSet<>()).add(target.metadataSchema);return result;
    }
    boolean overlaps(TaskDefinition other){Map<String,Set<String>> mine=spaces(false),all=spaces(true),theirs=other.spaces(false),theirAll=other.spaces(true);return intersects(mine,theirAll)||intersects(theirs,all);}
    private static boolean intersects(Map<String,Set<String>> a,Map<String,Set<String>> b){for(String destination:a.keySet())if(b.containsKey(destination))for(String name:a.get(destination))if(b.get(destination).contains(name))return true;return false;}
}
