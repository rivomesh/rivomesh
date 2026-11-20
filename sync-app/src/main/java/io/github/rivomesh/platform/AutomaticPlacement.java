package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.TargetProduct;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** A bounded candidate probe for automatic placement of SQL pipelines with an external SQL journal. */
final class AutomaticPlacement {
    private static final Set<String> SQL_PRODUCTS=Set.of("MYSQL","POSTGRESQL");
    private static final List<String> WORKER_COMPONENTS=List.of("io.github.rivomesh.Main","io.github.rivomesh.PipelineMain");

    private AutomaticPlacement(){}

    static boolean applies(JsonNode definition){
        if(definition.path("formatVersion").asInt()!=3)return false;
        JsonNode journal=definition.path("journal"),targets=definition.path("targets");
        String source=definition.path("source").path("type").asText(definition.path("sourceType").asText("MYSQL"));
        if(!SQL_PRODUCTS.contains(source)||!"EXTERNAL".equals(journal.path("mode").asText())
                ||!SQL_PRODUCTS.contains(journal.path("product").asText("POSTGRESQL"))
                ||journal.hasNonNull("writerAssignment")||journal.path("managedWriter").asBoolean()
                ||!targets.isArray()||targets.isEmpty())return false;
        for(JsonNode target:targets)if(!SQL_PRODUCTS.contains(target.path("product").asText()))return false;
        return true;
    }

    static Map<String,Object> receipt(TaskRequest probe){
        return Map.of("state","ELIGIBLE","configurationSha256",probe.identityHash);
    }

    static List<String> workerComponents(PipelineConfig pipeline){
        var components=new ArrayList<>(WORKER_COMPONENTS);
        boolean mysqlSource="MYSQL".equals(pipeline.effectiveSourceType());
        boolean postgresSource="POSTGRESQL".equals(pipeline.effectiveSourceType());
        if(mysqlSource||pipeline.journal.product==TargetProduct.MYSQL
                ||pipeline.targets.stream().anyMatch(target->target.product==TargetProduct.MYSQL))
            components.add("com.mysql.cj.jdbc.Driver");
        if(postgresSource||pipeline.journal.product==TargetProduct.POSTGRESQL
                ||pipeline.targets.stream().anyMatch(target->target.product==TargetProduct.POSTGRESQL))
            components.add("org.postgresql.Driver");
        if(mysqlSource)components.add("com.github.shyiko.mysql.binlog.BinaryLogClient");
        if(postgresSource)components.add("org.postgresql.replication.PGReplicationStream");
        return components;
    }

    static void requireWorker(PipelineConfig pipeline,Map<String,String> environment)throws Exception{
        if(Runtime.version().feature()!=21||!Files.isExecutable(TaskWorker.Jvm.javaExecutable()))
            throw new TaskService.Conflict("PLACEMENT_WORKER_UNAVAILABLE");
        requireLauncher(environment);
        ClassLoader loader=TaskWorker.class.getClassLoader();
        try{
            for(String component:workerComponents(pipeline))Class.forName(component,false,loader);
        }catch(ClassNotFoundException|LinkageError unavailable){
            throw new TaskService.Conflict("PLACEMENT_WORKER_COMPONENT_UNAVAILABLE");
        }
    }

    private static void requireLauncher(Map<String,String> environment)throws Exception{
        Process launcher;
        try{
            ProcessBuilder check=new ProcessBuilder(TaskWorker.Jvm.javaExecutable().toString(),"-version").redirectErrorStream(true);
            check.environment().clear();check.environment().putAll(environment);
            launcher=check.start();
        }catch(java.io.IOException unavailable){
            throw new TaskService.Conflict("PLACEMENT_WORKER_UNAVAILABLE");
        }
        try{
            if(!launcher.waitFor(3,TimeUnit.SECONDS)||launcher.exitValue()!=0)
                throw new TaskService.Conflict("PLACEMENT_WORKER_UNAVAILABLE");
            try(var output=launcher.getInputStream()){
                String version=new String(output.readNBytes(2048),StandardCharsets.US_ASCII).lines()
                        .filter(line->line.startsWith("openjdk version ")||line.startsWith("java version ")).findFirst().orElse("");
                int start=version.indexOf('"')+1;
                if(start==0||!version.startsWith("21",start)||version.length()<=start+2||!Set.of('.','"','-').contains(version.charAt(start+2)))
                    throw new TaskService.Conflict("PLACEMENT_WORKER_UNAVAILABLE");
            }
        }finally{
            if(launcher.isAlive())launcher.destroyForcibly();
        }
    }

    static void probeJournal(PipelineConfig pipeline)throws Exception{
        var journal=pipeline.journal;
        Properties properties=new Properties();
        properties.setProperty("user",journal.username);
        if(journal.password!=null)properties.setProperty("password",journal.password);
        boolean mysql=journal.product==TargetProduct.MYSQL;
        // These options come last so the probe remains bounded independently of task options.
        try(Connection connection=(mysql?new com.mysql.cj.jdbc.Driver():new org.postgresql.Driver())
                .connect(journalProbeUrl(journal.url,journal.product),properties)){
            if(connection==null)throw new TaskService.Conflict("PLACEMENT_JOURNAL_UNAVAILABLE");
            connection.setReadOnly(true);
            try(var statement=connection.createStatement()){
                if(!mysql)statement.setQueryTimeout(3);
                try(var found=statement.executeQuery("SELECT 1")){
                    if(!found.next()||found.getInt(1)!=1)throw new TaskService.Conflict("PLACEMENT_JOURNAL_UNAVAILABLE");
                }
            }
        }catch(java.sql.SQLException unavailable){
            throw new TaskService.Conflict("PLACEMENT_JOURNAL_UNAVAILABLE");
        }
    }
    static String journalProbeUrl(String url,TargetProduct product){
        String bounded=product==TargetProduct.MYSQL
                ?"connectTimeout=3000&socketTimeout=4000&autoReconnect=false"
                :"connectTimeout=3&loginTimeout=4&socketTimeout=4";
        return url+(url.contains("?")?"&":"?")+bounded;
    }
}
