package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** A bounded candidate probe for automatic placement of SQL pipelines with a PostgreSQL journal. */
final class AutomaticPlacement {
    private static final Set<String> SQL_TARGETS=Set.of("MYSQL","POSTGRESQL");
    private static final Set<String> WORKER_COMPONENTS=Set.of("io.github.rivomesh.Main","io.github.rivomesh.PipelineMain",
            "com.mysql.cj.jdbc.Driver","com.github.shyiko.mysql.binlog.BinaryLogClient","org.postgresql.Driver");

    private AutomaticPlacement(){}

    static boolean applies(JsonNode definition){
        if(definition.path("formatVersion").asInt()!=3)return false;
        JsonNode journal=definition.path("journal"),targets=definition.path("targets");
        String source=definition.path("source").path("type").asText(definition.path("sourceType").asText("MYSQL"));
        if(!"MYSQL".equals(source)||!"EXTERNAL".equals(journal.path("mode").asText())
                ||!"POSTGRESQL".equals(journal.path("product").asText("POSTGRESQL"))
                ||journal.hasNonNull("writerAssignment")||journal.path("managedWriter").asBoolean()
                ||!targets.isArray()||targets.isEmpty())return false;
        for(JsonNode target:targets)if(!SQL_TARGETS.contains(target.path("product").asText()))return false;
        return true;
    }

    static Map<String,Object> receipt(TaskRequest probe){
        return Map.of("state","ELIGIBLE","configurationSha256",probe.identityHash);
    }

    static void requireWorker(Map<String,String> environment)throws Exception{
        if(Runtime.version().feature()!=21||!Files.isExecutable(TaskWorker.Jvm.javaExecutable()))
            throw new TaskService.Conflict("PLACEMENT_WORKER_UNAVAILABLE");
        requireLauncher(environment);
        ClassLoader loader=TaskWorker.class.getClassLoader();
        try{
            for(String component:WORKER_COMPONENTS)Class.forName(component,false,loader);
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

    static void probeJournal(io.github.rivomesh.PipelineConfig pipeline)throws Exception{
        var journal=pipeline.journal;
        Properties properties=new Properties();
        properties.setProperty("user",journal.username);
        if(journal.password!=null)properties.setProperty("password",journal.password);
        // These properties come last so the probe remains bounded independently of task options.
        try(Connection connection=new org.postgresql.Driver().connect(journalProbeUrl(journal.url),properties)){
            if(connection==null)throw new TaskService.Conflict("PLACEMENT_JOURNAL_UNAVAILABLE");
            connection.setReadOnly(true);
            try(var statement=connection.createStatement()){
                statement.setQueryTimeout(3);
                try(var found=statement.executeQuery("SELECT 1")){
                    if(!found.next()||found.getInt(1)!=1)throw new TaskService.Conflict("PLACEMENT_JOURNAL_UNAVAILABLE");
                }
            }
        }catch(java.sql.SQLException unavailable){
            throw new TaskService.Conflict("PLACEMENT_JOURNAL_UNAVAILABLE");
        }
    }
    static String journalProbeUrl(String url){
        return url+(url.contains("?")?"&":"?")+"connectTimeout=3&loginTimeout=4&socketTimeout=4";
    }
}
