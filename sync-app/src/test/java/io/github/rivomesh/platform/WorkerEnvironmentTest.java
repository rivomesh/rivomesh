package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class WorkerEnvironmentTest {
    @TempDir Path directory;
    public static final class Probe {
        public static void main(String[] args)throws Exception{
            Map<String,Boolean> result=new LinkedHashMap<>();for(String key:Arrays.asList("TASK_PASSWORD","CONTROLLER_SECRET","RIVOMESH_CONTROL_PASSWORD","RIVOMESH_TOKEN","JAVA_TOOL_OPTIONS"))result.put(key,System.getenv(key)!=null);
            Files.write(Paths.get(args[1]).resolveSibling("probe.json"),Model.JSON.writeValueAsBytes(result));
        }
    }
    @Test void actualChildReceivesOnlyReferencedTaskValuesAndEssentialOsSettings()throws Exception{
        Path config=directory.resolve("worker.yml"),log=directory.resolve("worker.log");Files.write(config,"password: ${TASK_PASSWORD}\n".getBytes(StandardCharsets.UTF_8));
        Map<String,String> parent=new LinkedHashMap<>(System.getenv());parent.put("TASK_PASSWORD","task-private-value");parent.put("CONTROLLER_SECRET","controller-private-value");parent.put("RIVOMESH_CONTROL_PASSWORD","control-private-value");parent.put("RIVOMESH_TOKEN","management-private-value");parent.put("JAVA_TOOL_OPTIONS","-Dcontroller.only=true");
        TaskWorker worker=new TaskWorker.Jvm(config,log,128,Collections.singleton("CONTROLLER_SECRET"),parent,Probe.class.getName());try{long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(30);while(worker.alive()&&System.nanoTime()<deadline)Thread.sleep(50);assertFalse(worker.alive(),"Child JVM did not finish within30seconds");assertEquals(0,worker.exitCode());com.fasterxml.jackson.databind.JsonNode flags=Model.JSON.readTree(Files.readAllBytes(directory.resolve("probe.json")));assertTrue(flags.path("TASK_PASSWORD").asBoolean());for(String key:Arrays.asList("CONTROLLER_SECRET","RIVOMESH_CONTROL_PASSWORD","RIVOMESH_TOKEN","JAVA_TOOL_OPTIONS"))assertFalse(flags.path(key).asBoolean());}finally{worker.stop();}
    }
    @Test void controllerCredentialReferencesAreRejectedInsteadOfCopied()throws Exception{
        Map<String,String> env=Collections.singletonMap("CUSTOM_ADMIN_TOKEN","private-value");
        assertFalse(TaskWorker.Jvm.environment(Collections.singletonMap("HOME","private-value"),"",Collections.singleton("HOME")).containsKey("HOME"));
        for(String key:Arrays.asList("CUSTOM_ADMIN_TOKEN","RIVOMESH_CONTROL_PASSWORD","RIVOMESH_TOKEN","RIVOMESH_PREVIEW_TOKEN"))assertEquals("WORKER_ENVIRONMENT_DENIED",assertThrows(IllegalArgumentException.class,()->TaskWorker.Jvm.environment(env,"password: ${"+key+"}",Collections.singleton("CUSTOM_ADMIN_TOKEN"))).getMessage());
    }
}
