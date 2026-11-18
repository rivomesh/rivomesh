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
            Map<String,Boolean> result=new LinkedHashMap<>();for(String key:Arrays.asList("TASK_PASSWORD","OTHER_TASK","UNUSED_TASK","CONTROLLER_SECRET","RIVOMESH_CONTROL_PASSWORD","RIVOMESH_TOKEN","JAVA_TOOL_OPTIONS"))result.put(key,System.getenv(key)!=null);
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
    @Test void explicitlyGrantedTasksGetOnlyTheirUsedReferencesInActualSeparateWorkers()throws Exception{
        Map<String,String> parent=new LinkedHashMap<>(System.getenv());
        parent.put("TASK_PASSWORD","one");parent.put("OTHER_TASK","two");parent.put("UNUSED_TASK","unused");
        parent.put("CONTROLLER_SECRET","controller");
        Map<String,List<String>> grants=Map.of("one",List.of("TASK_PASSWORD","UNUSED_TASK"),"two",List.of("OTHER_TASK"));
        Set<String> protectedNames=Set.of("CONTROLLER_SECRET");
        WorkerSecretReferences.validate(grants,protectedNames);
        for(String task:List.of("one","two")){
            String name=task.equals("one")?"TASK_PASSWORD":"OTHER_TASK";
            Path config=directory.resolve(task+".yml");
            Files.writeString(config,"jobId: "+task+"\n# ${CONTROLLER_SECRET}\nsource:\n  password: ${"+name+"}\n");
            TaskWorker worker=new TaskWorker.Jvm(config,directory.resolve(task+".log"),128,protectedNames,grants,parent,Probe.class.getName());
            try{
                long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
                while(worker.alive()&&System.nanoTime()<deadline)Thread.sleep(50);
                assertFalse(worker.alive());
                assertEquals(0,worker.exitCode());
                var flags=Model.JSON.readTree(Files.readAllBytes(directory.resolve("probe.json")));
                assertTrue(flags.path(name).asBoolean());
                assertFalse(flags.path(task.equals("one")?"OTHER_TASK":"TASK_PASSWORD").asBoolean());
                assertFalse(flags.path("UNUSED_TASK").asBoolean());
                assertFalse(flags.path("CONTROLLER_SECRET").asBoolean());
            }finally{worker.stop();}
        }
    }
    @Test void oracleSourcePasswordReferencesUseTaskGrants()throws Exception{
        String config="formatVersion: 3\npipelineId: oracle\nsource:\n  type: ORACLE\n  oracle:\n"
                +"    root:\n      password: ${ROOT_PASSWORD}\n    pdb:\n      password: ${PDB_PASSWORD}";
        Set<String> names=Set.of("ROOT_PASSWORD","PDB_PASSWORD");
        assertEquals(names,WorkerSecretReferences.permitted(config,Map.of("oracle",List.copyOf(names))));
    }
    @Test void configuredGrantsCannotExposeProcessOverrides() {
        for(String name:List.of("JAVA_TOOL_OPTIONS","JDK_JAVA_OPTIONS","_JAVA_OPTIONS","CLASSPATH","LD_PRELOAD","DYLD_INSERT_LIBRARIES"))
            assertEquals("WORKER_SECRET_POLICY_INVALID",assertThrows(IllegalArgumentException.class,
                    ()->WorkerSecretReferences.validate(Map.of("task",List.of(name)),Set.of())).getMessage());
    }
    @Test void taskIdentityAndAnotherTaskReferenceCannotUseGrantedValues()throws Exception{
        Map<String,List<String>> grants=Map.of("job",List.of("ALLOWED"),"other",List.of("OTHER"));
        for(String document:List.of("jobId: job\nsource:\n  password: ${OTHER}",
                "jobId: job\nnodeId: ${ALLOWED}",
                "formatVersion: 3\npipelineId: job\ntargets:\n - id: ${ALLOWED}")){
            assertEquals("WORKER_SECRET_REFERENCE_DENIED",assertThrows(IllegalArgumentException.class,
                    ()->TaskWorker.Jvm.environment(Map.of("ALLOWED","value","OTHER","private"),document,Set.of(),grants)).getMessage());
        }
        assertEquals("WORKER_SECRET_REFERENCE_UNSET",assertThrows(IllegalArgumentException.class,
                ()->TaskWorker.Jvm.environment(Map.of(),"jobId: job\ntarget:\n  password: ${ALLOWED}",Set.of(),grants)).getMessage());
    }
}
