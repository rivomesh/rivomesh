package io.github.rivomesh.platform;

import io.github.rivomesh.JdbcConnections;
import java.sql.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="RIVOMESH_LEGACY_ARTIFACT",matches=".+")
class RecoverySkipIntegrationTest {
    @TempDir Path directory;
    @Test void nativeProtocolsThreeThroughSixRequireNormalStopAndActualOlderBinaryRefusesSeven()throws Exception{
        for(int protocol:Arrays.asList(3,4,5,6)){
            ClusterConfig old=new ClusterConfig();old.id="skip-upgrade";old.nodeId="old";old.namespace="skip_protocol_"+UUID.randomUUID().toString().replace("-","");old.driver=System.getenv("RIVOMESH_FAILURE_DB_DRIVER");old.url=System.getenv("RIVOMESH_FAILURE_DB_URL");old.username=System.getenv("RIVOMESH_FAILURE_DB_USER");old.password=System.getenv("RIVOMESH_FAILURE_DB_PASSWORD");old.sharedAccess=true;old.sharedTasks=true;old.separateExecution=protocol>=4;old.recoveryReview=protocol>=5;old.recoveryCommit=protocol>=6;old.taskKey=Base64.getEncoder().encodeToString(new byte[32]);old.heartbeatMillis=100;old.leaseMillis=1000;
            ClusterConfig next=io.github.rivomesh.Model.JSON.readValue(io.github.rivomesh.Model.JSON.writeValueAsBytes(old),ClusterConfig.class);next.nodeId="new";next.separateExecution=true;next.recoveryReview=true;next.recoveryCommit=true;next.recoverySkip=true;
            try{
                try(JdbcControlStore store=new JdbcControlStore(old)){
                    ControlStore.Node node=ControlStoreTest.node("old");if(old.separateExecution)node.role="CONTROLLER_WORKER";ControlStore.Lease lease=store.register(node,Collections.emptyList());assertEquals("GLOBAL_TASK_QUIESCENCE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(next)).getMessage());Thread.sleep(1250);assertEquals("GLOBAL_TASK_QUIESCENCE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(next)).getMessage());store.stopped(lease);
                    try(JdbcControlStore upgraded=new JdbcControlStore(next)){
                        assertEquals("CONTROL_STORE_PROTOCOL_MISMATCH",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(old)).getMessage());
                        if(protocol==6){Path log=directory.resolve("legacy-control.log");String cp=System.getenv("RIVOMESH_LEGACY_ARTIFACT")+File.pathSeparator+System.getenv("RIVOMESH_LEGACY_TEST_CLASSES");Process child=new ProcessBuilder(new File(System.getProperty("java.home"),"bin/java").toString(),"-Xmx128m","-cp",cp,"io.github.rivomesh.platform.OldControlProtocolProbe",next.namespace,next.id).redirectErrorStream(true).redirectOutput(log.toFile()).start();boolean ended=child.waitFor(30,TimeUnit.SECONDS);if(!ended)child.destroyForcibly();assertTrue(ended,"Legacy control probe timed out");String output=new String(Files.readAllBytes(log),java.nio.charset.StandardCharsets.UTF_8);assertEquals(0,child.exitValue(),output);assertTrue(output.contains("LEGACY_PROTOCOL6_REFUSED_PROTOCOL7"),output);}
                    }
                }
            }finally{try(Connection c=JdbcConnections.open(old.driver,old.url,old.username,old.password);Statement sql=c.createStatement()){sql.execute(old.driver.startsWith("com.mysql")?"DROP DATABASE IF EXISTS `"+old.namespace+"`":"DROP SCHEMA IF EXISTS \""+old.namespace+"\" CASCADE");}}
        }
    }
}
