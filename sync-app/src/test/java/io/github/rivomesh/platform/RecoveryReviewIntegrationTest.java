package io.github.rivomesh.platform;

import io.github.rivomesh.JdbcConnections;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.*;

/** Explicitly selected by the disposable SQL fixture; excluded from the offline app suite. */
@EnabledIfEnvironmentVariable(named="RIVOMESH_FAILURE_DB_URL",matches=".+")
class RecoveryReviewIntegrationTest {
    @Test void protocolsThreeAndFourRequireNormalStopBeforeFiveAndCannotRejoin()throws Exception{
        for(boolean separated:Arrays.asList(false,true)){
            ClusterConfig old=new ClusterConfig();old.id="review-upgrade";old.nodeId="old";old.namespace="review_protocol_"+UUID.randomUUID().toString().replace("-","");old.driver=System.getenv("RIVOMESH_FAILURE_DB_DRIVER");old.url=System.getenv("RIVOMESH_FAILURE_DB_URL");old.username=System.getenv("RIVOMESH_FAILURE_DB_USER");old.password=System.getenv("RIVOMESH_FAILURE_DB_PASSWORD");old.sharedAccess=true;old.sharedTasks=true;old.separateExecution=separated;old.taskKey=Base64.getEncoder().encodeToString(new byte[32]);old.heartbeatMillis=100;old.leaseMillis=1000;
            ClusterConfig next=io.github.rivomesh.Model.JSON.readValue(io.github.rivomesh.Model.JSON.writeValueAsBytes(old),ClusterConfig.class);next.nodeId="new";next.recoveryReview=true;
            try{
                try(JdbcControlStore store=new JdbcControlStore(old)){
                    ControlStore.Node member=ControlStoreTest.node("old");if(separated)member.role="CONTROLLER_WORKER";ControlStore.Lease lease=store.register(member,Collections.emptyList());
                    assertEquals("GLOBAL_TASK_QUIESCENCE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(next)).getMessage());Thread.sleep(1250);
                    assertEquals("GLOBAL_TASK_QUIESCENCE_REQUIRED",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(next)).getMessage());store.stopped(lease);
                    try(JdbcControlStore upgraded=new JdbcControlStore(next)){assertEquals("CONTROL_STORE_PROTOCOL_MISMATCH",assertThrows(TaskService.Conflict.class,()->new JdbcControlStore(old)).getMessage());}
                }
            }finally{try(Connection c=JdbcConnections.open(old.driver,old.url,old.username,old.password);Statement sql=c.createStatement()){sql.execute(old.driver.startsWith("com.mysql")?"DROP DATABASE IF EXISTS `"+old.namespace+"`":"DROP SCHEMA IF EXISTS \""+old.namespace+"\" CASCADE");}}
        }
    }
}
