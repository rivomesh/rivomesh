package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.v3.ha",matches="true")
class PipelineClusterIntegrationTest {
    static PipelineConfig pipeline(Config legacy){
        PipelineConfig p=new PipelineConfig();p.pipelineId=legacy.jobId;p.source=legacy.source;p.databases=Collections.singletonList(legacy.source.database);p.chunkSize=5;p.leaseMillis=5000;p.heartbeatMillis=500;p.pollIntervalMillis=100;p.retryDelayMillis=200;
        p.journal.mode="EXTERNAL";p.journal.product=TargetProduct.POSTGRESQL;p.journal.url=System.getProperty("target.url");p.journal.username="sync_test";p.journal.password=ClusterHarness.PASSWORD;p.journal.namespace=legacy.jobId+"_journal";
        for(boolean gauss:Arrays.asList(true,false)){
            PipelineConfig.Target t=new PipelineConfig.Target();t.id=gauss?"gauss":"postgres";t.product=gauss?TargetProduct.GAUSS:TargetProduct.POSTGRESQL;t.url=gauss?"jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10":System.getProperty("target.url");t.username="sync_test";t.password=ClusterHarness.PASSWORD;t.metadataSchema=legacy.target.metadataSchema;t.mappings.put(legacy.source.database,legacy.target.schema);p.targets.add(t);
        }return p;
    }
    static void cleanPostgres(PipelineConfig p)throws Exception{
        PipelineConfig.Target target=p.targets.get(1);try(Connection c=JdbcConnections.open("org.postgresql.Driver",target.url,target.username,target.password);Statement s=c.createStatement()){
            s.execute("DROP SCHEMA IF EXISTS "+Sql.pg(target.mappings.values().iterator().next())+" CASCADE");s.execute("DROP SCHEMA IF EXISTS "+Sql.pg(target.metadataSchema)+" CASCADE");s.execute("DROP SCHEMA IF EXISTS "+Sql.pg(p.journal.namespace)+" CASCADE");
        }
    }
    static int replicaError()throws Exception{try(Connection c=ClusterHarness.mysql("mysql-replica");Statement s=c.createStatement();ResultSet r=s.executeQuery("SHOW "+ClusterIntegrationTest.replicationKeyword(c)+" STATUS")){assertTrue(r.next());return r.getInt("Last_SQL_Errno");}}
    static void caughtUp(ClusterHarness h)throws Exception{String gtid=ClusterHarness.scalar("mysql","SELECT @@global.gtid_executed");h.await(()->Gtids.contains(ClusterHarness.scalar("mysql-replica","SELECT @@global.gtid_executed"),gtid)&&replicaError()==0,30);}
    private static void purgeUncapturedTransaction(ClusterHarness harness,PipelineConfig pipeline,JdbcJournal journal)throws Exception{
        String primary=harness.cleanupHost;
        SourcePosition checkpoint=journal.captured();
        long sequence=((Number)journal.status().get("lastSequence")).longValue();
        String owner=String.valueOf(journal.status().get("owner"));
        harness.kill(owner.startsWith("node-a-")?"node-a":"node-b");
        ClusterHarness.sql(primary,"INSERT INTO "+harness.sourceTable()+" VALUES (43,100,'missing-binlog')");
        ClusterHarness.sql(primary,"FLUSH BINARY LOGS");
        String currentFile=null;
        try(Connection connection=ClusterHarness.mysql(primary);Statement statement=connection.createStatement();
            ResultSet logs=statement.executeQuery("SHOW BINARY LOGS")){
            while(logs.next())currentFile=logs.getString(1);
        }
        assertNotNull(currentFile);
        assertTrue(currentFile.matches("[A-Za-z0-9._-]+"));
        ClusterHarness.sql(primary,"PURGE BINARY LOGS TO '"+currentFile+"'");
        assertFalse(Gtids.contains(checkpoint.getPayload().get("gtidSet"),
                ClusterHarness.scalar(primary,"SELECT @@global.gtid_purged")));
        harness.startPipeline("node-gap",pipeline);
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
        Map<String,Object> status=journal.status();
        while(!"ERROR".equals(status.get("run_state"))&&System.nanoTime()<deadline){
            Thread.sleep(100);
            status=journal.status();
        }
        assertEquals("ERROR",status.get("run_state"));
        assertEquals("SOURCE_LOG_GAP",status.get("errorCode"));
        assertEquals(checkpoint,journal.captured());
        assertEquals(sequence,((Number)journal.status().get("lastSequence")).longValue());
        assertEquals(0,harness.count("id=43"));
    }
    @Test void concurrentWorkersReconnectAndContinueAfterRealPrimaryPromotionWithoutResnapshot()throws Exception{
        Config legacy=ClusterHarness.config();legacy.source.topology="ASYNC";legacy.source.nodes=Arrays.asList(new Config.Endpoint("mysql",3306),new Config.Endpoint("mysql-replica",3306));
        // ClusterHarness business assertions intentionally use the separate openGauss destination.
        legacy.target.type="GAUSS";legacy.target.driver="io.github.rivomesh.gaussdb.jdbc.Driver";legacy.target.url="jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10";
        ClusterIntegrationTest.provisionReplica();PipelineConfig p=pipeline(legacy);
        try(ClusterHarness h=new ClusterHarness(legacy)){
            h.startPipeline("node-a",p);h.startPipeline("node-b",p);
            h.await(()->h.count("TRUE")==40,90);
            try(JdbcJournal journal=new JdbcJournal(p.journalSettings())){
                h.await(()->((Number)journal.status().get("snapshotBytes")).longValue()==0,30);Journal.SnapshotState snapshot=journal.snapshot("source",TableSchema.identity(legacy.source.database,"","items"));
                String owner=String.valueOf(journal.status().get("owner"));String victim=owner.startsWith("node-a-")?"node-a":"node-b";long epoch=((Number)journal.status().get("epoch")).longValue();h.kill(victim);
                ClusterHarness.sql("mysql","INSERT INTO "+h.sourceTable()+" VALUES (41,100,'after-worker-crash')");h.await(()->((Number)journal.status().get("epoch")).longValue()>epoch&&h.count("id=41")==1,40);
                // Inject a genuine replica SQL-thread failure and attempt promotion while it is unhealthy.
                caughtUp(h);ClusterIntegrationTest.replicaControl("mysql-replica","STOP"," SQL_THREAD");
                try(Connection c=ClusterHarness.mysql("mysql-replica");Statement s=c.createStatement()){s.execute("SET GLOBAL super_read_only=OFF");s.execute("SET SESSION sql_log_bin=0");s.execute("DELETE FROM "+h.sourceTable()+" WHERE id=1");s.execute("SET GLOBAL super_read_only=ON");}
                ClusterHarness.sql("mysql","UPDATE "+h.sourceTable()+" SET amount=200 WHERE id=1");ClusterIntegrationTest.replicaControl("mysql-replica","START"," SQL_THREAD");h.await(()->replicaError()==1032&&h.count("id=1 AND amount=200")==1,30);
                ClusterHarness.sql("mysql","SET GLOBAL super_read_only=ON");ClusterHarness.sql("mysql-replica","SET GLOBAL super_read_only=OFF");ClusterHarness.sql("mysql-replica","SET GLOBAL read_only=OFF");
                Thread.sleep(1500);assertEquals(1,h.count("id=1 AND amount=200"));
                try(Connection c=ClusterHarness.mysql("mysql-replica");Statement s=c.createStatement()){s.execute("SET SESSION sql_log_bin=0");s.execute("INSERT INTO "+h.sourceTable()+" VALUES (1,100,'initial')");s.execute("SET GLOBAL super_read_only=ON");}
                ClusterHarness.sql("mysql","SET GLOBAL super_read_only=OFF");ClusterHarness.sql("mysql","SET GLOBAL read_only=OFF");ClusterIntegrationTest.replicaControl("mysql-replica","START"," SQL_THREAD");caughtUp(h);
                // Physical shutdown breaks the original socket; promotion is explicitly performed by this HA fixture.
                try{ClusterHarness.sql("mysql","SHUTDOWN");}catch(SQLException expected){/* shutdown closes the connection */}
                ClusterIntegrationTest.replicaControl("mysql-replica","STOP","");ClusterIntegrationTest.replicaControl("mysql-replica","RESET"," ALL");ClusterHarness.sql("mysql-replica","SET GLOBAL super_read_only=OFF");ClusterHarness.sql("mysql-replica","SET GLOBAL read_only=OFF");h.cleanupHost="mysql-replica";
                ClusterHarness.sql("mysql-replica","INSERT INTO "+h.sourceTable()+" VALUES (42,100,'new-primary')");ClusterHarness.sql("mysql-replica","UPDATE "+h.sourceTable()+" SET amount=300 WHERE id=2");
                h.await(()->h.count("id=42")==1&&h.count("id=2 AND amount=300")==1,60);
                assertEquals(snapshot.snapshotId,journal.snapshot("source",snapshot.tableId).snapshotId);assertFalse(journal.snapshot("source",snapshot.tableId).retained);
                h.await(()->journal.subscriptions().stream().allMatch(sub->{try{return sub.acknowledged==((Number)journal.status().get("lastSequence")).longValue();}catch(Exception e){return false;}}),30);
                purgeUncapturedTransaction(h,p,journal);
            }
        }finally{cleanPostgres(p);}
    }
}
