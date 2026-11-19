package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.v3.group",matches="true")
class PipelineGroupIntegrationTest {
    @Test void electionDuringIncompleteMvccArtifactRebasesOnlyThatSnapshotAndCatchesUpBothTargets()throws Exception{
        GroupIntegrationTest.startGroup();Config legacy=ClusterHarness.config();legacy.source.topology="GROUP_REPLICATION";legacy.source.nodes=Arrays.asList(new Config.Endpoint("mysql",3306),new Config.Endpoint("mysql-2",3306),new Config.Endpoint("mysql-3",3306));
        boolean sqlTargets=Boolean.getBoolean("sync.v3.group.sqlTargets");
        if(sqlTargets){
            legacy.target.type="POSTGRESQL";
            legacy.target.driver="org.postgresql.Driver";
        }else{
            legacy.target.type="GAUSS";
            legacy.target.driver="io.github.rivomesh.gaussdb.jdbc.Driver";
            legacy.target.url="jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10";
        }
        PipelineConfig p=PipelineClusterIntegrationTest.pipeline(legacy,!sqlTargets);p.chunkSize=1;
        if(sqlTargets){
            PipelineConfig.Target mysql=new PipelineConfig.Target();
            mysql.id="mysql";
            mysql.product=TargetProduct.MYSQL;
            mysql.url="jdbc:mysql://mysql-target:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000";
            mysql.username="root";
            mysql.password=ClusterHarness.PASSWORD;
            mysql.metadataSchema=legacy.target.metadataSchema;
            mysql.mappings.put(legacy.source.database,legacy.target.schema);
            p.targets.add(mysql);
        }
        String sourceVersion=ClusterHarness.scalar("mysql","SELECT VERSION()");
        try(ClusterHarness h=new ClusterHarness(legacy);JdbcJournal journal=new JdbcJournal(p.journalSettings())){
            h.startPipeline("group-a",p);h.startPipeline("group-b",p);String tableId=TableSchema.identity(legacy.source.database,"","items");
            h.await(()->{Journal.SnapshotState s=journal.snapshot("source",tableId);return s!=null&&s.nextPart>0&&"BUILDING".equals(s.state);},90);
            Journal.SnapshotState incomplete=journal.snapshot("source",tableId);ClusterHarness.sql("mysql","UPDATE "+h.sourceTable()+" SET amount=777 WHERE id=40");
            try{ClusterHarness.sql("mysql","SHUTDOWN");}catch(SQLException expected){/* real primary process exits */}
            String[] primary={null};h.await(()->{
                for(String host:Arrays.asList("mysql-2","mysql-3"))try{if(GroupIntegrationTest.onlinePrimary(host)){primary[0]=host;return true;}}catch(SQLException ignored){/* election */}return false;
            },90);h.cleanupHost=primary[0];
            ClusterHarness.sql(primary[0],"INSERT INTO "+h.sourceTable()+" VALUES (99,999,'new-group-primary')");ClusterHarness.sql(primary[0],"DELETE FROM "+h.sourceTable()+" WHERE id=2");ClusterHarness.sql(primary[0],"UPDATE "+h.sourceTable()+" SET amount=888 WHERE id=1");
            h.await(()->h.count("id=99")==1&&h.count("id=2")==0&&h.count("id=1 AND amount=888")==1&&h.count("id=40 AND amount=777")==1
                    &&(!sqlTargets||mysqlMatchesAfterElection(legacy.target.schema)),90);
            h.await(()->((Number)journal.status().get("snapshotBytes")).longValue()==0&&((Number)journal.status().get("pendingBytes")).longValue()==0,30);
            Journal.SnapshotState completed=journal.snapshot("source",tableId);assertEquals("COMPLETE",completed.state);assertNotEquals(incomplete.snapshotId,completed.snapshotId);assertEquals(40,completed.rows);assertEquals(40,h.count("TRUE"));
            h.await(()->{
                long sequence=((Number)journal.status().get("lastSequence")).longValue();
                List<Journal.Subscription> subscriptions=journal.subscriptions();
                return subscriptions.size()==2&&subscriptions.stream().allMatch(subscription->subscription.acknowledged==sequence);
            },30);
            System.out.println("Group Replication elected "+primary[0]+" on MySQL "+sourceVersion+"; both subscriptions caught up");
        }finally{
            PipelineClusterIntegrationTest.cleanPostgres(p);
            if(sqlTargets)try(Connection c=ClusterHarness.mysql("mysql-target");Statement s=c.createStatement()){
                s.execute("DROP DATABASE IF EXISTS "+Sql.mysql(legacy.target.schema));
                s.execute("DROP DATABASE IF EXISTS "+Sql.mysql(legacy.target.metadataSchema));
            }
        }
    }

    private static boolean mysqlMatchesAfterElection(String schema)throws SQLException{
        String sql="SELECT COUNT(*),SUM(id=99),SUM(id=2),SUM(id=1 AND amount=888),SUM(id=40 AND amount=777) FROM "+Sql.mysql(schema)+".items";
        try(Connection c=ClusterHarness.mysql("mysql-target");Statement s=c.createStatement();ResultSet rows=s.executeQuery(sql)){
            return rows.next()&&rows.getInt(1)==40&&rows.getInt(2)==1&&rows.getInt(3)==0&&rows.getInt(4)==1&&rows.getInt(5)==1;
        }
    }
}
