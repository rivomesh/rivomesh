package io.github.rivomesh;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.group",matches="true")
class GroupIntegrationTest {
    @Test void groupReplicationElectionDuringSnapshotPreservesProgressAndReplaysChanges() throws Exception {
        startGroup();
        Config config=ClusterHarness.config(); config.source.topology="AUTO";
        config.chunkSize=1;
        config.source.nodes=Arrays.asList(new Config.Endpoint("mysql",3306),new Config.Endpoint("mysql-2",3306),new Config.Endpoint("mysql-3",3306));
        try(ClusterHarness h=new ClusterHarness(config)) {
            h.start("group-node-a"); h.start("group-node-b");
            h.await(() -> !h.store.tables().isEmpty() && h.store.tables().get(0).copiedRows>0 && !h.store.tables().get(0).done,90);
            String oldUuid=String.valueOf(h.store.status().get("source_uuid"));
            long copied=h.store.tables().get(0).copiedRows;
            ClusterHarness.sql("mysql","UPDATE "+h.sourceTable()+" SET amount=777 WHERE id=40");
            // Actual primary server failure leaves two members with a majority; Group Replication elects its replacement.
            try { ClusterHarness.sql("mysql","SHUTDOWN"); } catch(SQLException expected) { /* admin socket closed */ }
            final String[] replacement={null};
            h.await(() -> {
                for(String host:Arrays.asList("mysql-2","mysql-3")) {
                    try {
                        if("PRIMARY".equals(ClusterHarness.scalar(host,"SELECT MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@global.server_uuid AND MEMBER_STATE='ONLINE'"))) {
                            replacement[0]=host; return true;
                        }
                    } catch(SQLException ignored) { /* election in progress */ }
                }
                return false;
            },60);
            h.cleanupHost=replacement[0];
            String newUuid=ClusterHarness.scalar(replacement[0],"SELECT @@global.server_uuid"); assertNotEquals(oldUuid,newUuid);
            ClusterHarness.sql(replacement[0],"INSERT INTO "+h.sourceTable()+" VALUES (99,999,'new-group-primary')");
            ClusterHarness.sql(replacement[0],"DELETE FROM "+h.sourceTable()+" WHERE id=2");
            ClusterHarness.sql(replacement[0],"UPDATE "+h.sourceTable()+" SET amount=888 WHERE id=1");
            h.await(() -> {
                assertTrue(h.store.tables().get(0).copiedRows>=copied,"Persisted snapshot cursor must not reset during election");
                return newUuid.equals(h.store.status().get("source_uuid")) && "STREAMING".equals(h.store.phase())
                        && h.count("id=99")==1 && h.count("id=2")==0 && h.count("id=1 AND amount=888")==1 && h.count("id=40 AND amount=777")==1;
            },90);
            assertEquals(40,h.count("1=1")); assertEquals("STREAMING",h.store.phase());
        }
    }
    private static Connection admin(String host) throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://"+host+":3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=60000","root",ClusterHarness.PASSWORD);
    }
    static boolean onlinePrimary(String host)throws SQLException{
        try(Connection c=admin(host);Statement statement=c.createStatement()){
            boolean old="SLAVE".equals(ClusterIntegrationTest.replicationKeyword(c));
            String sql=old?"SELECT MEMBER_ID FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@global.server_uuid AND MEMBER_STATE='ONLINE'"
                    :"SELECT MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@global.server_uuid AND MEMBER_STATE='ONLINE'";
            String local;
            try(ResultSet result=statement.executeQuery(sql)){local=result.next()?result.getString(1):null;}
            if(local==null)return false;
            if(!old)return "PRIMARY".equals(local);
            try(ResultSet result=statement.executeQuery("SHOW GLOBAL STATUS LIKE 'group_replication_primary_member'")){
                return result.next()&&local.equalsIgnoreCase(result.getString(2));
            }
        }
    }

    static void startGroup() throws Exception {
        String group="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        for(String host:Arrays.asList("mysql","mysql-2","mysql-3")) {
            try(Connection c=admin(host); Statement s=c.createStatement()) {
                s.execute("SET SESSION sql_log_bin=0");
                boolean old="SLAVE".equals(ClusterIntegrationTest.replicationKeyword(c));
                s.execute("CREATE USER IF NOT EXISTS 'gr_recovery'@'%' IDENTIFIED BY 'Sync@Test123'");
                s.execute(old?"GRANT REPLICATION SLAVE ON *.* TO 'gr_recovery'@'%'"
                        :"GRANT REPLICATION SLAVE,CONNECTION_ADMIN,BACKUP_ADMIN ON *.* TO 'gr_recovery'@'%'");
                try { s.execute("RESET BINARY LOGS AND GTIDS"); } catch(SQLException e) { if(!"42000".equals(e.getSQLState()))throw e; s.execute("RESET MASTER"); }
                s.execute("SET GLOBAL group_replication_group_name='"+group+"'");
                s.execute("SET GLOBAL group_replication_local_address='"+host+":33061'");
                s.execute("SET GLOBAL group_replication_group_seeds='mysql:33061,mysql-2:33061,mysql-3:33061'");
                s.execute("SET GLOBAL group_replication_single_primary_mode=ON");
                s.execute("SET GLOBAL group_replication_ssl_mode='DISABLED'");
                s.execute("SET GLOBAL group_replication_recovery_use_ssl=OFF");
                if(old){
                    s.execute("CHANGE MASTER TO MASTER_USER='gr_recovery',MASTER_PASSWORD='Sync@Test123' FOR CHANNEL 'group_replication_recovery'");
                }else{
                    s.execute("SET GLOBAL group_replication_recovery_get_public_key=ON");
                    s.execute("SET GLOBAL group_replication_consistency='BEFORE_ON_PRIMARY_FAILOVER'");
                    s.execute("CHANGE REPLICATION SOURCE TO SOURCE_USER='gr_recovery',SOURCE_PASSWORD='Sync@Test123' FOR CHANNEL 'group_replication_recovery'");
                }
            }
        }
        try(Connection c=admin("mysql"); Statement s=c.createStatement()) {
            s.execute("SET GLOBAL group_replication_bootstrap_group=ON"); s.execute("START GROUP_REPLICATION"); s.execute("SET GLOBAL group_replication_bootstrap_group=OFF");
        }
        for(String host:Arrays.asList("mysql-2","mysql-3")) try(Connection c=admin(host); Statement s=c.createStatement()) { s.execute("START GROUP_REPLICATION"); }
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);
        while(System.nanoTime()<end) {
            if("3".equals(ClusterHarness.scalar("mysql","SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'")))return;
            Thread.sleep(200);
        }
        fail("Group members did not become ONLINE");
    }
}
