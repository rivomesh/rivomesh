package io.github.rivomesh;

import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.ha",matches="true")
class ClusterIntegrationTest {
    @Test void multiNodeTakeoverSqlPauseReplicaErrorAndRealPrimaryFailureResumeFromGtid() throws Exception {
        Config config = ClusterHarness.config(); config.source.topology = "ASYNC";
        config.source.nodes = Arrays.asList(new Config.Endpoint("mysql",3306), new Config.Endpoint("mysql-replica",3306));
        provisionReplica();
        try (ClusterHarness h = new ClusterHarness(config)) {
            h.start("node-a"); h.start("node-b"); // Simultaneous cold starts race on schema, job creation and lease.
            h.await(() -> "STREAMING".equals(h.store.phase()) && h.count("1=1") == 40, 90);
            String firstOwner = h.owner(); assertNotNull(firstOwner);
            assertTrue(h.processes.get("node-a").isAlive()); assertTrue(h.processes.get("node-b").isAlive());
            // Kill only the application's dump connection; source replication has its own user and remains intact.
            try (Connection c = ClusterHarness.mysql("mysql"); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SHOW PROCESSLIST")) {
                List<Long> ids = new ArrayList<>();
                while (rs.next()) if ("root".equals(rs.getString("User")) && rs.getString("Command").startsWith("Binlog Dump")) ids.add(rs.getLong("Id"));
                assertFalse(ids.isEmpty());
                for (Long id : ids) ClusterHarness.sql("mysql", "KILL " + id);
            }
            ClusterHarness.sql("mysql", "INSERT INTO " + h.sourceTable() + " VALUES (41,100,'after-disconnect')");
            h.await(() -> h.count("id=41") == 1, 30);
            long copied = h.store.tables().get(0).copiedRows;
            String victim = h.owner(); h.kill(victim);
            ClusterHarness.sql("mysql", "INSERT INTO " + h.sourceTable() + " VALUES (42,100,'after-node-crash')");
            h.await(() -> h.owner() != null && !victim.equals(h.owner()) && h.count("id=42") == 1, 30);
            assertEquals(copied, h.store.tables().get(0).copiedRows, "Takeover must not restart the full scan");
            h.start("node-c");
            // A target SQL constraint error pauses the shared job, including newly started standby nodes.
            try (Connection c = h.store.connect()) { Sql.execute(c, "ALTER TABLE " + h.targetTable() + " ADD CONSTRAINT amount_positive CHECK(amount>=0)"); }
            ClusterHarness.sql("mysql", "INSERT INTO " + h.sourceTable() + " VALUES (43,-1,'poison')");
            ClusterHarness.sql("mysql", "INSERT INTO " + h.sourceTable() + " VALUES (44,100,'after-poison')");
            h.await(() -> "PAUSED".equals(h.store.runState()), 30);
            Map<String,Object> failed = h.store.status();
            assertEquals("APPLY", failed.get("error_stage")); assertEquals("23514", failed.get("error_sqlstate"));
            assertEquals("items", failed.get("error_table")); assertTrue(((Number)failed.get("error_sequence")).longValue()>0);
            assertFalse(String.valueOf(failed.get("error_gtid")).isEmpty()); assertEquals(0,h.count("id>=43"));
            assertTrue(((Number)failed.get("pending_transactions")).longValue()>0);
            h.start("node-d"); Thread.sleep(1000);
            assertEquals("PAUSED", h.store.runState()); assertEquals(0,h.count("id>=43"));
            try (Connection c = h.store.connect()) { Sql.execute(c, "ALTER TABLE " + h.targetTable() + " DROP CONSTRAINT amount_positive"); }
            h.store.resume(); h.await(() -> h.count("id IN (43,44)")==2 && "RUNNING".equals(h.store.runState()),30);
            // Make the MySQL replica's SQL thread fail with error 1032; do not allow it to become the source.
            waitReplica(h, "mysql");
            ClusterHarness.sql("mysql-replica", "STOP REPLICA SQL_THREAD");
            try (Connection c=ClusterHarness.mysql("mysql-replica"); Statement s=c.createStatement()) {
                s.execute("SET GLOBAL super_read_only=OFF"); s.execute("SET SESSION sql_log_bin=0");
                s.execute("DELETE FROM "+h.sourceTable()+" WHERE id=1"); s.execute("SET GLOBAL super_read_only=ON");
            }
            ClusterHarness.sql("mysql", "UPDATE "+h.sourceTable()+" SET amount=200 WHERE id=1");
            ClusterHarness.sql("mysql-replica", "START REPLICA SQL_THREAD");
            h.await(() -> replicaError()==1032,30); h.await(() -> h.count("id=1 AND amount=200")==1,30);
            ClusterHarness.sql("mysql", "SET GLOBAL super_read_only=ON");
            ClusterHarness.sql("mysql-replica", "SET GLOBAL super_read_only=OFF"); ClusterHarness.sql("mysql-replica", "SET GLOBAL read_only=OFF");
            h.await(() -> "WAITING_SOURCE".equals(h.store.runState()) && "SOURCE_REPLICATION_ERROR".equals(h.store.status().get("error_code")),30);
            assertEquals(1,h.count("id=1 AND amount=200"));
            // Repair in the database, then resume its SQL thread. No sync-side skip or GTID rewrite is used.
            ClusterHarness.sql("mysql-replica", "SET GLOBAL super_read_only=ON");
            ClusterHarness.sql("mysql", "SET GLOBAL super_read_only=OFF"); ClusterHarness.sql("mysql", "SET GLOBAL read_only=OFF");
            try (Connection c=ClusterHarness.mysql("mysql-replica"); Statement s=c.createStatement()) {
                s.execute("SET GLOBAL super_read_only=OFF"); s.execute("SET SESSION sql_log_bin=0");
                s.execute("INSERT INTO "+h.sourceTable()+" VALUES (1,100,'initial')"); s.execute("SET GLOBAL super_read_only=ON");
            }
            ClusterHarness.sql("mysql-replica", "START REPLICA SQL_THREAD"); waitReplica(h,"mysql");
            h.await(() -> "RUNNING".equals(h.store.runState()),30);
            String oldUuid=String.valueOf(h.store.status().get("source_uuid"));
            String newUuid=ClusterHarness.scalar("mysql-replica","SELECT @@global.server_uuid");
            assertNotEquals(oldUuid,newUuid);
            // Real source process shutdown severs the socket. An external HA controller promotes the caught-up replica.
            try { ClusterHarness.sql("mysql","SHUTDOWN"); } catch(SQLException expected) { /* server closes admin connection */ }
            h.await(() -> "WAITING_SOURCE".equals(h.store.runState()),30);
            ClusterHarness.sql("mysql-replica","STOP REPLICA"); ClusterHarness.sql("mysql-replica","RESET REPLICA ALL");
            ClusterHarness.sql("mysql-replica","SET GLOBAL super_read_only=OFF"); ClusterHarness.sql("mysql-replica","SET GLOBAL read_only=OFF");
            h.cleanupHost="mysql-replica";
            ClusterHarness.sql("mysql-replica","INSERT INTO "+h.sourceTable()+" VALUES (45,100,'new-primary')");
            ClusterHarness.sql("mysql-replica","UPDATE "+h.sourceTable()+" SET amount=300 WHERE id=2");
            h.await(() -> newUuid.equals(h.store.status().get("source_uuid")) && h.count("id=45")==1 && h.count("id=2 AND amount=300")==1,60);
            assertEquals(copied,h.store.tables().get(0).copiedRows);
            assertEquals("STREAMING",h.store.phase());
            assertTrue(Gtids.contains(h.store.captured().gtidSet, oldUuid+":1"));
        }
    }
    static String replicationKeyword(Connection connection)throws SQLException{
        return connection.getMetaData().getDatabaseMajorVersion()<8?"SLAVE":"REPLICA";
    }

    static void replicaControl(String host,String action,String options)throws Exception{
        try(Connection connection=ClusterHarness.mysql(host);Statement statement=connection.createStatement()){
            statement.execute(action+" "+replicationKeyword(connection)+options);
        }
    }

    static void provisionReplica() throws Exception {
        try(Connection c=ClusterHarness.mysql("mysql"); Statement s=c.createStatement()) {
            s.execute("SET SESSION sql_log_bin=0"); s.execute("CREATE USER IF NOT EXISTS 'sync_repl'@'%' IDENTIFIED BY 'Sync@Test123'");
            s.execute("GRANT REPLICATION SLAVE ON *.* TO 'sync_repl'@'%'");
        }
        String baseline=ClusterHarness.scalar("mysql","SELECT @@global.gtid_executed");
        try(Connection c=ClusterHarness.mysql("mysql-replica"); Statement s=c.createStatement()) {
            s.execute("SET GLOBAL super_read_only=OFF"); s.execute("SET GLOBAL read_only=OFF");
            try { s.execute("RESET BINARY LOGS AND GTIDS"); } catch(SQLException e) { if(!"42000".equals(e.getSQLState()))throw e; s.execute("RESET MASTER"); }
            // Fresh controlled fixtures have identical system users and no business data; establish the bootstrap baseline.
            s.execute("SET GLOBAL gtid_purged='"+baseline+"'");
            boolean old="SLAVE".equals(replicationKeyword(c));
            s.execute(old?"CHANGE MASTER TO MASTER_HOST='mysql',MASTER_USER='sync_repl',MASTER_PASSWORD='Sync@Test123',MASTER_AUTO_POSITION=1"
                    :"CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql',SOURCE_USER='sync_repl',SOURCE_PASSWORD='Sync@Test123',SOURCE_AUTO_POSITION=1,GET_SOURCE_PUBLIC_KEY=1");
            s.execute("SET GLOBAL super_read_only=ON"); s.execute("START "+replicationKeyword(c));
        }
    }
    private static int replicaError() throws Exception {
        try(Connection c=ClusterHarness.mysql("mysql-replica"); Statement s=c.createStatement(); ResultSet rs=s.executeQuery("SHOW REPLICA STATUS")) {
            assertTrue(rs.next()); return rs.getInt("Last_SQL_Errno");
        }
    }
    private static void waitReplica(ClusterHarness h,String source) throws Exception {
        String executed=ClusterHarness.scalar(source,"SELECT @@global.gtid_executed");
        h.await(() -> Gtids.contains(ClusterHarness.scalar("mysql-replica","SELECT @@global.gtid_executed"),executed) && replicaError()==0,30);
    }
}
