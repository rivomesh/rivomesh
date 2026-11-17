package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

/** The same durable fan-out contract on real MySQL and PostgreSQL-family storage. */
@EnabledIfSystemProperty(named="sync.integration",matches="true")
class JournalIntegrationTest {
    private final JournalTest fixture=new JournalTest();
    SourceTransaction transaction(int ordinal){return fixture.transaction(ordinal);}
    TableSchema schema(){return fixture.schema();}
    void initialize(JdbcJournal j)throws Exception{fixture.initialize(j);}
    JdbcJournal.Settings settings(){
        JdbcJournal.Settings s=new JdbcJournal.Settings();s.namespace="journal_"+UUID.randomUUID().toString().replace("-","").substring(0,12);s.pipelineId="pipeline";
        s.driver=System.getProperty("target.driver","io.github.rivomesh.gaussdb.jdbc.Driver");
        s.url=System.getProperty("target.url","jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10");
        s.username="sync_test";s.password="Sync@Test123";return s;
    }
    @Test void externalMySqlAndPostgresFamilyStoragePreserveCommittedHistoryAndFences()throws Exception{
        JdbcJournal.Settings postgres=settings();
        JdbcJournal.Settings mysql=settings();mysql.driver="com.mysql.cj.jdbc.Driver";mysql.url="jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000";mysql.username="root";
        JdbcJournal.Settings gauss=settings();gauss.driver="io.github.rivomesh.gaussdb.jdbc.Driver";gauss.url="jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10";
        for(JdbcJournal.Settings s:Arrays.asList(mysql,postgres,gauss))try{
            try(JdbcJournal j=new JdbcJournal(s)){
                initialize(j);Journal.Lease lease=j.claim("node-1",30000);j.retrying(lease,"fast","TARGET_CONNECTION_RETRY");assertEquals("TARGET_CONNECTION_RETRY",j.subscriptions().get(0).errorCode);assertEquals(0,j.subscriptions().get(0).errorGeneration);j.retrying(lease,"fast",null);assertNull(j.subscriptions().get(0).errorCode);
                JdbcPublicationStore publication=new JdbcPublicationStore(j,"fast",lease);publication.activate(lease.epoch);assertNull(publication.progress());publication.progress(lease.epoch,null,"native-position-one");assertThrows(IllegalStateException.class,()->publication.progress(lease.epoch,null,"invalid-overwrite"));assertEquals("native-position-one",publication.progress());
                Journal.SnapshotState snapshot=j.beginSnapshot(lease,"source",schema(),transaction(1).position);
                j.appendSnapshot(lease,snapshot,0,Arrays.asList(Collections.singletonList(Cell.value("duplicate")),Collections.singletonList(Cell.value("duplicate"))));j.finishSnapshot(lease,snapshot);
                assertEquals(1,j.append(lease,transaction(1)));assertEquals(1,j.append(lease,transaction(1)));assertEquals(2,j.append(lease,transaction(2)));
                j.acknowledge(lease,"fast",1);j.acknowledge(lease,"fast",2);j.pause(lease,"slow","SQL_ERROR");assertEquals(0,j.collect(lease,100));j.release(lease);
                Journal.Lease next=j.claim("node-2",30000);assertTrue(next.epoch>lease.epoch);
                assertThrows(JdbcJournal.Fenced.class,()->j.append(lease,transaction(3)));j.release(next);
                assertThrows(JdbcJournal.Fenced.class,()->publication.progress(lease.epoch,"native-position-one","stale-position"));
            }
            try(JdbcJournal j=new JdbcJournal(s)){
                initialize(j);Journal.Lease lease=j.claim("node-3",30000);assertEquals(transaction(2).position,j.captured());
                JdbcPublicationStore publication=new JdbcPublicationStore(j,"fast",lease);publication.activate(lease.epoch);assertEquals("native-position-one",publication.progress());publication.progress(lease.epoch,"native-position-one","native-position-two");assertEquals("native-position-two",publication.progress());
                assertThrows(IllegalStateException.class,()->j.resume("slow",0));j.resume("slow",1);
                assertEquals(2,j.read("slow",100).size());
                Journal.SnapshotState snapshot=j.snapshot("source","table");assertEquals(2,j.snapshotParts("source","table",snapshot.snapshotId,0,10).get(0).rows.size());
                j.acknowledge(lease,"slow",1);j.acknowledge(lease,"slow",2);assertEquals(2,j.collect(lease,100));
                assertEquals(1,j.append(lease,transaction(1)));assertEquals(0,((Number)j.status().get("pendingBytes")).longValue());
            }
            String original=s.namespace;s.namespace=original+"_norm";
            try(JdbcJournal journal=new JdbcJournal(s)){NormalizationTest.contract(journal);}
            finally{try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);Statement st=c.createStatement()){
                st.execute(s.driver.startsWith("com.mysql")?"DROP DATABASE `"+s.namespace+"`":"DROP SCHEMA \""+s.namespace+"\" CASCADE");
            }finally{s.namespace=original;}}
        }finally{
            try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);Statement st=c.createStatement()){
                st.execute(s.driver.startsWith("com.mysql")?"DROP DATABASE `"+s.namespace+"`":"DROP SCHEMA \""+s.namespace+"\" CASCADE");
            }
        }
    }
}
