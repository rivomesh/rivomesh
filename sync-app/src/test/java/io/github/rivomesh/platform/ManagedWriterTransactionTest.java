package io.github.rivomesh.platform;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.Model;
import io.github.rivomesh.Sql;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.JournalWriterAssignment;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

/** Exercises the transactional SQL substrate without granting candidate admission through an endpoint. */
@EnabledIfSystemProperty(named="sync.handoff.transaction.jdbc",matches="jdbc:postgresql://127\\.0\\.0\\.1:[0-9]+/[^?]+")
class ManagedWriterTransactionTest {
    private static final String TASK="handoff-orders",PREPARATION="handoff_transaction_prepared",OWNER="first",CANDIDATE="second";
    private static final String EXECUTION="handoff_transaction_execution";
    private final String url=System.getProperty("sync.handoff.transaction.jdbc");
    private final String username=System.getProperty("sync.handoff.transaction.username","postgres");
    private final String password=System.getenv().getOrDefault("RM_HANDOFF_FIXTURE_PASSWORD","");
    private final String suffix=UUID.randomUUID().toString().substring(0,8),journal="handoff_journal_"+suffix;
    private final String firstDirectory=UUID.randomUUID().toString(),nextDirectory=UUID.randomUUID().toString();
    private final String first=UUID.randomUUID().toString(),next=UUID.randomUUID().toString();
    private final String generation=UUID.randomUUID().toString();
    private final SourcePosition position=new SourcePosition(1,"MYSQL","fixture-source",Map.of("gtidSet","fixture:1-2"));

    @Test void rollbackAndCommitPreserveOneNativeEpochAndSignedRetirement()throws Exception{
        PlatformConfig config=configuration();
        try{
            SharedAccessStore access=new SharedAccessStore(config,AccountDirectory.configured(config),null,Map.of());
            AccessControl.Principal actor=AccountDirectory.authenticate(access.accounts(),"Bearer "+config.authToken);
            GlobalTaskStore store=new GlobalTaskStore(config.cluster,access,config.cluster.taskKey,null,32);
            JdbcControlStore database=store.database();
            new GlobalTaskViews(config.cluster,store,new AgentDirectory(config.cluster,access,store),15000);
            seedJournal();
            String active="""
                    formatVersion: 3
                    pipelineId: %s
                    nodeId: %s
                    journal:
                      mode: EXTERNAL
                      url: %s
                      namespace: %s
                      managedWriter: true
                      writerAssignment: %s
                    """.formatted(TASK,OWNER,url,journal,first);
            GlobalTaskStore.Record original=store.create(TASK,OWNER,firstDirectory,active,"handoff_transaction_original",actor);
            store.service((c,accounts)->{store.applied(c,original,original.configurationVersion);return null;});
            String checkpoint=AuditStore.hash(Model.JSON.writeValueAsString(position)),nativeProof="a".repeat(64);
            JournalWriterAssignment.CutProof cut=new JournalWriterAssignment.CutProof(Codec.sha256(generation),nativeProof,List.of("pg"),true,
                    new JournalWriterAssignment.ManagedCut(OWNER,Codec.sha256(first),9,2,checkpoint));
            HandoffPreparations.Evidence evidence=new HandoffPreparations.Evidence("handoff_transaction_assess",original.revision,
                    original.configurationVersion,new HandoffPreparations.Binding(OWNER,firstDirectory,UUID.randomUUID().toString(),3),
                    new HandoffPreparations.Binding(CANDIDATE,nextDirectory,UUID.randomUUID().toString(),4),
                    new HandoffPreparations.JournalCut("9","2",checkpoint),"b".repeat(64),"c".repeat(64));
            String staged="d".repeat(64),successor=ManagedWriterDeclaration.successorYaml(active,OWNER,CANDIDATE,next);
            store.transaction(TASK,actor,(c,row,current)->{
                HandoffPreparations.Plan preparation=store.preparations().prepare(c,row,PREPARATION,current,evidence,
                        new HandoffPreparations.WriterActivation(Codec.sha256(first),next,Codec.sha256(generation),nativeProof,true),database.now(c));
                return store.preparations().bindExecution(c,preparation,EXECUTION);
            });
            sql("UPDATE "+table("job")+" SET owner='fixture-live',lease_until=9223372036854775807");
            assertEquals("HANDOFF_MANAGED_CONTROL_CUT_CHANGED",assertThrows(IllegalStateException.class,()->
                    store.transaction(TASK,actor,(c,row,current)->commit(store,c,row,current,cut,successor,staged))).getMessage());
            sql("UPDATE "+table("job")+" SET owner=NULL,lease_until=0");
            assertThrows(IOException.class,()->store.transaction(TASK,actor,(c,row,current)->{
                commit(store,c,row,current,cut,successor,staged);
                throw new IOException("fixture failure before commit");
            }));
            assertEquals(OWNER,store.get(TASK,actor).owner);
            assertEquals(original.configurationVersion,store.get(TASK,actor).activeConfigurationVersion);
            assertEquals("9",value("SELECT epoch FROM "+table("job")));
            assertEquals("RUNNING",value("SELECT run_state FROM "+table("job")));
            assertEquals("11",value("SELECT format_version FROM "+table("job")));
            assertEquals(first,value("SELECT assignment_id FROM "+table("writer_assignment")));
            assertEquals("1",value("SELECT COUNT(*) FROM "+table("writer_assignment_history")));
            HandoffPreparations.Plan live=store.service((c,accounts)->store.preparations().byRequest(c,PREPARATION,database.now(c)));
            assertEquals("PREPARED",live.phase());
            assertEquals(EXECUTION,live.executionRequest());
            HandoffPreparations.Plan moved=store.transaction(TASK,actor,(c,row,current)->commit(store,c,row,current,cut,successor,staged));
            GlobalTaskStore.Record installed=store.get(TASK,actor);
            assertEquals(CANDIDATE,installed.owner);
            assertEquals(nextDirectory,installed.directory);
            assertEquals(successor,installed.yaml);
            assertEquals("COMMITTED",moved.phase());
            assertEquals(10,moved.writerCommit().journalEpoch());
            assertEquals(installed.activeConfigurationVersion,moved.writerCommit().configuration());
            assertEquals(next,value("SELECT assignment_id FROM "+table("writer_assignment")));
            assertEquals("10",value("SELECT epoch FROM "+table("job")));
            assertEquals("HANDOFF_PARKED",value("SELECT run_state FROM "+table("job")));
            assertEquals("15",value("SELECT format_version FROM "+table("job")));
            assertEquals(generation,value("SELECT generation FROM "+table("job")));
            assertEquals("2",value("SELECT COUNT(*) FROM "+table("writer_assignment_history")));
            String retired=store.service((c,accounts)->store.retiredConfiguration(c,installed,OWNER,firstDirectory));
            assertEquals(active,retired);
            assertThrows(TaskService.Conflict.class,()->store.service((c,accounts)->
                    store.retiredConfiguration(c,installed,OWNER,UUID.randomUUID().toString())));
            try(JdbcJournal stale=new JdbcJournal(writerSettings(first,OWNER))){
                assertThrows(JdbcJournal.Fenced.class,()->stale.claim("former-worker",30000));
            }
            try(JdbcJournal admitted=new JdbcJournal(writerSettings(next,CANDIDATE))){
                assertThrows(JdbcJournal.PipelinePaused.class,()->admitted.claim("candidate-worker",30000));
                assertThrows(JdbcJournal.PipelinePaused.class,()->admitted.resumePipeline(0));
            }
            var recipientCut=new JournalWriterAssignment.CutProof(Codec.sha256(generation),nativeProof,List.of("pg"),true,
                    new JournalWriterAssignment.ManagedCut(CANDIDATE,Codec.sha256(next),10,2,checkpoint));
            store.service((c,accounts)->{
                JournalWriterAssignment.verifyRecipientControlCut(c,journal,TASK,first,next,recipientCut);
                return null;
            });
            assertEquals("10",value("SELECT epoch FROM "+table("job")));
            assertEquals("2",value("SELECT normalized_seq FROM "+table("job")));
        }finally{
            sql("DROP SCHEMA IF EXISTS "+Sql.pg(journal)+" CASCADE");
            sql("DROP SCHEMA IF EXISTS "+Sql.pg(config.cluster.namespace)+" CASCADE");
        }
    }

    @Test void externalJournalCommitReplaysOnlyTheSameParkedWriterCut()throws Exception{
        seedJournal();
        String checkpoint=AuditStore.hash(Model.JSON.writeValueAsString(position));
        var managed=new JournalWriterAssignment.ManagedCut(OWNER,Codec.sha256(first),9,2,checkpoint);
        var proof=new JournalWriterAssignment.CutProof(Codec.sha256(generation),"a".repeat(64),List.of("pg"),true,managed);
        try{
            JournalWriterAssignment.Placement placed;
            try(Connection journal=database()){
                journal.setAutoCommit(false);
                placed=JournalWriterAssignment.parkExternalRecipient(journal,this.journal,TASK,first,proof,next,CANDIDATE);
                journal.rollback();
            }
            assertEquals(first,value("SELECT assignment_id FROM "+table("writer_assignment")));
            try(Connection journal=database()){
                journal.setAutoCommit(false);
                assertEquals(placed,JournalWriterAssignment.parkExternalRecipient(journal,this.journal,TASK,first,proof,next,CANDIDATE));
                journal.commit();
            }
            try(Connection journal=database()){
                journal.setAutoCommit(false);
                assertEquals(placed,JournalWriterAssignment.parkExternalRecipient(journal,this.journal,TASK,first,proof,next,CANDIDATE));
                journal.commit();
            }
            assertEquals("10",value("SELECT epoch FROM "+table("job")));
            assertEquals("HANDOFF_PARKED",value("SELECT run_state FROM "+table("job")));
            assertEquals("2",value("SELECT COUNT(*) FROM "+table("writer_assignment_history")));
            try(Connection journal=database()){
                journal.setAutoCommit(false);
                assertThrows(IllegalStateException.class,()->JournalWriterAssignment.parkExternalRecipient(
                        journal,this.journal,TASK,first,proof,UUID.randomUUID().toString(),CANDIDATE));
                journal.rollback();
            }
            var changed=new JournalWriterAssignment.CutProof(Codec.sha256(generation),"b".repeat(64),List.of("pg"),true,managed);
            try(Connection journal=database()){
                journal.setAutoCommit(false);
                assertEquals("HANDOFF_RECIPIENT_NATIVE_HISTORY_CHANGED",assertThrows(IllegalStateException.class,()->
                        JournalWriterAssignment.parkExternalRecipient(journal,this.journal,TASK,first,changed,next,CANDIDATE)).getMessage());
                journal.rollback();
            }
            try(JdbcJournal stale=new JdbcJournal(writerSettings(first,OWNER));
                JdbcJournal parked=new JdbcJournal(writerSettings(next,CANDIDATE))){
                assertThrows(JdbcJournal.Fenced.class,()->stale.claim("old-worker",30000));
                assertThrows(JdbcJournal.PipelinePaused.class,()->parked.claim("new-worker",30000));
            }
        }finally{
            sql("DROP SCHEMA IF EXISTS "+Sql.pg(journal)+" CASCADE");
        }
    }

    @Test void sealedSplitApprovalRemainsReservedAfterReviewExpiry()throws Exception{
        PlatformConfig config=configuration();
        try{
            var access=new SharedAccessStore(config,AccountDirectory.configured(config),null,Map.of());
            var actor=AccountDirectory.authenticate(access.accounts(),"Bearer "+config.authToken);
            var store=new GlobalTaskStore(config.cluster,access,config.cluster.taskKey,null,32);
            var original=store.create(TASK,OWNER,firstDirectory,"formatVersion: 3\n",
                    "handoff_transaction_original",actor);
            String checkpoint=AuditStore.hash(Model.JSON.writeValueAsString(position));
            var cut=new JournalWriterAssignment.CutProof(Codec.sha256(generation),"a".repeat(64),List.of("pg"),true,
                    new JournalWriterAssignment.ManagedCut(OWNER,Codec.sha256(first),9,2,checkpoint));
            var candidate=new HandoffPreparations.Binding(CANDIDATE,nextDirectory,UUID.randomUUID().toString(),4);
            var evidence=new HandoffPreparations.Evidence("handoff_transaction_assess",original.revision,
                    original.configurationVersion,new HandoffPreparations.Binding(OWNER,firstDirectory,UUID.randomUUID().toString(),3),
                    candidate,new HandoffPreparations.JournalCut("9","2",checkpoint),"b".repeat(64),"c".repeat(64));
            var activation=new HandoffPreparations.WriterActivation(Codec.sha256(first),next,cut.journalGenerationSha256(),
                    cut.nativeEvidenceSha256(),false,true);
            var admission=new ManagedHandoff.CandidateAdmission(PREPARATION,candidate,"STOPPED","d".repeat(64),"e".repeat(64));
            var transfer=new HandoffPreparations.SplitTransfer(EXECUTION,admission,cut);
            HandoffPreparations plans=store.preparations();
            long expiry=store.service((c,accounts)->{
                long now=store.database().now(c);
                var plan=plans.prepare(c,original,PREPARATION,actor,evidence,activation,now);
                plan=plans.bindExecution(c,plan,EXECUTION);
                plans.sealSplitTransfer(c,plan,transfer,now);
                return plan.expires();
            });
            store.service((c,accounts)->{
                var pending=plans.byRequest(c,PREPARATION,expiry+1);
                assertEquals("PREPARED",pending.phase());
                assertEquals(transfer,plans.splitTransfer(c,pending));
                assertEquals(PREPARATION,plans.nextSplitTransfer(c,CANDIDATE,""));
                assertNull(plans.nextSplitTransfer(c,OWNER,""));
                assertEquals("HANDOFF_SPLIT_TRANSFER_PENDING",assertThrows(TaskService.Conflict.class,()->
                        plans.release(c,pending,"handoff_transaction_release",actor,expiry+1)).getMessage());
                assertEquals("HANDOFF_EXECUTION_REQUEST_CHANGED",assertThrows(TaskService.Conflict.class,()->
                        HandoffPreparations.requireExecution(pending,"handoff_transaction_replacement")).getMessage());
                return null;
            });
        }finally{
            sql("DROP SCHEMA IF EXISTS "+Sql.pg(config.cluster.namespace)+" CASCADE");
        }
    }

    private HandoffPreparations.Plan commit(GlobalTaskStore store,Connection c,GlobalTaskStore.Record row,
            AccessControl.Principal actor,JournalWriterAssignment.CutProof cut,String successor,String staged)throws Exception{
        HandoffPreparations.Plan plan=store.preparations().byRequest(c,PREPARATION,store.database().now(c));
        return store.commitPreparedWriter(c,row,plan,actor,cut,successor,staged,EXECUTION);
    }
    private PlatformConfig configuration(){
        PlatformConfig config=new PlatformConfig();config.authToken="handoff-txn-fixture-token-with-at-least-32-characters";
        config.cluster=new ClusterConfig();config.cluster.id="handoff-fixture";config.cluster.nodeId=OWNER;
        config.cluster.sharedAccess=true;config.cluster.sharedTasks=true;config.cluster.url=url;config.cluster.username=username;
        config.cluster.password=password;config.cluster.namespace="handoff_control_"+suffix;
        config.cluster.taskKey=GlobalTaskStoreTest.key(5);return config;
    }
    private JdbcJournal.Settings writerSettings(String assignment,String node){
        JdbcJournal.Settings s=new JdbcJournal.Settings();s.driver="org.postgresql.Driver";s.url=url;s.username=username;s.password=password;
        s.namespace=journal;s.pipelineId=TASK;s.managedWriter=true;s.writerAssignment=assignment;s.writerNode=node;return s;
    }
    private void seedJournal()throws Exception{
        sql("CREATE SCHEMA "+Sql.pg(journal));
        sql("CREATE TABLE "+table("job")+" (pipeline_id text PRIMARY KEY,format_version integer,fingerprint text,generation text,"
                +"epoch bigint,owner text,lease_until bigint,next_seq bigint,captured text,pending_bytes bigint,snapshot_bytes bigint,"
                +"normalized_seq bigint,identity_bytes bigint,run_state text,error_generation bigint,error_code text)");
        sql("CREATE TABLE "+table("writer_assignment")+" (pipeline_id text PRIMARY KEY,assignment_id text)");
        sql("CREATE TABLE "+table("writer_assignment_history")+" (pipeline_id text,assignment_id text,node_id text,previous_assignment text,"
                +"generation text,activated_epoch bigint,last_sequence bigint,checkpoint_hash text,evidence_hash text,PRIMARY KEY(pipeline_id,assignment_id))");
        sql("CREATE TABLE "+table("subscription")+" (pipeline_id text,subscription_id text,acknowledged bigint,run_state text)");
        try(Connection c=database()){
            JdbcControlStore.execute(c,"INSERT INTO "+table("writer_assignment")+" VALUES ('"+TASK+"','"+first+"')");
            JdbcControlStore.execute(c,"INSERT INTO "+table("writer_assignment_history")+" VALUES ('"+TASK+"','"+first+"','"+OWNER+"',NULL,'"+generation+"',0,0,NULL,NULL)");
            JdbcControlStore.execute(c,"INSERT INTO "+table("subscription")+" VALUES ('"+TASK+"','pg',2,'RUNNING')");
            try(PreparedStatement p=c.prepareStatement("INSERT INTO "+table("job")+" VALUES (?,11,?, ?,9,NULL,0,3,?,0,0,2,0,'RUNNING',0,NULL)")){
                p.setString(1,TASK);p.setString(2,"f".repeat(64));p.setString(3,generation);p.setString(4,Codec.encode(position));p.executeUpdate();
            }
        }
    }
    private String table(String name){return Sql.pg(journal)+".rm3_"+name;}
    private Connection database()throws SQLException{return JdbcConnections.open("org.postgresql.Driver",url,username,password);}
    private void sql(String statement)throws Exception{try(Connection c=database()){JdbcControlStore.execute(c,statement);}}
    private String value(String query)throws Exception{
        try(Connection c=database();Statement s=c.createStatement();ResultSet r=s.executeQuery(query)){assertTrue(r.next());return r.getString(1);}
    }
}
