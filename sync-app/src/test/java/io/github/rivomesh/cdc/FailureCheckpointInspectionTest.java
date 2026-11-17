package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.recovery.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class FailureCheckpointInspectionTest {
    @TempDir Path directory;
    private FailureReviewTest source;
    JdbcSink.Settings target;
    FailureEvidenceStore store;
    JdbcJournal journal;
    Journal.Lease lease;
    FailureEvidenceStore.Envelope retained;
    TableSchema table;
    SourceTransaction normalized;
    @BeforeEach void prepare()throws Exception{
        source=new FailureReviewTest();source.directory=directory;
        JdbcSinkTest fixture=new JdbcSinkTest();fixture.directory=directory;target=fixture.settings();
        if(System.getenv("RIVOMESH_FAILURE_DB_URL")!=null){
            JdbcJournal.Settings sql=source.settings();target.driver=sql.driver;target.url=sql.url;target.username=sql.username;target.password=sql.password;target.product=sql.driver.startsWith("com.mysql")?TargetProduct.MYSQL:TargetProduct.POSTGRESQL;
            target.metadataSchema="cp_"+UUID.randomUUID().toString().replace("-","");target.databases.put("sales","dst_"+UUID.randomUUID().toString().replace("-",""));
        }
        journal=new JdbcJournal(source.settings());journal.initialize(target.fingerprint,"generation",Collections.singletonList("target"));lease=journal.claim("owner",30000);
        table=JdbcSinkTest.schema(false);journal.saveSchema(lease,table);
        SourceTransaction raw=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"private-cp-value"));journal.append(lease,raw);
        normalized=journal.normalize(lease,journal.read("target",1).get(0),Collections.singletonMap(table.id,table),Collections.singletonMap(table.id,raw.position),(p,id)->false);
        try(JdbcSink sink=new JdbcSink(target)){sink.activate(lease.epoch);JdbcSinkTest.emptySnapshot(sink,lease.epoch,table);}
        SubscriberFailure failure=new SubscriberFailure();failure.phase=SubscriberFailure.Phase.TARGET_APPLY;failure.sequence=1;failure.transactionId=raw.id;failure.projectedHash=Codec.sha256(Codec.encode(normalized));journal.pauseFailure(lease,"target","SQL_23514",failure);
        FailureEvidence.Scope scope=new FailureEvidence.Scope();scope.taskId="pipeline";scope.ownerNodeId="agent";scope.ownerDirectoryId=UUID.randomUUID().toString();store=new FailureEvidenceStore(directory.resolve("evidence"),scope,new FailureEvidenceStore.Limits());
        retained=FailureEvidenceCapture.capture(journal,store,scope,UUID.randomUUID().toString(),"request_12345678","control-revision","admin","target",1,100000);
    }
    @AfterEach void cleanup()throws Exception{
        if(journal!=null)journal.close();
        if(source!=null)source.cleanupExternalNamespace();
        if(target!=null&&target.product!=TargetProduct.H2)try(Connection c=JdbcConnections.open(target.driver,target.url,target.username,target.password);Statement sql=c.createStatement()){
            for(String schema:Arrays.asList(target.metadataSchema,target.databases.get("sales")))sql.execute(target.product==TargetProduct.MYSQL?"DROP DATABASE IF EXISTS `"+schema+"`":"DROP SCHEMA IF EXISTS \""+schema+"\" CASCADE");
        }
    }
    FailureCheckpointInspection.Observation inspect()throws Exception{return FailureCheckpointInspection.inspect(journal,store.load(retained.value.id),target,()->JdbcSink.existing(target));}
    void commit()throws Exception{try(JdbcSink sink=JdbcSink.existing(target)){sink.prepare(lease.epoch,table);sink.apply(lease.epoch,1,normalized);}}
    static void code(String expected,org.junit.jupiter.api.function.Executable action){assertEquals(expected,assertThrows(IllegalStateException.class,action).getMessage());}
    void checkpoint(long epoch,long sequence,String id,String hash)throws Exception{
        try(Connection c=JdbcConnections.open(target.driver,target.url,target.username,target.password);PreparedStatement p=c.prepareStatement("UPDATE "+JdbcSinkTest.q(target,target.metadataSchema)+"."+JdbcSinkTest.q(target,"rm3_sink_job")+" SET epoch=?,stream_seq=?,transaction_id=?,transaction_hash=?")){
            p.setLong(1,epoch);p.setLong(2,sequence);p.setString(3,id);p.setString(4,hash);assertEquals(1,p.executeUpdate());
        }
    }
    @Test void unchangedNativeCheckpointIsOnlyAnObservationAndHasNoBusinessValues()throws Exception{
        String before=Codec.encode(journal.status());FailureCheckpointInspection.Observation result=inspect();
        assertEquals(FailureCheckpointInspection.Verdict.CHECKPOINT_NOT_ADVANCED,result.verdict);assertEquals(retained.hash,result.evidenceHash);assertFalse(result.targetFenceRequired);
        assertFalse(Codec.encode(result).contains("private-cp-value"));assertEquals(before,Codec.encode(journal.status()));assertEquals(0,JdbcSinkTest.count(target,"items"));assertEquals(0,result.acknowledged);assertEquals(0,result.targetCheckpoint.sequence);
    }
    @Test void realCommittedTransactionLeadingAckMustBeRecognizedWithoutApplyingAgain()throws Exception{
        commit();FailureCheckpointInspection.Observation result=inspect();assertEquals(FailureCheckpointInspection.Verdict.COMMITTED_UNACKNOWLEDGED,result.verdict);
        assertEquals(1,JdbcSinkTest.count(target,"items"));assertEquals(0,journal.subscriptions().get(0).acknowledged);assertEquals("PAUSED",journal.subscriptions().get(0).state);assertEquals(normalized.id,result.targetCheckpoint.transactionId);
        journal.close();journal=new JdbcJournal(source.settings());assertEquals(result.projectedHash,inspect().projectedHash);assertEquals(retained.hash,store.load(retained.value.id).hash);
    }
    @Test void transactionIdOrProjectionHashMismatchRefusesInsteadOfAssumingCommit()throws Exception{
        checkpoint(lease.epoch,1,"another-transaction",Codec.sha256(Codec.encode(normalized)));code("FAILURE_CHECKPOINT_CONTENT_MISMATCH",()->inspect());
        checkpoint(lease.epoch,1,normalized.id,Codec.sha256("different-projection"));code("FAILURE_CHECKPOINT_CONTENT_MISMATCH",()->inspect());assertEquals(0,journal.subscriptions().get(0).acknowledged);
    }
    @Test void targetSequenceAheadByMoreThanOneOrBehindAckRefuses()throws Exception{
        checkpoint(lease.epoch,2,normalized.id,Codec.sha256(Codec.encode(normalized)));code("FAILURE_CHECKPOINT_SEQUENCE_DIVERGED",()->inspect());
        checkpoint(lease.epoch,-1,null,null);code("FAILURE_CHECKPOINT_SEQUENCE_DIVERGED",()->inspect());
    }
    @Test void contradictoryEmptyCheckpointOrMissingPreviousReceiptRefuses()throws Exception{
        checkpoint(lease.epoch,0,"unexpected",null);code("FAILURE_CHECKPOINT_CONTENT_MISMATCH",()->inspect());
    }
    @Test void targetEpochAheadRefusesButLagIsReportedWithoutActivation()throws Exception{
        checkpoint(lease.epoch+1,0,null,null);code("FAILURE_CHECKPOINT_EPOCH_DIVERGED",()->inspect());checkpoint(0,0,null,null);
        assertTrue(inspect().targetFenceRequired);try(JdbcSink sink=JdbcSink.existing(target)){assertEquals(0,sink.checkpoint().epoch);}
    }
    @Test void resumedOrNewFailureGenerationInvalidatesHistoricalEvidence()throws Exception{
        journal.resume("target",1);code("FAILURE_REVIEW_REQUIRES_PAUSED",()->inspect());journal.pause(lease,"target","ANOTHER_FAILURE");code("ERROR_GENERATION_CHANGED",()->inspect());assertNotNull(store.load(retained.value.id));
    }
    @Test void ownerEpochChangeRequiresFreshEvidenceEvenForSameTransaction()throws Exception{
        journal.release(lease);lease=journal.claim("owner-restarted",30000);code("FAILURE_CHECKPOINT_JOURNAL_CHANGED",()->inspect());assertEquals(0,journal.subscriptions().get(0).acknowledged);
    }
    @Test void normalizationIsNeverCreatedByInspection()throws Exception{
        FailureReviewTest.sql(journal,"DELETE FROM "+journal.table("normalized"));code("FAILURE_CHECKPOINT_JOURNAL_CHANGED",()->inspect());
        FailureEvidence value=retained.value;value.review.normalized=null;value.review.normalizedHash=null;value.id=UUID.randomUUID().toString();retained=store.retain(value);
        code("FAILURE_CHECKPOINT_NORMALIZATION_REQUIRED",()->inspect());try(Connection c=journal.connect();Statement sql=c.createStatement();ResultSet rs=sql.executeQuery("SELECT COUNT(*) FROM "+journal.table("normalized"))){assertTrue(rs.next());assertEquals(0,rs.getInt(1));}
    }
    @Test void configurationAndProjectionChangesAreRefusedBeforeTargetRead()throws Exception{
        target.fingerprint=Codec.sha256("changed-config");code("FAILURE_CHECKPOINT_CONFIGURATION_CHANGED",()->inspect());target.fingerprint=retained.value.review.fingerprint;
        target.databases.clear();code("FAILURE_CHECKPOINT_PROJECTION_CHANGED",()->inspect());
    }
    @Test void resumeDuringTargetIoIsCaughtByFinalJournalCheck()throws Exception{
        code("FAILURE_REVIEW_REQUIRES_PAUSED",()->FailureCheckpointInspection.inspect(journal,store.load(retained.value.id),target,()->{journal.resume("target",1);return JdbcSink.existing(target);}));assertEquals(0,journal.subscriptions().get(0).acknowledged);
    }
    @Test void nontransactionalNativeMetadataCannotClaimACommittedTransaction()throws Exception{
        if(target.product!=TargetProduct.MYSQL){assertNotNull(inspect());return;}
        String name=JdbcSinkTest.q(target,target.metadataSchema)+"."+JdbcSinkTest.q(target,"rm3_sink_job");
        try(Connection c=JdbcConnections.open(target.driver,target.url,target.username,target.password);Statement sql=c.createStatement()){
            sql.execute("ALTER TABLE "+name+" CONVERT TO CHARACTER SET latin1, ENGINE=MyISAM");
            try{code("SINK_METADATA_ENGINE_NOT_TRANSACTIONAL",()->inspect());}finally{sql.execute("ALTER TABLE "+name+" ENGINE=InnoDB, CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");}
        }
        assertEquals(FailureCheckpointInspection.Verdict.CHECKPOINT_NOT_ADVANCED,inspect().verdict);
    }
    @Test void missingNativeMetadataRefusesWithoutRecreatingIt()throws Exception{
        try(Connection c=JdbcConnections.open(target.driver,target.url,target.username,target.password);Statement sql=c.createStatement()){sql.execute("DROP TABLE "+JdbcSinkTest.q(target,target.metadataSchema)+"."+JdbcSinkTest.q(target,"rm3_sink_job"));}
        if(target.product==TargetProduct.MYSQL)code("SINK_METADATA_TABLES_MISSING",()->inspect());else assertThrows(SQLException.class,()->inspect());try(Connection c=JdbcConnections.open(target.driver,target.url,target.username,target.password);ResultSet rs=c.getMetaData().getTables(null,null,"rm3_sink_job",null)){assertFalse(rs.next());}
    }
    @Test void projectionKeepsBarriersAndFiltersMultipleDatabasesWithoutMutatingEvidence()throws Exception{
        TableSchema other=Codec.decode(Codec.encode(table),TableSchema.class);other.database="other";other.id=TableSchema.identity("other","","items");
        Change excluded=JdbcSinkTest.insert(other,2,"excluded-value"),barrier=new Change();barrier.operation=Change.Operation.BARRIER;barrier.eventId="barrier-1";
        SourceTransaction tx=Codec.decode(Codec.encode(normalized),SourceTransaction.class);tx.changes.add(excluded);tx.changes.add(barrier);String original=Codec.encode(tx);
        Map<String,TableSchema> schemas=new HashMap<>();schemas.put(table.id,table);schemas.put(other.id,other);
        SourceTransaction projected=TransactionProjection.project(tx,c->schemas.get(c.tableId),t->target.databases.containsKey(t.database));assertEquals(2,projected.changes.size());assertEquals(Change.Operation.BARRIER,projected.changes.get(1).operation);assertEquals(original,Codec.encode(tx));
        assertEquals(Codec.sha256(Codec.encode(projected)),Codec.sha256(Codec.encode(TransactionProjection.project(tx,c->schemas.get(c.tableId),t->"sales".equals(t.database)))));
    }
}
