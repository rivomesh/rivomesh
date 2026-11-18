package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.recovery.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Public service facade against actual PostgreSQL/MySQL journals and business tables. */
@EnabledIfEnvironmentVariable(named="RIVOMESH_FAILURE_DB_URL",matches=".+")
class GapVerificationServiceTest {
    @TempDir Path directory;
    JdbcGapVerificationTest setup;PipelineConfig config;String id="reviewed_gap_plan_1234",at="2026-11-17T12:00:00Z";
    FailureReviewTest source;
    @BeforeEach void prepare()throws Exception{
        source=new FailureReviewTest();source.directory=directory;JdbcJournal.Settings sql=source.settings();
        config=new PipelineConfig();config.pipelineId="pipeline";config.databases.add("sales");config.journal.mode="EXTERNAL";config.journal.namespace=sql.namespace;config.journal.url=sql.url;config.journal.username=sql.username;config.journal.password=sql.password;config.journal.product=sql.driver.startsWith("com.mysql")?TargetProduct.MYSQL:TargetProduct.POSTGRESQL;
        PipelineConfig.Target target=new PipelineConfig.Target();target.id="target";target.product=config.journal.product;target.url=sql.url;target.username=sql.username;target.password=sql.password;target.metadataSchema="meta_"+UUID.randomUUID().toString().replace("-","");target.mappings.put("sales","dst_"+UUID.randomUUID().toString().replace("-",""));config.targets.add(target);
        setup=new JdbcGapVerificationTest();setup.directory=directory;setup.fixture=new FailureCheckpointInspectionTest();FailureCheckpointInspectionTest f=setup.fixture;
        f.target=config.targetSettings(target,"generation");f.journal=new JdbcJournal(config.journalSettings());f.journal.initialize(config.fingerprint(),"generation",config.subscribers());f.lease=f.journal.claim("owner",30000);f.table=JdbcSinkTest.schema(false);f.journal.saveSchema(f.lease,f.table);
        SourceTransaction raw=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(f.table,1,"private-cp-value"));f.journal.append(f.lease,raw);f.normalized=f.journal.normalize(f.lease,f.journal.read("target",1).get(0),Collections.singletonMap(f.table.id,f.table),Collections.singletonMap(f.table.id,raw.position),(position,key)->false);
        try(JdbcSink sink=new JdbcSink(f.target)){sink.activate(f.lease.epoch);JdbcSinkTest.emptySnapshot(sink,f.lease.epoch,f.table);}
        SubscriberFailure failure=new SubscriberFailure();failure.phase=SubscriberFailure.Phase.TARGET_APPLY;failure.sequence=1;failure.transactionId=raw.id;failure.projectedHash=Codec.sha256(Codec.encode(f.normalized));f.journal.pauseFailure(f.lease,"target","SQL_23514",failure);Journal.PipelineState paused=f.journal.pausePipeline();try(JdbcSink sink=JdbcSink.existing(f.target)){sink.activate(paused.epoch);}
        FailureEvidence.Scope scope=new FailureEvidence.Scope();scope.taskId="pipeline";scope.ownerNodeId="agent";scope.ownerDirectoryId=UUID.randomUUID().toString();f.store=new FailureEvidenceStore(directory.resolve("evidence"),scope,new FailureEvidenceStore.Limits());f.retained=FailureEvidenceCapture.capture(f.journal,f.store,scope,UUID.randomUUID().toString(),"review_original_1234","paused-revision","planner","target",1,100000);
        setup.receipt=TransactionSkipRecovery.execute(f.journal,f.retained,f.target,"original_skip_1234","planner","Reviewed the complete transaction and accepted the explicit gap",true).receipt;try(Connection c=f.journal.connect()){setup.original=JdbcSkipJournal.load(f.journal,c,"original_skip_1234").plan;}setup.createReport();setup.report.close();setup.report=null;
    }
    @AfterEach void close()throws Exception{try{if(setup!=null){setup.close();if(setup.fixture!=null)setup.fixture.cleanup();}}finally{if(source!=null)source.cleanupExternalNamespace();}}
    GapVerificationPlan plan()throws Exception{return GapVerification.prepare(config,directory.resolve("reports"),setup.plan.reportId,"target",Collections.singleton(setup.original.id),id,"planner","Review all affected tables at the sealed common cut",at,10000,setup.plan.errorGeneration);}
    GapVerificationReceipt execute(GapVerificationPlan plan)throws Exception{return GapVerification.execute(config,directory.resolve("reports"),plan.id,plan.hash(),"target",plan.errorGeneration,"execute_gap_12345678","approver","Approve the reviewed scope and target-exclusive writing");}
    @Test void preparePersistsOneImmutablePlanAndCanReplayAfterTheCutMoves()throws Exception{
        GapVerificationPlan plan=plan();assertEquals(plan.hash(),plan().hash());assertEquals("PREPARED",GapVerification.read(config,id).get("state"));assertEquals(0,JdbcSinkTest.count(setup.fixture.target,"items"));
        setup.fixture.journal.resumePipeline(plan.errorGeneration);assertEquals(plan.hash(),plan().hash());assertThrows(IllegalStateException.class,()->GapVerification.prepare(config,directory.resolve("reports"),plan.reportId,"target",Collections.singleton(setup.original.id),id,"planner","Changed reason cannot reuse the stored plan request",at,10000,plan.errorGeneration));
    }
    @Test void historicalMatchStillRequiresFreshRowsAndSuccessfulVerificationKeepsHistoryAndPause()throws Exception{
        GapVerificationPlan plan=plan();assertEquals("GAP_LIVE_DATA_DIFFERENCES",assertThrows(IllegalStateException.class,()->execute(plan)).getMessage());assertEquals("PREPARED",GapVerification.read(config,id).get("state"));
        setup.restoreLiveRows();GapVerificationReceipt receipt=execute(plan);assertEquals("planner",receipt.plan.createdBy);assertEquals("approver",receipt.executedBy);assertEquals("VERIFIED_AT_COMMON_CUT",GapVerification.read(config,id).get("state"));Map<String,Object> state=setup.fixture.journal.status();assertEquals(0,state.get("openGapCount"));assertEquals(1,state.get("verifiedGapCount"));assertEquals(false,state.get("hasOpenGaps"));assertEquals(true,state.get("hasGaps"));assertEquals("PAUSED",state.get("run_state"));
    }
    @Test void committedReceiptReplaysWithoutTheReportAndChangedApprovalRefuses()throws Exception{
        GapVerificationPlan plan=plan();setup.restoreLiveRows();String hash=execute(plan).hash();Files.move(directory.resolve("reports"),directory.resolve("removed-report"));setup.fixture.journal.resumePipeline(plan.errorGeneration);
        assertEquals(hash,execute(plan).hash());assertEquals("GAP_VERIFICATION_APPROVAL_CONFLICT",assertThrows(IllegalStateException.class,()->GapVerification.execute(config,directory.resolve("reports"),plan.id,plan.hash(),"target",plan.errorGeneration,"another_execute_1234","another-admin","Changed approval is a new operation and cannot rewrite a committed receipt")).getMessage());
    }
    @Test void gapPageShowsPayloadRetentionAndKeepsTheReceiptAfterCollection()throws Exception{
        List<?> entries=(List<?>)RecoveryLedger.gaps(config,"target","0",20).get("items");
        assertEquals(1,entries.size());
        Map<?,?> open=(Map<?,?>)entries.getFirst();
        assertEquals("OPEN",open.get("state"));assertEquals(true,open.get("journalPayloadRetained"));
        assertFalse(Codec.encode(open).contains("private-cp-value"));

        GapVerificationPlan plan=plan();setup.restoreLiveRows();GapVerificationReceipt receipt=execute(plan);
        JdbcJournal journal=setup.fixture.journal;journal.resumePipeline(plan.errorGeneration);
        assertEquals(1,journal.collect(journal.claim("collect-owner",30000),1));
        List<?> retained=(List<?>)RecoveryLedger.gaps(config,"target","0",20).get("items");
        assertEquals(1,retained.size());
        Map<?,?> verified=(Map<?,?>)retained.getFirst();
        assertEquals("VERIFIED_AT_COMMON_CUT",verified.get("state"));assertEquals(false,verified.get("journalPayloadRetained"));
        assertEquals(open.get("receiptHash"),verified.get("receiptHash"));assertEquals(receipt.hash(),verified.get("verificationHash"));
    }
    @Test void staleApprovalHashOrFailureGenerationCannotWriteAReceipt()throws Exception{
        GapVerificationPlan plan=plan();setup.restoreLiveRows();assertEquals("GAP_VERIFICATION_APPROVAL_SCOPE_CHANGED",assertThrows(IllegalStateException.class,()->GapVerification.execute(config,directory.resolve("reports"),plan.id,Codec.sha256("wrong-scope"),"target",plan.errorGeneration,"execute_stale_1234","approver","Approved this different scope hash")).getMessage());
        assertEquals("GAP_VERIFICATION_APPROVAL_SCOPE_CHANGED",assertThrows(IllegalStateException.class,()->GapVerification.execute(config,directory.resolve("reports"),plan.id,plan.hash(),"target",plan.errorGeneration+1,"execute_stale_5678","approver","Approved a mismatched failure generation")).getMessage());assertEquals("PREPARED",GapVerification.read(config,id).get("state"));
    }
    @Test void changedStoredPlanAndLostPlanTableAreNotRecreated()throws Exception{
        plan();FailureReviewTest.sql(setup.fixture.journal,"UPDATE "+setup.fixture.journal.table("gap_verification_plan")+" SET payload='{}'");assertEquals("GAP_VERIFICATION_PLAN_CHANGED",assertThrows(IllegalStateException.class,()->GapVerification.read(config,id)).getMessage());
        FailureReviewTest.sql(setup.fixture.journal,"DROP TABLE "+setup.fixture.journal.table("gap_verification_plan"));assertEquals("SKIP_METADATA_LOST",assertThrows(IllegalStateException.class,()->setup.fixture.journal.verifyIdentity(setup.plan.fingerprint,Collections.singletonList("target"))).getMessage());
    }
}
