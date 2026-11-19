package io.github.rivomesh.platform;

import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class HandoffEvidenceAdoptionTest {
    @TempDir Path home;
    private final HandoffPreparations.Binding original=new HandoffPreparations.Binding("original","original-dir","origin-one",3);
    private final HandoffPreparations.Binding candidate=new HandoffPreparations.Binding("candidate","candidate-dir","agent-two",9);
    private final SharedHandoffEvidence.ClosedReceipt closed=new SharedHandoffEvidence.ClosedReceipt("closed_fixture_01",
            "a".repeat(64),"admin","ADMIN",TaskRequest.Kind.ACTION,"SUCCEEDED","VALIDATION_MATCH");

    @Test void publicationRecoversInterruptedCopyAndReadOnlyQueriesPreserveEveryOriginalByte()throws Exception{
        Fixture f=prepare();
        assertNull(readWorkspace(f,null,null));
        var adopted=HandoffEvidenceAdoption.install(f.state(),f.plan(),cipher());
        assertEquals(candidate,adopted.candidate());assertEquals(1,adopted.reports());assertEquals(1,adopted.closedReceipts());
        assertNull(readWorkspace(f,f.report(),null));
        Path workspace=readWorkspace(f,f.report(),adopted),report=workspace.resolve(f.report());
        assertEquals(1,((List<?>)ValidationStore.catalog(workspace,"",25).get("reports")).size());
        try(ValidationStore read=ValidationStore.openForRead(workspace,f.report())){
            assertEquals("VALIDATION_MATCH",read.summary().get("code"));
        }
        assertEquals("VALIDATION_REPORT_HANDOFF_READ_ONLY",assertThrows(SyncFailure.class,
                ()->ValidationStore.open(workspace,f.report())).code);
        for(String name:List.of("rows.mv.db","summary.json"))assertArrayEquals(Files.readAllBytes(f.original().resolve(f.report()).resolve(name)),
                Files.readAllBytes(report.resolve(name)));
        assertEquals(closed,HandoffEvidenceAdoption.receipt(f.state(),f.plan().task(),candidate.node(),candidate.directory(),
                adopted.committedRevision(),f.local(),cipher(),closed.request(),original.node()::equals,adopted));
        assertNull(HandoffEvidenceAdoption.receipt(f.state(),f.plan().task(),candidate.node(),candidate.directory(),
                adopted.committedRevision(),f.local(),cipher(),closed.request(),candidate.node()::equals,adopted));
        assertThrows(java.util.NoSuchElementException.class,()->HandoffEvidenceAdoption.reports(f.state(),f.plan().task(),candidate.node(),candidate.directory(),
                adopted.committedRevision(),f.local(),cipher(),f.report(),candidate.node()::equals,adopted));

        Path pending=workspace.resolveSibling(workspace.getFileName()+".pending");
        Files.move(workspace,pending);
        Path partial=pending.resolve(f.report()).resolve("summary.json");
        Files.move(partial,partial.resolveSibling("summary.json.part"));
        Files.writeString(partial.resolveSibling("summary.json.part"),"interrupted bytes");
        assertNull(readWorkspace(f,null,adopted));
        assertEquals("VALIDATION_REPORT_CUSTODY_STAGED",assertThrows(SyncFailure.class,
                ()->ValidationStore.openForRead(pending,f.report())).code);
        Files.delete(f.archive());
        assertEquals(adopted,HandoffEvidenceAdoption.install(f.state(),f.plan(),cipher()));
        assertEquals(adopted,HandoffEvidenceAdoption.install(f.state(),phase(f.plan(),"EVIDENCE_ADOPTED",adopted),cipher()));
        assertFalse(Files.exists(pending));
        assertTrue(Files.isRegularFile(f.staged().resolve("closed-receipts.json")));
        assertTrue(Files.isRegularFile(f.staged().resolve("reports").resolve(f.report()).resolve(ValidationStore.STAGED_ROWS_FILE)));
        assertArrayEquals(Files.readAllBytes(f.original().resolve(f.report()).resolve("summary.json")),Files.readAllBytes(report.resolve("summary.json")));
        assertFalse(Files.exists(f.state().resolve("validations")));
    }

    @Test void verifiesProjectedAuthorityAndRefusesOccupiedOrChangedEvidenceWithoutWritingIt()throws Exception{
        Fixture f=prepare();
        assertEquals("HANDOFF_RECIPIENT_PROJECTION_REQUIRED",assertThrows(TaskService.Conflict.class,
                ()->HandoffEvidenceAdoption.install(f.state(),phase(f.plan(),"COMMITTED",null),cipher())).getMessage());
        assertFalse(Files.exists(f.state().resolve("handoff-adopted")));
        var ready=HandoffEvidenceAdoption.install(f.state(),f.plan(),cipher());
        assertEquals("HANDOFF_ADOPTED_SCOPE_CHANGED",assertThrows(TaskService.Conflict.class,()->
                HandoffEvidenceAdoption.reports(f.state(),f.plan().task(),candidate.node(),"foreign-directory",ready.committedRevision(),f.local(),cipher(),null,original.node()::equals,ready)).getMessage());
        var changed=new HandoffEvidenceAdoption.Receipt(ready.preparation(),ready.task(),ready.candidate(),ready.committedRevision(),
                ready.configurationSha256(),ready.stagingSha256(),"0".repeat(64),ready.reports(),ready.closedReceipts());
        assertEquals("HANDOFF_ADOPTED_SCOPE_CHANGED",assertThrows(TaskService.Conflict.class,()->readWorkspace(f,null,changed)).getMessage());
        Path workspace=readWorkspace(f,null,ready),parked=workspace.resolveSibling(workspace.getFileName()+".held");
        Files.move(workspace,parked);HandoffStaging.createDirectory(workspace);
        Path unrelated=Files.writeString(workspace.resolve("keep.txt"),"unrelated");
        assertEquals("HANDOFF_ADOPTED_PATH_OCCUPIED",assertThrows(TaskService.Conflict.class,
                ()->HandoffEvidenceAdoption.install(f.state(),f.plan(),cipher())).getMessage());
        assertEquals("unrelated",Files.readString(unrelated));
        Files.delete(unrelated);Files.delete(workspace);Files.move(parked,workspace);
        Path receipts=workspace.resolve("closed-receipts.json");
        Files.writeString(receipts,"[]");
        assertEquals("HANDOFF_ADOPTED_BYTES_CHANGED",assertThrows(TaskService.Conflict.class,()->
                HandoffEvidenceAdoption.receipt(f.state(),f.plan().task(),candidate.node(),candidate.directory(),ready.committedRevision(),
                        f.local(),cipher(),closed.request(),original.node()::equals,ready)).getMessage());
        assertEquals("HANDOFF_STAGING_BYTES_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->HandoffEvidenceAdoption.install(f.state(),f.plan(),cipher())).getMessage());
        assertEquals("[]",Files.readString(receipts));
    }

    private record Fixture(Path state,Path original,Path archive,Path staged,String report,
                           HandoffPreparations.Plan plan,ManagedRecipientImport.Receipt local){}
    private Fixture prepare()throws Exception{
        Path root=home.toRealPath(),source=root.resolve("original"),shared=root.resolve("shared"),state=root.resolve("candidate");
        for(Path dir:List.of(source,shared,state))HandoffStaging.createDirectory(dir);
        String report=HandoffStagingTest.completedReport(source);
        var scope=new SharedHandoffEvidence.Scope("cluster","orders","assessed_01","revision_001","configuration_001",original.directory(),candidate.directory());
        var archive=SharedHandoffEvidence.export(shared,scope,source,List.of(closed));
        var binding=new HandoffStaging.Binding("prepared_01","b".repeat(64),candidate,scope,archive);
        HandoffStaging.stage(state,shared,binding,cipher());
        String assignment=UUID.randomUUID().toString();
        String yaml="""
                formatVersion: 3
                pipelineId: orders
                nodeId: original
                journal:
                  mode: EXTERNAL
                  product: POSTGRESQL
                  url: jdbc:postgresql://store/control
                  managedWriter: true
                  writerAssignment: %s
                """.formatted(assignment);
        var activation=new HandoffPreparations.WriterActivation(Codec.sha256(assignment),UUID.randomUUID().toString(),"d".repeat(64),"e".repeat(64),true);
        var evidence=new HandoffPreparations.Evidence(scope.assessment(),scope.revision(),scope.configuration(),original,candidate,
                new HandoffPreparations.JournalCut("23","12","c".repeat(64)),binding.evidence(),archive.sha256());
        var prepared=new HandoffPreparations.Plan(binding.preparation(),scope.task(),1,"PREPARED","admin","actor",evidence,
                100,900100,null,null,0,activation,null,null,null);
        var ready=HandoffCandidateAdmission.prepare(state,binding,prepared,yaml,cipher(),successor->{});
        var committed=new HandoffPreparations.WriterCommit(prepared.actor(),ready.stagingSha256(),ready.configurationSha256(),
                "revision_002","configuration_002",24);
        var projected=new HandoffPreparations.Plan(prepared.id(),prepared.task(),prepared.sequence(),"PROJECTED",prepared.actor(),
                prepared.actorRevision(),evidence,100,900100,null,null,200,activation,committed,"execute_001",null);
        var local=new ManagedRecipientImport.Receipt(prepared.id(),ready.configurationSha256(),ready.stagingSha256());
        return new Fixture(state,source,SharedHandoffEvidence.stagingSource(shared,scope,archive),state.resolve("handoff-staging").resolve(binding.key()),report,projected,local);
    }
    private static HandoffPreparations.Plan phase(HandoffPreparations.Plan plan,String phase,HandoffEvidenceAdoption.Receipt adoption){
        return new HandoffPreparations.Plan(plan.id(),plan.task(),plan.sequence(),phase,plan.actor(),plan.actorRevision(),plan.evidence(),
                plan.created(),plan.expires(),null,null,plan.closedAt(),plan.activation(),plan.writerCommit(),plan.executionRequest(),adoption);
    }
    private Path readWorkspace(Fixture f,String id,HandoffEvidenceAdoption.Receipt authorized)throws Exception{
        return HandoffEvidenceAdoption.reports(f.state(),f.plan().task(),candidate.node(),candidate.directory(),
                f.plan().writerCommit().revision(),f.local(),cipher(),id,original.node()::equals,authorized);
    }
    private static ControlCipher cipher(){return new ControlCipher(Base64.getEncoder().encodeToString(new byte[32]),"cluster","control");}
}
