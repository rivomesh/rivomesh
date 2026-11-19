package io.github.rivomesh.platform;

import io.github.rivomesh.cdc.Codec;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class HandoffCandidateAdmissionTest {
    @TempDir Path home;
    private final String originalAssignment=UUID.randomUUID().toString(),successorAssignment=UUID.randomUUID().toString();
    private final HandoffPreparations.Binding original=new HandoffPreparations.Binding("original","original-dir","original-incarnation",7);
    private final HandoffPreparations.Binding candidate=new HandoffPreparations.Binding("candidate","candidate-dir","candidate-incarnation",19);
    private final SharedHandoffEvidence.Scope scope=new SharedHandoffEvidence.Scope("cluster","orders","assessed_01",
            "revision_001","configuration_001",original.directory(),candidate.directory());
    private final String originalYaml="""
            formatVersion: 3
            pipelineId: orders
            nodeId: original
            journal:
              mode: EXTERNAL
              product: POSTGRESQL
              url: jdbc:postgresql://store/control
              managedWriter: true
              writerAssignment: %s
              password: synthetic-candidate-credential
            """.formatted(originalAssignment);

    @Test void retryAfterRestartReusesTheExactPrivateReadyReceiptWithoutPublishingLiveConfiguration()throws Exception{
        Fixture f=stage();
        Path unrelated=Files.writeString(f.state().resolve("unrelated.yml"),"preserved");
        HandoffCandidateAdmission.Ready ready=HandoffCandidateAdmission.prepare(f.state(),f.staged(),f.plan(),originalYaml,cipher(),yaml->{
            ManagedWriterDeclaration declaration=ManagedWriterDeclaration.read(yaml);
            assertEquals(candidate.node(),declaration.node());
            assertEquals(successorAssignment,declaration.assignment());
            assertTrue(yaml.contains("synthetic-candidate-credential"));
        });
        assertEquals("STOPPED",ready.desired());
        assertEquals(candidate,ready.binding());
        assertEquals(f.plan().id(),ready.preparation());
        assertEquals(Codec.sha256(Files.readAllBytes(f.custody().resolve("evidence.seal"))),ready.stagingSha256());
        Path receipt=readinessPath(f.state());
        byte[] sealed=Files.readAllBytes(receipt);
        assertFalse(Files.readString(receipt).contains("synthetic-candidate-credential"));
        Files.delete(f.archive());
        assertEquals(ready,HandoffCandidateAdmission.prepare(f.state(),f.staged(),f.plan(),originalYaml,cipher(),yaml->{}));
        assertArrayEquals(sealed,Files.readAllBytes(receipt));
        assertEquals("HANDOFF_CANDIDATE_COMMIT_REQUIRED",assertThrows(TaskService.Conflict.class,
                ()->HandoffCandidateAdmission.verifyCommitted(f.state(),f.plan(),cipher())).getMessage());

        var committed=committed(f.plan(),ready,ready.configurationSha256());
        var verified=HandoffCandidateAdmission.verifyCommitted(f.state(),committed,cipher());
        assertEquals(ready,verified.receipt());
        String exact=ManagedWriterDeclaration.successorYaml(originalYaml,original.node(),candidate.node(),successorAssignment);
        assertEquals(exact,verified.successorYaml());
        assertEquals(Codec.sha256(exact),verified.receipt().configurationSha256());
        assertFalse(verified.toString().contains("synthetic-candidate-credential"));
        assertEquals("preserved",Files.readString(unrelated));
        assertFalse(Files.exists(f.state().resolve("tasks")));
        assertFalse(Files.exists(f.state().resolve("validations")));
        assertFalse(Files.exists(f.state().resolve("controller.json")));
    }

    @Test void changedEnrollmentConfigurationEvidenceAndCorruptedSealRefuseAdmission()throws Exception{
        Fixture f=stage();
        var otherAgent=new HandoffPreparations.Binding(candidate.node(),candidate.directory(),"new-incarnation",candidate.epoch()+1);
        var foreign=new HandoffStaging.Binding(f.staged().preparation(),f.staged().evidence(),otherAgent,scope,f.staged().archive());
        assertEquals("HANDOFF_CANDIDATE_ADMISSION_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->HandoffCandidateAdmission.prepare(f.state(),foreign,f.plan(),originalYaml,cipher(),yaml->fail("stale binding"))).getMessage());
        assertThrows(IllegalStateException.class,()->HandoffCandidateAdmission.prepare(f.state(),f.staged(),f.plan(),
                originalYaml,cipher(),yaml->{throw new IllegalStateException("candidate Validator refused");}));
        assertFalse(Files.exists(f.state().resolve("handoff-admission")));
        assertEquals("HANDOFF_CANDIDATE_CONFIGURATION_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->HandoffCandidateAdmission.prepare(f.state(),f.staged(),f.plan(),
                        originalYaml.replace(originalAssignment,UUID.randomUUID().toString()),cipher(),yaml->{})).getMessage());

        var ready=HandoffCandidateAdmission.prepare(f.state(),f.staged(),f.plan(),originalYaml,cipher(),yaml->{});
        assertEquals("HANDOFF_CANDIDATE_ADMISSION_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->HandoffCandidateAdmission.prepare(f.state(),f.staged(),f.plan(),
                        originalYaml.replace("synthetic-candidate-credential","different-credential"),cipher(),yaml->{})).getMessage());
        var mismatched=committed(f.plan(),ready,"0".repeat(64));
        assertEquals("HANDOFF_CANDIDATE_ADMISSION_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->HandoffCandidateAdmission.verifyCommitted(f.state(),mismatched,cipher())).getMessage());
        var committed=committed(f.plan(),ready,ready.configurationSha256());
        Path receipts=f.custody().resolve("closed-receipts.json");
        byte[] original=Files.readAllBytes(receipts);
        Files.writeString(receipts,"[]");
        assertEquals("HANDOFF_STAGING_BYTES_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->HandoffCandidateAdmission.verifyCommitted(f.state(),committed,cipher())).getMessage());
        Files.write(receipts,original);
        Files.writeString(readinessPath(f.state()),"v1.corrupted");
        assertEquals("CONTROL_PAYLOAD_INVALID",assertThrows(TaskService.Conflict.class,
                ()->HandoffCandidateAdmission.verifyCommitted(f.state(),committed,cipher())).getMessage());
    }

    private record Fixture(Path state,Path custody,Path archive,HandoffStaging.Binding staged,HandoffPreparations.Plan plan){}
    private Fixture stage()throws Exception{
        Path root=home.toRealPath();
        Path source=root.resolve("original"),shared=root.resolve("shared"),state=root.resolve("candidate");
        for(Path directory:List.of(source,shared,state))HandoffStaging.createDirectory(directory);
        var receipt=new SharedHandoffEvidence.ClosedReceipt("completed_01","a".repeat(64),"admin","ADMIN",
                TaskRequest.Kind.ACTION,"SUCCEEDED","LOCAL_STOPPED");
        var archive=SharedHandoffEvidence.export(shared,scope,source,List.of(receipt));
        var staged=new HandoffStaging.Binding("prepared_01","b".repeat(64),candidate,scope,archive);
        var evidence=new HandoffPreparations.Evidence(scope.assessment(),scope.revision(),scope.configuration(),
                original,candidate,new HandoffPreparations.JournalCut("9","21","c".repeat(64)),staged.evidence(),archive.sha256());
        var activation=new HandoffPreparations.WriterActivation(Codec.sha256(originalAssignment),successorAssignment,
                "d".repeat(64),"e".repeat(64),true);
        var plan=new HandoffPreparations.Plan(staged.preparation(),scope.task(),1,"PREPARED","admin","actorRevision",
                evidence,100,900_100,null,null,0,activation,null,null,null);
        HandoffStaging.stage(state,shared,staged,cipher());
        return new Fixture(state,state.resolve("handoff-staging").resolve(staged.key()),
                SharedHandoffEvidence.stagingSource(shared,scope,archive),staged,plan);
    }
    private static HandoffPreparations.Plan committed(HandoffPreparations.Plan plan,HandoffCandidateAdmission.Ready ready,String configurationSha){
        var commit=new HandoffPreparations.WriterCommit(plan.actor(),ready.stagingSha256(),configurationSha,
                "revision_002","configuration_002",10);
        return new HandoffPreparations.Plan(plan.id(),plan.task(),plan.sequence(),"COMMITTED",plan.actor(),plan.actorRevision(),
                plan.evidence(),plan.created(),plan.expires(),null,null,200,plan.activation(),commit,null,null);
    }
    private static Path readinessPath(Path state)throws Exception{
        try(var children=Files.list(state.resolve("handoff-admission"))){
            var files=children.toList();
            assertEquals(1,files.size());
            return files.get(0);
        }
    }
    private static ControlCipher cipher(){
        return new ControlCipher(Base64.getEncoder().encodeToString(new byte[32]),"cluster","control");
    }
}
