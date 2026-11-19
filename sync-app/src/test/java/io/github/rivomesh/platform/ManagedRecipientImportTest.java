package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.Config;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.TargetProduct;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedRecipientImportTest {
    private static final String TASK="sales",CANDIDATE="second",ORIGINAL="first",PREPARATION="recipient_preparation";
    private static final String EXECUTION="recipient_original_execution";
    private static final ObjectMapper YAML=new ObjectMapper(new YAMLFactory());
    @TempDir Path directory;

    @Test void reopensLostImportWithExactlyOneImmutableVersionAndNoWorkerAdmission()throws Exception{
        PlatformConfig config=config(CANDIDATE);
        String identity=UUID.randomUUID().toString(),candidateDirectory;
        ManagedRecipientImport.Grant grant;
        ManagedRecipientImport.FrozenConfig frozen;
        ManagedRecipientImport.Receipt receipt;
        PlatformTest.Workers workers=new PlatformTest.Workers();
        try(TaskService local=TaskService.deferredOwner(config,backend(),workers)){
            candidateDirectory=local.ownerDirectoryIdentity();local.bindGlobal(identity,candidateDirectory);
            grant=grant(identity,candidateDirectory); frozen=frozen(grant);
            assertFalse(grant.toString().contains("source-secret"));assertFalse(frozen.toString().contains("source-secret"));
            HandoffPreparations.WriterCommit commit=grant.committed().writerCommit();
            // An interrupted immutable version write can leave the document before its manifest.
            Path pending=versionDirectory(config).resolve(commit.configuration()+".yml");
            PrivateFiles.directory(pending.getParent());
            PrivateFiles.write(pending,frozen.successorYaml().getBytes(StandardCharsets.UTF_8));
            receipt=local.installStoppedRecipient(grant,frozen);
            assertEquals("STOPPED",local.view(TASK).get("desired"));
            assertEquals(commit.configuration(),local.view(TASK).get("configurationVersion"));
            assertEquals(commit.revision(),local.view(TASK).get("revision"));
            assertEquals(frozen.successorYaml(),Files.readString(pending));
            assertEquals(receipt,local.installStoppedRecipient(grant,frozen));
            assertBlocked(local,"HANDOFF_RECIPIENT_IMPORT_REQUIRED");
        }
        Path projection=Path.of(config.tasksDirectory).resolve(TASK+".yml");
        Path worker=Path.of(config.dataDirectory).resolve("worker-configs").resolve(TASK+".yml");
        Files.delete(projection);Files.delete(worker);
        try(TaskService reopened=TaskService.deferredOwner(config,backend(),workers)){
            assertEquals(candidateDirectory,reopened.ownerDirectoryIdentity());reopened.bindGlobal(identity,candidateDirectory);
            assertEquals(receipt,reopened.installStoppedRecipient(grant,frozen));
            assertTrue(Files.isRegularFile(projection));assertTrue(Files.isRegularFile(worker));
            assertEquals("STOPPED",reopened.view(TASK).get("workerState"));
            reopened.beginOwnerScheduling();PlatformTest.await(()->Boolean.TRUE.equals(reopened.view(TASK).get("statusAvailable")));
            assertBlocked(reopened,"HANDOFF_RECIPIENT_IMPORT_REQUIRED");
            assertEquals(0,workers.starts.get());
        }
        try(var files=Files.list(versionDirectory(config))){assertEquals(2,files.count());}
    }

    @Test void rejectsDifferentDirectoryTamperedFrozenConfigAndConflictingPrivateVersion()throws Exception{
        PlatformConfig config=config(CANDIDATE);String identity=UUID.randomUUID().toString();
        try(TaskService local=TaskService.deferredOwner(config,backend(),new PlatformTest.Workers())){
            String candidateDirectory=local.ownerDirectoryIdentity();local.bindGlobal(identity,candidateDirectory);
            ManagedRecipientImport.Grant foreign=grant(identity,UUID.randomUUID().toString());
            assertThrows(ControlStore.Fenced.class,()->local.installStoppedRecipient(foreign,frozen(foreign)));
            ManagedRecipientImport.Grant grant=grant(identity,candidateDirectory);ManagedRecipientImport.FrozenConfig valid=frozen(grant);
            ManagedRecipientImport.FrozenConfig tampered=new ManagedRecipientImport.FrozenConfig(valid.preparation(),valid.candidate(),
                    valid.desired(),valid.configurationSha256(),valid.stagingSha256(),valid.successorYaml()+"# changed after sealing\n");
            assertEquals("HANDOFF_RECIPIENT_FROZEN_CONFIG_CHANGED",assertThrows(TaskService.Conflict.class,
                    ()->local.installStoppedRecipient(grant,tampered)).getMessage());
            assertFalse(Files.exists(versionDirectory(config)));
            Path orphan=versionDirectory(config).resolve(grant.committed().writerCommit().configuration()+".yml");
            PrivateFiles.directory(orphan.getParent());PrivateFiles.write(orphan,"unrelated".getBytes(StandardCharsets.UTF_8));
            assertEquals("HANDOFF_RECIPIENT_LOCAL_CONFIGURATION_CHANGED",assertThrows(java.io.IOException.class,
                    ()->local.installStoppedRecipient(grant,valid)).getMessage());
            assertEquals("unrelated",Files.readString(orphan));
            assertThrows(NoSuchElementException.class,()->local.view(TASK));
            assertFalse(Files.exists(Path.of(config.tasksDirectory).resolve(TASK+".yml")));
        }
    }

    @Test void retainsTheFormerStoppedConfigurationButRefusesItsLocalCommands()throws Exception{
        PlatformConfig config=config(ORIGINAL);PlatformTest.Workers workers=new PlatformTest.Workers();
        try(TaskService local=TaskService.deferredOwner(config,backend(),workers)){
            local.bindGlobal(UUID.randomUUID().toString(),local.ownerDirectoryIdentity());
            String original=yaml(ORIGINAL,UUID.randomUUID().toString());
            local.create(original,"recipient_former_initial",admin(local));
            local.retainRetiredOwners(Set.of(TASK));
            assertEquals("STOPPED",local.view(TASK).get("desired"));
            assertTrue(local.ownerDocuments().containsKey(TASK));
            local.beginOwnerScheduling();PlatformTest.await(()->Boolean.TRUE.equals(local.view(TASK).get("statusAvailable")));
            assertBlocked(local,"HANDOFF_ORIGINAL_RETIRED");
            assertEquals(0,workers.starts.get());
        }
    }

    @Test void signedAdoptionContinuesOnlyTheBoundRequestAndPreservesGlobalStartGuard()throws Exception{
        GlobalTaskStoreTest fixture=new GlobalTaskStoreTest();fixture.initialize();
        GlobalTaskStore store=fixture.a;
        ManagedRecipientImport.Grant grant=grant(store.identity,fixture.directory);
        HandoffPreparations.Plan source=grant.committed();
        store.create(TASK,CANDIDATE,fixture.directory,grant.successorYaml(),"recipient_global_core",fixture.admin);
        store.transaction(TASK,fixture.admin,(c,record,actor)->{
            long now=store.database().now(c);
            HandoffPreparations.Plan preparation=store.preparations().prepare(c,record,source.id(),actor,source.evidence(),source.activation(),now);
            store.preparations().bindExecution(c,preparation,EXECUTION);
            return null;
        });
        fixture.b.transaction(TASK,fixture.admin,(c,record,actor)->{
            long now=store.database().now(c);
            HandoffPreparations.Plan preparation=store.preparations().latest(c,TASK,now);
            assertEquals(EXECUTION,preparation.executionRequest());
            assertEquals(preparation,store.preparations().bindExecution(c,preparation,EXECUTION));
            assertEquals("HANDOFF_EXECUTION_REQUEST_CHANGED",assertThrows(TaskService.Conflict.class,()->
                    store.preparations().bindExecution(c,preparation,"replacement_execution")).getMessage());
            assertEquals(CANDIDATE,record.owner);
            HandoffPreparations.Plan committed=store.preparations().commitWriter(c,preparation,source.writerCommit(),now);
            ManagedRecipientImport.Receipt receipt=new ManagedRecipientImport.Receipt(committed.id(),source.writerCommit().configurationSha256(),
                    source.writerCommit().stagingSha256());
            HandoffEvidenceAdoption.Receipt adoption=adoption(committed,"4".repeat(64),source.evidence().candidate());
            assertEquals("HANDOFF_RECIPIENT_EVIDENCE_CHANGED",assertThrows(TaskService.Conflict.class,()->
                    store.preparations().recordEvidenceAdoption(c,committed,adoption)).getMessage());
            store.preparations().recordProjected(c,committed,receipt);
            HandoffPreparations.Plan projected=store.preparations().latest(c,TASK,now);
            assertEquals("PROJECTED",projected.phase());
            assertEquals(List.of(new HandoffPreparations.PendingRecipient(TASK,source.id(),"PROJECTED",null)),
                    store.preparations().pendingRecipient(c,CANDIDATE,fixture.directory,"",10));
            HandoffEvidenceAdoption.Receipt stranger=adoption(projected,"4".repeat(64),source.evidence().original());
            assertEquals("HANDOFF_RECIPIENT_EVIDENCE_CHANGED",assertThrows(TaskService.Conflict.class,()->
                    store.preparations().recordEvidenceAdoption(c,projected,stranger)).getMessage());
            store.preparations().recordEvidenceAdoption(c,projected,adoption);
            return null;
        });
        store.transaction(TASK,fixture.admin,(c,record,actor)->{
            long now=store.database().now(c);
            HandoffPreparations.Plan adopted=store.preparations().latest(c,TASK,now);
            HandoffEvidenceAdoption.Receipt adoption=adoption(adopted,"4".repeat(64),source.evidence().candidate());
            assertEquals("EVIDENCE_ADOPTED",adopted.phase());
            assertEquals(EXECUTION,adopted.executionRequest());
            assertEquals(adoption,adopted.evidenceAdoption());
            assertEquals(Map.of("validationReports",1,"closedReceipts",2),HandoffPreparations.summary(adopted).get("adoptedEvidence"));
            assertFalse(HandoffPreparations.summary(adopted).toString().contains(adoption.adoptionSha256()));
            store.preparations().recordEvidenceAdoption(c,adopted,adoption);
            assertEquals("HANDOFF_RECIPIENT_EVIDENCE_CHANGED",assertThrows(TaskService.Conflict.class,()->
                    store.preparations().recordEvidenceAdoption(c,adopted,adoption(adopted,"5".repeat(64),source.evidence().candidate()))).getMessage());
            assertEquals(adopted,store.preparations().retirement(c,TASK,source.evidence().original().node(),
                    source.evidence().original().directory()));
            assertTrue(store.preparations().pendingRecipient(c,CANDIDATE,fixture.directory,"",10).isEmpty());
            return null;
        });
        String revision=store.get(TASK,fixture.admin).revision;
        assertEquals("HANDOFF_RECIPIENT_IMPORT_REQUIRED",assertThrows(TaskService.Conflict.class,()->
                store.transaction(TASK,fixture.admin,(c,task,actor)->{store.advance(c,task);return null;})).getMessage());
        assertEquals(revision,store.get(TASK,fixture.admin).revision);
    }

    @Test void revalidatesDatabaseAuthorityAfterPrivateReadBeforeAnyLocalWrite()throws Exception{
        PlatformConfig config=config(CANDIDATE);String identity=UUID.randomUUID().toString();
        try(TaskService local=TaskService.deferredOwner(config,backend(),new PlatformTest.Workers())){
            String candidate=local.ownerDirectoryIdentity();local.bindGlobal(identity,candidate);
            ManagedRecipientImport.Grant first=grant(identity,candidate),different=grant(identity,candidate);
            List<String> steps=new ArrayList<>();
            ManagedRecipientImport.AuthorityReader authority=()->{
                steps.add("authority");return steps.size()==1?first:different;
            };
            ManagedRecipientImport.FrozenReader sealed=plan->{
                assertEquals(List.of("authority"),steps);
                assertEquals(first.committed(),plan);
                steps.add("sealed");
                return frozen(first);
            };
            assertEquals("HANDOFF_RECIPIENT_AUTHORITY_CHANGED",assertThrows(TaskService.Conflict.class,()->
                    ManagedRecipientImport.install(local,authority,sealed)).getMessage());
            assertEquals(List.of("authority","sealed","authority"),steps);
            assertFalse(Files.exists(versionDirectory(config)));
            assertThrows(NoSuchElementException.class,()->local.view(TASK));
        }
    }

    private static void assertBlocked(TaskService local,String code)throws Exception{
        TaskRequest start=TaskRequest.of(TaskRequest.Kind.ACTION,TASK);start.action="START";
        start.revision=(String)local.view(TASK).get("revision");start.requestId="recipient_start_refused";
        assertEquals(code,assertThrows(TaskService.Conflict.class,()->new LocalTaskPlane(local).invoke(start,admin(local))).getMessage());
    }
    private static AccessControl.Principal admin(TaskService local){
        return AccountDirectory.authenticate(local.sharedAccess().accounts(),"Bearer "+GlobalTaskStoreTest.TOKEN);
    }
    private Path versionDirectory(PlatformConfig c){return Path.of(c.dataDirectory).resolve("config-versions").resolve(TASK);}
    private PlatformConfig config(String node){
        PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=GlobalTaskStoreTest.TOKEN;c.pollMillis=100;
        c.tasksDirectory=directory.resolve(node+"-tasks").toString();c.dataDirectory=directory.resolve(node+"-controller").toString();
        c.cluster=new ClusterConfig();c.cluster.sharedAccess=true;c.cluster.sharedTasks=true;c.cluster.nodeId=node;c.cluster.id="recipient";
        c.cluster.driver="org.h2.Driver";c.cluster.url="jdbc:h2:mem:recipient_"+UUID.randomUUID().toString().replace("-","")
                +";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
        c.cluster.username="sa";c.cluster.password="";c.cluster.taskKey=GlobalTaskStoreTest.key(8);return c;
    }
    private static String yaml(String node,String assignment)throws Exception{
        PipelineConfig pipeline=new PipelineConfig();pipeline.nodeId=node;pipeline.pipelineId=TASK;pipeline.source=PlatformTest.task(TASK).source;
        pipeline.databases.add(TASK);pipeline.journal.mode="EXTERNAL";pipeline.journal.url="jdbc:postgresql://journal/fixture";
        pipeline.journal.username="journal-user";pipeline.journal.managedWriter=true;pipeline.journal.writerAssignment=assignment;
        PipelineConfig.Target target=new PipelineConfig.Target();target.id="pg";target.username="target-user";
        target.url="jdbc:postgresql://destination/fixture";target.product=TargetProduct.POSTGRESQL;target.mappings.put(TASK,"copied_sales");
        pipeline.targets.add(target);return YAML.writeValueAsString(pipeline);
    }
    private static ManagedRecipientImport.Grant grant(String identity,String candidateDirectory)throws Exception{
        String next=UUID.randomUUID().toString(),successor=yaml(CANDIDATE,next);
        String staged="b".repeat(64);long now=Instant.now().toEpochMilli();
        HandoffPreparations.Binding former=new HandoffPreparations.Binding(ORIGINAL,UUID.randomUUID().toString(),UUID.randomUUID().toString(),4);
        HandoffPreparations.Binding candidate=new HandoffPreparations.Binding(CANDIDATE,candidateDirectory,UUID.randomUUID().toString(),5);
        HandoffPreparations.Evidence evidence=new HandoffPreparations.Evidence("recipient_assessment",UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),former,candidate,new HandoffPreparations.JournalCut("7","2","c".repeat(64)),
                "d".repeat(64),"e".repeat(64));
        HandoffPreparations.WriterCommit commit=new HandoffPreparations.WriterCommit("legacy-admin",staged,AuditStore.hash(successor),
                UUID.randomUUID().toString(),UUID.randomUUID().toString(),8);
        HandoffPreparations.Plan plan=new HandoffPreparations.Plan(PREPARATION,TASK,1,"COMMITTED","legacy-admin",UUID.randomUUID().toString(),evidence,
                now,now+15*60_000,null,null,now,new HandoffPreparations.WriterActivation("f".repeat(64),next,"0".repeat(64),"1".repeat(64),true),commit,EXECUTION,null);
        return new ManagedRecipientImport.Grant(identity,plan,successor);
    }
    private static HandoffEvidenceAdoption.Receipt adoption(HandoffPreparations.Plan plan,String digest,HandoffPreparations.Binding receiver){
        return new HandoffEvidenceAdoption.Receipt(plan.id(),plan.task(),receiver,plan.writerCommit().revision(),
                plan.writerCommit().configurationSha256(),plan.writerCommit().stagingSha256(),digest,1,2);
    }
    private static ManagedRecipientImport.FrozenConfig frozen(ManagedRecipientImport.Grant grant){
        HandoffPreparations.Plan plan=grant.committed();
        return new ManagedRecipientImport.FrozenConfig(plan.id(),plan.evidence().candidate(),"STOPPED",plan.writerCommit().configurationSha256(),
                plan.writerCommit().stagingSha256(),grant.successorYaml());
    }
    private static TaskBackend backend(){
        return new TaskBackend(){
            public Map<String,Object> status(Config config){return Map.of("run_state","RUNNING");}
            public Map<String,Object> status(TaskDefinition task){return status(task.legacy);}
            public Map<String,Object> check(Config config){return Map.of();}
            public void pause(Config config){}
            public void resume(Config config,long generation){}
        };
    }
}
