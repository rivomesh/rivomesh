package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class GlobalConfigurationTest {
    @TempDir Path directory;
    GlobalTaskPlaneTest base;OwnerQueries queries;ScheduledExecutorService readers;
    final AtomicReference<Throwable> readerFailure=new AtomicReference<>();
    @BeforeEach void initialize()throws Exception{
        base=new GlobalTaskPlaneTest();base.directory=directory;base.initialize();base.create();base.execution.cycle();base.publish();
        OwnerExecutionTest fixture=base.fixture;queries=new OwnerQueries(fixture.config.cluster,fixture.fixture.a,fixture.owner.a,10,30000);
        readers=Executors.newSingleThreadScheduledExecutor();readers.scheduleWithFixedDelay(()->{try{for(String id:queries.pending(fixture.owner.lease,fixture.fixture.directory,10))queries.serve(id,fixture.owner.lease,fixture.fixture.directory,new LocalTaskPlane(fixture.local));}catch(Throwable failure){if(!readers.isShutdown())readerFailure.compareAndSet(null,failure);}},0,10,TimeUnit.MILLISECONDS);
    }
    @AfterEach void close()throws Exception{if(readers!=null){readers.shutdownNow();readers.awaitTermination(5,TimeUnit.SECONDS);}if(base!=null)base.close();assertNull(readerFailure.get());}
    Map<String,Object> configuration(GlobalTaskPlane entry)throws Exception{return entry.invoke(TaskRequest.of(TaskRequest.Kind.CONFIGURATION,"job"),base.fixture.admin());}
    TaskRequest update(String id,Map<String,Object> configuration)throws Exception{TaskRequest r=TaskRequest.of(TaskRequest.Kind.CONFIG_UPDATE,"job");r.requestId=id;r.revision=(String)configuration.get("revision");r.document=((JsonNode)configuration.get("config")).deepCopy();return r;}
    void execute(TaskRequest request,String status)throws Exception{PlatformTest.await(()->{base.execution.cycle();base.publish();return status.equals(base.fixture.status(request.requestId));});}
    @SuppressWarnings("unchecked") List<Map<String,Object>> history()throws Exception{return(List<Map<String,Object>>)base.b.invoke(TaskRequest.of(TaskRequest.Kind.CONFIG_HISTORY,"job"),base.fixture.admin()).get("versions");}
    @Test void referencesFromOneEntryCanBeEditedAtAnotherAndBecomeActiveOnlyAfterOwnerSuccess()throws Exception{
        Map<String,Object> before=configuration(base.a);String old=(String)before.get("activeConfigurationVersion");assertEquals(before.get("configurationVersion"),old);TaskRequest request=update("global_edit_password",before);((ObjectNode)request.document.path("source")).put("password","next-source-private-password");
        Map<String,Object> pending=base.b.invoke(request,base.fixture.admin());assertTrue((Boolean)pending.get("configurationPending"));assertEquals(old,pending.get("activeConfigurationVersion"));assertNotEquals(old,pending.get("configurationVersion"));execute(request,"SUCCEEDED");
        Map<String,Object> after=configuration(base.a);assertEquals(after.get("configurationVersion"),after.get("activeConfigurationVersion"));assertFalse(Model.JSON.writeValueAsString(after).contains("next-source-private-password"));assertTrue(new String(Files.readAllBytes(directory.resolve("tasks/job.yml")),java.nio.charset.StandardCharsets.UTF_8).contains("next-source-private-password"));
        base.a.invoke(request,base.fixture.admin());assertEquals(2,history().size());
    }
    @Test void sharedHistoryReferenceRestoresThroughANewConfigurationCommit()throws Exception{
        TaskRequest request=update("global_edit_password",configuration(base.a));((ObjectNode)request.document.path("source")).put("password","next-source-private-password");base.b.invoke(request,base.fixture.admin());execute(request,"SUCCEEDED");List<Map<String,Object>> versions=history();assertEquals(2,versions.size());
        TaskRequest restore=TaskRequest.of(TaskRequest.Kind.RESTORE,"job");restore.requestId="global_restore_password";restore.revision=(String)configuration(base.a).get("revision");restore.reference=(String)versions.get(1).get("reference");base.a.invoke(restore,base.fixture.admin());execute(restore,"SUCCEEDED");base.b.invoke(restore,base.fixture.admin());assertEquals(3,history().size());
        String yaml=new String(Files.readAllBytes(directory.resolve("tasks/job.yml")),java.nio.charset.StandardCharsets.UTF_8);assertTrue(yaml.contains("source-secret"));assertFalse(yaml.contains("next-source-private-password"));
    }
    @Test void remotePreviewUsesResolvedSecretsAndTheOwnerRevisionWithoutChangingConfiguration()throws Exception{
        Map<String,Object> before=configuration(base.a);TaskRequest preview=update("unused_preview_id",before);preview.kind=TaskRequest.Kind.CONFIG_PREVIEW;preview.requestId=null;((ObjectNode)preview.document.path("source")).put("password","preview-private-password");Map<String,Object> result=base.b.invoke(preview,base.fixture.admin());assertEquals(false,result.get("migrationRequired"));assertTrue(result.get("changedPaths").toString().contains("/source/password"));assertEquals(before.get("revision"),configuration(base.a).get("revision"));assertEquals(1,history().size());assertFalse(Model.JSON.writeValueAsString(result).contains("preview-private-password"));
    }
    @Test void rejectedProposalLeavesTheOldActiveConfigurationAndOwnerDataIntact()throws Exception{
        Map<String,Object> before=configuration(base.a);TaskRequest request=update("global_rejected_migration",before);((ObjectNode)request.document.path("source")).put("database","another-database");base.b.invoke(request,base.fixture.admin());execute(request,"REJECTED");
        Map<String,Object> after=configuration(base.a);assertEquals(before.get("activeConfigurationVersion"),after.get("activeConfigurationVersion"));assertNotEquals(after.get("configurationVersion"),after.get("activeConfigurationVersion"));assertEquals("CONFIG_MIGRATION_REQUIRED",base.fixture.commands.view(request.requestId,base.fixture.admin()).get("code"));assertEquals("job",base.fixture.local.view("job").get("sourceDatabase"));
    }
    @Test void staleRevisionAndTamperedSecretReferenceCannotCreateAProposal()throws Exception{
        Map<String,Object> before=configuration(base.a);TaskRequest request=update("global_tampered_secret",before);ObjectNode source=(ObjectNode)request.document.path("source");ObjectNode reference=Model.JSON.createObjectNode();reference.put("$rivomeshSecret","forged");source.set("password",reference);
        assertEquals("CONFIG_SECRET_REFERENCE_CHANGED",assertThrows(TaskService.Conflict.class,()->base.b.invoke(request,base.fixture.admin())).getMessage());request.document=((JsonNode)before.get("config")).deepCopy();request.revision=UUID.randomUUID().toString();assertEquals("REVISION_CHANGED",assertThrows(TaskService.Conflict.class,()->base.a.invoke(request,base.fixture.admin())).getMessage());assertEquals(1,history().size());
    }
    @Test void historicalMetadataAndLogicalOrderingAreAuthenticatedAndPaginated()throws Exception{
        GlobalTaskStoreTest fixture=base.fixture.fixture;GlobalTaskStore.Record record=fixture.a.get("job",base.fixture.admin());
        for(int n=0;n<22;n++)record=fixture.a.update("job",record.revision,"version-"+n,"global_history_"+n,base.fixture.admin());
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.CONFIG_HISTORY,"job");Map<String,Object> first=base.b.invoke(request,base.fixture.admin());assertEquals(20,((List<?>)first.get("versions")).size());request.cursor=(String)first.get("nextCursor");Map<String,Object> second=base.a.invoke(request,base.fixture.admin());assertEquals(3,((List<?>)second.get("versions")).size());assertFalse(Model.JSON.writeValueAsString(first).contains("version-"));
        base.fixture.owner.sql("UPDATE rivomesh_control.rmc_task_version SET actor_id='forged'");assertEquals("GLOBAL_CONFIG_INVALID",assertThrows(TaskService.Conflict.class,()->configuration(base.a)).getMessage());
    }
}
