package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.PipelineValidationBarrier;
import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class HandoffStagingTest {
    @TempDir Path home;
    private final ControlCipher guard=new ControlCipher(Base64.getEncoder().encodeToString(new byte[32]),"cluster","control");
    private final SharedHandoffEvidence.Scope scope=new SharedHandoffEvidence.Scope("cluster","task","assessment-001",
            UUID.randomUUID().toString(),"configuration","original-directory","candidate-directory");
    private final HandoffPreparations.Binding candidate=new HandoffPreparations.Binding("candidate",scope.candidateDirectory(),"agent-incarnation",1);
    private final SharedHandoffEvidence.ClosedReceipt receipt=new SharedHandoffEvidence.ClosedReceipt("request_001",
            "a".repeat(64),"admin","ADMIN",TaskRequest.Kind.ACTION,"SUCCEEDED","LOCAL_STOPPED");

    @Test void sealedCustodyPublishesPrivateReportAndReceiptBytesAndSurvivesRetry()throws Exception{
        Path root=home.toRealPath(),original=privateDirectory(root,"original"),shared=privateDirectory(root,"shared"),
                data=privateDirectory(root,"candidate");
        String id=completedReport(original);
        byte[] hotRows=Files.readAllBytes(original.resolve(id).resolve("rows.mv.db"));
        byte[] summary=Files.readAllBytes(original.resolve(id).resolve("summary.json"));
        Path live=privateDirectory(data,"validations");
        Files.writeString(live.resolve("unrelated"),"untouched");
        SharedHandoffEvidence.Archive proof=SharedHandoffEvidence.export(shared,scope,original,List.of(receipt));
        var bound=binding(proof);
        Map<String,Object> staged=HandoffStaging.stage(data,shared,bound,guard);
        assertEquals("STAGED",staged.get("state"));assertEquals(false,staged.get("executionAuthorized"));
        assertFalse(staged.toString().contains(receipt.signature()));
        Path complete=data.resolve("handoff-staging").resolve(bound.key()),report=complete.resolve("reports").resolve(id);
        assertArrayEquals(hotRows,Files.readAllBytes(report.resolve(ValidationStore.STAGED_ROWS_FILE)));
        assertArrayEquals(summary,Files.readAllBytes(report.resolve("summary.staged.json")));
        assertEquals("VALIDATION_REPORT_CUSTODY_STAGED",assertThrows(io.github.rivomesh.SyncFailure.class,
                ()->ValidationStore.open(complete.resolve("reports"),id)).code);
        assertFalse(Files.exists(report.resolve("rows.mv.db")));
        assertEquals("untouched",Files.readString(live.resolve("unrelated")));
        assertArrayEquals(hotRows,Files.readAllBytes(original.resolve(id).resolve("rows.mv.db")));
        assertEquals(List.of(receipt),Arrays.asList(Model.JSON.readValue(Files.readAllBytes(complete.resolve("closed-receipts.json")),
                SharedHandoffEvidence.ClosedReceipt[].class)));

        Path pending=complete.resolveSibling(bound.key()+".pending");
        Files.move(complete,pending);
        Files.delete(SharedHandoffEvidence.stagingSource(shared,scope,proof));
        assertEquals(staged,HandoffStaging.stage(data,shared,bound,guard));
        assertEquals(staged,HandoffStaging.stage(data,shared,bound,guard));
        var foreign=new HandoffStaging.Binding(bound.preparation(),"b".repeat(64),candidate,scope,proof);
        assertEquals("HANDOFF_STAGING_SCOPE_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->HandoffStaging.stage(data,shared,foreign,guard)).getMessage());
        try(ValidationStore.ReportReservation held=ValidationStore.reserveReportDirectory(report)){
            assertEquals("HANDOFF_STAGING_REPORT_UNVERIFIED",assertThrows(TaskService.Conflict.class,
                    ()->HandoffStaging.stage(data,shared,bound,guard)).getMessage());
        }
        Files.writeString(report.resolve("archive-reclaim.json"),"unverified tombstone");
        assertEquals("HANDOFF_STAGING_REPORT_UNVERIFIED",assertThrows(TaskService.Conflict.class,
                ()->HandoffStaging.stage(data,shared,bound,guard)).getMessage());
        Files.delete(report.resolve("archive-reclaim.json"));
        Files.writeString(report.resolve(ValidationStore.STAGED_ROWS_FILE),"changed");
        assertEquals("HANDOFF_STAGING_BYTES_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->HandoffStaging.stage(data,shared,bound,guard)).getMessage());
    }

    @Test void onlyExpectedArchiveEntriesCanPublishAndRetryClearsItsOwnPartialStage()throws Exception{
        Path root=home.toRealPath(),original=privateDirectory(root,"original"),shared=privateDirectory(root,"shared"),
                data=privateDirectory(root,"candidate");
        SharedHandoffEvidence.Archive valid=SharedHandoffEvidence.export(shared,scope,original,List.of(receipt));
        Path zip=SharedHandoffEvidence.stagingSource(shared,scope,valid);
        byte[] exact=Files.readAllBytes(zip);
        try(ZipOutputStream output=new ZipOutputStream(Files.newOutputStream(zip))){
            add(output,"scope.json",Model.JSON.writeValueAsBytes(scope));
            add(output,"closed-receipts.json",Model.JSON.writeValueAsBytes(List.of(receipt)));
            add(output,"../outside.json",new byte[]{42});
        }
        byte[] malformed=Files.readAllBytes(zip);
        String actual=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(malformed));
        var forged=new SharedHandoffEvidence.Archive(actual,malformed.length,0,1);
        assertEquals("HANDOFF_STAGING_ENTRY_INVALID",assertThrows(TaskService.Conflict.class,
                ()->HandoffStaging.stage(data,shared,binding(forged),guard)).getMessage());
        Path stage=data.resolve("handoff-staging").resolve(binding(valid).key());
        assertFalse(Files.exists(stage));assertFalse(Files.exists(data.resolve("outside.json")));
        Files.write(zip,exact);
        assertEquals("STAGED",HandoffStaging.stage(data,shared,binding(valid),guard).get("state"));
        assertTrue(Files.isRegularFile(stage.resolve("closed-receipts.json")));
    }

    private HandoffStaging.Binding binding(SharedHandoffEvidence.Archive archive){
        return new HandoffStaging.Binding("prepared_001","f".repeat(64),candidate,scope,archive);
    }
    private static Path privateDirectory(Path root,String name)throws Exception{
        return Files.createDirectory(root.resolve(name),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    }
    private static void add(ZipOutputStream archive,String name,byte[] content)throws Exception{
        archive.putNextEntry(new ZipEntry(name));archive.write(content);archive.closeEntry();
    }
    private static String completedReport(Path root)throws Exception{
        TableSchema table=new TableSchema();table.database="sales";table.name="events";
        table.id=TableSchema.identity("sales","","events");table.primaryKey.add("id");
        TableSchema.Column id=new TableSchema.Column();id.name="id";id.type=TableSchema.Type.INT64;table.columns.add(id);
        var certificate=new PipelineValidationBarrier.Certificate();certificate.id=UUID.randomUUID().toString();
        certificate.fingerprint="a".repeat(64);certificate.generation="generation";certificate.epoch=1;certificate.sequence=1;
        certificate.selectedTargets.add("target");certificate.schemas.put(table.id,Codec.sha256(Codec.encode(table)));
        try(ValidationStore report=ValidationStore.create(root,certificate.id,certificate,List.of(table),Map.of("target",Set.of(table.id)),new ValidationStore.Limits())){
            var row=List.of(Cell.value("1"));
            report.source(table.id,row);report.finishSourceTable(table.id);report.completeSource();
            report.target("target",table.id,null,row);report.finishTarget("target",table.id);report.complete();
        }
        return certificate.id;
    }
}
