package io.github.rivomesh.platform;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SharedHandoffEvidenceTest {
    @TempDir Path temporary;
    private Path shared,reports;
    private SharedHandoffEvidence.Scope scope;

    @BeforeEach void prepare()throws Exception{
        Path work=temporary.toRealPath();
        shared=Files.createDirectory(work.resolve("shared"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        reports=Files.createDirectory(work.resolve("reports"));
        scope=new SharedHandoffEvidence.Scope("cluster","task","assessment-a","revision-a","configuration-a","origin","candidate");
    }

    private Path report(String state)throws Exception{
        String id=UUID.randomUUID().toString();
        Path folder=Files.createDirectory(reports.resolve(id));
        Files.createFile(folder.resolve("validation.lock"));
        Files.write(folder.resolve("rows.mv.db"),new byte[]{8,4,13,1});
        Files.writeString(folder.resolve("summary.json"),"{\"id\":\""+id+"\",\"state\":\""+state+"\",\"sourceComplete\":true}");
        return folder;
    }

    private Path archive()throws Exception{
        try(DirectoryStream<Path> paths=Files.newDirectoryStream(shared,"*.zip")){return paths.iterator().next();}
    }

    @Test void copiesOnlyCompletedReportsAndRetainsTheExactOriginalBytes()throws Exception{
        Path complete=report("COMPLETE"),incomplete=report("SOURCE_READY");
        byte[] saved=Files.readAllBytes(complete.resolve("rows.mv.db"));
        SharedHandoffEvidence.Archive proof=SharedHandoffEvidence.export(shared,scope,reports,List.of());
        assertEquals(1,proof.reports());
        SharedHandoffEvidence.verify(shared,scope,proof);
        try(ZipFile zip=new ZipFile(archive().toFile())){
            assertNull(zip.getEntry(incomplete.getFileName()+"/rows.mv.db"));
            try(var bytes=zip.getInputStream(zip.getEntry(complete.getFileName()+"/rows.mv.db"))){
                assertArrayEquals(saved,bytes.readAllBytes());
            }
        }
        assertArrayEquals(saved,Files.readAllBytes(complete.resolve("rows.mv.db")));
        assertTrue(Files.exists(incomplete.resolve("summary.json")));
    }

    @Test void verifiesBothBindingAndBytesUnderAnIndependentRoot()throws Exception{
        report("COMPLETE");
        SharedHandoffEvidence.Archive proof=SharedHandoffEvidence.export(shared,scope,reports,List.of());
        Path candidate=Files.createDirectory(temporary.toRealPath().resolve("candidate"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        assertEquals("HANDOFF_ARCHIVE_MISSING",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.verify(candidate,scope,proof)).getMessage());
        Path copied=Files.copy(archive(),candidate.resolve(archive().getFileName()));
        SharedHandoffEvidence.verify(candidate,scope,proof);
        SharedHandoffEvidence.Scope different=new SharedHandoffEvidence.Scope("cluster","task","assessment-b",
                "revision-a","configuration-a","origin","candidate");
        assertEquals("HANDOFF_ARCHIVE_SCOPE_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.verify(candidate,different,proof)).getMessage());
        byte[] changed=Files.readAllBytes(copied);changed[changed.length-1]^=1;Files.write(copied,changed);
        assertEquals("HANDOFF_ARCHIVE_BYTES_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.verify(candidate,scope,proof)).getMessage());
    }

    @Test void busyReportCannotReplaceACompletedArchiveAndStagingCleanupIsScoped()throws Exception{
        Path ready=report("COMPLETE");
        SharedHandoffEvidence.Archive previous=SharedHandoffEvidence.export(shared,scope,reports,List.of());
        Path result=archive(), abandoned=shared.resolve(result.getFileName()+".old.tmp"),unrelated=shared.resolve("other.tmp");
        Files.writeString(abandoned,"abandoned");Files.writeString(unrelated,"other");
        try(FileChannel file=FileChannel.open(ready.resolve("validation.lock"),StandardOpenOption.WRITE);
            FileLock held=file.lock()){
            assertEquals("HANDOFF_ARCHIVE_BUSY",assertThrows(TaskService.Conflict.class,
                    ()->SharedHandoffEvidence.export(shared,scope,reports,List.of())).getMessage());
        }
        assertFalse(Files.exists(abandoned));
        assertTrue(Files.exists(unrelated));
        SharedHandoffEvidence.verify(shared,scope,previous);
        Files.setPosixFilePermissions(shared,PosixFilePermissions.fromString("rwxr-x---"));
        assertEquals("HANDOFF_ARCHIVE_MOUNT_PERMISSIONS_UNVERIFIED",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.verify(shared,scope,previous)).getMessage());
    }

    @Test void validatesArchivedClosureAndCountEvenWithoutAValidationReport()throws Exception{
        SharedHandoffEvidence.ClosedReceipt receipt=new SharedHandoffEvidence.ClosedReceipt("closed_request_1","1".repeat(64),
                "operator","OPERATOR",TaskRequest.Kind.ACTION,"SUCCEEDED","LOCAL_STOPPED");
        SharedHandoffEvidence.Archive proof=SharedHandoffEvidence.export(shared,scope,reports,List.of(receipt));
        assertEquals(0,proof.reports());
        assertEquals(1,proof.closedReceipts());
        SharedHandoffEvidence.verify(shared,scope,proof);
        var wrongCount=new SharedHandoffEvidence.Archive(proof.sha256(),proof.bytes(),0,2);
        assertEquals("HANDOFF_CLOSED_RECEIPT_INVALID",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.verify(shared,scope,wrongCount)).getMessage());
        assertThrows(TaskService.Conflict.class,()->new SharedHandoffEvidence.ClosedReceipt("closed_request_2","2".repeat(64),
                "operator","OPERATOR",TaskRequest.Kind.ACTION,"UNKNOWN","OWNER_RECEIPT_REQUIRED"));
    }
}
