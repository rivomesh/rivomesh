package io.github.rivomesh.platform;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.Journal;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.SubscriberFailure;
import io.github.rivomesh.recovery.FailureEvidence;
import io.github.rivomesh.recovery.FailureEvidenceStore;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
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

    private FailureEvidence historicalFailure() {
        var failure=new FailureEvidence();
        failure.id=UUID.randomUUID().toString();
        failure.requestId="request_failure_1";
        failure.configurationRevision="original-failure-revision";
        failure.createdBy="administrator";
        failure.createdAt=Instant.now().toString();
        failure.scope=new FailureEvidence.Scope();
        failure.scope.taskId="task";
        failure.scope.ownerNodeId="origin";
        failure.scope.ownerDirectoryId=UUID.randomUUID().toString();
        var review=new Journal.FailureReview();
        review.fingerprint=Codec.sha256("config");
        review.pipelineId="task";
        review.generation="first-generation";
        review.epoch=review.sequence=review.failureEpoch=1;
        review.subscription=new Journal.Subscription();
        review.subscription.id="target";
        review.subscription.state="PAUSED";
        review.subscription.errorGeneration=1;
        review.failure=new SubscriberFailure();
        review.failure.phase=SubscriberFailure.Phase.TARGET_APPLY;
        review.raw=new SourceTransaction();
        review.raw.id="first-transaction";
        review.raw.streamGeneration=review.generation;
        review.raw.position=new SourcePosition(1,"MYSQL","source",Map.of("file","mysql-bin.000001","offset","42"));
        review.raw.attributes.put("fixture","private-retained-content");
        review.rawHash=Codec.sha256(Codec.encode(review.raw));
        failure.review=review;
        return failure;
    }

    @Test void verifiesHistoricalFailureBytesAndKeepsTheSourceSigningKeyPrivate()throws Exception{
        FailureEvidence failure=historicalFailure();
        Path original=temporary.toRealPath().resolve("failures");
        var inputs=new SharedHandoffEvidence.Failures(original,failure.scope);
        scope=new SharedHandoffEvidence.Scope(scope.cluster(),scope.task(),scope.assessment(),scope.revision(),
                scope.configuration(),failure.scope.ownerDirectoryId,scope.candidateDirectory());
        assertEquals("HANDOFF_NO_SHAREABLE_REPORTS",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.export(shared,scope,reports,List.of(),inputs)).getMessage());
        assertFalse(Files.exists(original));

        var store=new FailureEvidenceStore(original,failure.scope,new FailureEvidenceStore.Limits());
        store.retain(failure);
        byte[] signingKey=Files.readAllBytes(original.resolve("evidence.key"));
        String record=failure.id+".json";
        byte[] authority=Files.readAllBytes(original.resolve("authority.json")),retained=Files.readAllBytes(original.resolve(record));
        SharedHandoffEvidence.Archive proof=SharedHandoffEvidence.export(shared,scope,reports,List.of(),inputs);
        assertEquals(1,proof.failureRecords());
        assertEquals(0,proof.reports());
        SharedHandoffEvidence.verify(shared,scope,proof);
        var wrongCount=new SharedHandoffEvidence.Archive(proof.sha256(),proof.bytes(),0,0,2);
        assertEquals("HANDOFF_FAILURE_ARCHIVE_INVALID",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.verify(shared,scope,wrongCount)).getMessage());
        try(ZipFile archive=new ZipFile(archive().toFile())){
            assertEquals(Set.of("scope.json","failure-evidence/authority.json","failure-evidence/"+record),
                    new HashSet<>(archive.stream().map(file->file.getName()).toList()));
            try(var bytes=archive.getInputStream(archive.getEntry("failure-evidence/authority.json"))){
                assertArrayEquals(authority,bytes.readAllBytes());
            }
            try(var bytes=archive.getInputStream(archive.getEntry("failure-evidence/"+record))){
                assertArrayEquals(retained,bytes.readAllBytes());
            }
        }
        assertArrayEquals(signingKey,Files.readAllBytes(original.resolve("evidence.key")));
        assertArrayEquals(retained,Files.readAllBytes(original.resolve(record)));
        Files.writeString(original.resolve(record),Files.readString(original.resolve(record))
                .replace("private-retained-content","modified-retained-content"));
        assertEquals("HANDOFF_ORIGINAL_FAILURE_EVIDENCE_UNVERIFIED",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.export(shared,scope,reports,List.of(),inputs)).getMessage());
        SharedHandoffEvidence.verify(shared,scope,proof);
        Files.delete(original.resolve("evidence.key"));
        assertEquals("HANDOFF_ORIGINAL_FAILURE_EVIDENCE_UNVERIFIED",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.export(shared,scope,reports,List.of(),inputs)).getMessage());
        assertFalse(Files.exists(original.resolve("evidence.key")));
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
        try(var reservation=ValidationStore.reserveReportDirectory(ready)){
            assertEquals("HANDOFF_ARCHIVE_BUSY",assertThrows(TaskService.Conflict.class,
                    ()->SharedHandoffEvidence.export(shared,scope,reports,List.of())).getMessage());
        }
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
