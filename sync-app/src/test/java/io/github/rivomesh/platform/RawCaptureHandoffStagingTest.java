package io.github.rivomesh.platform;

import io.github.rivomesh.CaptureArchiveReceipt;
import io.github.rivomesh.CaptureArchiveReceiptTest;
import io.github.rivomesh.cdc.Codec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class RawCaptureHandoffStagingTest {
    @TempDir Path temporary;
    private Path data,shared,original,sourceZip;
    private RawCaptureHandoffStaging.Binding binding;
    private String originalFile;
    private ControlCipher cipher;

    @BeforeEach void setup()throws Exception{
        Path root=temporary.toRealPath();
        data=privateDirectory(root.resolve("candidate"));
        shared=privateDirectory(root.resolve("shared"));
        original=root.resolve("original");
        var originalOwner=new CaptureArchiveReceipt.Owner("orders","origin",uuid());
        originalFile=CaptureArchiveReceiptTest.retainFixture(original,originalOwner);
        var candidate=new HandoffPreparations.Binding("candidate",uuid(),uuid(),1);
        Path identity=Files.writeString(data.resolve("owner-directory-id"),candidate.directory());
        Files.setPosixFilePermissions(identity,PosixFilePermissions.fromString("rw-------"));
        var scope=new SharedHandoffEvidence.Scope("test-cluster","orders","assessment_01",uuid(),uuid(),
                originalOwner.directory(),candidate.directory());
        var proof=SharedHandoffEvidence.exportCapture(shared,scope,original,originalOwner);
        binding=new RawCaptureHandoffStaging.Binding("prepare_raw_01",Codec.sha256("trusted-preparation"),candidate,scope,proof);
        sourceZip=SharedHandoffEvidence.verifiedRawStagingSource(shared,scope,proof);
        cipher=new ControlCipher(Base64.getEncoder().encodeToString(new byte[32]),scope.cluster(),uuid());
    }

    private static String uuid(){return UUID.randomUUID().toString();}
    private static Path privateDirectory(Path path)throws Exception{
        return Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    }
    private Path published(){return data.resolve("handoff-raw-staging").resolve(binding.key());}

    @Test void stagedArchiveIsExactPrivateAndInspectableWithoutTheSharedMount()throws Exception{
        assertTrue(RawCaptureHandoffStaging.inspect(data,binding,cipher).isEmpty());
        assertFalse(Files.exists(data.resolve("handoff-raw-staging")));
        var result=RawCaptureHandoffStaging.stage(data,shared,binding,cipher);
        assertEquals("STAGED",result.state());
        assertFalse(result.executionAuthorized());
        assertEquals(1,result.rawCaptures());
        assertEquals(binding.archive().sha256(),result.archiveSha256());
        Path staged=published().resolve("capture.zip");
        byte[] input=Files.readAllBytes(sourceZip);
        assertArrayEquals(input,Files.readAllBytes(staged));
        assertEquals(PosixFilePermissions.fromString("rwx------"),Files.getPosixFilePermissions(published()));
        for(String name:Set.of("capture.zip","evidence.seal"))
            assertEquals(PosixFilePermissions.fromString("rw-------"),Files.getPosixFilePermissions(published().resolve(name)));
        try(var files=Files.list(published())){
            assertEquals(Set.of("capture.zip","evidence.seal"),Set.copyOf(files.map(f->f.getFileName().toString()).toList()));
        }
        try(ZipFile zip=new ZipFile(staged.toFile())){
            assertEquals(Set.of("scope.json","capture/"+originalFile,"capture-receipts/"+originalFile),
                    Set.copyOf(zip.stream().map(ZipEntry::getName).toList()));
        }
        Files.move(shared,shared.resolveSibling("unmounted"));
        assertEquals(result,RawCaptureHandoffStaging.inspect(data,binding,cipher).orElseThrow());
        assertEquals(result,RawCaptureHandoffStaging.stage(data,null,binding,cipher));
        assertFalse(Files.exists(data.resolve("capture-evidence")));
        assertFalse(Files.exists(data.resolve("worker-configs")));
        assertEquals(32,Files.size(original.resolve(".capture-key")));
    }

    @Test void changedCandidateBindingAndDamagedPublicationAreRefusedWithoutOverwrite()throws Exception{
        RawCaptureHandoffStaging.stage(data,shared,binding,cipher);
        var previous=binding.candidate();
        var changedCandidate=new HandoffPreparations.Binding(previous.node(),previous.directory(),uuid(),previous.epoch()+1);
        var changed=new RawCaptureHandoffStaging.Binding(binding.preparation(),binding.evidence(),changedCandidate,
                binding.scope(),binding.archive());
        assertEquals("HANDOFF_RAW_STAGING_SCOPE_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->RawCaptureHandoffStaging.inspect(data,changed,cipher)).getMessage());
        Path file=published().resolve("capture.zip");
        byte[] damaged=Files.readAllBytes(file);
        damaged[damaged.length-1]^=1;
        Files.write(file,damaged);
        assertEquals("HANDOFF_ARCHIVE_BYTES_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->RawCaptureHandoffStaging.stage(data,shared,binding,cipher)).getMessage());
        assertArrayEquals(damaged,Files.readAllBytes(file));
    }

    @Test void finishesAnInterruptedPendingStageWithAnExactArchiveWhileOffline()throws Exception{
        Path root=privateDirectory(data.resolve("handoff-raw-staging"));
        Path pending=privateDirectory(root.resolve(binding.key()+".pending"));
        Files.copy(sourceZip,pending.resolve("capture.zip"));
        Files.setPosixFilePermissions(pending.resolve("capture.zip"),PosixFilePermissions.fromString("rw-------"));
        Files.writeString(pending.resolve("seal.part"),"interrupted write");
        Files.setPosixFilePermissions(pending.resolve("seal.part"),PosixFilePermissions.fromString("rw-------"));
        assertTrue(RawCaptureHandoffStaging.inspect(data,binding,cipher).isEmpty());
        var result=RawCaptureHandoffStaging.stage(data,null,binding,cipher);
        assertEquals(result,RawCaptureHandoffStaging.inspect(data,binding,cipher).orElseThrow());
        assertFalse(Files.exists(pending));
    }

    @Test void mismatchedLocalDirectoryIsRefusedBeforeWritingStage()throws Exception{
        Files.writeString(data.resolve("owner-directory-id"),uuid());
        assertEquals("HANDOFF_RAW_STAGING_SCOPE_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->RawCaptureHandoffStaging.stage(data,shared,binding,cipher)).getMessage());
        assertFalse(Files.exists(data.resolve("handoff-raw-staging")));
    }

    @Test void rejectsUnexpectedZipEntriesBeforeCopyingTheArchive()throws Exception{
        byte[] before=Files.readAllBytes(sourceZip);
        Path modified=shared.resolve("rewritten.zip");
        try(ZipFile zip=new ZipFile(sourceZip.toFile());
            ZipOutputStream out=new ZipOutputStream(Files.newOutputStream(modified))){
            for(ZipEntry entry:zip.stream().toList()){
                out.putNextEntry(new ZipEntry(entry.getName()));
                try(var bytes=zip.getInputStream(entry)){bytes.transferTo(out);}
                out.closeEntry();
            }
            out.putNextEntry(new ZipEntry(".capture-key"));
            out.write(new byte[32]);
            out.closeEntry();
        }
        Files.move(modified,sourceZip,StandardCopyOption.REPLACE_EXISTING);
        byte[] forged=Files.readAllBytes(sourceZip);
        assertNotEquals(Codec.sha256(before),Codec.sha256(forged));
        var descriptor=new SharedHandoffEvidence.Archive(Codec.sha256(forged),forged.length,0,0,0,1);
        var supplied=new RawCaptureHandoffStaging.Binding(binding.preparation(),binding.evidence(),
                binding.candidate(),binding.scope(),descriptor);
        assertEquals("HANDOFF_RAW_CAPTURE_ARCHIVE_INVALID",assertThrows(TaskService.Conflict.class,
                ()->RawCaptureHandoffStaging.stage(data,shared,supplied,cipher)).getMessage());
        assertFalse(Files.exists(published()));
        assertFalse(Files.exists(published().resolveSibling(binding.key()+".pending").resolve("capture.zip")));
    }
}
