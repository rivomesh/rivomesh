package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CaptureArchiveReceiptTest {
    @TempDir Path temporary;
    private Path archive;
    private CaptureArchiveReceipt.Owner owner;
    private CaptureArchiveReceipt.Capture capture;
    private String reference;

    @BeforeEach void setup()throws IOException{
        archive=MysqlContentArchive.directory(temporary.toRealPath().resolve("private"),true);
        owner=new CaptureArchiveReceipt.Owner("test-task","original-agent",UUID.randomUUID().toString());
        capture=new CaptureArchiveReceipt.Capture(owner,Codec.sha256("configuration"),"generation",
                Codec.sha256("checkpoint"),1,Codec.sha256("boundary"));
        reference=capture.file().substring(0,64);
    }

    private Map<String,Object> report(CaptureArchiveReceipt.Capture context,byte[] frames){
        return Map.of("taskId",context.owner().task(),"checkpointHash",context.checkpointHash(),
                "errorGeneration",context.errorGeneration(),"nextTransactionContents",
                Map.of("boundaryHash",context.boundaryHash(),"bytes",Integer.toString(frames.length),
                        "contentSha256",Codec.sha256(frames)));
    }

    private Map<String,String> retain(byte[] frames,CaptureArchiveReceipt.Capture attested)throws IOException{
        return MysqlContentArchive.retain(archive,report(capture,frames),frames,reference,attested);
    }

    private Map<String,Object> existing(CaptureArchiveReceipt.Capture expected)throws IOException{
        return MysqlContentArchive.existing(archive,reference,owner.task(),capture.checkpointHash(),
                capture.boundaryHash(),capture.errorGeneration(),expected);
    }

    @Test void authenticatesOriginalScopeAndExactBytesAcrossReopen()throws Exception{
        byte[] frames={9,8,7};
        var result=retain(frames,capture);
        byte[] retained=Files.readAllBytes(archive.resolve(capture.file()));
        assertEquals(Map.of("file",capture.file(),"archiveSha256",Codec.sha256(retained)),result);
        assertEquals(capture,CaptureArchiveReceipt.verifyIfPresent(archive,owner,capture.file(),retained));
        assertEquals(result,existing(capture).get("retainedEvidence"));
        var other=new CaptureArchiveReceipt.Owner(owner.task(),owner.node(),UUID.randomUUID().toString());
        assertThrows(IOException.class,()->CaptureArchiveReceipt.verifyIfPresent(archive,other,capture.file(),retained));
        for(Path file:new Path[]{archive.resolve(".capture-key"),CaptureArchiveReceipt.path(archive,capture.file())})
            assertEquals(PosixFilePermissions.fromString("rw-------"),Files.getPosixFilePermissions(file));
    }

    @Test void selfConsistentReplacementCannotChangeTheAuthenticatedLocatorOrBytes()throws Exception{
        retain(new byte[]{9,8,7},capture);
        Path replaced=MysqlContentArchive.directory(temporary.toRealPath().resolve("other"),true);
        byte[] altered={6,5,4};
        MysqlContentArchive.retain(replaced,report(capture,altered),altered,reference,null);
        Files.write(archive.resolve(capture.file()),Files.readAllBytes(replaced.resolve(capture.file())));
        assertNotNull(existing(null)); // The historical content checks alone are not authentication.
        assertEquals("MYSQL_BINLOG_EVIDENCE_ATTESTATION_INVALID",
                assertThrows(IOException.class,()->existing(capture)).getMessage());
        Path receipt=CaptureArchiveReceipt.path(archive,capture.file());
        var signed=Codec.JSON.readTree(Files.readAllBytes(receipt));
        var binding=(com.fasterxml.jackson.databind.node.ObjectNode)signed.path("binding");
        binding.put("sha256",Codec.sha256(Files.readAllBytes(archive.resolve(capture.file()))));
        Files.write(receipt,Codec.JSON.writeValueAsBytes(signed));
        assertEquals("MYSQL_BINLOG_EVIDENCE_ATTESTATION_INVALID",
                assertThrows(IOException.class,()->existing(capture)).getMessage());
    }

    @Test void legacyRetryCannotSignExistingUnboundFile()throws Exception{
        byte[] frames={1,2,3};
        var original=retain(frames,null);
        assertEquals(original,existing(capture).get("retainedEvidence"));
        assertEquals(original,retain(frames,capture));
        assertNull(CaptureArchiveReceipt.verifyIfPresent(archive,owner,capture.file(),
                Files.readAllBytes(archive.resolve(capture.file()))));
        assertFalse(Files.exists(archive.resolve(".capture-key")));
        assertFalse(Files.exists(CaptureArchiveReceipt.path(archive,capture.file())));
    }

    @Test void prepublishedReceiptPinsAnInterruptedCaptureAndKeyCannotBeReseeded()throws Exception{
        byte[] frames={7,7,7};
        var original=retain(frames,capture);
        Path receipt=CaptureArchiveReceipt.path(archive,capture.file());
        byte[] originalReceipt=Files.readAllBytes(receipt);
        Files.delete(archive.resolve(capture.file()));
        assertNull(existing(capture));
        assertThrows(IOException.class,()->retain(new byte[]{8,8,8},capture));
        assertFalse(Files.exists(archive.resolve(capture.file())));
        assertEquals(original,retain(frames,capture));
        assertArrayEquals(originalReceipt,Files.readAllBytes(receipt));
        Files.delete(archive.resolve(".capture-key"));
        assertEquals("MYSQL_BINLOG_EVIDENCE_KEY_MISSING",assertThrows(IOException.class,()->existing(capture)).getMessage());

        var another=new CaptureArchiveReceipt.Capture(owner,capture.fingerprint(),capture.generation(),
                capture.checkpointHash(),2,Codec.sha256("next-boundary"));
        String next=another.file().substring(0,64);
        assertEquals("MYSQL_BINLOG_EVIDENCE_KEY_MISSING",assertThrows(IOException.class,
                ()->MysqlContentArchive.retain(archive,report(another,frames),frames,next,another)).getMessage());
        assertFalse(Files.exists(archive.resolve(".capture-key")));
        assertFalse(Files.exists(archive.resolve(another.file())));
    }
}
