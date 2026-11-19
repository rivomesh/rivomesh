package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Private historical evidence storage on a candidate. The ZIP remains a single inactive artifact. */
final class RawCaptureHandoffStaging {
    private static final String ROOT="handoff-raw-staging",LOCK="staging.lock",ZIP="capture.zip",SEAL="evidence.seal",TEMP_SEAL="seal.part";
    private static final int MAX_STAGES=16,MAX_SEAL=16*1024;
    private static final Set<String> COMPLETE=Set.of(ZIP,SEAL),PARTIAL=Set.of(ZIP,TEMP_SEAL);

    /**
     * Expected binding from a current authenticated preparation. The caller supplies the same
     * descriptor from the original Agent's ARCHIVED and the candidate's VERIFIED custody result.
     */
    record Binding(String preparation,String evidence,HandoffPreparations.Binding candidate,
                   SharedHandoffEvidence.Scope scope,SharedHandoffEvidence.Archive archive){
        Binding{
            if(preparation==null||!preparation.matches("[A-Za-z0-9_-]{8,128}")||evidence==null||!evidence.matches("[a-f0-9]{64}")
                    ||candidate==null||candidate.epoch()<1||!segment(candidate.node(),64)||!uuid(candidate.directory())
                    ||!uuid(candidate.incarnation())||scope==null||!segment(scope.task(),128)||!segment(scope.cluster(),128)
                    ||scope.assessment()==null||!scope.assessment().matches("[A-Za-z0-9_-]{8,128}")
                    ||!uuid(scope.revision())||!uuid(scope.configuration())||!uuid(scope.originalDirectory())
                    ||!Objects.equals(candidate.directory(),scope.candidateDirectory())||archive==null||archive.rawCaptures()<1)
                throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        }
        String key(){return Codec.sha256(Codec.encode(List.of(scope.cluster(),scope.task(),preparation)));}
        private static boolean segment(String value,int limit){
            return value!=null&&value.length()<=limit&&value.matches("[A-Za-z0-9][A-Za-z0-9._-]*");
        }
        private static boolean uuid(String value){
            return value!=null&&value.matches("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}");
        }
    }
    private record Stored(int version,Binding binding){}
    record Proof(Binding binding,String sealSha256){
        Proof{
            if(binding==null||sealSha256==null||!sealSha256.matches("[a-f0-9]{64}"))
                throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        }
        boolean matches(HandoffPreparations.Plan plan){
            var evidence=plan.evidence();
            var scope=binding.scope();
            return plan.id().equals(binding.preparation())&&plan.task().equals(scope.task())
                    &&evidence.proof().equals(binding.evidence())&&evidence.candidate().equals(binding.candidate())
                    &&evidence.original().directory().equals(scope.originalDirectory())
                    &&evidence.assessment().equals(scope.assessment())&&evidence.revision().equals(scope.revision())
                    &&evidence.configuration().equals(scope.configuration());
        }
    }
    record Receipt(String state,String preparationId,String candidateNodeId,String evidenceSha256,String archiveSha256,
                   long archiveBytes,int rawCaptures,String stagedSealSha256,boolean executionAuthorized){}

    private RawCaptureHandoffStaging(){}

    static synchronized Receipt stage(Path candidateData,Path shared,Binding binding,ControlCipher cipher)throws Exception{
        Path root=root(candidateData,binding);
        if(Files.notExists(root,LinkOption.NOFOLLOW_LINKS)){
            createDirectory(root);
            force(root.getParent());
        }
        requirePrivate(root,true);
        Path lockFile=root.resolve(LOCK);
        if(!Files.notExists(lockFile,LinkOption.NOFOLLOW_LINKS))requirePrivate(lockFile,false);
        try(FileChannel channel=privateChannel(lockFile,StandardOpenOption.CREATE);
            FileLock lock=channel.tryLock()){
            if(lock==null)throw refused("HANDOFF_RAW_STAGING_BUSY");
            Path complete=root.resolve(binding.key()),pending=root.resolve(binding.key()+".pending");
            if(!Files.notExists(complete,LinkOption.NOFOLLOW_LINKS))return verify(complete,binding,cipher);
            if(Files.notExists(pending,LinkOption.NOFOLLOW_LINKS)){
                try(var entries=Files.list(root)){
                    if(entries.limit(MAX_STAGES+1L).count()>MAX_STAGES)throw refused("HANDOFF_RAW_STAGING_CAPACITY");
                }
                createDirectory(pending);
            }
            requirePrivate(pending,true);
            if(Files.notExists(pending.resolve(SEAL),LinkOption.NOFOLLOW_LINKS))
                completePending(pending,shared,binding,cipher);
            Receipt receipt=verify(pending,binding,cipher);
            Files.move(pending,complete,StandardCopyOption.ATOMIC_MOVE);
            force(root);
            return receipt;
        }catch(OverlappingFileLockException busy){throw refused("HANDOFF_RAW_STAGING_BUSY");}
    }

    /** No writes and no dependency on the source mount or live task registry. */
    static Optional<Receipt> inspect(Path candidateData,Binding binding,ControlCipher cipher)throws Exception{
        Path root=root(candidateData,binding);
        if(Files.notExists(root,LinkOption.NOFOLLOW_LINKS))return Optional.empty();
        requirePrivate(root,true);
        Path complete=root.resolve(binding.key());
        if(Files.notExists(complete,LinkOption.NOFOLLOW_LINKS))return Optional.empty();
        return Optional.of(verify(complete,binding,cipher));
    }

    static Proof requireProof(Path data,Binding binding,ControlCipher cipher)throws Exception{
        Receipt receipt=inspect(data,binding,cipher).orElseThrow(()->refused("HANDOFF_RAW_STAGING_REQUIRED"));
        return new Proof(binding,receipt.stagedSealSha256());
    }

    static Path verifiedZip(Path data,Proof required,ControlCipher cipher)throws Exception{
        if(!required.equals(requireProof(data,required.binding(),cipher)))
            throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        return root(data,required.binding()).resolve(required.binding().key()).resolve(ZIP);
    }

    private static void completePending(Path pending,Path shared,Binding binding,ControlCipher cipher)throws Exception{
        names(pending,PARTIAL,false);
        Path zip=pending.resolve(ZIP),sealPart=pending.resolve(TEMP_SEAL);
        if(!Files.notExists(zip,LinkOption.NOFOLLOW_LINKS)){
            requirePrivate(zip,false);
            try{SharedHandoffEvidence.verifyStagedRawCapture(zip,binding.scope(),binding.archive());}
            catch(TaskService.Conflict incomplete){
                if(!"HANDOFF_ARCHIVE_BYTES_CHANGED".equals(incomplete.getMessage()))throw incomplete;
                Files.delete(zip);
            }
        }
        if(Files.notExists(zip,LinkOption.NOFOLLOW_LINKS)){
            Path source=SharedHandoffEvidence.verifiedRawStagingSource(shared,binding.scope(),binding.archive());
            copy(source,zip,binding.archive().bytes());
            SharedHandoffEvidence.verifyStagedRawCapture(zip,binding.scope(),binding.archive());
        }
        // A recovered pending copy may have stopped before the original file force.
        try(FileChannel file=FileChannel.open(zip,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){
            file.force(true);
        }
        // The only other known unpublished file is this stage's interrupted seal write.
        if(!Files.notExists(sealPart,LinkOption.NOFOLLOW_LINKS)){
            requirePrivate(sealPart,false);
            Files.delete(sealPart);
        }
        byte[] seal=cipher.seal(purpose(binding),Model.JSON.writeValueAsString(new Stored(1,binding)))
                .getBytes(StandardCharsets.UTF_8);
        if(seal.length>MAX_SEAL)throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        try(FileChannel channel=privateChannel(sealPart,StandardOpenOption.CREATE_NEW)){
            ByteBuffer buffer=ByteBuffer.wrap(seal);
            while(buffer.hasRemaining())channel.write(buffer);
            channel.force(true);
        }
        Files.move(sealPart,pending.resolve(SEAL),StandardCopyOption.ATOMIC_MOVE);
        force(pending);
    }

    private static Receipt verify(Path directory,Binding expected,ControlCipher cipher)throws Exception{
        requirePrivate(directory,true);
        names(directory,COMPLETE,true);
        Path zip=directory.resolve(ZIP),sealFile=directory.resolve(SEAL);
        requirePrivate(zip,false);
        requirePrivate(sealFile,false);
        byte[] seal;
        try(InputStream in=Files.newInputStream(sealFile,LinkOption.NOFOLLOW_LINKS)){
            seal=in.readNBytes(MAX_SEAL+1);
        }
        if(seal.length==0||seal.length>MAX_SEAL)throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        Stored stored=Model.JSON.readValue(cipher.open(purpose(expected),new String(seal,StandardCharsets.UTF_8)),Stored.class);
        if(stored==null||stored.version()!=1||!expected.equals(stored.binding()))
            throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        SharedHandoffEvidence.verifyStagedRawCapture(zip,expected.scope(),expected.archive());
        return new Receipt("STAGED",expected.preparation(),expected.candidate().node(),expected.evidence(),
                expected.archive().sha256(),expected.archive().bytes(),expected.archive().rawCaptures(),Codec.sha256(seal),false);
    }

    private static void copy(Path source,Path target,long expectedBytes)throws IOException{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12),copied=0;
        try(FileChannel input=FileChannel.open(source,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS);
            FileChannel output=privateChannel(target,StandardOpenOption.CREATE_NEW)){
            if(input.size()!=expectedBytes)throw refused("HANDOFF_ARCHIVE_BYTES_CHANGED");
            ByteBuffer buffer=ByteBuffer.allocate(64*1024);
            for(int count;(count=input.read(buffer))!=-1;){
                if(Thread.currentThread().isInterrupted()||System.nanoTime()-deadline>=0)
                    throw refused("HANDOFF_RAW_STAGING_TIMEOUT");
                copied+=count;
                if(copied>expectedBytes)throw refused("HANDOFF_ARCHIVE_BYTES_CHANGED");
                buffer.flip();
                while(buffer.hasRemaining())output.write(buffer);
                buffer.clear();
            }
            if(copied!=expectedBytes)throw refused("HANDOFF_ARCHIVE_BYTES_CHANGED");
        }
    }

    private static String purpose(Binding binding){return "handoff-raw-stage/"+binding.key();}

    private static Path root(Path candidateData,Binding binding)throws IOException{
        if(!Files.isDirectory(candidateData,LinkOption.NOFOLLOW_LINKS))throw refused("HANDOFF_RAW_STAGING_PATH_UNSAFE");
        Path directory=candidateData.toRealPath(),identity=directory.resolve("owner-directory-id");
        requirePrivate(identity,false);
        try(InputStream input=Files.newInputStream(identity,LinkOption.NOFOLLOW_LINKS)){
            byte[] bytes=input.readNBytes(65);
            if(bytes.length>64||!binding.candidate().directory().equals(new String(bytes,StandardCharsets.US_ASCII).trim()))
                throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        }
        return directory.resolve(ROOT);
    }

    private static FileChannel privateChannel(Path path,StandardOpenOption create)throws IOException{
        return FileChannel.open(path,Set.of(create,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    }

    private static void createDirectory(Path path)throws IOException{
        Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    }

    private static void requirePrivate(Path path,boolean directory)throws IOException{
        boolean type=directory?Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS):Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS);
        if(!type||Files.getPosixFilePermissions(path,LinkOption.NOFOLLOW_LINKS).stream()
                .anyMatch(mode->!mode.name().startsWith("OWNER_")))throw refused("HANDOFF_RAW_STAGING_PATH_UNSAFE");
    }

    private static void names(Path path,Set<String> allowed,boolean complete)throws IOException{
        int checked=0;
        try(var files=Files.newDirectoryStream(path)){
            for(Path file:files){
                if(!allowed.contains(file.getFileName().toString()))throw refused("HANDOFF_RAW_STAGING_ENTRY_INVALID");
                checked++;
            }
        }
        if(complete&&checked!=allowed.size())throw refused("HANDOFF_RAW_STAGING_ENTRY_INVALID");
    }

    private static void force(Path directory)throws IOException{
        try(FileChannel channel=FileChannel.open(directory,StandardOpenOption.READ)){channel.force(true);}
    }

    private static TaskService.Conflict refused(String code){return new TaskService.Conflict(code);}
}
