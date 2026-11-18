package io.github.rivomesh.validation;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.cdc.Codec;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A sealed recovery copy of a complete report, kept on an operator-provisioned private mount. */
public final class ValidationArchive {
    public static final String COPY="VALIDATION_ARCHIVE_COPY";
    public static final String RESTORE="VALIDATION_ARCHIVE_RESTORE";
    private static final String DESCRIPTOR="archive.json", RESTORATION="archive-restore.json",
            SUMMARY="summary.json", ROWS="rows.mv.db";
    private static final long MAX_ROWS_BYTES=128L*1024*1024, MAX_SUMMARY_BYTES=1024*1024;
    private static final List<String> FILES=List.of(SUMMARY,ROWS);
    private static final Set<PosixFilePermission> PRIVATE=Set.of(PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,PosixFilePermission.OWNER_EXECUTE);

    private ValidationArchive(){}

    public record Request(String reportId,String expectedReviewId,String expectedArchiveId,boolean approved){
        public void validate(String action){
            if(reportId==null||!ValidationStore.reportId(reportId)||!approved
                    ||!(COPY.equals(action)&&id(expectedReviewId)&&expectedArchiveId==null
                    ||RESTORE.equals(action)&&id(expectedArchiveId)&&expectedReviewId==null))
                throw new IllegalArgumentException("VALIDATION_ARCHIVE_REQUEST_INVALID");
        }
    }

    public record Scope(String taskId,String ownerDirectoryId,String reportId){
        public Scope{
            if(taskId==null||!taskId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                    ||ownerDirectoryId==null||!ValidationStore.reportId(ownerDirectoryId)
                    ||reportId==null||!ValidationStore.reportId(reportId))
                throw new IllegalArgumentException("VALIDATION_ARCHIVE_SCOPE_INVALID");
        }
    }
    public record FileSeal(long bytes,String sha256){}
    public record Seal(int version,Scope scope,String archiveId,String reviewId,String actorId,
                       String archivedAt,Map<String,FileSeal> files){}
    public record Restoration(int version,String archiveId,String requestId,String actorId,boolean complete){}

    public static Seal observe(Path directory,String reportId)throws Exception{
        Path file=directory.resolve(DESCRIPTOR);
        if(!Files.exists(file,LinkOption.NOFOLLOW_LINKS))return null;
        return seal(file,reportId);
    }

    static boolean hasDescriptor(Path directory){
        return Files.isRegularFile(directory.resolve(DESCRIPTOR),LinkOption.NOFOLLOW_LINKS);
    }

    public static boolean restoringRequired(Path directory,String reportId)throws Exception{
        if(Files.exists(directory.resolve(ROWS),LinkOption.NOFOLLOW_LINKS)
                &&!Files.exists(directory.resolve(RESTORATION),LinkOption.NOFOLLOW_LINKS))return false;
        Seal archive=observe(directory,reportId);
        if(archive==null)return false;
        Restoration restoration=restoration(directory,archive);
        return !Files.exists(directory.resolve(ROWS),LinkOption.NOFOLLOW_LINKS)
                ||restoration!=null&&!restoration.complete();
    }

    public static Seal execute(Path archiveRoot,Path reportRoot,Scope scope,Request request,String action,
                               String requestId,String actorId)throws Exception{
        request.validate(action);
        if(!scope.reportId().equals(request.reportId())||!id(requestId)
                ||actorId==null||!actorId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}"))
            throw failure("VALIDATION_ARCHIVE_SCOPE_INVALID");
        Path root=privateRoot(archiveRoot);
        Path source=ValidationStore.reportPath(reportRoot,scope.reportId());
        if(!Files.isDirectory(source,LinkOption.NOFOLLOW_LINKS))throw failure("VALIDATION_REPORT_NOT_CREATED");
        Path directory=source.toRealPath();
        if(!directory.equals(source.getParent().toRealPath().resolve(scope.reportId()))
                ||root.startsWith(directory.getParent())||directory.startsWith(root))
            throw failure("VALIDATION_ARCHIVE_PATH_INVALID");
        ValidationStore.claimLocalWriter(directory);
        try(FileChannel channel=FileChannel.open(directory.resolve("validation.lock"),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
            FileLock lock=acquire(channel)){
            Seal current=observe(directory,scope.reportId());
            if(current!=null&&!scope.equals(current.scope()))throw failure("VALIDATION_ARCHIVE_SCOPE_INVALID");
            if(COPY.equals(action))
                return copy(root,reportRoot,directory,scope,request,requestId,actorId,current);
            return restore(root,directory,request,requestId,actorId,current);
        }finally{
            ValidationStore.releaseLocalWriter(directory);
        }
    }

    private static Seal copy(Path root,Path reportRoot,Path directory,Scope scope,Request request,
                             String requestId,String actorId,Seal current)throws Exception{
        if(current!=null){
            if(!requestId.equals(current.archiveId())||!actorId.equals(current.actorId())
                    ||!request.expectedReviewId().equals(current.reviewId()))
                throw failure("VALIDATION_ARCHIVE_ALREADY_SEALED");
            verify(location(root,current),current);
            return current;
        }
        ValidationRetention.Review review=ValidationRetention.observe(reportRoot,scope.reportId());
        if(review==null||!request.expectedReviewId().equals(review.requestId()))
            throw failure("VALIDATION_RETENTION_REVIEW_CHANGED");
        Path destination=location(root,scope,requestId);
        if(Files.exists(destination,LinkOption.NOFOLLOW_LINKS)){
            Seal recovered=seal(destination.resolve(DESCRIPTOR),scope.reportId());
            if(!scope.equals(recovered.scope())||!requestId.equals(recovered.archiveId())||!actorId.equals(recovered.actorId())
                    ||!review.requestId().equals(recovered.reviewId()))throw failure("VALIDATION_ARCHIVE_COLLISION");
            verify(destination,recovered);
            for(String file:FILES)requireEqual(directory.resolve(file),file,recovered.files().get(file));
            installMetadata(directory,DESCRIPTOR,recovered);
            return recovered;
        }
        byte[] summary=bounded(directory.resolve(SUMMARY),MAX_SUMMARY_BYTES);
        JsonNode projected=Model.JSON.readTree(summary);
        if(projected==null||!scope.reportId().equals(projected.path("id").textValue())
                ||!"COMPLETE".equals(projected.path("state").textValue())||!projected.path("sourceComplete").asBoolean())
            throw failure("VALIDATION_RETENTION_REPORT_INCOMPLETE");
        Path stage=Files.createTempDirectory(root,".preparing-");
        try{
            ValidationStore.owner(stage,true);
            Map<String,FileSeal> snapshots=new LinkedHashMap<>();
            for(String file:FILES)snapshots.put(file,transfer(directory.resolve(file),stage.resolve(file),limit(file)));
            Seal sealed=new Seal(1,scope,requestId,review.requestId(),actorId,Instant.now().toString(),snapshots);
            installMetadata(stage,DESCRIPTOR,sealed);
            verify(stage,sealed);
            Files.move(stage,destination,StandardCopyOption.ATOMIC_MOVE);
            forceDirectory(root);
            installMetadata(directory,DESCRIPTOR,sealed);
            return sealed;
        }finally{
            if(Files.isDirectory(stage,LinkOption.NOFOLLOW_LINKS)){
                for(String name:List.of(DESCRIPTOR,ROWS,SUMMARY))Files.deleteIfExists(stage.resolve(name));
                Files.deleteIfExists(stage);
            }
        }
    }

    private static Seal restore(Path root,Path directory,Request request,String requestId,String actorId,Seal current)throws Exception{
        if(current==null||!request.expectedArchiveId().equals(current.archiveId()))
            throw failure("VALIDATION_ARCHIVE_CHANGED");
        Restoration previous=restoration(directory,current);
        if(previous!=null&&requestId.equals(previous.requestId())){
            if(!actorId.equals(previous.actorId()))throw failure("VALIDATION_ARCHIVE_RESTORE_CHANGED");
            if(previous.complete())return current;
        }
        Path copy=location(root,current);
        verify(copy,current);
        for(String file:FILES){
            Path existing=directory.resolve(file);
            if(Files.exists(existing,LinkOption.NOFOLLOW_LINKS))requireEqual(existing,file,current.files().get(file));
        }
        installMetadata(directory,RESTORATION,new Restoration(1,current.archiveId(),requestId,actorId,false));
        for(String file:FILES){
            Path destination=directory.resolve(file);
            if(Files.exists(destination,LinkOption.NOFOLLOW_LINKS))continue;
            Path staged=Files.createTempFile(directory,"archive-restore-",".tmp");
            try{
                if(!current.files().get(file).equals(transfer(copy.resolve(file),staged,limit(file))))
                    throw failure("VALIDATION_ARCHIVE_BYTES_CHANGED");
                Files.move(staged,destination,StandardCopyOption.ATOMIC_MOVE);
                forceDirectory(directory);
            }finally{
                Files.deleteIfExists(staged);
            }
        }
        installMetadata(directory,RESTORATION,new Restoration(1,current.archiveId(),requestId,actorId,true));
        return current;
    }

    private static Restoration restoration(Path directory,Seal archive)throws Exception{
        Path path=directory.resolve(RESTORATION);
        if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS))return null;
        try{
            Restoration state=Model.JSON.readValue(bounded(path,2048),Restoration.class);
            if(state==null||state.version()!=1||!archive.archiveId().equals(state.archiveId())||!id(state.requestId())
                    ||state.actorId()==null||!state.actorId().matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}"))
                throw new IllegalArgumentException();
            return state;
        }catch(Exception invalid){
            throw failure("VALIDATION_ARCHIVE_RESTORE_CHANGED");
        }
    }

    private static void verify(Path directory,Seal expected)throws Exception{
        if(!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS)
                ||!expected.equals(seal(directory.resolve(DESCRIPTOR),expected.scope().reportId())))
            throw failure("VALIDATION_ARCHIVE_DESCRIPTOR_CHANGED");
        for(String file:FILES)if(!expected.files().get(file).equals(transfer(directory.resolve(file),null,limit(file))))
            throw failure("VALIDATION_ARCHIVE_BYTES_CHANGED");
    }

    private static void requireEqual(Path original,String file,FileSeal expected)throws Exception{
        if(!expected.equals(transfer(original,null,limit(file))))throw failure("VALIDATION_ARCHIVE_HOT_FILE_CHANGED");
    }

    private static FileSeal transfer(Path source,Path destination,long maximum)throws Exception{
        if(!Files.isRegularFile(source,LinkOption.NOFOLLOW_LINKS)||Files.size(source)>maximum)
            throw failure("VALIDATION_ARCHIVE_FILE_INVALID");
        MessageDigest digest=MessageDigest.getInstance("SHA-256");
        long length=0;
        try(InputStream input=Files.newInputStream(source,LinkOption.NOFOLLOW_LINKS);
            FileChannel output=destination==null?null:FileChannel.open(destination,StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){
            if(destination!=null)ValidationStore.owner(destination,false);
            byte[] bytes=new byte[64*1024];
            for(int count;(count=input.read(bytes))!=-1;){
                if(count>maximum-length)throw failure("VALIDATION_ARCHIVE_FILE_INVALID");
                length+=count;digest.update(bytes,0,count);
                if(output!=null){ByteBuffer buffer=ByteBuffer.wrap(bytes,0,count);while(buffer.hasRemaining())output.write(buffer);}
            }
            if(output!=null)output.force(true);
        }
        if(length==0)throw failure("VALIDATION_ARCHIVE_FILE_INVALID");
        return new FileSeal(length,HexFormat.of().formatHex(digest.digest()));
    }

    private static Seal seal(Path descriptor,String reportId)throws Exception{
        byte[] bytes=bounded(descriptor,4096);
        try{
            Seal seal=Model.JSON.readValue(bytes,Seal.class);
            if(seal==null||seal.version()!=1||!reportId.equals(seal.scope().reportId())||!id(seal.archiveId())
                    ||!id(seal.reviewId())||seal.actorId()==null||!seal.actorId().matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
                    ||!seal.files().keySet().equals(Set.copyOf(FILES)))
                throw new IllegalArgumentException();
            Instant.parse(seal.archivedAt());
            for(String file:FILES){
                FileSeal entry=seal.files().get(file);
                if(entry.bytes()<1||entry.bytes()>limit(file)||!entry.sha256().matches("[0-9a-f]{64}"))
                    throw new IllegalArgumentException();
            }
            return seal;
        }catch(Exception invalid){
            throw failure("VALIDATION_ARCHIVE_DESCRIPTOR_CHANGED");
        }
    }

    private static byte[] bounded(Path path,long maximum)throws Exception{
        if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw failure("VALIDATION_ARCHIVE_FILE_INVALID");
        try(InputStream input=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)){
            byte[] bytes=input.readNBytes((int)maximum+1);
            if(bytes.length==0||bytes.length>maximum)throw failure("VALIDATION_ARCHIVE_FILE_INVALID");
            return bytes;
        }
    }

    private static void installMetadata(Path directory,String filename,Object metadata)throws Exception{
        Path stage=Files.createTempFile(directory,"archive-seal-",".tmp");
        try{
            ValidationStore.owner(stage,false);
            try(FileChannel channel=FileChannel.open(stage,StandardOpenOption.WRITE)){
                ByteBuffer bytes=ByteBuffer.wrap(Model.JSON.writeValueAsBytes(metadata));
                while(bytes.hasRemaining())channel.write(bytes);
                channel.force(true);
            }
            Files.move(stage,directory.resolve(filename),StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(directory);
        }finally{
            Files.deleteIfExists(stage);
        }
    }

    private static Path privateRoot(Path configured)throws Exception{
        if(configured==null)throw failure("VALIDATION_ARCHIVE_DISABLED");
        if(!Files.isDirectory(configured,LinkOption.NOFOLLOW_LINKS))throw failure("VALIDATION_ARCHIVE_PATH_INVALID");
        Path canonical=configured.toRealPath();
        try{
            if(!PRIVATE.containsAll(Files.getPosixFilePermissions(canonical,LinkOption.NOFOLLOW_LINKS)))
                throw failure("VALIDATION_ARCHIVE_PATH_INVALID");
        }catch(UnsupportedOperationException ignored){
            // The operator's ACL protects platforms without POSIX permissions.
        }
        return canonical;
    }

    private static Path location(Path root,Seal seal){return location(root,seal.scope(),seal.archiveId());}
    private static Path location(Path root,Scope scope,String id){
        return root.resolve("report-"+Codec.sha256(Codec.encode(List.of(scope,id))));
    }
    private static long limit(String file){return ROWS.equals(file)?MAX_ROWS_BYTES:MAX_SUMMARY_BYTES;}
    private static boolean id(String value){return value!=null&&value.matches("[A-Za-z0-9_-]{8,128}");}
    private static SyncFailure failure(String code){return new SyncFailure(code,"VALIDATE",false,"Validation archive could not complete.");}

    private static FileLock acquire(FileChannel channel)throws Exception{
        try{
            FileLock lock=channel.tryLock();
            if(lock!=null)return lock;
        }catch(OverlappingFileLockException ignored){
            // File locks also cover other processes using the report.
        }
        throw failure("VALIDATION_REPORT_BUSY");
    }
    private static void forceDirectory(Path path)throws Exception{
        try(FileChannel directory=FileChannel.open(path,StandardOpenOption.READ)){
            directory.force(true);
        }catch(AccessDeniedException|UnsupportedOperationException ignored){
            // The copied data and descriptor have already been forced.
        }
    }
}
