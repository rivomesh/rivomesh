package io.github.rivomesh.validation;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.SyncFailure;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/** Records an explicit minimum hold on validation evidence under the report's existing lock. */
public final class ValidationRetention {
    private static final String FILE="retention.json";
    public static final String ACTION="VALIDATION_RETENTION_REVIEW";
    public static final int MAXIMUM_DAYS=3650;

    private ValidationRetention(){}

    public record Request(String reportId,String expectedReviewId,int minimumDays,boolean approved){
        public void validate(){
            if(reportId==null||!ValidationStore.reportId(reportId)
                    ||expectedReviewId!=null&&!requestId(expectedReviewId)||!approved
                    ||minimumDays<1||minimumDays>MAXIMUM_DAYS)
                throw new IllegalArgumentException("VALIDATION_RETENTION_REVIEW_INVALID");
        }
    }

    public record Review(int version,String reportId,String requestId,String actorId,String previousReviewId,int minimumDays,
                         String reviewedAt,String minimumRetainUntil){}

    public static Review observe(Path root,String reportId)throws Exception{
        return read(ValidationStore.reportPath(root,reportId),reportId);
    }

    public static Review review(Path root,Request request,String requestId,String actorId,int policyMinimumDays)throws Exception{
        request.validate();
        if(!requestId(requestId)||actorId==null||!actorId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}"))
            throw new IllegalArgumentException("VALIDATION_RETENTION_REVIEW_INVALID");
        Path original=ValidationStore.reportPath(root,request.reportId());
        if(!Files.isDirectory(original,LinkOption.NOFOLLOW_LINKS))
            throw failure("VALIDATION_REPORT_NOT_CREATED");
        Path directory=original.toRealPath();
        if(!directory.equals(original.getParent().toRealPath().resolve(request.reportId()))
                ||!Files.isRegularFile(directory.resolve("validation.lock"),LinkOption.NOFOLLOW_LINKS))
            throw failure("VALIDATION_PATH_INVALID");
        ValidationStore.claimLocalWriter(directory);
        try(FileChannel channel=FileChannel.open(directory.resolve("validation.lock"),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
            FileLock lock=acquire(channel)){
            Review previous=read(directory,request.reportId());
            if(previous!=null&&requestId.equals(previous.requestId())){
                if(!actorId.equals(previous.actorId())||request.minimumDays()!=previous.minimumDays()
                        ||!Objects.equals(request.expectedReviewId(),previous.previousReviewId()))
                    throw failure("REQUEST_ID_REUSED");
                return previous;
            }
            if(policyMinimumDays==0)throw failure("VALIDATION_RETENTION_REVIEW_DISABLED");
            if(request.minimumDays()<policyMinimumDays)throw failure("VALIDATION_RETENTION_MINIMUM_NOT_MET");
            if(!Objects.equals(request.expectedReviewId(),previous==null?null:previous.requestId()))
                throw failure("VALIDATION_RETENTION_REVIEW_CHANGED");
            requireCompleted(directory,request.reportId());
            Instant now=Instant.now();
            Instant minimum=now.plus(request.minimumDays(),ChronoUnit.DAYS);
            if(previous!=null&&minimum.isBefore(Instant.parse(previous.minimumRetainUntil())))
                throw failure("VALIDATION_RETENTION_CANNOT_SHORTEN");
            Review next=new Review(1,request.reportId(),requestId,actorId,request.expectedReviewId(),
                    request.minimumDays(),now.toString(),minimum.toString());
            write(directory,next);
            return next;
        }finally{
            ValidationStore.releaseLocalWriter(directory);
        }
    }

    private static void requireCompleted(Path directory,String reportId)throws Exception{
        if(ValidationArchive.restoringRequired(directory,reportId))
            throw failure("VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED");
        if(!Files.isRegularFile(directory.resolve("rows.mv.db"),LinkOption.NOFOLLOW_LINKS))
            throw failure("VALIDATION_REPORT_NOT_CREATED");
        byte[] bytes=bounded(directory.resolve("summary.json"),1024*1024);
        if(bytes==null)throw failure("VALIDATION_RETENTION_REPORT_INCOMPLETE");
        JsonNode summary=Model.JSON.readTree(bytes);
        if(summary==null||!reportId.equals(summary.path("id").textValue())
                ||!"COMPLETE".equals(summary.path("state").textValue())||!summary.path("sourceComplete").asBoolean())
            throw failure("VALIDATION_RETENTION_REPORT_INCOMPLETE");
    }

    private static FileLock acquire(FileChannel channel)throws Exception{
        try{
            FileLock lock=channel.tryLock();
            if(lock!=null)return lock;
        }catch(OverlappingFileLockException busy){
            throw failure("VALIDATION_REPORT_BUSY");
        }
        throw failure("VALIDATION_REPORT_BUSY");
    }

    private static Review read(Path directory,String reportId)throws Exception{
        if(!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS))
            throw failure("VALIDATION_REPORT_NOT_CREATED");
        byte[] bytes=bounded(directory.resolve(FILE),4096);
        if(bytes==null)return null;
        try{
            Review review=Model.JSON.readValue(bytes,Review.class);
            if(review==null||review.version()!=1||!reportId.equals(review.reportId())||!requestId(review.requestId())
                    ||review.actorId()==null||!review.actorId().matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
                    ||review.previousReviewId()!=null&&(!requestId(review.previousReviewId())||review.requestId().equals(review.previousReviewId()))
                    ||review.minimumDays()<1||review.minimumDays()>MAXIMUM_DAYS
                    ||!Instant.parse(review.reviewedAt()).plus(review.minimumDays(),ChronoUnit.DAYS)
                        .equals(Instant.parse(review.minimumRetainUntil())))
                throw new IllegalArgumentException("VALIDATION_RETENTION_METADATA_INVALID");
            return review;
        }catch(Exception invalid){
            throw failure("VALIDATION_RETENTION_METADATA_INVALID");
        }
    }

    private static byte[] bounded(Path file,int maximum)throws Exception{
        if(!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)){
            if(!Files.exists(file,LinkOption.NOFOLLOW_LINKS))return null;
            throw failure("VALIDATION_RETENTION_METADATA_INVALID");
        }
        try(InputStream input=Files.newInputStream(file,LinkOption.NOFOLLOW_LINKS)){
            byte[] bytes=input.readNBytes(maximum+1);
            if(bytes.length==0||bytes.length>maximum)throw failure("VALIDATION_RETENTION_METADATA_INVALID");
            return bytes;
        }catch(NoSuchFileException absent){
            return null;
        }
    }

    private static void write(Path directory,Review review)throws Exception{
        Path temporary=Files.createTempFile(directory,"retention-",".tmp");
        try{
            ValidationStore.owner(temporary,false);
            ByteBuffer data=ByteBuffer.wrap(Model.JSON.writeValueAsBytes(review));
            try(FileChannel channel=FileChannel.open(temporary,StandardOpenOption.WRITE)){
                while(data.hasRemaining())channel.write(data);
                channel.force(true);
            }
            Files.move(temporary,directory.resolve(FILE),StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            try(FileChannel parent=FileChannel.open(directory,StandardOpenOption.READ)){
                parent.force(true);
            }catch(java.nio.file.AccessDeniedException|UnsupportedOperationException ignored){
                // File contents were forced before the atomic replacement.
            }
        }finally{
            Files.deleteIfExists(temporary);
        }
    }

    private static boolean requestId(String id){return id!=null&&id.matches("[A-Za-z0-9_-]{8,128}");}
    private static SyncFailure failure(String code){
        return new SyncFailure(code,"VALIDATE",false,"Validation retention review could not be recorded.");
    }
}
