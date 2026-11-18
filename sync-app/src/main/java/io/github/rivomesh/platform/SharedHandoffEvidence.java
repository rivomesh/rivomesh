package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.validation.ValidationArchive;
import io.github.rivomesh.validation.ValidationStore;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.*;

/** An opt-in copy of completed validation reports and authenticated receipts on a private shared mount. */
final class SharedHandoffEvidence {
    private static final int MAX_REPORTS=32,MAX_CHILDREN=256,MAX_SUMMARY=1024*1024;
    static final int MAX_RECEIPTS=64;
    private static final int MAX_RECEIPT_BYTES=128*1024;
    private static final long MAX_BYTES=64L*1024*1024;
    record Scope(String cluster,String task,String assessment,String revision,String configuration,
                 String originalDirectory,String candidateDirectory){}
    record ClosedReceipt(String request,String signature,String actor,String role,TaskRequest.Kind kind,
                         String phase,String code){
        ClosedReceipt{
            if(request==null||!request.matches("[A-Za-z0-9_-]{8,128}")||signature==null||!signature.matches("[a-f0-9]{64}")
                    ||actor==null||actor.length()>64||!Set.of("ADMIN","OPERATOR").contains(Objects.toString(role,""))
                    ||kind==null||!Set.of(TaskRequest.Kind.CREATE,TaskRequest.Kind.CONFIG_UPDATE,TaskRequest.Kind.RESTORE,
                    TaskRequest.Kind.ACTION).contains(kind)||!Set.of("SUCCEEDED","FAILED","CANCELLED").contains(Objects.toString(phase,""))
                    ||code==null||!code.matches("[A-Z0-9_]{1,120}"))throw refusal("HANDOFF_CLOSED_RECEIPT_INVALID");
        }
    }
    record ReceiptPlan(List<ClosedReceipt> receipts,String refusal){
        ReceiptPlan{
            if(receipts==null||receipts.size()>MAX_RECEIPTS||refusal!=null&&!receipts.isEmpty())
                throw SharedHandoffEvidence.refusal("HANDOFF_CLOSED_RECEIPT_INVALID");
            receipts=List.copyOf(receipts);
        }
    }
    record Archive(String sha256,long bytes,int reports,int closedReceipts){
        Archive{
            if(sha256==null||!sha256.matches("[0-9a-f]{64}")||bytes<1||bytes>MAX_BYTES+1024*1024
                    ||reports<0||reports>MAX_REPORTS||closedReceipts<0||closedReceipts>MAX_RECEIPTS
                    ||reports+closedReceipts==0)throw refusal("HANDOFF_ARCHIVE_DESCRIPTOR_INVALID");
        }
    }
    private SharedHandoffEvidence(){}

    static Archive export(Path configured,Scope scope,Path reports,List<ClosedReceipt> receipts)throws Exception{
        Path root=privateRoot(configured),destination=archive(root,scope);
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
        Path lockPath=root.resolve(destination.getFileName()+".lock");
        try(FileChannel channel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){
            PrivateFiles.ownerOnly(lockPath);
            try(FileLock lock=tryLock(channel)){
                if(Files.isSymbolicLink(destination))throw refusal("HANDOFF_ARCHIVE_PATH_UNSAFE");
                try(DirectoryStream<Path> temporaries=Files.newDirectoryStream(root,destination.getFileName()+".*.tmp")){
                    for(Path temporary:temporaries){
                        checkDeadline(deadline);
                        if(!Files.isRegularFile(temporary,LinkOption.NOFOLLOW_LINKS))
                            throw refusal("HANDOFF_ARCHIVE_PATH_UNSAFE");
                        Files.deleteIfExists(temporary);
                    }
                }
                Path stage=Files.createTempFile(root,destination.getFileName()+".",".tmp");
                try{
                    PrivateFiles.ownerOnly(stage);
                    int count=write(stage,scope,reports,receipts,deadline);
                    if(count==0&&receipts.isEmpty())throw refusal("HANDOFF_NO_SHAREABLE_REPORTS");
                    Archive proof=new Archive(hash(stage,deadline),Files.size(stage),count,receipts.size());
                    try(FileChannel file=FileChannel.open(stage,StandardOpenOption.WRITE)){file.force(true);}
                    Files.move(stage,destination,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
                    try(FileChannel directory=FileChannel.open(root,StandardOpenOption.READ)){directory.force(true);}
                    catch(AccessDeniedException|UnsupportedOperationException ignored){/* File data is already forced. */}
                    return proof;
                }finally{Files.deleteIfExists(stage);}
            }
        }
    }

    static void verify(Path configured,Scope expected,Archive proof)throws Exception{
        Path original=archive(privateRoot(configured),expected);
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
        if(!Files.isRegularFile(original,LinkOption.NOFOLLOW_LINKS))
            throw refusal("HANDOFF_ARCHIVE_MISSING");
        try(FileChannel file=FileChannel.open(original,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){
            if(file.size()!=proof.bytes()||!proof.sha256().equals(hash(Channels.newInputStream(file),deadline)))
                throw refusal("HANDOFF_ARCHIVE_BYTES_CHANGED");
            file.position(0);
            try(ZipInputStream zip=new ZipInputStream(Channels.newInputStream(file))){
                ZipEntry scope=zip.getNextEntry();
                if(scope==null||!"scope.json".equals(scope.getName()))throw refusal("HANDOFF_ARCHIVE_SCOPE_CHANGED");
                byte[] bytes=zip.readNBytes(4097);
                if(bytes.length<1||bytes.length>4096)throw refusal("HANDOFF_ARCHIVE_SCOPE_CHANGED");
                Scope observed=Model.JSON.readValue(bytes,Scope.class);
                if(!expected.equals(observed))throw refusal("HANDOFF_ARCHIVE_SCOPE_CHANGED");
                if(proof.closedReceipts()>0){
                    ZipEntry records=zip.getNextEntry();
                    if(records==null||!"closed-receipts.json".equals(records.getName()))
                        throw refusal("HANDOFF_CLOSED_RECEIPT_INVALID");
                    byte[] payload=zip.readNBytes(MAX_RECEIPT_BYTES+1);
                    if(payload.length>MAX_RECEIPT_BYTES)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
                    ClosedReceipt[] receipts=Model.JSON.readValue(payload,ClosedReceipt[].class);
                    if(receipts.length!=proof.closedReceipts()
                            ||Arrays.stream(receipts).map(ClosedReceipt::request).distinct().count()!=receipts.length)
                        throw refusal("HANDOFF_CLOSED_RECEIPT_INVALID");
                }
            }
        }
    }

    private static int write(Path stage,Scope scope,Path reports,List<ClosedReceipt> receipts,long deadline)throws Exception{
        rejectLinks(reports);
        List<Path> candidates=new ArrayList<>();
        if(Files.isDirectory(reports,LinkOption.NOFOLLOW_LINKS))try(DirectoryStream<Path> paths=Files.newDirectoryStream(reports)){
            int seen=0;
            for(Path path:paths){
                checkDeadline(deadline);
                if(++seen>MAX_CHILDREN)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
                if(path.getFileName().toString().matches("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"))
                    candidates.add(path);
            }
        }
        if(candidates.size()>MAX_REPORTS)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
        candidates.sort(Comparator.comparing(Path::getFileName));
        long remaining=MAX_BYTES;
        int archived=0;
        try(ZipOutputStream out=new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(stage)))){
            out.setLevel(0);
            entry(out,"scope.json",new ByteArrayInputStream(Model.JSON.writeValueAsBytes(scope)),4096,deadline);
            if(!receipts.isEmpty())entry(out,"closed-receipts.json",new ByteArrayInputStream(Model.JSON.writeValueAsBytes(receipts)),
                    MAX_RECEIPT_BYTES,deadline);
            for(Path directory:candidates){
                rejectLinks(directory);
                if(!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS))throw refusal("HANDOFF_REPORT_PATH_UNSAFE");
                try(ValidationStore.ReportReservation reservation=reserveReport(directory);
                    FileChannel channel=FileChannel.open(directory.resolve("validation.lock"),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
                    FileLock lock=tryLock(channel)){
                    String id=directory.getFileName().toString();
                    if(ValidationArchive.restoringRequired(directory,id))throw refusal("HANDOFF_REPORT_ARCHIVE_RESTORE_REQUIRED");
                    Path summary=directory.resolve("summary.json"),rows=directory.resolve("rows.mv.db");
                    byte[] bytes=summary(summary);
                    JsonNode report=Model.JSON.readTree(bytes);
                    if(!id.equals(report.path("id").textValue()))throw refusal("HANDOFF_REPORT_SCOPE_CHANGED");
                    if(!"COMPLETE".equals(report.path("state").textValue())||!report.path("sourceComplete").asBoolean())continue;
                    if(!Files.isRegularFile(rows,LinkOption.NOFOLLOW_LINKS))throw refusal("HANDOFF_REPORT_PATH_UNSAFE");
                    remaining-=entry(out,id+"/summary.json",new ByteArrayInputStream(bytes),remaining,deadline);
                    try(InputStream input=Files.newInputStream(rows,LinkOption.NOFOLLOW_LINKS)){
                        remaining-=entry(out,id+"/rows.mv.db",input,remaining,deadline);
                    }
                    archived++;
                }
            }
        }
        return archived;
    }
    private static byte[] summary(Path path)throws IOException{
        if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw refusal("HANDOFF_REPORT_PATH_UNSAFE");
        try(InputStream input=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)){
            byte[] bytes=input.readNBytes(MAX_SUMMARY+1);
            if(bytes.length==0||bytes.length>MAX_SUMMARY)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
            return bytes;
        }
    }
    private static long entry(ZipOutputStream destination,String name,InputStream input,long limit,long deadline)throws IOException{
        ZipEntry entry=new ZipEntry(name);entry.setTime(0);destination.putNextEntry(entry);
        long bytes=0;byte[] buffer=new byte[64*1024];
        for(int n;(n=input.read(buffer))!=-1;){
            checkDeadline(deadline);
            if(n>limit-bytes)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
            destination.write(buffer,0,n);bytes+=n;
        }
        destination.closeEntry();
        return bytes;
    }
    private static String hash(Path archive,long deadline)throws Exception{
        try(InputStream input=Files.newInputStream(archive,LinkOption.NOFOLLOW_LINKS)){
            return hash(input,deadline);
        }
    }
    private static String hash(InputStream input,long deadline)throws Exception{
        MessageDigest digest=MessageDigest.getInstance("SHA-256");
        long bytes=0;byte[] buffer=new byte[64*1024];
        for(int n;(n=input.read(buffer))!=-1;){
            checkDeadline(deadline);
            bytes+=n;if(bytes>MAX_BYTES+1024*1024)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
            digest.update(buffer,0,n);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
    private static Path archive(Path root,Scope scope){
        return root.resolve("handoff-"+Codec.sha256(Codec.encode(List.of(scope.cluster,scope.task,scope.originalDirectory)))+".zip");
    }
    private static Path privateRoot(Path root)throws IOException{
        if(root==null)throw refusal("HANDOFF_ARCHIVE_DISABLED");
        rejectLinks(root);
        if(!Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS))throw refusal("HANDOFF_ARCHIVE_MOUNT_MISSING");
        Set<PosixFilePermission> mode;
        try{mode=Files.getPosixFilePermissions(root,LinkOption.NOFOLLOW_LINKS);}
        catch(UnsupportedOperationException unsupported){throw refusal("HANDOFF_ARCHIVE_MOUNT_PERMISSIONS_UNVERIFIED");}
        if(mode.stream().anyMatch(permission->!permission.name().startsWith("OWNER_")))
            throw refusal("HANDOFF_ARCHIVE_MOUNT_PERMISSIONS_UNVERIFIED");
        return root;
    }
    private static void rejectLinks(Path directory){
        for(Path path=directory;path!=null;path=path.getParent())
            if(Files.isSymbolicLink(path))throw refusal("HANDOFF_ARCHIVE_PATH_UNSAFE");
    }
    private static FileLock tryLock(FileChannel channel)throws IOException{
        try{
            FileLock lock=channel.tryLock();
            if(lock!=null)return lock;
        }catch(OverlappingFileLockException ignored){/* In-process writer holds the same lock. */}
        throw refusal("HANDOFF_ARCHIVE_BUSY");
    }
    private static ValidationStore.ReportReservation reserveReport(Path directory)throws Exception{
        try{return ValidationStore.reserveReportDirectory(directory);}
        catch(io.github.rivomesh.SyncFailure busy){throw refusal("HANDOFF_ARCHIVE_BUSY");}
    }
    private static void checkDeadline(long deadline){
        if(Thread.currentThread().isInterrupted()||System.nanoTime()-deadline>=0)
            throw refusal("HANDOFF_ARCHIVE_TIMEOUT");
    }
    private static TaskService.Conflict refusal(String code){return new TaskService.Conflict(code);}
}
