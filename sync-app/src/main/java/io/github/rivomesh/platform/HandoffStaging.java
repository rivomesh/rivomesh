package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.validation.ValidationArchive;
import io.github.rivomesh.validation.ValidationStore;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Candidate-side custody only. Nothing here is a task directory, a worker input, or a command registry. */
final class HandoffStaging {
    private static final String RECEIPTS="closed-receipts.json",REPORTS="reports",SEAL="evidence.seal",
            SUMMARY="summary.json",ROWS="rows.mv.db",LOCK="validation.lock",FAILURES="failure-evidence/";
    private static final String STAGED_ROWS=ValidationStore.STAGED_ROWS_FILE,STAGED_SUMMARY="summary.staged.json";
    private static final long MAX_BYTES=64L*1024*1024,MAX_ARCHIVE=MAX_BYTES+1024*1024;
    private static final int MAX_STAGES=16,MAX_SEAL=64*1024,MAX_SUMMARY=1024*1024,MAX_RECEIPTS=128*1024;
    private static final String UUID_PATTERN="[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}";
    record Binding(String preparation,String evidence,HandoffPreparations.Binding candidate,
                   SharedHandoffEvidence.Scope scope,SharedHandoffEvidence.Archive archive){
        Binding{
            if(preparation==null||!preparation.matches("[A-Za-z0-9_-]{8,128}")||evidence==null||!evidence.matches("[a-f0-9]{64}")
                    ||candidate==null||candidate.epoch()<1||scope==null||!Objects.equals(candidate.directory(),scope.candidateDirectory())
                    ||archive==null||archive.closedReceipts()==0)throw refusal("HANDOFF_STAGING_SCOPE_CHANGED");
        }
        String key(){return Codec.sha256(Codec.encode(List.of(scope.cluster(),scope.task(),preparation)));}
    }
    record FileSeal(long bytes,String sha256){}
    record Stage(int version,Binding binding,Map<String,FileSeal> files){}
    record Verified(Path directory,Stage manifest,String sealSha256){}

    private HandoffStaging(){}

    @FunctionalInterface interface VerifiedStage<T>{ T accept(String sealSha256)throws Exception; }
    @FunctionalInterface interface VerifiedFiles<T>{ T accept(Verified verified)throws Exception; }

    static <T> T withVerifiedStage(Path state,Binding binding,ControlCipher cipher,VerifiedStage<T> use)throws Exception{
        return withVerifiedFiles(state,binding,cipher,verified->use.accept(verified.sealSha256()));
    }
    static synchronized <T> T withVerifiedFiles(Path state,Binding binding,ControlCipher cipher,VerifiedFiles<T> use)throws Exception{
        Path root=state.toRealPath().resolve("handoff-staging");
        secureDirectory(root);
        Path lockFile=root.resolve("staging.lock");
        secureFile(lockFile);
        try(FileChannel channel=FileChannel.open(lockFile,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
            FileLock lock=channel.tryLock()){
            if(lock==null)throw refusal("HANDOFF_STAGING_BUSY");
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
            return use.accept(verifyStage(root.resolve(binding.key()),binding,cipher,deadline));
        }catch(OverlappingFileLockException busy){throw refusal("HANDOFF_STAGING_BUSY");}
    }

    static synchronized Map<String,Object> stage(Path data,Path shared,Binding binding,ControlCipher cipher)throws Exception{
        Path root=data.toRealPath().resolve("handoff-staging");
        if(Files.notExists(root,LinkOption.NOFOLLOW_LINKS)){
            createDirectory(root);
            force(data.toRealPath());
        }
        secureDirectory(root);
        Path lockPath=root.resolve("staging.lock");
        if(!Files.notExists(lockPath,LinkOption.NOFOLLOW_LINKS))secureFile(lockPath);
        try(FileChannel channel=FileChannel.open(lockPath,Set.of(StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            FileLock lock=channel.tryLock()){
            if(lock==null)throw refusal("HANDOFF_STAGING_BUSY");
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
            String key=binding.key();
            Path complete=root.resolve(key),pending=root.resolve(key+".pending");
            boolean replay=Files.exists(complete,LinkOption.NOFOLLOW_LINKS);
            if(replay){
                verifyStage(complete,binding,cipher,deadline);
            }else{
                if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS)){
                    secureDirectory(pending);
                    if(Files.exists(pending.resolve(SEAL),LinkOption.NOFOLLOW_LINKS)){
                        verifyStage(pending,binding,cipher,deadline);
                    }else{
                        clearPartial(pending);
                    }
                }
                if(Files.notExists(pending,LinkOption.NOFOLLOW_LINKS)){
                    try(var children=Files.list(root)){
                        if(children.limit(MAX_STAGES+1L).count()>MAX_STAGES)
                            throw refusal("HANDOFF_STAGING_CAPACITY");
                    }
                    createDirectory(pending);
                    Path source=SharedHandoffEvidence.stagingSource(shared,binding.scope(),binding.archive());
                    Map<String,FileSeal> files=extract(source,pending,binding,deadline);
                    Stage manifest=new Stage(1,binding,files);
                    checkFiles(pending,manifest,deadline);
                    String envelope=cipher.seal("handoff-stage/"+key,Model.JSON.writeValueAsString(manifest));
                    writeFile(pending.resolve("seal.part"),new ByteArrayInputStream(envelope.getBytes(java.nio.charset.StandardCharsets.UTF_8)),MAX_SEAL,deadline);
                    Files.move(pending.resolve("seal.part"),pending.resolve(SEAL),StandardCopyOption.ATOMIC_MOVE);
                    force(pending);
                }
                Files.move(pending,complete,StandardCopyOption.ATOMIC_MOVE);
                force(root);
            }
            return Map.of("state","STAGED","preparationId",binding.preparation(),"taskRevision",binding.scope().revision(),
                    "candidateNodeId",binding.candidate().node(),"evidenceSha256",binding.evidence(),"archiveSha256",binding.archive().sha256(),
                    "reports",binding.archive().reports(),"closedReceipts",binding.archive().closedReceipts(),"executionAuthorized",false);
        }catch(OverlappingFileLockException busy){throw refusal("HANDOFF_STAGING_BUSY");}
    }

    private static Map<String,FileSeal> extract(Path source,Path pending,Binding binding,long deadline)throws Exception{
        Map<String,FileSeal> files=new TreeMap<>();
        try(FileChannel file=FileChannel.open(source,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){
            if(file.size()!=binding.archive().bytes())throw refusal("HANDOFF_ARCHIVE_BYTES_CHANGED");
            MessageDigest hash=MessageDigest.getInstance("SHA-256");
            try(DigestInputStream raw=new DigestInputStream(Channels.newInputStream(file),hash);
                ZipInputStream zip=new ZipInputStream(raw)){
                expect(zip.getNextEntry(),"scope.json");
                if(!binding.scope().equals(Model.JSON.readValue(readEntry(zip,4096),SharedHandoffEvidence.Scope.class)))
                    throw refusal("HANDOFF_ARCHIVE_SCOPE_CHANGED");
                expect(zip.getNextEntry(),RECEIPTS);
                byte[] receipts=readEntry(zip,MAX_RECEIPTS);
                verifyReceipts(receipts,binding.archive().closedReceipts());
                files.put(RECEIPTS,writeFile(pending.resolve(RECEIPTS),new ByteArrayInputStream(receipts),MAX_RECEIPTS,deadline));
                long remaining=MAX_BYTES;
                if(binding.archive().failureRecords()>0){
                    expect(zip.getNextEntry(),FAILURES+"authority.json");
                    remaining-=readEntry(zip,4096).length;
                    String prior="";
                    for(int count=0;count<binding.archive().failureRecords();count++){
                        ZipEntry entry=zip.getNextEntry();
                        String name=entry==null?"":entry.getName();
                        if(!name.matches(FAILURES+UUID_PATTERN+"\\.json")||name.compareTo(prior)<=0)
                            throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
                        remaining-=stream(zip,null,Math.min(remaining,8L*1024*1024),deadline).bytes();
                        prior=name;
                    }
                }
                if(binding.archive().reports()>0)createDirectory(pending.resolve(REPORTS));
                String previous="";
                for(int count=0;count<binding.archive().reports();count++){
                    ZipEntry entry=zip.getNextEntry();
                    String name=entry==null?"":entry.getName();
                    if(!name.matches(UUID_PATTERN+"/"+SUMMARY.replace(".","\\.")))
                        throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
                    String id=name.substring(0,36);
                    if(id.compareTo(previous)<=0)throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
                    previous=id;
                    Path report=pending.resolve(REPORTS).resolve(id);
                    createDirectory(report);
                    Files.createFile(report.resolve(LOCK),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
                    byte[] summary=readEntry(zip,MAX_SUMMARY);
                    remaining-=summary.length;
                    verifySummary(summary,id);
                    String prefix=REPORTS+"/"+id+"/";
                    files.put(prefix+STAGED_SUMMARY,writeFile(report.resolve(STAGED_SUMMARY),new ByteArrayInputStream(summary),MAX_SUMMARY,deadline));
                    expect(zip.getNextEntry(),id+"/"+ROWS);
                    FileSeal rows=writeFile(report.resolve(STAGED_ROWS),zip,remaining,deadline);
                    remaining-=rows.bytes();
                    files.put(prefix+STAGED_ROWS,rows);
                    force(report);
                }
                if(zip.getNextEntry()!=null)throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
                byte[] tail=new byte[8192];
                while(raw.read(tail)!=-1){
                    checkDeadline(deadline);
                    if(file.position()>MAX_ARCHIVE)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
                }
                if(file.position()!=binding.archive().bytes()||!binding.archive().sha256().equals(HexFormat.of().formatHex(hash.digest())))
                    throw refusal("HANDOFF_ARCHIVE_BYTES_CHANGED");
                if(binding.archive().reports()>0)force(pending.resolve(REPORTS));
                return files;
            }
        }
    }

    private static Verified verifyStage(Path stage,Binding expected,ControlCipher cipher,long deadline)throws Exception{
        secureDirectory(stage);
        Path seal=stage.resolve(SEAL);
        byte[] sealedBytes=readFile(seal,MAX_SEAL);
        String envelope=new String(sealedBytes,java.nio.charset.StandardCharsets.UTF_8);
        Stage stored=Model.JSON.readValue(cipher.open("handoff-stage/"+expected.key(),envelope),Stage.class);
        if(stored==null||stored.version()!=1||!expected.equals(stored.binding()))throw refusal("HANDOFF_STAGING_SCOPE_CHANGED");
        checkFiles(stage,stored,deadline);
        return new Verified(stage,stored,Codec.sha256(sealedBytes));
    }

    private static void checkFiles(Path stage,Stage manifest,long deadline)throws Exception{
        SharedHandoffEvidence.Archive archive=manifest.binding().archive();
        Map<String,FileSeal> files=manifest.files();
        if(files==null||files.size()!=1+2*archive.reports()||!files.containsKey(RECEIPTS))
            throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
        checkFile(stage.resolve(RECEIPTS),files.get(RECEIPTS),MAX_RECEIPTS,deadline);
        verifyReceipts(readFile(stage.resolve(RECEIPTS),MAX_RECEIPTS),archive.closedReceipts());
        Path reports=stage.resolve(REPORTS);
        if(archive.reports()>0){
            secureDirectory(reports);
            int observed=0;
            try(var children=Files.newDirectoryStream(reports)){
                for(Path report:children){
                    String id=report.getFileName().toString();
                    if(!id.matches(UUID_PATTERN)||++observed>archive.reports())throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
                    secureDirectory(report);
                    Path lockPath=report.resolve(LOCK);secureFile(lockPath);
                    try(ValidationStore.ReportReservation reservation=ValidationStore.reserveReportDirectory(report);
                        FileChannel channel=FileChannel.open(lockPath,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
                        FileLock lock=channel.tryLock()){
                        if(lock==null)throw refusal("HANDOFF_STAGING_REPORT_BUSY");
                        if(ValidationArchive.restoringRequired(report,id))throw refusal("HANDOFF_STAGING_REPORT_ARCHIVED");
                        exactNames(report,Set.of(STAGED_SUMMARY,STAGED_ROWS,LOCK));
                        String prefix=REPORTS+"/"+id+"/";
                        checkFile(report.resolve(STAGED_ROWS),files.get(prefix+STAGED_ROWS),MAX_BYTES,deadline);
                        checkFile(report.resolve(STAGED_SUMMARY),files.get(prefix+STAGED_SUMMARY),MAX_SUMMARY,deadline);
                        JsonNode summary=verifySummary(readFile(report.resolve(STAGED_SUMMARY),MAX_SUMMARY),id);
                        ValidationStore.verifyStagedReport(report,id,summary.path("code").textValue());
                        checkFile(report.resolve(STAGED_ROWS),files.get(prefix+STAGED_ROWS),MAX_BYTES,deadline);
                    }catch(io.github.rivomesh.SyncFailure|OverlappingFileLockException rejected){
                        throw refusal("HANDOFF_STAGING_REPORT_UNVERIFIED");
                    }
                }
            }
            if(observed!=archive.reports())throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
        }
        Set<String> names=new HashSet<>(Set.of(RECEIPTS));
        if(archive.reports()>0)names.add(REPORTS);
        if(Files.exists(stage.resolve(SEAL),LinkOption.NOFOLLOW_LINKS))names.add(SEAL);
        exactNames(stage,names);
    }

    private static void clearPartial(Path pending)throws Exception{
        try(var children=Files.newDirectoryStream(pending)){
            for(Path child:children){
                String name=child.getFileName().toString();
                if(REPORTS.equals(name)){
                    secureDirectory(child);
                    try(var reports=Files.newDirectoryStream(child)){
                        for(Path report:reports){
                            if(!report.getFileName().toString().matches(UUID_PATTERN))throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
                            secureDirectory(report);
                            clearPartialReport(report);
                        }
                    }
                    Files.delete(child);
                }else removePartial(child,Set.of(RECEIPTS,"seal.part"));
            }
        }
        Files.delete(pending);
    }
    private static void clearPartialReport(Path report)throws Exception{
        Path lockPath=report.resolve(LOCK);
        try(ValidationStore.ReportReservation reservation=ValidationStore.reserveReportDirectory(report)){
            if(Files.exists(lockPath,LinkOption.NOFOLLOW_LINKS)){
                secureFile(lockPath);
                try(FileChannel channel=FileChannel.open(lockPath,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
                    FileLock lock=channel.tryLock()){
                    if(lock==null)throw refusal("HANDOFF_STAGING_REPORT_BUSY");
                    try(var artifacts=Files.newDirectoryStream(report)){
                        for(Path artifact:artifacts)if(!artifact.equals(lockPath))
                            removePartial(artifact,Set.of(STAGED_ROWS,STAGED_SUMMARY));
                    }
                }
                Files.delete(lockPath);
            }
            Files.delete(report);
        }catch(io.github.rivomesh.SyncFailure|OverlappingFileLockException busy){
            throw refusal("HANDOFF_STAGING_REPORT_BUSY");
        }
    }
    private static void removePartial(Path path,Set<String> names)throws Exception{
        if(!names.contains(path.getFileName().toString()))throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
        secureFile(path);Files.delete(path);
    }
    static void exactNames(Path path,Set<String> names)throws Exception{
        Set<String> observed=new HashSet<>();
        try(var children=Files.newDirectoryStream(path)){
            for(Path child:children){
                String name=child.getFileName().toString();
                if(!names.contains(name))throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
                observed.add(name);
            }
        }
        if(!names.equals(observed))throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
    }
    private static void verifyReceipts(byte[] payload,int expected)throws Exception{
        SharedHandoffEvidence.ClosedReceipt[] receipts=Model.JSON.readerFor(SharedHandoffEvidence.ClosedReceipt[].class)
                .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readValue(payload);
        if(receipts==null||receipts.length!=expected||Arrays.stream(receipts).anyMatch(receipt->receipt==null
                ||!receipt.actor().matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}"))
                ||Arrays.stream(receipts).map(SharedHandoffEvidence.ClosedReceipt::request).distinct().count()!=expected)
            throw refusal("HANDOFF_CLOSED_RECEIPT_INVALID");
    }
    private static JsonNode verifySummary(byte[] payload,String id)throws Exception{
        JsonNode summary=Model.JSON.reader().with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(payload);
        if(summary==null||!id.equals(summary.path("id").textValue())||!"COMPLETE".equals(summary.path("state").textValue())
                ||!summary.path("sourceComplete").isBoolean()||!summary.path("sourceComplete").booleanValue())
            throw refusal("HANDOFF_STAGING_REPORT_UNVERIFIED");
        return summary;
    }
    private static void expect(ZipEntry entry,String name){
        if(entry==null||entry.isDirectory()||!name.equals(entry.getName()))throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
    }
    private static byte[] readEntry(InputStream input,int maximum)throws Exception{
        byte[] bytes=input.readNBytes(maximum+1);
        if(bytes.length==0||bytes.length>maximum)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
        return bytes;
    }
    static byte[] readFile(Path path,int maximum)throws Exception{
        secureFile(path);
        try(InputStream input=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)){
            return readEntry(input,maximum);
        }
    }
    static void checkFile(Path path,FileSeal expected,long maximum,long deadline)throws Exception{
        secureFile(path);
        try(InputStream input=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)){
            if(expected==null||!expected.equals(stream(input,null,maximum,deadline)))
                throw refusal("HANDOFF_STAGING_BYTES_CHANGED");
        }
    }
    static FileSeal writeFile(Path path,InputStream input,long maximum,long deadline)throws Exception{
        try(FileChannel output=FileChannel.open(path,Set.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){
            FileSeal seal=stream(input,output,maximum,deadline);
            output.force(true);
            return seal;
        }
    }
    private static FileSeal stream(InputStream input,FileChannel output,long maximum,long deadline)throws Exception{
        MessageDigest hash=MessageDigest.getInstance("SHA-256");
        byte[] buffer=new byte[64*1024];
        long length=0;
        for(int count;(count=input.read(buffer))!=-1;){
            checkDeadline(deadline);
            length+=count;
            if(length>maximum)throw refusal("HANDOFF_ARCHIVE_LIMIT_EXCEEDED");
            hash.update(buffer,0,count);
            if(output!=null){
                ByteBuffer content=ByteBuffer.wrap(buffer,0,count);
                while(content.hasRemaining())output.write(content);
            }
        }
        if(length==0)throw refusal("HANDOFF_STAGING_ENTRY_INVALID");
        return new FileSeal(length,HexFormat.of().formatHex(hash.digest()));
    }
    static void createDirectory(Path path)throws IOException{
        Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    }
    static void secureDirectory(Path path)throws Exception{
        if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw refusal("HANDOFF_STAGING_PATH_UNSAFE");
        checkPermissions(path);
    }
    static void secureFile(Path path)throws Exception{
        if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw refusal("HANDOFF_STAGING_PATH_UNSAFE");
        checkPermissions(path);
    }
    private static void checkPermissions(Path path)throws IOException{
        if(Files.getPosixFilePermissions(path,LinkOption.NOFOLLOW_LINKS).stream().anyMatch(mode->!mode.name().startsWith("OWNER_")))
            throw refusal("HANDOFF_STAGING_PATH_UNSAFE");
    }
    private static void checkDeadline(long deadline){
        if(Thread.currentThread().isInterrupted()||System.nanoTime()>=deadline)throw refusal("HANDOFF_STAGING_TIMEOUT");
    }
    static void force(Path directory)throws IOException{
        try(FileChannel channel=FileChannel.open(directory,StandardOpenOption.READ)){channel.force(true);}
    }
    private static TaskService.Conflict refusal(String code){return new TaskService.Conflict(code);}
}
