package io.github.rivomesh;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.cdc.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.stream.Stream;

/** Publishes explicitly requested raw evidence in an operator-owned private POSIX directory. */
final class MysqlContentArchive {
    private static final int MAX_FILE=8*1024*1024;
    private static final long MAX_DIRECTORY=256L*1024*1024;
    private static final Set<PosixFilePermission> DIRECTORY_MODE=PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> FILE_MODE=PosixFilePermissions.fromString("rw-------");

    private MysqlContentArchive() {}

    static Path directory(Path location,boolean create)throws IOException{
        Path root=location.toAbsolutePath().normalize();
        for(Path component=root;component!=null;component=component.getParent())
            if(Files.isSymbolicLink(component))throw new IOException("MYSQL_BINLOG_EVIDENCE_DIRECTORY_INVALID");
        if(create)Files.createDirectories(root,PosixFilePermissions.asFileAttribute(DIRECTORY_MODE));
        if(!Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS)||!DIRECTORY_MODE.equals(Files.getPosixFilePermissions(root)))
            throw new IOException("MYSQL_BINLOG_EVIDENCE_DIRECTORY_NOT_PRIVATE");
        return root;
    }

    static Map<String,Object> existing(Path location,String reference,String pipelineId,String checkpointHash,
                                       String boundaryHash,long errorGeneration)throws IOException{
        return existing(location,reference,pipelineId,checkpointHash,boundaryHash,errorGeneration,null);
    }
    static Map<String,Object> existing(Path location,String reference,String pipelineId,String checkpointHash,
                                       String boundaryHash,long errorGeneration,CaptureArchiveReceipt.Capture capture)throws IOException{
        Path path=location.resolve(reference+".json");
        if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS))return null;
        byte[] content=read(path,MAX_FILE);
        if(capture!=null)CaptureArchiveReceipt.verifyExpected(location,capture,path.getFileName().toString(),content);
        JsonNode archive=Codec.JSON.readTree(content),review=archive.path("review");
        JsonNode proof=review.path("nextTransactionContents");
        byte[] frames;
        try{frames=Base64.getDecoder().decode(archive.path("base64Frames").asText());}
        catch(IllegalArgumentException invalid){throw new IOException("MYSQL_BINLOG_EVIDENCE_ARCHIVE_CONFLICT");}
        if(archive.path("formatVersion").asInt()!=1||!pipelineId.equals(review.path("taskId").asText())
                ||!checkpointHash.equals(review.path("checkpointHash").asText())
                ||!boundaryHash.equals(proof.path("boundaryHash").asText())||errorGeneration!=review.path("errorGeneration").asLong(-1)
                ||frames.length<1||!Integer.toString(frames.length).equals(proof.path("bytes").asText())
                ||!Codec.sha256(frames).equals(proof.path("contentSha256").asText()))
            throw new IOException("MYSQL_BINLOG_EVIDENCE_ARCHIVE_CONFLICT");
        @SuppressWarnings("unchecked") Map<String,Object> result=Codec.JSON.convertValue(review,Map.class);
        result.put("retainedEvidence",receipt(path,content));
        return result;
    }

    static Map<String,String> retain(Path location,Map<String,Object> report,byte[] frames,String reference,
                                     CaptureArchiveReceipt.Capture capture)throws IOException{
        Path root=directory(location,false);
        Map<String,Object> archive=new LinkedHashMap<>();
        archive.put("formatVersion",1);
        archive.put("review",report);
        archive.put("base64Frames",Base64.getEncoder().encodeToString(frames));
        byte[] bytes=Codec.encode(archive).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if(bytes.length>MAX_FILE)throw new IOException("MYSQL_BINLOG_EVIDENCE_CAPACITY");
        Path destination=root.resolve((reference==null?Codec.sha256(bytes):reference)+".json");
        Path lock=root.resolve(".archive-lock");
        // The lock serializes archive capacity checks and publication across CLI and owner processes.
        if(!Files.exists(lock,LinkOption.NOFOLLOW_LINKS))try{Files.createFile(lock,PosixFilePermissions.asFileAttribute(FILE_MODE));}
        catch(FileAlreadyExistsException concurrent){/* Another process created the lock. */}
        try(FileChannel authority=FileChannel.open(lock,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
            FileLock ignored=authority.lock()){
            if(Files.exists(destination,LinkOption.NOFOLLOW_LINKS)){
                if(!Arrays.equals(read(destination,MAX_FILE),bytes))throw new IOException("MYSQL_BINLOG_EVIDENCE_ARCHIVE_CONFLICT");
                if(capture!=null)CaptureArchiveReceipt.verifyExpected(root,capture,destination.getFileName().toString(),bytes);
                return receipt(destination,bytes);
            }
            String locator=destination.getFileName().toString();
            long required=bytes.length;
            if(capture!=null&&Files.notExists(CaptureArchiveReceipt.path(root,locator),LinkOption.NOFOLLOW_LINKS))
                required+=CaptureArchiveReceipt.MAX_BYTES+CaptureArchiveReceipt.KEY_BYTES;
            long used=0;
            try(Stream<Path> files=Files.list(root)){
                Iterator<Path> iterator=files.iterator();
                while(iterator.hasNext()){
                    Path file=iterator.next();
                    if(file.getFileName().toString().matches("[.]capture-[0-9]+[.]tmp")
                            &&Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)&&FILE_MODE.equals(Files.getPosixFilePermissions(file))){
                        Files.delete(file);
                        continue;
                    }
                    if(Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))used=Math.addExact(used,Files.size(file));
                    if(used>MAX_DIRECTORY-required)throw new IOException("MYSQL_BINLOG_EVIDENCE_CAPACITY");
                }
            }
            if(capture!=null)CaptureArchiveReceipt.retain(root,capture,locator,bytes);
            publish(destination,bytes);
            return receipt(destination,bytes);
        }
    }

    /** Atomically installs immutable private bytes while the caller holds the directory lock. */
    static void publish(Path destination,byte[] bytes)throws IOException{
        Path root=destination.getParent();
        Path temp=Files.createTempFile(root,".capture-",".tmp",PosixFilePermissions.asFileAttribute(FILE_MODE));
        try{
            try(FileChannel channel=FileChannel.open(temp,StandardOpenOption.WRITE)){
                ByteBuffer data=ByteBuffer.wrap(bytes);
                while(data.hasRemaining())channel.write(data);
                channel.force(true);
            }
            Files.createLink(destination,temp);
            try(FileChannel channel=FileChannel.open(root,StandardOpenOption.READ)){channel.force(true);}
        }finally{Files.deleteIfExists(temp);}
    }

    private static Map<String,String> receipt(Path file,byte[] content){
        Map<String,String> result=new LinkedHashMap<>();
        result.put("file",file.getFileName().toString());
        result.put("archiveSha256",Codec.sha256(content));
        return result;
    }

    static byte[] read(Path path,int limit)throws IOException{
        if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)||!FILE_MODE.equals(Files.getPosixFilePermissions(path)))
            throw new IOException("MYSQL_BINLOG_EVIDENCE_ARCHIVE_CONFLICT");
        try(InputStream input=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS);ByteArrayOutputStream out=new ByteArrayOutputStream()){
            byte[] buffer=new byte[8192];int count;
            while((count=input.read(buffer))!=-1){
                if(out.size()>limit-count)throw new IOException("MYSQL_BINLOG_EVIDENCE_ARCHIVE_CONFLICT");
                out.write(buffer,0,count);
            }
            return out.toByteArray();
        }
    }
}
