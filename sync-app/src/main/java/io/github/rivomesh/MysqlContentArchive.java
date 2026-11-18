package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Publishes explicitly requested raw evidence in an operator-owned private POSIX directory. */
final class MysqlContentArchive {
    private static final Set<PosixFilePermission> DIRECTORY_MODE=PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> FILE_MODE=PosixFilePermissions.fromString("rw-------");

    private MysqlContentArchive() {}

    static Map<String,String> retain(Path location,Map<String,Object> report,byte[] frames)throws IOException{
        Path directory=location.toAbsolutePath().normalize();
        for(Path component=directory;component!=null;component=component.getParent())
            if(Files.isSymbolicLink(component))throw new IOException("MYSQL_BINLOG_EVIDENCE_DIRECTORY_INVALID");
        if(!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS)||!DIRECTORY_MODE.equals(Files.getPosixFilePermissions(directory)))
            throw new IOException("MYSQL_BINLOG_EVIDENCE_DIRECTORY_NOT_PRIVATE");
        Map<String,Object> archive=new LinkedHashMap<>();
        archive.put("formatVersion",1);
        archive.put("review",report);
        archive.put("base64Frames",Base64.getEncoder().encodeToString(frames));
        String content=Codec.encode(archive);
        byte[] bytes=content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String filename=Codec.sha256(content)+".json";
        Path destination=directory.resolve(filename),temp=Files.createTempFile(directory,".capture-",".tmp",PosixFilePermissions.asFileAttribute(FILE_MODE));
        try{
            try(FileChannel channel=FileChannel.open(temp,StandardOpenOption.WRITE)){
                ByteBuffer data=ByteBuffer.wrap(bytes);
                while(data.hasRemaining())channel.write(data);
                channel.force(true);
            }
            try{Files.createLink(destination,temp);}
            catch(FileAlreadyExistsException exists){
                if(!Files.isRegularFile(destination,LinkOption.NOFOLLOW_LINKS)||!FILE_MODE.equals(Files.getPosixFilePermissions(destination))
                        ||Files.size(destination)!=bytes.length||!Arrays.equals(Files.readAllBytes(destination),bytes))
                    throw new IOException("MYSQL_BINLOG_EVIDENCE_ARCHIVE_CONFLICT");
            }
            try(FileChannel channel=FileChannel.open(directory,StandardOpenOption.READ)){channel.force(true);}
            return Collections.singletonMap("file",filename);
        }finally{Files.deleteIfExists(temp);}
    }
}
