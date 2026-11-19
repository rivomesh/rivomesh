package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/** Immutable config documents and hash-linked metadata; the registry head is the only commit point. */
final class ConfigurationVersions {
    static final class Version {
        public String id,taskId,parent,parentHash,documentHash,action,actorId,actorRole,requestId,createdAt;
    }
    static final class Stored {final Version version;final String hash,yaml;final Path document;Stored(Version v,String hash,String yaml,Path document){version=v;this.hash=hash;this.yaml=yaml;this.document=document;}}
    private final Path directory;
    ConfigurationVersions(Path data)throws IOException{directory=data.resolve("config-versions");PrivateFiles.directory(directory);}
    private Path task(String id)throws IOException{if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw new IOException("CONFIG_VERSION_INVALID");Path task=directory.resolve(id);PrivateFiles.directory(task);return task;}
    private static void version(String id)throws IOException{if(id==null||!id.matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"))throw new IOException("CONFIG_VERSION_INVALID");}
    Stored create(String task,String yaml,String action,String request,AccessControl.Principal actor,String parent,String parentHash)throws Exception{
        byte[] bytes=yaml.getBytes(StandardCharsets.UTF_8);if(bytes.length>1024*1024)throw new IllegalArgumentException("CONFIG_TOO_LARGE");if(parent!=null){version(parent);if(parentHash==null||!parentHash.matches("[a-f0-9]{64}"))throw new IOException("CONFIG_VERSION_INVALID");}
        Version metadata=new Version();metadata.id=UUID.randomUUID().toString();metadata.taskId=task;metadata.parent=parent;metadata.parentHash=parentHash;metadata.documentHash=ConfigurationDocuments.hash(bytes);metadata.action=action;metadata.requestId=request;metadata.createdAt=Instant.now().toString();if(actor!=null){metadata.actorId=actor.id;metadata.actorRole=actor.role.name();}
        Path base=task(task),document=base.resolve(metadata.id+".yml"),manifest=base.resolve(metadata.id+".json");if(Files.exists(document)||Files.exists(manifest))throw new IOException("CONFIG_VERSION_EXISTS");PrivateFiles.write(document,bytes);byte[] encoded=Model.JSON.writeValueAsBytes(metadata);PrivateFiles.write(manifest,encoded);return new Stored(metadata,ConfigurationDocuments.hash(encoded),yaml,document);
    }
    Stored importHandoff(String task,String id,String yaml,String preparation,long committedAt)throws Exception{
        version(id);
        byte[] bytes=yaml.getBytes(StandardCharsets.UTF_8);
        if(bytes.length>1024*1024)throw new IllegalArgumentException("CONFIG_TOO_LARGE");
        Version metadata=new Version();metadata.id=id;metadata.taskId=task;metadata.documentHash=ConfigurationDocuments.hash(bytes);
        metadata.action="AGENT_HANDOFF";metadata.requestId=preparation;metadata.createdAt=Instant.ofEpochMilli(committedAt).toString();
        byte[] encoded=Model.JSON.writeValueAsBytes(metadata);
        Path base=task(task),document=base.resolve(id+".yml"),manifest=base.resolve(id+".json");
        boolean documentExists=matchesOrMissing(document,bytes,1024*1024),manifestExists=matchesOrMissing(manifest,encoded,16384);
        if(!documentExists)PrivateFiles.write(document,bytes);
        if(!manifestExists)PrivateFiles.write(manifest,encoded);
        return new Stored(metadata,ConfigurationDocuments.hash(encoded),yaml,document);
    }
    private static boolean matchesOrMissing(Path file,byte[] expected,int limit)throws IOException{
        if(!Files.exists(file,LinkOption.NOFOLLOW_LINKS))return false;
        if(!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)||!Arrays.equals(expected,PrivateFiles.read(file,limit)))
            throw new IOException("HANDOFF_RECIPIENT_LOCAL_CONFIGURATION_CHANGED");
        return true;
    }
    Stored read(String task,String id,String expected)throws Exception{
        version(id);Path base=task(task),manifest=base.resolve(id+".json"),document=base.resolve(id+".yml");if(Files.isSymbolicLink(manifest)||Files.isSymbolicLink(document))throw new IOException("CONFIG_VERSION_CORRUPT");byte[] encoded=PrivateFiles.read(manifest,16384);String hash=ConfigurationDocuments.hash(encoded);if(expected!=null&&!expected.equals(hash))throw new IOException("CONFIG_VERSION_CORRUPT");Version metadata=Model.JSON.readValue(encoded,Version.class);byte[] bytes=PrivateFiles.read(document,1024*1024);
        if(!task.equals(metadata.taskId)||!id.equals(metadata.id)||!ConfigurationDocuments.hash(bytes).equals(metadata.documentHash)||metadata.createdAt==null||metadata.action==null||metadata.parent==null&&metadata.parentHash!=null||metadata.parent!=null&&(metadata.parentHash==null||!metadata.parentHash.matches("[a-f0-9]{64}")))throw new IOException("CONFIG_VERSION_CORRUPT");if(metadata.parent!=null)version(metadata.parent);return new Stored(metadata,hash,new String(bytes,StandardCharsets.UTF_8),document);
    }
}
