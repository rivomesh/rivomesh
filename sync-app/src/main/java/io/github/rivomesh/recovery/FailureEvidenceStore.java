package io.github.rivomesh.recovery;

import io.github.rivomesh.cdc.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.*;
import java.util.*;
import java.util.stream.Stream;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Private immutable files; atomic publication under a cross-process quota lock. No automatic GC. */
public final class FailureEvidenceStore {
    private static final com.fasterxml.jackson.databind.ObjectMapper READER=Codec.JSON.copy()
            .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    public static final class Limits {
        public long maximumBytes=256L*1024*1024;
        public int maximumRecords=1000,maximumRecordBytes=8*1024*1024;
        void validate(){if(maximumBytes<1024||maximumRecords<1||maximumRecords>100000||maximumRecordBytes<1024||maximumRecordBytes>16*1024*1024)throw new IllegalArgumentException("FAILURE_EVIDENCE_LIMIT_INVALID");}
    }
    public static final class Envelope {public FailureEvidence value;public String hash,signature;}
    public static final class Authority {public int version=1;public FailureEvidence.Scope scope;public String storeId,signature;}
    private interface Locked<T>{T run(byte[] key)throws Exception;}
    @FunctionalInterface public interface CustodyCopy {void write(String name,byte[] bytes)throws IOException;}
    public record CustodyCount(int records,long bytes){}
    private final Path root;
    private final FailureEvidence.Scope scope;
    private final Limits limits;
    public FailureEvidenceStore(Path directory,FailureEvidence.Scope expected,Limits supplied)throws Exception{
        this(directory,expected,supplied,true);
    }
    private FailureEvidenceStore(Path directory,FailureEvidence.Scope expected,Limits supplied,boolean initialize)throws Exception{
        expected.validate();supplied.validate();scope=Codec.decode(Codec.encode(expected),FailureEvidence.Scope.class);limits=Codec.decode(Codec.encode(supplied),Limits.class);
        root=directory.toAbsolutePath().normalize();safePath(root);
        if(!initialize)return;
        try{Files.createDirectories(root,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}
        catch(UnsupportedOperationException unsupported){Files.createDirectories(root);}
        protect(root,true);locked(key->null);
    }
    /**
     * Copies an existing scoped authority and its verified, exact record bytes under the publication lock.
     * The caller stages the output and publishes it only after this call succeeds. No key or lock bytes are delivered.
     */
    public static CustodyCount copyVerified(Path directory,FailureEvidence.Scope expected,int maximumRecords,long maximumBytes,
                                   CustodyCopy copy)throws Exception{
        if(maximumRecords<1||maximumRecords>1000||maximumBytes<1)
            throw fail("FAILURE_EVIDENCE_LIMIT_INVALID");
        FailureEvidenceStore store=new FailureEvidenceStore(directory,expected,new Limits(),false);
        if(Files.notExists(store.root,LinkOption.NOFOLLOW_LINKS))return new CustodyCount(0,0);
        if(!Files.isDirectory(store.root,LinkOption.NOFOLLOW_LINKS))
            throw fail("FAILURE_EVIDENCE_PATH_INVALID");
        requirePrivate(store.root);
        Path lockPath=store.root.resolve("evidence.lock");
        if(!Files.isRegularFile(lockPath,LinkOption.NOFOLLOW_LINKS))
            throw fail("FAILURE_EVIDENCE_PATH_INVALID");
        requirePrivate(lockPath);
        try(FileChannel channel=FileChannel.open(lockPath,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
            FileLock lock=tryLock(channel)){
            return store.copyUnderLock(maximumRecords,maximumBytes,copy);
        }
    }
    private CustodyCount copyUnderLock(int maximumRecords,long maximumBytes,CustodyCopy copy)throws Exception{
        Path keyFile=root.resolve("evidence.key"),authorityFile=root.resolve("authority.json");
        requirePrivate(keyFile);
        requirePrivate(authorityFile);
        byte[] key=readBytes(keyFile,32);
        try{
            byte[] authority=readBytes(authorityFile,4096);
            if(key.length!=32)throw fail("FAILURE_EVIDENCE_KEY_INVALID");
            verifyAuthority(authority,key);
            List<Path> records=new ArrayList<>();
            try(DirectoryStream<Path> files=Files.newDirectoryStream(root)){
                for(Path path:files){
                    String name=path.getFileName().toString();
                    if(List.of("evidence.key","authority.json","evidence.lock").contains(name))continue;
                    if(!name.matches("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\\.json"))
                        throw fail("FAILURE_EVIDENCE_PATH_INVALID");
                    if(records.size()>=maximumRecords)throw fail("FAILURE_EVIDENCE_CAPACITY");
                    records.add(path);
                }
            }
            if(records.isEmpty())return new CustodyCount(0,0);
            records.sort(Comparator.comparing(Path::getFileName));
            if(authority.length>maximumBytes)throw fail("FAILURE_EVIDENCE_CAPACITY");
            long remaining=maximumBytes-authority.length;
            copy.write("authority.json",authority);
            for(Path path:records){
                requirePrivate(path);
                byte[] bytes=readBytes(path,limits.maximumRecordBytes);
                if(bytes.length>remaining)throw fail("FAILURE_EVIDENCE_CAPACITY");
                verifyRecord(path,bytes,key);
                remaining-=bytes.length;
                copy.write(path.getFileName().toString(),bytes);
            }
            return new CustodyCount(records.size(),maximumBytes-remaining);
        }finally{Arrays.fill(key,(byte)0);}
    }
    public Envelope retain(FailureEvidence proposed)throws Exception{
        proposed.validate();if(!same(scope,proposed.scope))throw fail("FAILURE_EVIDENCE_OWNER_CHANGED");
        byte[] payload=encode(proposed,limits.maximumRecordBytes);FailureEvidence frozen=decode(payload,FailureEvidence.class);frozen.validate();String hash=sha(payload);
        return locked(key->{
            Path destination=record(frozen.id);
            if(Files.exists(destination,LinkOption.NOFOLLOW_LINKS)){
                Envelope existing=read(destination,key);if(!hash.equals(existing.hash))throw fail("FAILURE_EVIDENCE_ID_CONFLICT");return existing;
            }
            Envelope envelope=new Envelope();envelope.value=frozen;envelope.hash=hash;envelope.signature=sign(key,"record",hash);
            byte[] bytes=encode(envelope,limits.maximumRecordBytes);long used=0;int records=0;
            try(Stream<Path> paths=Files.list(root)){
                for(Iterator<Path> it=paths.iterator();it.hasNext();){Path file=it.next();if(Files.isSymbolicLink(file)||!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))throw fail("FAILURE_EVIDENCE_PATH_INVALID");used=Math.addExact(used,Files.size(file));if(file.getFileName().toString().matches("[0-9a-f-]{36}\\.json"))records++;}
            }
            if(records>=limits.maximumRecords||bytes.length>limits.maximumBytes||used>limits.maximumBytes-bytes.length)throw fail("FAILURE_EVIDENCE_CAPACITY");
            publish(destination,bytes);return read(destination,key);
        });
    }
    public Envelope load(String id)throws Exception{return locked(key->read(record(id),key));}
    public Map<String,Object> summary(String id)throws Exception{
        Envelope envelope=load(id);FailureEvidence value=envelope.value;Journal.FailureReview review=value.review;Map<String,Object> out=new LinkedHashMap<>();
        out.put("id",value.id);out.put("hash",envelope.hash);out.put("createdAt",value.createdAt);out.put("createdBy",value.createdBy);out.put("taskId",scope.taskId);out.put("configurationRevision",value.configurationRevision);
        out.put("ownerNodeId",scope.ownerNodeId);out.put("ownerDirectoryId",scope.ownerDirectoryId);out.put("fingerprint",review.fingerprint);out.put("failureEpoch",review.failureEpoch);
        Map<String,String> position=new TreeMap<>();for(String key:Arrays.asList("gtidSet","file","offset","serverUuid","lsn","scn","dbid","incarnation"))if(review.raw.position.getPayload().containsKey(key))position.put(key,review.raw.position.getPayload().get(key));out.put("sourcePosition",position);
        out.put("subscriptionId",review.subscription.id);out.put("errorGeneration",review.subscription.errorGeneration);out.put("acknowledged",review.subscription.acknowledged);out.put("sequence",review.sequence);out.put("epoch",review.epoch);out.put("generation",review.generation);out.put("failurePhase",review.failure.phase.name());
        out.put("transactionId",review.raw.id);out.put("sourceType",review.raw.position.getSourceType());out.put("sourceId",review.raw.position.getSourceId());out.put("rawHash",review.rawHash);out.put("normalizedHash",review.normalizedHash);out.put("rawEvents",review.raw.changes.size());out.put("normalizedEvents",review.normalized==null?null:review.normalized.changes.size());out.put("schemaCount",value.schemas.size());
        out.put("firstEventId",review.raw.changes.isEmpty()?null:review.raw.changes.get(0).eventId);out.put("lastEventId",review.raw.changes.isEmpty()?null:review.raw.changes.get(review.raw.changes.size()-1).eventId);
        out.put("beforeImages",images(review.raw,value.schemas,true));out.put("afterImages",images(review.raw,value.schemas,false));List<Map<String,Object>> tables=new ArrayList<>();
        for(TableSchema schema:value.schemas.values()){if(tables.size()==100)break;Map<String,Object> table=new LinkedHashMap<>();table.put("id",schema.id);table.put("version",schema.version);table.put("name",schema.qualifiedName());tables.add(table);}out.put("tables",tables);out.put("tablesTruncated",value.schemas.size()>tables.size());
        return out;
    }
    public Map<String,Object> events(String id,boolean normalized,int from,int limit,int maximumBytes)throws Exception{
        if(from<0||limit<1||limit>100||maximumBytes<1024||maximumBytes>256*1024)throw new IllegalArgumentException("FAILURE_EVIDENCE_PAGE_INVALID");
        Envelope envelope=load(id);SourceTransaction tx=normalized?envelope.value.review.normalized:envelope.value.review.raw;
        if(tx==null)throw fail("FAILURE_EVIDENCE_NOT_NORMALIZED");if(from>tx.changes.size())throw new IllegalArgumentException("FAILURE_EVIDENCE_PAGE_INVALID");
        Map<String,Object> page=new LinkedHashMap<>();page.put("id",id);page.put("hash",envelope.hash);page.put("view",normalized?"NORMALIZED":"RAW");page.put("total",tx.changes.size());List<Map<String,Object>> events=new ArrayList<>();page.put("events",events);page.put("next",null);
        for(int i=from;i<tx.changes.size()&&events.size()<limit;i++){
            Change change=tx.changes.get(i);Map<String,Object> item=new LinkedHashMap<>();item.put("ordinal",i);item.put("event",change);TableSchema schema=envelope.value.schemas.get(change.tableId+"/"+change.schemaVersion);
            if(schema!=null){item.put("table",schema.qualifiedName());List<Map<String,Object>> columns=new ArrayList<>();for(TableSchema.Column column:schema.columns){Map<String,Object> descriptor=new LinkedHashMap<>();descriptor.put("name",column.name);descriptor.put("type",column.type.name());descriptor.put("sourceType",column.sourceType);descriptor.put("length",column.length);descriptor.put("nullable",column.nullable);descriptor.put("precision",column.precision);descriptor.put("scale",column.scale);descriptor.put("generationKind",column.generationKind);columns.add(descriptor);}item.put("columns",columns);}
            events.add(item);page.put("next",i+1<tx.changes.size()?i+1:null);
            try{encode(page,maximumBytes);}catch(IllegalArgumentException tooLarge){
                if(!"FAILURE_EVIDENCE_TOO_LARGE".equals(tooLarge.getMessage()))throw tooLarge;events.remove(events.size()-1);if(events.isEmpty())throw fail("FAILURE_EVIDENCE_EVENT_TOO_LARGE");page.put("next",i);break;
            }
        }return page;
    }
    private static Map<String,Integer> images(SourceTransaction transaction,Map<String,TableSchema> schemas,boolean before){
        int full=0,partial=0,absent=0;for(Change change:transaction.changes){List<Cell> row=before?change.before:change.after;if(row==null){absent++;continue;}TableSchema schema=schemas.get(change.tableId+"/"+change.schemaVersion);boolean complete=schema!=null&&row.size()==schema.columns.size();for(Cell cell:row)if(!cell.present)complete=false;if(complete)full++;else partial++;}
        Map<String,Integer> result=new LinkedHashMap<>();result.put("full",full);result.put("partial",partial);result.put("absent",absent);return result;
    }
    private <T>T locked(Locked<T> operation)throws Exception{
        safePath(root);Path lockPath=root.resolve("evidence.lock");
        try(FileChannel channel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){
            protect(lockPath,false);
            try(FileLock lock=tryLock(channel)){
                Path keyPath=root.resolve("evidence.key");
                if(!Files.exists(keyPath,LinkOption.NOFOLLOW_LINKS)){
                    try(Stream<Path> files=Files.list(root)){if(files.anyMatch(p->p.getFileName().toString().endsWith(".json")))throw fail("FAILURE_EVIDENCE_KEY_MISSING");}
                    byte[] secret=new byte[32];new SecureRandom().nextBytes(secret);publish(keyPath,secret);
                }
                byte[] key=readBytes(keyPath,32);if(key.length!=32)throw fail("FAILURE_EVIDENCE_KEY_INVALID");
                Path authorityPath=root.resolve("authority.json");
                if(!Files.exists(authorityPath,LinkOption.NOFOLLOW_LINKS)){
                    try(Stream<Path> files=Files.list(root)){if(files.anyMatch(p->p.getFileName().toString().matches("[0-9a-f-]{36}\\.json")))throw fail("FAILURE_EVIDENCE_AUTHORITY_MISSING");}
                    Authority authority=new Authority();authority.scope=scope;authority.storeId=UUID.randomUUID().toString();authority.signature=sign(key,"authority",authorityHash(authority));publish(authorityPath,encode(authority,4096));
                }
                verifyAuthority(readBytes(authorityPath,4096),key);
                return operation.run(key);
            }
        }
    }
    private static FileLock tryLock(FileChannel channel)throws IOException{
        FileLock lock;
        try{lock=channel.tryLock();}catch(OverlappingFileLockException busy){throw fail("FAILURE_EVIDENCE_BUSY");}
        if(lock==null)throw fail("FAILURE_EVIDENCE_BUSY");
        return lock;
    }
    private void verifyAuthority(byte[] bytes,byte[] key)throws Exception{
        Authority authority=decode(bytes,Authority.class);
        if(authority.version!=1||authority.scope==null||!same(scope,authority.scope))
            throw fail("FAILURE_EVIDENCE_OWNER_CHANGED");
        FailureEvidence.uuid(authority.storeId);
        if(!verify(sign(key,"authority",authorityHash(authority)),authority.signature))
            throw fail("FAILURE_EVIDENCE_SIGNATURE_INVALID");
    }
    private String authorityHash(Authority authority){return Codec.sha256(Codec.encode(Arrays.asList(authority.version,authority.storeId,authority.scope)));}
    private Path record(String id){FailureEvidence.uuid(id);return root.resolve(id+".json");}
    private Envelope read(Path path,byte[] key)throws Exception{
        if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS))throw fail("FAILURE_EVIDENCE_NOT_FOUND");
        return verifyRecord(path,readBytes(path,limits.maximumRecordBytes),key);
    }
    private Envelope verifyRecord(Path path,byte[] bytes,byte[] key)throws Exception{
        Envelope envelope=decode(bytes,Envelope.class);
        if(envelope.value==null||!FailureEvidence.hash(envelope.hash)||!verify(sign(key,"record",envelope.hash),envelope.signature))throw fail("FAILURE_EVIDENCE_SIGNATURE_INVALID");
        if(!envelope.hash.equals(sha(encode(envelope.value,limits.maximumRecordBytes))))throw fail("FAILURE_EVIDENCE_CONTENT_CHANGED");
        envelope.value.validate();if(!same(scope,envelope.value.scope)||!path.equals(record(envelope.value.id)))throw fail("FAILURE_EVIDENCE_OWNER_CHANGED");return envelope;
    }
    private static boolean same(Object a,Object b){return Codec.encode(a).equals(Codec.encode(b));}
    private static void safePath(Path path)throws Exception{for(Path current=path;current!=null;current=current.getParent())if(Files.isSymbolicLink(current))throw fail("FAILURE_EVIDENCE_PATH_INVALID");}
    private static void requirePrivate(Path path)throws IOException{
        if(Files.isSymbolicLink(path)||Files.getPosixFilePermissions(path,LinkOption.NOFOLLOW_LINKS).stream()
                .anyMatch(mode->!mode.name().startsWith("OWNER_")))throw fail("FAILURE_EVIDENCE_PATH_INVALID");
    }
    private static void protect(Path path,boolean directory)throws Exception{
        safePath(path);try{Files.setPosixFilePermissions(path,PosixFilePermissions.fromString(directory?"rwx------":"rw-------"));}catch(UnsupportedOperationException ignored){/* Installation ACLs must protect non-POSIX systems. */}
    }
    private static byte[] readBytes(Path path,int maximum)throws Exception{
        safePath(path);if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)||Files.size(path)>maximum)throw fail("FAILURE_EVIDENCE_PATH_OR_SIZE_INVALID");
        try(InputStream in=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS);ByteArrayOutputStream out=new ByteArrayOutputStream()){byte[] buffer=new byte[8192];int count;while((count=in.read(buffer))!=-1){if(out.size()>maximum-count)throw fail("FAILURE_EVIDENCE_TOO_LARGE");out.write(buffer,0,count);}return out.toByteArray();}
    }
    private static byte[] encode(Object value,int maximum)throws Exception{
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();OutputStream bounded=new OutputStream(){
            public void write(int b)throws IOException{if(bytes.size()>=maximum)throw new IOException("LIMIT");bytes.write(b);}
            public void write(byte[] b,int off,int length)throws IOException{if(bytes.size()>maximum-length)throw new IOException("LIMIT");bytes.write(b,off,length);}
        };
        try{Codec.JSON.writeValue(bounded,value);}catch(Exception tooLarge){throw new IllegalArgumentException("FAILURE_EVIDENCE_TOO_LARGE");}return bytes.toByteArray();
    }
    private static <T>T decode(byte[] bytes,Class<T> type){try{return READER.readValue(bytes,type);}catch(Exception corrupt){throw fail("FAILURE_EVIDENCE_CORRUPT");}}
    private static String sha(byte[] bytes)throws Exception{return hex(MessageDigest.getInstance("SHA-256").digest(bytes));}
    private static String sign(byte[] key,String kind,String hash)throws Exception{Mac mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(key,"HmacSHA256"));return hex(mac.doFinal(("rivomesh.failure.v1\0"+kind+"\0"+hash).getBytes(StandardCharsets.UTF_8)));}
    private static String hex(byte[] bytes){StringBuilder out=new StringBuilder();for(byte b:bytes)out.append(String.format("%02x",b&255));return out.toString();}
    private static boolean verify(String a,String b){return FailureEvidence.hash(b)&&MessageDigest.isEqual(a.getBytes(StandardCharsets.US_ASCII),b.getBytes(StandardCharsets.US_ASCII));}
    private static void publish(Path destination,byte[] bytes)throws Exception{
        Path temporary=Files.createTempFile(destination.getParent(),".evidence-",".pending");
        try{
            protect(temporary,false);try(FileChannel out=FileChannel.open(temporary,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){ByteBuffer buffer=ByteBuffer.wrap(bytes);while(buffer.hasRemaining())out.write(buffer);out.force(true);}
            Files.move(temporary,destination,StandardCopyOption.ATOMIC_MOVE);
            try(FileChannel directory=FileChannel.open(destination.getParent(),StandardOpenOption.READ)){directory.force(true);}catch(AccessDeniedException|UnsupportedOperationException ignored){/* Some platforms cannot fsync directories. */}
        }finally{Files.deleteIfExists(temporary);}
    }
    private static IllegalStateException fail(String code){return new IllegalStateException(code);}
}
