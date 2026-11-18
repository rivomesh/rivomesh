package io.github.rivomesh.validation;

import io.github.rivomesh.*;
import io.github.rivomesh.cdc.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Private disk staging. Source scans are sealed before target scans can produce differences. */
public final class ValidationStore implements AutoCloseable {
    private static final String SOURCE="_source";
    private static final Set<Path> LOCAL_WRITERS=java.util.concurrent.ConcurrentHashMap.newKeySet();
    public static final class Limits {
        public long maximumBytes=1024L*1024*1024,maximumRows=1000000000L;
        public int maximumRowBytes=4*1024*1024;
        public void validate(){if(maximumBytes<1024*1024||maximumRows<1||maximumRowBytes<1024||maximumRowBytes>32*1024*1024||maximumRowBytes>maximumBytes/4)throw new IllegalArgumentException("VALIDATION_LIMIT_INVALID");}
    }
    public static final class TableResult {
        public String target,tableId,sourceRoot,targetRoot;
        public long sourceRows,targetRows,missing,extra,changed;
        public boolean matches(){return missing==0&&extra==0&&changed==0;}
    }
    public static final class Manifest {
        public int version=1;
        public String state="SCANNING_SOURCE",code,sourceSeal;
        public PipelineValidationBarrier.Certificate certificate;
        public Limits limits;
        public long logicalBytes,rows;
        public Map<String,TableSchema> schemas=new TreeMap<>();
        public Map<String,Set<String>> targetTables=new TreeMap<>();
        public Set<String> completedSourceTables=new TreeSet<>();
        public Map<String,String> sourceRoots=new TreeMap<>();
        public Map<String,TableResult> results=new TreeMap<>();
        public boolean sourceComplete;
    }
    public static final class Difference {
        public String identityHash,kind,expectedHash,actualHash;
    }
    public static final class RowPair {public List<Cell> expected,actual;public String expectedRowId,actualRowId;}
    private final Path directory;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private Connection db;
    private Manifest manifest;
    private int pending;
    private boolean closed;

    public static ValidationStore create(Path root,String id,PipelineValidationBarrier.Certificate certificate,List<TableSchema> schemas,Map<String,Set<String>> targets,Limits limits)throws Exception{
        limits.validate();Path directory=reportPath(root,id);
        Manifest m=new Manifest();m.certificate=Codec.decode(Codec.encode(certificate),PipelineValidationBarrier.Certificate.class);m.limits=Codec.decode(Codec.encode(limits),Limits.class);
        if(!id.equals(m.certificate.id)||!targets.keySet().equals(certificate.selectedTargets))throw new IllegalArgumentException("VALIDATION_SCOPE_INVALID");
        for(TableSchema table:schemas){table.validate();if(!Objects.equals(certificate.schemas.get(table.id),Codec.sha256(Codec.encode(table))))throw new IllegalArgumentException("VALIDATION_SCHEMA_MISMATCH");m.schemas.put(table.id,Codec.decode(Codec.encode(table),TableSchema.class));}
        for(Map.Entry<String,Set<String>> entry:targets.entrySet()){if(!m.schemas.keySet().containsAll(entry.getValue()))throw new IllegalArgumentException("VALIDATION_SCOPE_INVALID");m.targetTables.put(entry.getKey(),new TreeSet<>(entry.getValue()));}
        if(m.targetTables.values().stream().allMatch(Set::isEmpty))throw new IllegalArgumentException("VALIDATION_NO_TABLES");
        boolean newRoot=!Files.exists(directory.getParent());Files.createDirectories(directory.getParent());if(newRoot)owner(directory.getParent(),true);Files.createDirectory(directory);owner(directory,true);
        return new ValidationStore(directory,m);
    }
    public static ValidationStore open(Path root,String id)throws Exception{return new ValidationStore(reportPath(root,id),null);}

    /** Metadata only. This observer never acquires the report lock or opens its database. */
    public static Map<String,Object> storage(Path root,String id)throws Exception{
        Path directory=reportPath(root,id);
        if(!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS))throw fail("VALIDATION_REPORT_NOT_CREATED");
        Path observedDirectory=directory.toRealPath();
        if(!observedDirectory.equals(directory.getParent().toRealPath().resolve(id)))throw fail("VALIDATION_PATH_INVALID");
        Map<String,Long> sizes=new LinkedHashMap<>();
        for(String name:List.of("rows.mv.db","rows.trace.db","summary.json")){
            try{
                BasicFileAttributes attributes=Files.readAttributes(observedDirectory.resolve(name),BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                if(!attributes.isRegularFile())throw fail("VALIDATION_PATH_INVALID");
                sizes.put(name,attributes.size());
            }catch(NoSuchFileException absent){
                if("rows.mv.db".equals(name))throw fail("VALIDATION_REPORT_NOT_CREATED");
            }
        }
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("id",id);
        result.put("files",sizes);
        result.put("observedAt",Instant.now().toString());
        result.put("activity",LOCAL_WRITERS.contains(observedDirectory)?"PROTECTED_ACTIVE":"ACTIVITY_UNKNOWN");
        return result;
    }

    private record ReportEntry(String id,long created) {
        String cursor(){return created+"_"+id;}
        Map<String,String> view(){return Map.of("id",id,"createdAt",Instant.ofEpochMilli(created).toString());}
    }

    /** Lists immutable report identities without opening any report database or reading row values. */
    public static Map<String,Object> catalog(Path root,String after,int limit)throws IOException{
        if(limit<1||limit>25||after==null)throw new IllegalArgumentException("VALIDATION_CATALOG_PAGE_INVALID");
        ReportEntry cursor=null;
        if(!after.isEmpty()){
            if(!after.matches("[0-9]{1,18}_[a-f0-9-]{36}"))
                throw new IllegalArgumentException("VALIDATION_CATALOG_PAGE_INVALID");
            int split=after.indexOf('_');
            String id=after.substring(split+1);
            if(!reportId(id))throw new IllegalArgumentException("VALIDATION_CATALOG_PAGE_INVALID");
            cursor=new ReportEntry(id,Long.parseLong(after.substring(0,split)));
        }
        Path directory=root.toAbsolutePath().normalize();
        if(Files.isSymbolicLink(directory))throw new IOException("VALIDATION_PATH_INVALID");
        Comparator<ReportEntry> recency=Comparator.comparingLong(ReportEntry::created).reversed()
                .thenComparing(ReportEntry::id);
        NavigableSet<ReportEntry> matches=new TreeSet<>(recency);
        if(Files.exists(directory,LinkOption.NOFOLLOW_LINKS)){
            try(DirectoryStream<Path> children=Files.newDirectoryStream(directory)){
                for(Path entry:children){
                    String id=entry.getFileName().toString();
                    if(!reportId(id)||!Files.isDirectory(entry,LinkOption.NOFOLLOW_LINKS)
                            ||!Files.isRegularFile(entry.resolve("rows.mv.db"),LinkOption.NOFOLLOW_LINKS)
                            ||!Files.isRegularFile(entry.resolve("summary.json"),LinkOption.NOFOLLOW_LINKS))continue;
                    BasicFileAttributes attributes;
                    try{attributes=Files.readAttributes(entry,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);}
                    catch(NoSuchFileException removed){continue;}
                    ReportEntry found=new ReportEntry(id,Math.max(0,attributes.creationTime().toMillis()));
                    if(cursor!=null&&recency.compare(found,cursor)<=0)continue;
                    matches.add(found);
                    if(matches.size()>limit+1)matches.pollLast();
                }
            }
        }
        boolean more=matches.size()>limit;
        if(more)matches.pollLast();
        Map<String,Object> page=new LinkedHashMap<>();
        page.put("reports",matches.stream().map(ReportEntry::view).toList());
        page.put("nextCursor",more?matches.last().cursor():null);
        return page;
    }

    static boolean reportId(String id){
        if(!id.matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"))return false;
        return UUID.fromString(id).toString().equals(id);
    }
    static Path reportPath(Path root,String id)throws Exception{
        if(id==null||!UUID.fromString(id).toString().equals(id))throw new IllegalArgumentException("VALIDATION_ID_INVALID");Path base=root.toAbsolutePath().normalize();
        if(base.toString().indexOf(';')>=0||base.toString().chars().anyMatch(c->c<32)||Files.isSymbolicLink(base))throw new IOException("VALIDATION_PATH_INVALID");return base.resolve(id);
    }
    static void owner(Path path,boolean dir)throws IOException{if(Files.isSymbolicLink(path))throw new IOException("VALIDATION_SYMLINK");try{Files.setPosixFilePermissions(path,PosixFilePermissions.fromString(dir?"rwx------":"rw-------"));}catch(UnsupportedOperationException ignored){}}
    static void claimLocalWriter(Path directory)throws SyncFailure{
        if(!LOCAL_WRITERS.add(directory))throw fail("VALIDATION_REPORT_BUSY");
    }
    static void releaseLocalWriter(Path directory){LOCAL_WRITERS.remove(directory);}

    private ValidationStore(Path path,Manifest initial)throws Exception{
        if(!Files.isDirectory(path)||Files.isSymbolicLink(path))throw new IOException("VALIDATION_REPORT_MISSING");directory=path.toRealPath();path=directory;
        if(directory.toString().indexOf(';')>=0||directory.toString().chars().anyMatch(c->c<32))throw new IOException("VALIDATION_PATH_INVALID");
        claimLocalWriter(directory);
        FileChannel channel=null;FileLock acquired=null;
        try{
            Path file=path.resolve("validation.lock");if(Files.isSymbolicLink(file))throw new IOException("VALIDATION_SYMLINK");channel=FileChannel.open(file,StandardOpenOption.CREATE,StandardOpenOption.WRITE);owner(file,false);
            try{acquired=channel.tryLock();}catch(OverlappingFileLockException busy){throw fail("VALIDATION_REPORT_BUSY");}
            if(acquired==null)throw fail("VALIDATION_REPORT_BUSY");
        }catch(Exception failure){if(channel!=null)try{channel.close();}catch(Exception close){failure.addSuppressed(close);}LOCAL_WRITERS.remove(directory);throw failure;}
        lockChannel=channel;lock=acquired;
        try{
            try(DirectoryStream<Path> staging=Files.newDirectoryStream(directory,"summary-*.tmp")){
                for(Path file:staging)
                    if(file.getFileName().toString().matches("summary-[0-9]+\\.tmp")
                            &&Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))Files.deleteIfExists(file);
            }
            for(String name:Arrays.asList("rows.mv.db","rows.trace.db","summary.json"))if(Files.isSymbolicLink(path.resolve(name)))throw new IOException("VALIDATION_SYMLINK");
            String url="jdbc:h2:file:"+path.resolve("rows")+";WRITE_DELAY=0;DB_CLOSE_ON_EXIT=FALSE;CACHE_SIZE=8192;MAX_MEMORY_ROWS=1000"+(initial==null?";IFEXISTS=TRUE":"");
            db=JdbcConnections.open("org.h2.Driver",url,"","");db.setAutoCommit(false);
            if(initial!=null){
                try(Statement sql=db.createStatement()){
                    sql.execute("CREATE TABLE rmv_meta(item_key VARCHAR(32) PRIMARY KEY,payload CLOB NOT NULL,content_hash VARCHAR(64) NOT NULL)");
                    sql.execute("CREATE TABLE rmv_row(side VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,key_hash VARCHAR(64) NOT NULL,key_data CLOB NOT NULL,row_hash VARCHAR(64) NOT NULL,canonical_data CLOB NOT NULL,raw_data CLOB NOT NULL,raw_hash VARCHAR(64) NOT NULL,row_id VARCHAR(64),row_bytes BIGINT NOT NULL,PRIMARY KEY(side,table_id,key_hash))");
                    sql.execute("CREATE TABLE rmv_identity(table_id VARCHAR(64) NOT NULL,row_id VARCHAR(64) NOT NULL,row_hash VARCHAR(64) NOT NULL,payload CLOB NOT NULL,consumed BOOLEAN NOT NULL,PRIMARY KEY(table_id,row_id))");
                    sql.execute("CREATE INDEX rmv_identity_match ON rmv_identity(table_id,row_hash,consumed,row_id)");
                    sql.execute("CREATE TABLE rmv_diff(target_id VARCHAR(128) NOT NULL,table_id VARCHAR(64) NOT NULL,key_hash VARCHAR(64) NOT NULL,kind VARCHAR(16) NOT NULL,expected_hash VARCHAR(64),actual_hash VARCHAR(64),PRIMARY KEY(target_id,table_id,key_hash))");
                }manifest=initial;save();
            }else{
                load();manifest.limits.validate();
                if(!manifest.sourceComplete&&!"FAILED".equals(manifest.state)){manifest.state="INTERRUPTED_SOURCE";manifest.code="VALIDATION_NEW_SOURCE_VIEW_REQUIRED";save();}
            }
            protect();project();
        }catch(Exception failure){if(db!=null)try{db.close();}catch(Exception close){failure.addSuppressed(close);}try{lock.release();}catch(Exception close){failure.addSuppressed(close);}try{lockChannel.close();}catch(Exception close){failure.addSuppressed(close);}LOCAL_WRITERS.remove(directory);throw failure;}
    }
    private static SyncFailure fail(String code){return new SyncFailure(code,"VALIDATE",false,"The validation workspace cannot complete this operation. Inspect its state before retrying.");}
    private void load()throws Exception{try(PreparedStatement p=db.prepareStatement("SELECT payload,content_hash FROM rmv_meta WHERE item_key='manifest'");ResultSet rows=p.executeQuery()){if(!rows.next())throw fail("VALIDATION_MANIFEST_MISSING");String payload=rows.getString(1);if(!Codec.sha256(payload).equals(rows.getString(2)))throw fail("VALIDATION_MANIFEST_CORRUPT");manifest=Codec.decode(payload,Manifest.class);if(manifest.version!=1)throw fail("VALIDATION_FORMAT_UNSUPPORTED");}}
    private void save()throws Exception{
        String payload=Codec.encode(manifest);try(PreparedStatement p=db.prepareStatement("MERGE INTO rmv_meta(item_key,payload,content_hash) KEY(item_key) VALUES ('manifest',?,?)")){p.setString(1,payload);p.setString(2,Codec.sha256(payload));p.executeUpdate();}db.commit();pending=0;protect();project();
    }
    private void project()throws Exception{
        Path temporary=Files.createTempFile(directory,"summary-",".tmp");try{owner(temporary,false);byte[] data=Codec.encode(summary()).getBytes(StandardCharsets.UTF_8);try(FileChannel out=FileChannel.open(temporary,StandardOpenOption.WRITE)){ByteBuffer buffer=ByteBuffer.wrap(data);while(buffer.hasRemaining())out.write(buffer);out.force(true);}Files.move(temporary,directory.resolve("summary.json"),StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);}finally{Files.deleteIfExists(temporary);}
    }
    private void protect()throws IOException{for(String name:Arrays.asList("rows.mv.db","rows.trace.db"))if(Files.exists(directory.resolve(name)))owner(directory.resolve(name),false);}
    private TableSchema schema(String id)throws Exception{TableSchema t=manifest.schemas.get(id);if(t==null)throw fail("VALIDATION_TABLE_UNKNOWN");return t;}
    private Set<String> sourceTables(){Set<String> result=new TreeSet<>();for(Set<String> ids:manifest.targetTables.values())result.addAll(ids);return result;}
    private void reserve(long bytes,long rows)throws Exception{if(bytes<0||Math.addExact(manifest.logicalBytes,bytes)>manifest.limits.maximumBytes||Math.addExact(manifest.rows,rows)>manifest.limits.maximumRows)throw fail("VALIDATION_STORAGE_QUOTA_EXCEEDED");manifest.logicalBytes+=bytes;manifest.rows+=rows;}
    private void flushIfNeeded()throws Exception{if(++pending>=128)flush();}
    public void flush()throws Exception{
        if(Files.size(directory.resolve("rows.mv.db"))>manifest.limits.maximumBytes)throw fail("VALIDATION_PHYSICAL_QUOTA_EXCEEDED");save();
    }
    private void rowLimit(List<Cell> row)throws Exception{long bytes=64L*row.size();for(Cell cell:row){if(cell==null||!cell.present)throw fail("VALIDATION_ROW_INCOMPLETE");if(cell.value!=null)bytes=Math.addExact(bytes,cell.value.getBytes(StandardCharsets.UTF_8).length);if(bytes>manifest.limits.maximumRowBytes)throw fail("VALIDATION_ROW_TOO_LARGE");}}
    public Manifest manifest(){return Codec.decode(Codec.encode(manifest),Manifest.class);}
    public Map<String,Object> summary(){Map<String,Object> out=new LinkedHashMap<>();out.put("id",manifest.certificate.id);out.put("state",manifest.state);out.put("code",manifest.code);out.put("sourceComplete",manifest.sourceComplete);out.put("rows",manifest.rows);out.put("logicalBytes",manifest.logicalBytes);out.put("maximumBytes",manifest.limits.maximumBytes);out.put("targets",manifest.targetTables);out.put("results",manifest.results);return out;}
    public void identity(String tableId,JdbcJournal.IdentityRow row)throws Exception{
        if(manifest.sourceComplete||!"SCANNING_SOURCE".equals(manifest.state)||manifest.completedSourceTables.contains(tableId)||!schema(tableId).keyless()||!sourceTables().contains(tableId))throw fail("VALIDATION_SOURCE_STATE_INVALID");
        if(row.rowId==null||!row.rowId.matches("[0-9a-f]{64}")||!Codec.sha256(row.payload).equals(row.hash))throw fail("ROW_IDENTITY_CHECKSUM_MISMATCH");
        long payloadBytes=row.payload.getBytes(StandardCharsets.UTF_8).length;if(payloadBytes>manifest.limits.maximumRowBytes)throw fail("VALIDATION_ROW_TOO_LARGE");long bytes=payloadBytes+256L;reserve(bytes,0);
        try(PreparedStatement p=db.prepareStatement("INSERT INTO rmv_identity(table_id,row_id,row_hash,payload,consumed) VALUES (?,?,?,?,FALSE)")){p.setString(1,tableId);p.setString(2,row.rowId);p.setString(3,row.hash);p.setString(4,row.payload);p.executeUpdate();}flushIfNeeded();
    }
    public void source(String tableId,List<Cell> row)throws Exception{
        if(manifest.sourceComplete||!"SCANNING_SOURCE".equals(manifest.state)||manifest.completedSourceTables.contains(tableId)||!sourceTables().contains(tableId))throw fail("VALIDATION_SOURCE_STATE_INVALID");
        TableSchema table=schema(tableId);rowLimit(row);String canonical=RowValues.canonical(table,row),occurrence=null;
        if(table.keyless())try(PreparedStatement p=db.prepareStatement("SELECT row_id,payload FROM rmv_identity WHERE table_id=? AND row_hash=? AND consumed=FALSE ORDER BY row_id LIMIT 1 FOR UPDATE")){
            p.setString(1,tableId);p.setString(2,Codec.sha256(canonical));try(ResultSet found=p.executeQuery()){if(!found.next())throw fail("VALIDATION_SOURCE_IDENTITY_MISMATCH");occurrence=found.getString(1);if(!canonical.equals(found.getString(2)))throw fail("ROW_HASH_COLLISION");}
            try(PreparedStatement consumed=db.prepareStatement("UPDATE rmv_identity SET consumed=TRUE WHERE table_id=? AND row_id=?")){consumed.setString(1,tableId);consumed.setString(2,occurrence);consumed.executeUpdate();}
        }
        put(SOURCE,table,occurrence,row,canonical);
    }
    private void put(String side,TableSchema table,String occurrence,List<Cell> row,String canonical)throws Exception{
        String raw=Codec.encode(row),key=RowValues.identity(table,row,occurrence);if(raw.getBytes(StandardCharsets.UTF_8).length>manifest.limits.maximumRowBytes||canonical.getBytes(StandardCharsets.UTF_8).length>manifest.limits.maximumRowBytes)throw fail("VALIDATION_ROW_TOO_LARGE");
        long bytes=raw.getBytes(StandardCharsets.UTF_8).length+(long)canonical.getBytes(StandardCharsets.UTF_8).length+key.getBytes(StandardCharsets.UTF_8).length+512;reserve(bytes,1);
        try(PreparedStatement p=db.prepareStatement("INSERT INTO rmv_row(side,table_id,key_hash,key_data,row_hash,canonical_data,raw_data,raw_hash,row_id,row_bytes) VALUES (?,?,?,?,?,?,?,?,?,?)")){
            p.setString(1,side);p.setString(2,table.id);p.setString(3,Codec.sha256(key));p.setString(4,key);p.setString(5,Codec.sha256(canonical));p.setString(6,canonical);p.setString(7,raw);p.setString(8,Codec.sha256(raw));p.setString(9,occurrence);p.setLong(10,bytes);p.executeUpdate();
        }catch(SQLException duplicate){if("23505".equals(duplicate.getSQLState()))throw fail("VALIDATION_DUPLICATE_IDENTITY");throw duplicate;}flushIfNeeded();
    }
    public void finishSourceTable(String tableId)throws Exception{
        if(manifest.sourceComplete||!"SCANNING_SOURCE".equals(manifest.state)||!sourceTables().contains(tableId))throw fail("VALIDATION_SOURCE_STATE_INVALID");
        try(PreparedStatement p=db.prepareStatement("SELECT COUNT(*) FROM rmv_identity WHERE table_id=? AND consumed=FALSE")){p.setString(1,tableId);try(ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)!=0)throw fail("VALIDATION_SOURCE_IDENTITY_MISMATCH");}}
        manifest.completedSourceTables.add(tableId);manifest.sourceRoots.put(tableId,root(SOURCE,tableId));flush();
    }
    public void completeSource()throws Exception{
        if(manifest.sourceComplete)return;if(!"SCANNING_SOURCE".equals(manifest.state)||!manifest.completedSourceTables.equals(sourceTables()))throw fail("VALIDATION_SOURCE_SCAN_INCOMPLETE");manifest.sourceSeal=sourceSeal();manifest.sourceComplete=true;manifest.state="SOURCE_READY";flush();
    }
    public void target(String target,String tableId,String occurrence,List<Cell> row)throws Exception{
        if(!manifest.sourceComplete||!Arrays.asList("SOURCE_READY","SCANNING_TARGETS").contains(manifest.state)||!manifest.targetTables.containsKey(target)||!manifest.targetTables.get(target).contains(tableId)||manifest.results.containsKey(target+"/"+tableId))throw fail("VALIDATION_TARGET_STATE_INVALID");
        rowLimit(row);TableSchema table=schema(tableId);manifest.state="SCANNING_TARGETS";put(target,table,occurrence,row,RowValues.canonical(table,row));
    }
    private void verifyRows(String side,String tableId)throws Exception{
        TableSchema table=schema(tableId);
        try(PreparedStatement p=db.prepareStatement("SELECT key_hash,key_data,row_hash,canonical_data,raw_data,raw_hash,row_id FROM rmv_row WHERE side=? AND table_id=?")){
            p.setString(1,side);p.setString(2,tableId);p.setFetchSize(128);try(ResultSet rows=p.executeQuery()){while(rows.next()){
                String raw=rows.getString(5),canonical=rows.getString(4),key=rows.getString(2);
                if(raw.getBytes(StandardCharsets.UTF_8).length>manifest.limits.maximumRowBytes||canonical.getBytes(StandardCharsets.UTF_8).length>manifest.limits.maximumRowBytes||!Codec.sha256(raw).equals(rows.getString(6))||!Codec.sha256(canonical).equals(rows.getString(3))||!Codec.sha256(key).equals(rows.getString(1)))throw fail("VALIDATION_ROW_CHECKSUM_MISMATCH");
                List<Cell> values=RowValues.decode(raw);rowLimit(values);
                if(!canonical.equals(RowValues.canonical(table,values))||!key.equals(RowValues.identity(table,values,rows.getString(7))))throw fail("VALIDATION_ROW_CHECKSUM_MISMATCH");
            }}
        }
    }
    private String sourceSeal(){
        PipelineValidationBarrier.Certificate certificate=Codec.decode(Codec.encode(manifest.certificate),PipelineValidationBarrier.Certificate.class);certificate.selectedTargets=new TreeSet<>(certificate.selectedTargets);
        Map<String,Set<String>> targets=new TreeMap<>();for(Map.Entry<String,Set<String>> entry:manifest.targetTables.entrySet())targets.put(entry.getKey(),new TreeSet<>(entry.getValue()));
        return Codec.sha256(Codec.encode(Arrays.asList(certificate,manifest.schemas,manifest.sourceRoots,targets)));
    }
    /** Verify that the source manifest still describes the sealed report scope, without loading row values. */
    public void verifySeal()throws Exception{
        if(!manifest.sourceComplete||!manifest.completedSourceTables.equals(sourceTables())||!Objects.equals(manifest.sourceSeal,sourceSeal()))throw fail("VALIDATION_SOURCE_SEAL_MISMATCH");
    }
    public void verifySource()throws Exception{
        verifySeal();for(String tableId:sourceTables()){verifyRows(SOURCE,tableId);if(!Objects.equals(root(SOURCE,tableId),manifest.sourceRoots.get(tableId)))throw fail("VALIDATION_SOURCE_SEAL_MISMATCH");}
    }
    private static void digest(MessageDigest hash,String value){byte[] bytes=value.getBytes(StandardCharsets.UTF_8);hash.update(ByteBuffer.allocate(4).putInt(bytes.length).array());hash.update(bytes);}
    private String root(String side,String table)throws Exception{
        MessageDigest digest=MessageDigest.getInstance("SHA-256");try(PreparedStatement p=db.prepareStatement("SELECT key_data,canonical_data FROM rmv_row WHERE side=? AND table_id=? ORDER BY key_hash")){p.setString(1,side);p.setString(2,table);p.setFetchSize(128);try(ResultSet rows=p.executeQuery()){while(rows.next()){digest(digest,rows.getString(1));digest(digest,rows.getString(2));}}}StringBuilder out=new StringBuilder();for(byte b:digest.digest())out.append(String.format("%02x",b&255));return out.toString();
    }
    private long count(String side,String table)throws Exception{try(PreparedStatement p=db.prepareStatement("SELECT COUNT(*) FROM rmv_row WHERE side=? AND table_id=?")){p.setString(1,side);p.setString(2,table);try(ResultSet rows=p.executeQuery()){rows.next();return rows.getLong(1);}}}
    public TableResult finishTarget(String target,String tableId)throws Exception{
        if(!manifest.sourceComplete||!manifest.targetTables.containsKey(target)||!manifest.targetTables.get(target).contains(tableId))throw fail("VALIDATION_TARGET_STATE_INVALID");if(manifest.results.containsKey(target+"/"+tableId))return Codec.decode(Codec.encode(manifest.results.get(target+"/"+tableId)),TableResult.class);if(!Arrays.asList("SOURCE_READY","SCANNING_TARGETS").contains(manifest.state))throw fail("VALIDATION_TARGET_STATE_INVALID");
        verifySeal();verifyRows(SOURCE,tableId);verifyRows(target,tableId);if(!Objects.equals(root(SOURCE,tableId),manifest.sourceRoots.get(tableId)))throw fail("VALIDATION_SOURCE_SEAL_MISMATCH");
        try(PreparedStatement p=db.prepareStatement("SELECT COUNT(*) FROM rmv_row s JOIN rmv_row t ON s.table_id=t.table_id AND s.key_hash=t.key_hash WHERE s.side=? AND t.side=? AND s.table_id=? AND s.key_data<>t.key_data")){p.setString(1,SOURCE);p.setString(2,target);p.setString(3,tableId);try(ResultSet r=p.executeQuery()){r.next();if(r.getLong(1)!=0)throw fail("VALIDATION_IDENTITY_HASH_COLLISION");}}
        try(PreparedStatement p=db.prepareStatement("INSERT INTO rmv_diff SELECT ?,s.table_id,s.key_hash,CASE WHEN t.key_hash IS NULL THEN 'MISSING' ELSE 'CHANGED' END,s.row_hash,t.row_hash FROM rmv_row s LEFT JOIN rmv_row t ON s.table_id=t.table_id AND s.key_hash=t.key_hash AND t.side=? WHERE s.side=? AND s.table_id=? AND (t.key_hash IS NULL OR s.canonical_data<>t.canonical_data)")){p.setString(1,target);p.setString(2,target);p.setString(3,SOURCE);p.setString(4,tableId);p.executeUpdate();}
        try(PreparedStatement p=db.prepareStatement("INSERT INTO rmv_diff SELECT ?,t.table_id,t.key_hash,'EXTRA',NULL,t.row_hash FROM rmv_row t LEFT JOIN rmv_row s ON s.table_id=t.table_id AND s.key_hash=t.key_hash AND s.side=? WHERE t.side=? AND t.table_id=? AND s.key_hash IS NULL")){p.setString(1,target);p.setString(2,SOURCE);p.setString(3,target);p.setString(4,tableId);p.executeUpdate();}
        TableResult result=new TableResult();result.target=target;result.tableId=tableId;result.sourceRows=count(SOURCE,tableId);result.targetRows=count(target,tableId);result.sourceRoot=manifest.sourceRoots.get(tableId);result.targetRoot=root(target,tableId);
        try(PreparedStatement p=db.prepareStatement("SELECT kind,COUNT(*) FROM rmv_diff WHERE target_id=? AND table_id=? GROUP BY kind")){p.setString(1,target);p.setString(2,tableId);try(ResultSet rows=p.executeQuery()){while(rows.next())switch(rows.getString(1)){case "MISSING":result.missing=rows.getLong(2);break;case "EXTRA":result.extra=rows.getLong(2);break;case "CHANGED":result.changed=rows.getLong(2);break;default:throw fail("VALIDATION_DIFF_CORRUPT");}}}
        manifest.results.put(target+"/"+tableId,result);flush();return result;
    }
    public void complete()throws Exception{if("COMPLETE".equals(manifest.state))return;if(!Arrays.asList("SOURCE_READY","SCANNING_TARGETS").contains(manifest.state))throw fail("VALIDATION_REPORT_STATE_INVALID");if(!manifest.sourceComplete)throw fail("VALIDATION_SOURCE_SCAN_INCOMPLETE");for(Map.Entry<String,Set<String>> target:manifest.targetTables.entrySet())for(String table:target.getValue())if(!manifest.results.containsKey(target.getKey()+"/"+table))throw fail("VALIDATION_TARGET_SCAN_INCOMPLETE");manifest.state="COMPLETE";manifest.code=manifest.results.values().stream().allMatch(TableResult::matches)?"VALIDATION_MATCH":"VALIDATION_DIFFERENCES";flush();}
    public List<Difference> differences(String target,String tableId,String after,int limit)throws Exception{
        if(!"COMPLETE".equals(manifest.state)||limit<1||limit>200||after==null||!after.isEmpty()&&!after.matches("[0-9a-f]{64}")||!manifest.targetTables.containsKey(target)||!manifest.targetTables.get(target).contains(tableId))throw fail("VALIDATION_DIFF_PAGE_INVALID");List<Difference> result=new ArrayList<>();
        try(PreparedStatement p=db.prepareStatement("SELECT key_hash,kind,expected_hash,actual_hash FROM rmv_diff WHERE target_id=? AND table_id=? AND key_hash>? ORDER BY key_hash LIMIT ?")){p.setString(1,target);p.setString(2,tableId);p.setString(3,after);p.setInt(4,limit);try(ResultSet rows=p.executeQuery()){while(rows.next()){Difference d=new Difference();d.identityHash=rows.getString(1);d.kind=rows.getString(2);d.expectedHash=rows.getString(3);d.actualHash=rows.getString(4);result.add(d);}}}return result;
    }
    /** Bounded administrative row access; inspect lengths before the JDBC driver fetches CLOB payloads. */
    public RowPair differenceValues(String target,String tableId,String identityHash,long maximumBytes)throws Exception{
        if(!"COMPLETE".equals(manifest.state)||identityHash==null||!identityHash.matches("[0-9a-f]{64}")||maximumBytes<1024||maximumBytes>64L*1024*1024||!manifest.targetTables.containsKey(target)||!manifest.targetTables.get(target).contains(tableId))throw fail("VALIDATION_VALUES_UNAVAILABLE");
        Difference difference=new Difference();difference.identityHash=identityHash;
        try(PreparedStatement p=db.prepareStatement("SELECT kind,expected_hash,actual_hash FROM rmv_diff WHERE target_id=? AND table_id=? AND key_hash=?")){p.setString(1,target);p.setString(2,tableId);p.setString(3,identityHash);try(ResultSet rows=p.executeQuery()){if(!rows.next())throw fail("VALIDATION_DIFFERENCE_NOT_FOUND");difference.kind=rows.getString(1);difference.expectedHash=rows.getString(2);difference.actualHash=rows.getString(3);}}
        try(PreparedStatement p=db.prepareStatement("SELECT COALESCE(SUM(OCTET_LENGTH(raw_data)),0) FROM rmv_row WHERE table_id=? AND key_hash=? AND (side=? OR side=?)")){p.setString(1,tableId);p.setString(2,identityHash);p.setString(3,SOURCE);p.setString(4,target);try(ResultSet rows=p.executeQuery()){rows.next();if(rows.getLong(1)>maximumBytes)throw fail("VALIDATION_DETAIL_TOO_LARGE");}}
        RowPair pair=values(target,tableId,identityHash);TableSchema table=schema(tableId);
        String expected=pair.expected==null?null:RowValues.canonical(table,pair.expected),actual=pair.actual==null?null:RowValues.canonical(table,pair.actual);
        if(!Objects.equals(difference.expectedHash,expected==null?null:Codec.sha256(expected))||!Objects.equals(difference.actualHash,actual==null?null:Codec.sha256(actual))
                ||expected==null&&actual==null||!Objects.equals(difference.kind,expected==null?"EXTRA":actual==null?"MISSING":expected.equals(actual)?"EQUAL":"CHANGED"))throw fail("VALIDATION_DIFF_CORRUPT");
        if(pair.expected!=null&&!identityHash.equals(Codec.sha256(RowValues.identity(table,pair.expected,pair.expectedRowId)))||pair.actual!=null&&!identityHash.equals(Codec.sha256(RowValues.identity(table,pair.actual,pair.actualRowId))))throw fail("VALIDATION_DIFF_CORRUPT");
        return pair;
    }
    public void verifyTarget(String target,String tableId)throws Exception{
        TableResult result=manifest.results.get(target+"/"+tableId);if(!"COMPLETE".equals(manifest.state)||result==null)throw fail("VALIDATION_TARGET_STATE_INVALID");verifyRows(target,tableId);if(!Objects.equals(root(target,tableId),result.targetRoot))throw fail("VALIDATION_TARGET_SEAL_MISMATCH");
    }
    public RowPair values(String target,String tableId,String identityHash)throws Exception{
        if(!"COMPLETE".equals(manifest.state)||!manifest.targetTables.containsKey(target)||!manifest.targetTables.get(target).contains(tableId))throw fail("VALIDATION_VALUES_UNAVAILABLE");RowPair pair=new RowPair();
        try(PreparedStatement p=db.prepareStatement("SELECT side,raw_data,raw_hash,row_id FROM rmv_row WHERE table_id=? AND key_hash=? AND (side=? OR side=?)")){p.setString(1,tableId);p.setString(2,identityHash);p.setString(3,SOURCE);p.setString(4,target);try(ResultSet rows=p.executeQuery()){while(rows.next()){String raw=rows.getString(2);if(!Codec.sha256(raw).equals(rows.getString(3)))throw fail("VALIDATION_ROW_CHECKSUM_MISMATCH");if(rows.getString(1).equals(SOURCE)){pair.expected=RowValues.decode(raw);pair.expectedRowId=rows.getString(4);}else{pair.actual=RowValues.decode(raw);pair.actualRowId=rows.getString(4);}}}}return pair;
    }
    /** Retry target scans against the sealed source; never merge data from a new source view. */
    public void restartTargets()throws Exception{
        if(!manifest.sourceComplete||"COMPLETE".equals(manifest.state))throw fail("VALIDATION_TARGET_STATE_INVALID");db.rollback();load();
        try(PreparedStatement p=db.prepareStatement("SELECT COUNT(*),COALESCE(SUM(row_bytes),0) FROM rmv_row WHERE side<>?")){p.setString(1,SOURCE);try(ResultSet rows=p.executeQuery()){rows.next();manifest.rows=Math.subtractExact(manifest.rows,rows.getLong(1));manifest.logicalBytes=Math.subtractExact(manifest.logicalBytes,rows.getLong(2));}}
        try(PreparedStatement p=db.prepareStatement("DELETE FROM rmv_row WHERE side<>?")){p.setString(1,SOURCE);p.executeUpdate();}
        try(Statement sql=db.createStatement()){sql.executeUpdate("DELETE FROM rmv_diff");}manifest.results.clear();manifest.state="SOURCE_READY";manifest.code=null;save();
    }
    public void failed(String code)throws Exception{db.rollback();load();manifest.state="FAILED";manifest.code=code;save();}
    @Override public void close()throws Exception{if(closed)return;closed=true;try{try{db.rollback();}finally{db.close();}}finally{try{lock.release();}finally{try{lockChannel.close();}finally{LOCAL_WRITERS.remove(directory);}}}}
}
