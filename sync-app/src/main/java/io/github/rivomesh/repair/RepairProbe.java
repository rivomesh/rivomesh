package io.github.rivomesh.repair;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.cdc.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/** Private bounded disk sort for live target roots; never loads a table into Java memory. */
public final class RepairProbe implements AutoCloseable {
    public static final class Result {public String root;public long rows;}
    private final Path directory;
    private final Connection db;
    private final TableSchema schema;
    private final long maximumBytes,maximumRows;
    private final int maximumRowBytes;
    private long bytes,rows;
    private int pending;
    private boolean closed;
    public RepairProbe(Path root,TableSchema table,long maximumBytes,long maximumRows,int maximumRowBytes)throws Exception{
        if(maximumBytes<1024*1024||maximumRows<1||maximumRowBytes<1024||maximumRowBytes>32*1024*1024)throw new IllegalArgumentException("REPAIR_PROBE_LIMIT_INVALID");
        schema=Codec.decode(Codec.encode(table),TableSchema.class);schema.validate();
        Path base=root.toAbsolutePath().normalize();if(base.toString().contains(";")||base.toString().chars().anyMatch(c->c<32)||Files.isSymbolicLink(base))throw new IOException("REPAIR_PROBE_PATH_INVALID");
        boolean created=!Files.exists(base);Files.createDirectories(base);if(created)owner(base,true);if(base.toRealPath().toString().contains(";")||base.toRealPath().toString().chars().anyMatch(c->c<32))throw new IOException("REPAIR_PROBE_PATH_INVALID");directory=Files.createTempDirectory(base,"probe-");owner(directory,true);
        this.maximumBytes=maximumBytes;this.maximumRows=maximumRows;this.maximumRowBytes=maximumRowBytes;
        Connection opened=null;try{
            opened=JdbcConnections.open("org.h2.Driver","jdbc:h2:file:"+directory.resolve("probe")+";WRITE_DELAY=0;DB_CLOSE_ON_EXIT=FALSE;CACHE_SIZE=8192;MAX_MEMORY_ROWS=1000","","");
            try(Statement sql=opened.createStatement()){sql.execute("CREATE TABLE rmp_row(key_hash VARCHAR(64) PRIMARY KEY,key_data CLOB NOT NULL,canonical_data CLOB NOT NULL)");}opened.setAutoCommit(false);db=opened;protect();
        }catch(Exception failure){if(opened!=null)try{opened.close();}catch(Exception close){failure.addSuppressed(close);}try{cleanup();}catch(Exception close){failure.addSuppressed(close);}throw failure;}
    }
    private static void owner(Path path,boolean directory)throws IOException{try{Files.setPosixFilePermissions(path,PosixFilePermissions.fromString(directory?"rwx------":"rw-------"));}catch(UnsupportedOperationException ignored){}}
    private void protect()throws IOException{for(String name:Arrays.asList("probe.mv.db","probe.trace.db"))if(Files.exists(directory.resolve(name)))owner(directory.resolve(name),false);}
    private void check()throws Exception{if(closed)throw new IllegalStateException("REPAIR_PROBE_CLOSED");if(Thread.currentThread().isInterrupted())throw new InterruptedException("REPAIR_INTERRUPTED");}
    public void append(TargetRead.Row row)throws Exception{
        check();String canonical=RowValues.canonical(schema,row.values),key=RowValues.identity(schema,row.values,row.rowId);int canonicalBytes=canonical.getBytes(StandardCharsets.UTF_8).length;long size=canonicalBytes+(long)key.getBytes(StandardCharsets.UTF_8).length+128;
        if(canonicalBytes>maximumRowBytes||Math.addExact(bytes,size)>maximumBytes||rows>=maximumRows)throw new IllegalStateException("REPAIR_PROBE_QUOTA_EXCEEDED");
        try(PreparedStatement p=db.prepareStatement("INSERT INTO rmp_row VALUES(?,?,?)")){p.setString(1,Codec.sha256(key));p.setString(2,key);p.setString(3,canonical);p.executeUpdate();}catch(SQLException duplicate){if("23505".equals(duplicate.getSQLState()))throw new IllegalStateException("REPAIR_DUPLICATE_IDENTITY",duplicate);throw duplicate;}
        bytes+=size;rows++;if(++pending>=128)flush();
    }
    private void flush()throws Exception{check();if(Files.size(directory.resolve("probe.mv.db"))>maximumBytes)throw new IllegalStateException("REPAIR_PROBE_PHYSICAL_QUOTA_EXCEEDED");db.commit();pending=0;protect();}
    private static void digest(MessageDigest digest,String value){byte[] encoded=value.getBytes(StandardCharsets.UTF_8);digest.update(ByteBuffer.allocate(4).putInt(encoded.length).array());digest.update(encoded);}
    public Result finish()throws Exception{
        flush();MessageDigest digest=MessageDigest.getInstance("SHA-256");try(Statement sql=db.createStatement()){sql.setFetchSize(128);try(ResultSet data=sql.executeQuery("SELECT key_data,canonical_data FROM rmp_row ORDER BY key_hash")){while(data.next()){check();digest(digest,data.getString(1));digest(digest,data.getString(2));}}}
        Result result=new Result();result.rows=rows;StringBuilder hex=new StringBuilder();for(byte b:digest.digest())hex.append(String.format("%02x",b&255));result.root=hex.toString();return result;
    }
    public void reset()throws Exception{check();db.rollback();try(Statement sql=db.createStatement()){sql.executeUpdate("DELETE FROM rmp_row");}db.commit();bytes=0;rows=0;pending=0;}
    private void cleanup()throws IOException{for(String name:Arrays.asList("probe.mv.db","probe.trace.db","probe.lock.db")){Path path=directory.resolve(name);if(Files.isSymbolicLink(path))throw new IOException("REPAIR_PROBE_SYMLINK");Files.deleteIfExists(path);}Files.deleteIfExists(directory);}
    @Override public void close()throws Exception{if(closed)return;closed=true;try{db.close();}finally{cleanup();}}
}
