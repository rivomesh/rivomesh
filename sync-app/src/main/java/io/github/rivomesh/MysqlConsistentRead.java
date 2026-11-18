package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** Bounded source write gate and one InnoDB MVCC view for all declared databases. */
final class MysqlConsistentRead implements SourceAdapter.ConsistentRead {
    private final MysqlCdcSource source;
    private final Map<String,TableSchema> tables=new LinkedHashMap<>();
    private final ScheduledExecutorService deadlines;
    private final AtomicBoolean closed=new AtomicBoolean();
    private final long readDeadline;
    private final AtomicReference<String> expiration=new AtomicReference<>();
    private final String gateSession=UUID.randomUUID().toString(), readSession=UUID.randomUUID().toString();
    private volatile Connection gate,read;
    private volatile boolean writesReleased;
    private ScheduledFuture<?> gateTimer;
    private SourcePosition baseline;
    private Cursor active;

    MysqlConsistentRead(MysqlCdcSource source,List<TableSchema> expected,long gateMillis,long readMillis)throws Exception{
        if(expected==null||expected.isEmpty()||gateMillis<1000||gateMillis>120000||readMillis<gateMillis||readMillis>86400000)throw new IllegalArgumentException("CONSISTENT_READ_LIMIT_INVALID");
        this.source=source;for(TableSchema table:expected){table.validate();if(tables.put(table.id,Codec.decode(Codec.encode(table),TableSchema.class))!=null)throw new IllegalArgumentException("CONSISTENT_READ_TABLE_DUPLICATE");}
        readDeadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(readMillis);
        deadlines=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"rivomesh-source-read-deadline");t.setDaemon(true);return t;});
        Thread owner=Thread.currentThread();deadlines.scheduleWithFixedDelay(()->{if(owner.isInterrupted())expire("VALIDATION_CANCELLED");},100,100,TimeUnit.MILLISECONDS);
        try{
            deadlines.schedule(()->expire("SOURCE_CONSISTENT_READ_TIMED_OUT"),Math.max(1,TimeUnit.NANOSECONDS.toMillis(readDeadline-System.nanoTime())),TimeUnit.MILLISECONDS);
            gate=source.readConnection();check();
            sessionTimeout(gate,gateMillis);
            markSession(gate,gateSession);
            gateTimer=deadlines.schedule(()->{if(!writesReleased)expire("SOURCE_WRITE_GATE_TIMED_OUT");},gateMillis,TimeUnit.MILLISECONDS);
            try(Statement sql=gate.createStatement()){sql.setQueryTimeout((int)Math.max(1,Math.min(10,gateMillis/1000)));sql.execute("FLUSH TABLES WITH READ LOCK");}
            check();
            List<TableSchema> actual=source.discover();
            if(actual.size()!=tables.size())throw failure("SOURCE_SCHEMA_CHANGED_BEFORE_VALIDATION");
            for(TableSchema table:actual)if(!Codec.encode(table).equals(Codec.encode(tables.get(table.id))))throw failure("SOURCE_SCHEMA_CHANGED_BEFORE_VALIDATION");
            read=source.readConnection();sessionTimeout(read,readMillis);read.setAutoCommit(false);
            markSession(read,readSession);
            try(Statement sql=read.createStatement()){
                sql.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");sql.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                // Pin table definitions in this transaction before releasing the source gate.
                for(TableSchema table:tables.values())try(ResultSet ignored=sql.executeQuery("SELECT "+columns(table)+" FROM "+qualified(table)+" LIMIT 0")){check();}
            }
            baseline=source.position(gate);verify();
        }catch(Exception error){close();if(expiration.get()!=null)throw failure(expiration.get());throw error;}
    }
    private static void sessionTimeout(Connection c,long millis)throws SQLException{
        try(Statement sql=c.createStatement()){sql.execute("SET SESSION wait_timeout="+(TimeUnit.MILLISECONDS.toSeconds(millis)+3));}
    }
    private static void markSession(Connection connection,String marker)throws SQLException{
        try(PreparedStatement sql=connection.prepareStatement("SET @rivomesh_consistent_view_session=?")){
            sql.setString(1,marker);sql.execute();
        }
    }
    private void verifySession(Connection connection,String marker)throws Exception{
        try(Statement sql=connection.createStatement()){
            sql.setQueryTimeout(3);
            try(ResultSet row=sql.executeQuery("SELECT @@global.server_uuid,@@global.read_only,"
                    +"@@global.super_read_only,@rivomesh_consistent_view_session")){
                if(!row.next()||!Objects.equals(baseline.getPayload().get("serverUuid"),row.getString(1))
                        ||row.getBoolean(2)||row.getBoolean(3))throw failure("SOURCE_VALIDATION_BACKEND_CHANGED");
                if(!marker.equals(row.getString(4)))throw failure("SOURCE_VALIDATION_SESSION_CHANGED");
            }
        }
    }
    private static String columns(TableSchema table){return MysqlCdcSource.readColumns(table);}
    private static String qualified(TableSchema table){return Sql.mysql(table.database)+"."+Sql.mysql(table.name);}
    private static SyncFailure failure(String code){return new SyncFailure(code,"VALIDATE",false,"The source validation view is unavailable; retain the paused state and create a new validation run.");}
    private void check()throws Exception{
        if(expiration.get()!=null)throw failure(expiration.get());
        if(closed.get())throw failure("SOURCE_CONSISTENT_READ_CLOSED");
        if(Thread.currentThread().isInterrupted())throw new InterruptedException("VALIDATION_INTERRUPTED");
        if(System.nanoTime()-readDeadline>=0){expire("SOURCE_CONSISTENT_READ_TIMED_OUT");throw failure(expiration.get());}
    }
    private static void abort(Connection c){if(c!=null)try{c.abort(Runnable::run);}catch(Exception ignored){try{c.close();}catch(Exception suppressed){/* server-side idle timeout is also bounded */}}}
    private void expire(String code){if(!closed.get()&&expiration.compareAndSet(null,code)){abort(gate);abort(read);}}
    @Override public SourcePosition baseline(){if(baseline==null)throw new IllegalStateException("SOURCE_BASELINE_UNAVAILABLE");return baseline;}
    @Override public synchronized void releaseWrites()throws Exception{
        check();if(writesReleased)return;
        verify();
        try(Statement sql=gate.createStatement()){sql.setQueryTimeout(2);sql.execute("UNLOCK TABLES");}
        verifySession(gate,gateSession);
        check();writesReleased=true;gateTimer.cancel(false);
        sessionTimeout(gate,Math.max(1000,TimeUnit.NANOSECONDS.toMillis(readDeadline-System.nanoTime())));
    }
    @Override public synchronized void verify()throws Exception{
        check();
        verifySession(gate,gateSession);
        if(active==null)verifySession(read,readSession);
        check();
    }
    @Override public synchronized SourceAdapter.Snapshot table(TableSchema table)throws Exception{
        check();if(active!=null)throw failure("SOURCE_VALIDATION_CURSOR_ACTIVE");
        TableSchema expected=tables.get(table.id);if(expected==null||!Codec.encode(expected).equals(Codec.encode(table)))throw failure("SOURCE_VALIDATION_SCHEMA_MISMATCH");
        verifySession(read,readSession);
        PreparedStatement query=read.prepareStatement("SELECT "+columns(table)+" FROM "+qualified(table),ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        try{query.setFetchSize(Integer.MIN_VALUE);query.setQueryTimeout((int)Math.max(1,TimeUnit.NANOSECONDS.toSeconds(readDeadline-System.nanoTime())));ResultSet rows=query.executeQuery();active=new Cursor(expected,query,rows);return active;}
        catch(Exception error){query.close();throw error;}
    }
    private final class Cursor implements SourceAdapter.Snapshot {
        final TableSchema table;final PreparedStatement statement;final ResultSet result;boolean finished,disposed;
        Cursor(TableSchema table,PreparedStatement statement,ResultSet result){this.table=table;this.statement=statement;this.result=result;}
        public SourcePosition baseline(){return MysqlConsistentRead.this.baseline();}
        public List<List<Cell>> next(int maximum)throws Exception{
            check();if(disposed||maximum<1||maximum>10000)throw new IllegalArgumentException("SNAPSHOT_READ_INVALID");
            List<List<Cell>> rows=new ArrayList<>();while(rows.size()<maximum){check();if(!result.next()){finished=true;break;}List<Cell> row=new ArrayList<>();for(int i=0;i<table.columns.size();i++)row.add(MysqlCdcSource.jdbc(table,i,result,i+1));rows.add(row);}return rows;
        }
        public void close()throws Exception{
            if(disposed)return;disposed=true;
            // Early cursor close must not drain an unbounded source result in the background.
            if(!finished)expire("SOURCE_VALIDATION_CURSOR_INCOMPLETE");
            try{result.close();}catch(SQLException cleanup){if(finished)throw cleanup;}
            finally{try{statement.close();}catch(SQLException cleanup){if(finished)throw cleanup;}finally{active=null;}}
        }
    }
    @Override public synchronized void close(){
        if(!closed.compareAndSet(false,true))return;
        deadlines.shutdownNow();
        if(active!=null){abort(read);try{active.close();}catch(Exception ignored){/* connection already aborted */}}
        if(read!=null)try{if(!read.isClosed())read.rollback();}catch(Exception ignored){}finally{try{read.close();}catch(Exception ignored){}}
        if(!writesReleased)abort(gate);
        else if(gate!=null)try{gate.close();}catch(Exception ignored){}
    }
}
