package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.SourceAdapter;
import io.github.rivomesh.cdc.TableSchema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Holds publication tables stable until a flushed pgoutput message has reached the selected targets. */
final class PostgresConsistentRead implements SourceAdapter.ConsistentRead {
    private final Map<String,TableSchema> tables=new TreeMap<>();
    private final long gateDeadline,readDeadline;
    private Connection gate,read;
    private SourcePosition baseline;
    private boolean closed;

    PostgresConsistentRead(PostgresqlCdcSource source,PostgresSourcePosition positions,
                           List<TableSchema> expected,long gateMillis,long readMillis)throws Exception{
        if(expected==null||expected.isEmpty()||gateMillis<1000||gateMillis>120000
                ||readMillis<gateMillis||readMillis>86400000)
            throw new IllegalArgumentException("CONSISTENT_READ_LIMIT_INVALID");
        for(TableSchema table:expected){
            table.validate();
            if(tables.put(table.id,table)!=null)
                throw new IllegalArgumentException("CONSISTENT_READ_TABLE_DUPLICATE");
        }
        long opened=System.nanoTime();
        gateDeadline=opened+TimeUnit.MILLISECONDS.toNanos(gateMillis);
        readDeadline=opened+TimeUnit.MILLISECONDS.toNanos(readMillis);
        String relations=tables.values().stream()
                .map(table->"ONLY "+Sql.pg(table.database)+"."+Sql.pg(table.name))
                .sorted().collect(Collectors.joining(","));
        try{
            gate=source.sql();
            read=source.sql();
            check();
            openTransaction(gate,false,gateDeadline);
            try(Statement sql=gate.createStatement()){
                sql.execute("LOCK TABLE "+relations+" IN SHARE MODE");
            }
            openTransaction(read,true,readDeadline);
            try(Statement sql=read.createStatement()){
                sql.execute("LOCK TABLE "+relations+" IN ACCESS SHARE MODE");
                PostgresqlCdcSource.configureSnapshot(sql);
            }
            Map<String,String> reviewed=new TreeMap<>(),current=new TreeMap<>();
            tables.forEach((key,table)->reviewed.put(key,Codec.encode(table)));
            for(TableSchema table:source.inspectTables(read).values())current.put(table.id,Codec.encode(table));
            if(!reviewed.equals(current))throw unavailable("SOURCE_SCHEMA_CHANGED_BEFORE_VALIDATION",null);
            verify();
            try(var marker=gate.prepareStatement("SELECT pg_catalog.pg_logical_emit_message(false,?::text,?::text,true)::text")){
                marker.setString(1,positions.validationMessagePrefix());
                marker.setString(2,PgOutput.VALIDATION_MESSAGE);
                try(ResultSet row=marker.executeQuery()){
                    if(!row.next())throw unavailable("POSTGRESQL_COMMON_CUT_MARKER_UNAVAILABLE",null);
                    baseline=positions.at(PostgresSourcePosition.parse(row.getString(1)));
                }
            }
            verify();
        }catch(Exception failure){
            try{close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}
            if(failure instanceof SQLException sql&&"42501".equals(sql.getSQLState()))
                throw unavailable("POSTGRESQL_COMMON_CUT_PRIVILEGE_REQUIRED",sql);
            throw failure;
        }
    }

    private static void openTransaction(Connection connection,boolean readOnly,long deadline)throws SQLException{
        long remaining=Math.max(1,TimeUnit.NANOSECONDS.toMillis(deadline-System.nanoTime()));
        connection.setNetworkTimeout(Runnable::run,(int)Math.min(remaining,15000));
        connection.setReadOnly(readOnly);
        if(readOnly)connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.setAutoCommit(false);
        try(Statement sql=connection.createStatement()){
            sql.execute("SET LOCAL transaction_timeout="+remaining);
        }
    }

    private void check()throws Exception{
        if(closed)throw unavailable("SOURCE_CONSISTENT_READ_CLOSED",null);
        if(Thread.currentThread().isInterrupted())throw new InterruptedException("VALIDATION_INTERRUPTED");
        long now=System.nanoTime();
        if(now-readDeadline>=0)throw unavailable("SOURCE_CONSISTENT_READ_TIMED_OUT",null);
        if(gate!=null&&now-gateDeadline>=0)throw unavailable("SOURCE_WRITE_GATE_TIMED_OUT",null);
    }

    private static void verifyPrimary(Connection connection)throws Exception{
        try(Statement sql=connection.createStatement();
            ResultSet row=sql.executeQuery("SELECT pg_catalog.pg_is_in_recovery()")){
            if(!row.next()||row.getBoolean(1))throw unavailable("SOURCE_VALIDATION_BACKEND_CHANGED",null);
        }
    }

    @Override public SourcePosition baseline(){return baseline;}

    @Override public void verify()throws Exception{
        check();
        if(gate!=null)verifyPrimary(gate);
        verifyPrimary(read);
        check();
    }

    @Override public void releaseWrites()throws Exception{
        verify();
        if(gate==null)return;
        gate.rollback();
        gate.close();
        gate=null;
    }

    @Override public SourceAdapter.Snapshot table(TableSchema table)throws Exception{
        check();
        TableSchema expected=tables.get(table.id);
        if(gate!=null||expected==null||!Codec.encode(table).equals(Codec.encode(expected)))
            throw unavailable("VALIDATION_SCHEMA_MISMATCH",null);
        return PostgresqlCdcSource.snapshotRows(read,expected,baseline,false);
    }

    @Override public void close()throws Exception{
        if(closed)return;
        closed=true;
        try{if(gate!=null)gate.close();}
        finally{if(read!=null)read.close();}
    }

    private static SyncFailure unavailable(String code,Throwable cause){
        return new SyncFailure(code,"VALIDATE",false,"PostgreSQL could not retain the reviewed validation cut.",
                cause,null,0,null);
    }
}
