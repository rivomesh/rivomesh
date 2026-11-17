package io.github.rivomesh.cdc;

import io.github.rivomesh.MysqlDuration;
import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns one connection from fence acquisition through EOF; never executes DDL or commits. */
final class JdbcTargetRead implements TargetRead {
    interface Guard {void verify(Connection connection)throws Exception;}
    private final Connection connection;
    private final TableSchema table;
    private final TargetProduct product;
    private final int rowBytes;
    private final long deadline;
    private final String sql;
    private final ScheduledExecutorService timer;
    private final AtomicBoolean expired=new AtomicBoolean();
    private volatile boolean closed;
    private PreparedStatement query;
    private ResultSet result;
    private Row buffered;
    private boolean finished;

    JdbcTargetRead(Connection c,TableSchema t,TargetProduct product,String sql,int rowBytes,long millis,Guard guard)throws Exception{
        connection=c;table=t;this.product=product;this.rowBytes=rowBytes;this.sql=sql;deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(millis);
        timer=Executors.newSingleThreadScheduledExecutor(r->{Thread thread=new Thread(r,"rivomesh-target-read-deadline");thread.setDaemon(true);return thread;});
        Thread owner=Thread.currentThread();timer.scheduleWithFixedDelay(()->{if(owner.isInterrupted()){expired.set(true);abort();}},100,100,TimeUnit.MILLISECONDS);
        timer.schedule(()->{expired.set(true);abort();},millis,TimeUnit.MILLISECONDS);
        try{
            c.setAutoCommit(false);guard.verify(c);verify();
            scan();
        }catch(Exception failure){close();if(expired.get())throw new IllegalStateException("VALIDATION_TARGET_READ_TIMED_OUT",failure);throw failure;}
    }
    private void scan()throws Exception{
        verify();finished=false;buffered=null;
        query=connection.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
        query.setFetchSize(product.mysqlDialect()?Integer.MIN_VALUE:128);
        query.setQueryTimeout((int)Math.max(1,TimeUnit.NANOSECONDS.toSeconds(deadline-System.nanoTime())));
        result=query.executeQuery();verify();
    }
    /** Repair keeps this connection and physical checkpoint lock through both scans and DML. */
    void finishScan()throws Exception{
        verify();if(!finished||buffered!=null)throw new IllegalStateException("REPAIR_SCAN_INCOMPLETE");
        if(result!=null){result.close();result=null;}if(query!=null){query.close();query=null;}
    }
    void rescan()throws Exception{finishScan();scan();}
    private void abort(){try{connection.abort(Runnable::run);}catch(Exception unsupported){try{connection.close();}catch(Exception ignored){}}}
    @Override public void verify(){
        if(expired.get()||System.nanoTime()-deadline>=0)throw new IllegalStateException("VALIDATION_TARGET_READ_TIMED_OUT");
        if(closed)throw new IllegalStateException("VALIDATION_TARGET_READ_CLOSED");
        if(Thread.currentThread().isInterrupted())throw new IllegalStateException("VALIDATION_TARGET_READ_INTERRUPTED");
    }
    private String text(int index,int maximum)throws Exception{
        try(Reader reader=result.getCharacterStream(index)){
            if(reader==null)return null;StringBuilder value=new StringBuilder();char[] chunk=new char[2048];
            for(int n;(n=reader.read(chunk))!=-1;){verify();if((long)value.length()+n>maximum)throw new IllegalStateException("VALIDATION_ROW_TOO_LARGE");value.append(chunk,0,n);}return value.toString();
        }
    }
    private String binary(int index,int maximum)throws Exception{
        try(InputStream stream=result.getBinaryStream(index)){
            if(stream==null)return null;ByteArrayOutputStream value=new ByteArrayOutputStream();byte[] chunk=new byte[2048];
            for(int n;(n=stream.read(chunk))!=-1;){verify();if((long)value.size()+n>(long)maximum*3/4)throw new IllegalStateException("VALIDATION_ROW_TOO_LARGE");value.write(chunk,0,n);}return Base64.getEncoder().encodeToString(value.toByteArray());
        }
    }
    private Cell cell(int index,TableSchema.Column column,int maximum)throws Exception{
        String value;
        switch(column.type){
            case BYTES:case GEOMETRY:value=binary(index,maximum);break;
            case STRING:case JSON:value=text(index,maximum);break;
            case DATE:java.sql.Date date=result.getDate(index);value=date==null?null:date.toString();break;
            case DATETIME:case TIMESTAMP:Timestamp timestamp=result.getTimestamp(index);value=timestamp==null?null:timestamp.toString();break;
            case TIME:
                if(product.mysqlDialect()||product==TargetProduct.H2)value=result.getString(index);
                else{BigDecimal micros=result.getBigDecimal(index);value=micros==null?null:MysqlDuration.format(micros.longValueExact());}break;
            case BOOLEAN:
                int jdbc=result.getMetaData().getColumnType(index);
                if(!product.mysqlDialect()&&(jdbc==Types.BOOLEAN||jdbc==Types.BIT)){boolean flag=result.getBoolean(index);value=result.wasNull()?null:Boolean.toString(flag);}
                else{value=result.getString(index);if(value!=null)value=value.toLowerCase(Locale.ROOT);}break;
            case DECIMAL:case UINT64:value=product==TargetProduct.DM&&DmStorage.textDecimal(column)?text(index,maximum):result.getString(index);break;
            default:value=result.getString(index);
        }
        return Cell.value(value);
    }
    private Row row()throws Exception{
        Row row=new Row();row.values=new ArrayList<>();long bytes=64L*table.columns.size();
        for(int i=0;i<table.columns.size();i++){
            verify();if(bytes>rowBytes)throw new IllegalStateException("VALIDATION_ROW_TOO_LARGE");Cell cell=cell(i+1,table.columns.get(i),(int)(rowBytes-bytes));row.values.add(cell);
            if(cell.value!=null)bytes+=cell.value.getBytes(StandardCharsets.UTF_8).length;
        }
        if(bytes>rowBytes)throw new IllegalStateException("VALIDATION_ROW_TOO_LARGE");
        if(table.keyless()){row.rowId=result.getString(table.columns.size()+1);if(row.rowId==null||!row.rowId.matches("[0-9a-f]{64}"))throw new IllegalStateException("ROW_IDENTITY_REQUIRED");}
        RowValues.canonical(table,row.values);return row;
    }
    @Override public List<Row> next(int maximum)throws Exception{
        verify();if(maximum<1||maximum>1000)throw new IllegalArgumentException("VALIDATION_TARGET_PAGE_INVALID");List<Row> rows=new ArrayList<>();long bytes=0;
        try{
            while(rows.size()<maximum){
                verify();Row next=buffered;buffered=null;if(next==null){if(finished)break;if(!result.next()){finished=true;break;}next=row();}
                long size=Codec.encode(next).getBytes(StandardCharsets.UTF_8).length;
                if(!rows.isEmpty()&&bytes+size>rowBytes){buffered=next;break;}rows.add(next);bytes+=size;
            }
            verify();return rows;
        }catch(Exception failure){abort();if(expired.get())throw new IllegalStateException("VALIDATION_TARGET_READ_TIMED_OUT",failure);throw failure;}
    }
    @Override public void close()throws Exception{
        if(closed)return;closed=true;timer.shutdownNow();
        // Connector/J can drain an unfinished streaming result during close. Abort instead.
        if(!finished)abort();
        Exception failure=null;
        try{if(result!=null)result.close();}catch(Exception e){if(finished)failure=e;}
        try{if(query!=null)query.close();}catch(Exception e){if(finished&&failure==null)failure=e;}
        try{if(!connection.isClosed())connection.rollback();}catch(Exception e){if(finished&&failure==null)failure=e;}
        try{connection.close();}catch(Exception e){if(finished&&failure==null)failure=e;}
        if(failure!=null)throw failure;
    }
}
