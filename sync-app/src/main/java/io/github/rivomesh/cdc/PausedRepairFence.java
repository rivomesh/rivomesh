package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineValidationBarrier;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Holds the journal job row: resume and worker ownership changes wait until repair releases it. */
public final class PausedRepairFence implements AutoCloseable {
    private final JdbcJournal journal;
    private final Connection connection;
    private final PipelineValidationBarrier.Certificate cut;
    private final ScheduledExecutorService timer;
    private final AtomicBoolean expired=new AtomicBoolean();
    private final long deadline;
    private boolean closed;
    public PausedRepairFence(JdbcJournal journal,PipelineValidationBarrier.Certificate cut,long millis)throws Exception{
        if(cut==null||millis<1000||millis>86400000)throw new IllegalArgumentException("REPAIR_FENCE_INVALID");
        this.journal=journal;this.cut=Codec.decode(Codec.encode(cut),PipelineValidationBarrier.Certificate.class);
        connection=journal.connect();deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(millis);
        timer=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"rivomesh-repair-fence-deadline");t.setDaemon(true);return t;});
        Thread owner=Thread.currentThread();timer.scheduleWithFixedDelay(()->{if(owner.isInterrupted()){expired.set(true);abort();}},100,100,TimeUnit.MILLISECONDS);
        timer.schedule(()->{expired.set(true);abort();},millis,TimeUnit.MILLISECONDS);
        try{connection.setAutoCommit(false);verify();}catch(Exception failure){try{close();}catch(Exception close){failure.addSuppressed(close);}throw failure;}
    }
    private void abort(){try{connection.abort(Runnable::run);}catch(Exception unsupported){try{connection.close();}catch(Exception ignored){}}}
    public void verify()throws Exception{
        if(closed||expired.get()||System.nanoTime()-deadline>=0||Thread.currentThread().isInterrupted())throw new IllegalStateException("REPAIR_FENCE_EXPIRED");
        Map<String,Object> state=journal.locked(connection);
        if(!Objects.equals(cut.generation,state.get("generation"))||!"PAUSED".equals(state.get("state"))||cut.epoch!=(long)state.get("epoch")||cut.errorGeneration!=(long)state.get("errorGeneration")||cut.sequence!=(long)state.get("next")-1||cut.sequence!=(long)state.get("normalized")||!Objects.equals(Codec.encode(cut.capturedPosition),state.get("captured")==null?"null":state.get("captured")))throw new IllegalStateException("VALIDATION_BARRIER_MOVED");
        if(expired.get()||connection.isClosed())throw new IllegalStateException("REPAIR_FENCE_EXPIRED");
    }
    @Override public void close()throws Exception{
        if(closed)return;closed=true;timer.shutdownNow();try{if(!connection.isClosed())connection.rollback();}finally{connection.close();}
    }
}
