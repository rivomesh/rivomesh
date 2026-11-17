package io.github.rivomesh.cdc;

import java.sql.Connection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Includes checkpoint/dependency/receipt waits, before a target row cursor exists. */
final class RepairConnectionGuard implements AutoCloseable {
    private final ScheduledExecutorService timer;
    private final AtomicBoolean expired=new AtomicBoolean();
    private final Thread owner=Thread.currentThread();
    RepairConnectionGuard(Connection connection,long millis){
        timer=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"rivomesh-repair-connection");t.setDaemon(true);return t;});
        Runnable abort=()->{expired.set(true);try{connection.abort(Runnable::run);}catch(Exception ignored){try{connection.close();}catch(Exception ignoredClose){}}};
        timer.schedule(abort,millis,TimeUnit.MILLISECONDS);timer.scheduleWithFixedDelay(()->{if(owner.isInterrupted())abort.run();},100,100,TimeUnit.MILLISECONDS);
    }
    void verify(){if(owner.isInterrupted())throw new IllegalStateException("REPAIR_INTERRUPTED");if(expired.get())throw new IllegalStateException("REPAIR_TIMED_OUT");}
    public void close(){timer.shutdownNow();}
}
