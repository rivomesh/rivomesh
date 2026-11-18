package io.github.rivomesh.cdc;

import java.sql.Connection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/** Includes checkpoint/dependency/receipt waits, before a target row cursor exists. */
final class RepairConnectionGuard implements AutoCloseable {
    private final ScheduledExecutorService timer;
    private final AtomicBoolean expired=new AtomicBoolean();
    private final Thread owner=Thread.currentThread();
    private final BooleanSupplier stopped;
    RepairConnectionGuard(Connection connection,long millis){this(connection,millis,()->false);}
    RepairConnectionGuard(Connection connection,long millis,BooleanSupplier stopped){
        this.stopped=stopped;
        timer=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"rivomesh-repair-connection");t.setDaemon(true);return t;});
        Runnable abort=()->{expired.set(true);try{connection.abort(Runnable::run);}catch(Exception ignored){try{connection.close();}catch(Exception ignoredClose){}}};
        timer.schedule(abort,millis,TimeUnit.MILLISECONDS);timer.scheduleWithFixedDelay(()->{if(interrupted()&&!expired.get())abort.run();},100,100,TimeUnit.MILLISECONDS);
    }
    private boolean interrupted(){return owner.isInterrupted()||stopped.getAsBoolean();}
    void verify(){if(interrupted())throw new IllegalStateException("REPAIR_INTERRUPTED");if(expired.get())throw new IllegalStateException("REPAIR_TIMED_OUT");}
    public void close(){timer.shutdownNow();}
}
