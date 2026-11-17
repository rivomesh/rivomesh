package io.github.rivomesh;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** Continues renewing while the owner is blocked in a source query or preparing a long snapshot chunk. */
final class LeaseHeartbeat implements AutoCloseable {
    private final ScheduledExecutorService executor;
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    LeaseHeartbeat(StateStore store, Config config) {
        executor = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "sync-lease"); t.setDaemon(true); return t; });
        executor.scheduleWithFixedDelay(() -> {
            if (failure.get() != null) return;
            try { store.heartbeat(); } catch (Exception e) { failure.compareAndSet(null, e); }
        }, config.heartbeatMillis, config.heartbeatMillis, TimeUnit.MILLISECONDS);
    }
    void check() throws Exception { if (failure.get() != null) throw failure.get(); }
    @Override public void close() {
        executor.shutdownNow();
        try { executor.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
