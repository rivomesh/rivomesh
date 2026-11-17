package io.github.rivomesh;

import io.github.rivomesh.Model.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SyncEngine implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SyncEngine.class);
    private final Config config;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile Thread runner;
    private volatile String stage = "INITIALIZE";
    private long lastSourceCheck;

    public SyncEngine(Config config) { config.validate(); this.config = config; }
    public void run() throws Exception {
        runner = Thread.currentThread(); int attempts = 0; boolean initialized = false;
        try {
            while (running.get()) {
                // A new immutable owner token for every session fences callbacks left behind by a failed connection.
                StateStore store = new StateStore(config);
                boolean claimed = false;
                try {
                    stage = "INITIALIZE";
                    if (!initialized) { store.initialize(); initialized = true; }
                    if (!store.exists()) {
                        stage = "SOURCE";
                        MysqlSource source = new SourceSelector(config).select(null, "");
                        Position baseline = source.position();
                        List<TableProgress> tables = source.discover();
                        Position discovery = source.position();
                        store.create(source.identity(), baseline, discovery.gtidSet, tables);
                    }
                    store.validateConfig(); store.claim(); claimed = true;
                    try (LeaseHeartbeat heartbeat = new LeaseHeartbeat(store, config)) { runSession(store, heartbeat); }
                    attempts = 0;
                } catch (StateStore.JobPausedException e) {
                    waitFor(config.pollIntervalMillis);
                } catch (StateStore.JobBusyException e) {
                    waitFor(config.pollIntervalMillis);
                } catch (StateStore.LeaseLostException e) {
                    LOG.warn("节点 {} 已失去任务所有权，重新等待接管", config.nodeId);
                    waitFor(config.pollIntervalMillis);
                } catch (Exception e) {
                    if (!running.get()) return;
                    if (!claimed && !Sql.retryable(e)) throw e;
                    try { if (claimed) store.recordFailure(e, stage); }
                    catch (StateStore.LeaseLostException lost) { LOG.warn("错误发生后租约已转移，旧会话不修改共享状态"); }
                    catch (Exception record) { e.addSuppressed(record); LOG.warn("暂时无法持久化错误状态: {}", record.getClass().getSimpleName()); }
                    if (!Sql.retryable(e)) {
                        // Existing jobs remain paused across process restarts and across all standby nodes.
                        LOG.error("任务 {} 暂停，阶段 {}，类型 {}；修复后执行 resume", config.jobId, stage, e.getClass().getSimpleName());
                        waitFor(config.pollIntervalMillis);
                    } else {
                        long delay = retryDelay(++attempts);
                        LOG.warn("任务 {} 暂时中断，阶段 {}，{} ms 后重新发现源并从 GTID 恢复", config.jobId, stage, delay);
                        waitFor(delay);
                    }
                } finally {
                    try { store.release(); } catch (Exception e) { LOG.warn("未能释放租约，将由超时接管"); }
                }
            }
        } finally { runner = null; }
    }
    private long retryDelay(int attempts) {
        long multiplier = 1L << Math.min(attempts - 1, 16);
        long ceiling = Math.min(config.maxRetryDelayMillis, config.retryDelayMillis * multiplier);
        return ceiling <= 1 ? ceiling : ceiling / 2 + ThreadLocalRandom.current().nextLong(ceiling - ceiling / 2 + 1);
    }
    private void waitFor(long millis) throws InterruptedException {
        try { if (running.get()) Thread.sleep(millis); }
        catch (InterruptedException e) { if (running.get()) throw e; }
    }
    private void sourceHealth(MysqlSource source, StateStore store, LeaseHeartbeat heartbeat, BinlogCapture capture) throws Exception {
        heartbeat.check(); capture.check();
        if (System.nanoTime() - lastSourceCheck < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(config.source.healthCheckMillis)) return;
        Position checkpoint = store.captured();
        MysqlSource selected = new SourceSelector(config).select(checkpoint, checkpoint.requiredGtidSet);
        if (!source.identity().equals(selected.identity())) throw new SyncFailure("SOURCE_CHANGED", "SOURCE", true, "主库角色变化，从共享 GTID 重新建立采集连接");
        lastSourceCheck = System.nanoTime();
    }
    private void runSession(StateStore store, LeaseHeartbeat heartbeat) throws Exception {
        stage = "SOURCE";
        Position checkpoint = store.captured();
        MysqlSource source = new SourceSelector(config).select(checkpoint, checkpoint.requiredGtidSet);
        store.validateIdentity(source.identity(), source.discover());
        heartbeat.check(); store.activate(source.identity(), source.endpoint().toString());
        LOG.info("节点 {} 接管任务 {}，源 {}，GTID {}", config.nodeId, config.jobId, source.endpoint(), checkpoint.gtidSet);
        List<TableProgress> progress = store.tables(); Map<String, Table> tables = new LinkedHashMap<>();
        stage = "PREPARE";
        for (TableProgress table : progress) { heartbeat.check(); tables.put(table.table.name, table.table); store.prepare(table); }
        lastSourceCheck = 0;
        try (BinlogCapture capture = new BinlogCapture(config, store, tables, source)) {
            stage = "CAPTURE"; capture.start();
            if ("SNAPSHOT".equals(store.phase())) {
                for (TableProgress table : progress) {
                    while (running.get() && !table.done) {
                        stage = "SNAPSHOT"; sourceHealth(source, store, heartbeat, capture);
                        List<List<String>> rows = source.readChunk(table);
                        String readWatermark = source.position().gtidSet;
                        sourceHealth(source, store, heartbeat, capture);
                        try { store.commitChunk(table, rows, readWatermark); }
                        catch (Exception e) { throw new SyncFailure("SNAPSHOT_ERROR", "SNAPSHOT", Sql.retryable(e), "全量分块未提交", e, table.table.name, 0, null); }
                        LOG.info("全量 {} 已提交 {} 行，完成={}", table.table.name, table.copiedRows, table.done);
                    }
                    if (!running.get()) return;
                }
                Position boundary = source.position();
                while (running.get() && !capture.reached(boundary)) { sourceHealth(source, store, heartbeat, capture); waitFor(config.pollIntervalMillis); }
                if (!running.get()) return;
                store.beginCatchup(); LOG.info("全量完成，回放至 GTID 边界 {}", boundary.gtidSet);
            }
            String previous = store.phase();
            while (running.get()) {
                sourceHealth(source, store, heartbeat, capture); stage = "APPLY";
                boolean applied = store.applyNext(tables);
                String phase = store.phase();
                if (!phase.equals(previous)) { LOG.info("任务 {} 进入 {}", config.jobId, phase); previous = phase; }
                if (!applied) waitFor(config.pollIntervalMillis);
            }
        }
    }
    @Override public void close() {
        running.set(false); Thread thread = runner;
        if (thread != null && thread != Thread.currentThread()) {
            thread.interrupt();
            try { thread.join(10000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
