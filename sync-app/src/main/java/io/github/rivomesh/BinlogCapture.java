package io.github.rivomesh;

import io.github.rivomesh.Model.*;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/** A disconnect always creates a new client from the last durably committed transaction. */
public final class BinlogCapture implements AutoCloseable {
    private final Config config;
    private final StateStore store;
    private final Map<String, Table> tables;
    private final Map<Long, TableMapEventData> tableIds = new HashMap<>();
    private final List<Change> pending = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final BinaryLogClient client;
    private final MysqlSource source;
    private String pendingGtid;
    private volatile String committedGtids;
    private volatile long lastEventNanos = System.nanoTime();
    private volatile boolean closing;
    private String file;
    private volatile Position observed;

    public BinlogCapture(Config config, StateStore store, Map<String, Table> tables) throws Exception {
        this(config, store, tables, new MysqlSource(config));
    }
    public BinlogCapture(Config config, StateStore store, Map<String, Table> tables, MysqlSource source) throws Exception {
        this.config = config; this.store = store; this.tables = tables;
        this.source = source;
        Position position = store.captured(); file = position.file;
        committedGtids = position.gtidSet;
        client = new BinaryLogClient(source.endpoint().host, source.endpoint().port, config.source.username, config.source.password);
        client.setServerId(config.source.serverId);
        client.setGtidSet(position.gtidSet);
        client.setGtidSetFallbackToPurged(false);
        client.setUseBinlogFilenamePositionInGtidMode(false);
        client.setSSLMode(SSLMode.valueOf(config.source.sslMode));
        // Built-in reconnect can resume mid-transaction; application reconnect must use the durable offset.
        client.setKeepAlive(false);
        client.setHeartbeatInterval(1000);
        client.setConnectTimeout(10000);
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
                EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE);
        client.setEventDeserializer(deserializer);
        client.registerEventListener(event -> {
            if (failure.get() != null || closing) return;
            lastEventNanos = System.nanoTime();
            try {
                if (source.serverId() != 0 && client.getMasterServerId() != source.serverId())
                    throw new SyncFailure("SOURCE_CHANGED", "CAPTURE", true, "复制连接后端与 JDBC 发现的节点不同，重新发现主库");
                accept(event);
            } catch (Exception e) {
                failure.compareAndSet(null, e instanceof SyncFailure ? e : new SyncFailure("CAPTURE_ERROR", "CAPTURE", Sql.retryable(e),
                        "binlog 采集失败，未推进失败事务的 GTID", e, null, 0, pendingGtid));
            } catch (Throwable e) { failure.compareAndSet(null, e); }
        });
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override public void onCommunicationFailure(BinaryLogClient c, Exception e) { if (!closing) failure.compareAndSet(null, e); }
            @Override public void onEventDeserializationFailure(BinaryLogClient c, Exception e) { if (!closing) failure.compareAndSet(null, new IllegalStateException("binlog 解析失败，未推进位点", e)); }
            @Override public void onDisconnect(BinaryLogClient c) { if (!closing) failure.compareAndSet(null, new IOException("binlog 连接断开，将从持久化位点重连")); }
        });
    }

    public void start() throws Exception { client.connect(10000); }

    void accept(Event event) throws Exception {
        EventData data = event.getData();
        EventHeaderV4 header = event.getHeader();
        if (data instanceof RotateEventData) {
            if (!pending.isEmpty() || pendingGtid != null) throw new IllegalStateException("未提交事务中发生 binlog 切换");
            RotateEventData rotate = (RotateEventData) data;
            file = rotate.getBinlogFilename(); tableIds.clear();
            observed = new Position(file, rotate.getBinlogPosition());
            return;
        }
        Position next = new Position(file, header.getNextPosition());
        if (header.getEventType().name().contains("ANONYMOUS") || header.getEventType().name().contains("TAGGED"))
            throw new IllegalStateException("不支持 anonymous 或 tagged GTID 事务，未推进位点");
        if (data instanceof GtidEventData) {
            if (pendingGtid != null || !pending.isEmpty()) throw new IllegalStateException("前一 GTID 事务未提交");
            pendingGtid = ((GtidEventData) data).getGtid(); Gtids.parse(pendingGtid);
        } else if (data instanceof TableMapEventData) {
            TableMapEventData map = (TableMapEventData) data;
            tableIds.put(map.getTableId(), map);
            if (config.source.database.equals(map.getDatabase())) {
                Table table = tables.get(map.getTable());
                if (table == null || table.columns.size() != map.getColumnTypes().length) throw new IllegalStateException("未知表或表结构变化: " + map.getTable());
            }
        } else if (data instanceof WriteRowsEventData) {
            WriteRowsEventData rows = (WriteRowsEventData) data;
            Table table = table(rows.getTableId());
            if (table != null) {
                fullImage(table, rows.getIncludedColumns());
                for (Object[] row : rows.getRows()) add(new Change(table.name, false, table.binlogRow(row)));
            }
        } else if (data instanceof DeleteRowsEventData) {
            DeleteRowsEventData rows = (DeleteRowsEventData) data;
            Table table = table(rows.getTableId());
            if (table != null) {
                fullImage(table, rows.getIncludedColumns());
                for (Object[] row : rows.getRows()) add(new Change(table.name, true, table.binlogRow(row)));
            }
        } else if (data instanceof UpdateRowsEventData) {
            UpdateRowsEventData rows = (UpdateRowsEventData) data;
            Table table = table(rows.getTableId());
            if (table != null) {
                fullImage(table, rows.getIncludedColumnsBeforeUpdate()); fullImage(table, rows.getIncludedColumns());
                for (Map.Entry<java.io.Serializable[], java.io.Serializable[]> row : rows.getRows()) {
                    // Remove old keys too: UPDATE can change a primary key.
                    add(new Change(table.name, true, table.binlogRow(row.getKey())));
                    add(new Change(table.name, false, table.binlogRow(row.getValue())));
                }
            }
        } else if (data instanceof XidEventData) {
            commit(next);
        } else if (data instanceof QueryEventData) {
            QueryEventData query = (QueryEventData) data;
            String sql = query.getSql().trim();
            if (sql.equalsIgnoreCase("BEGIN")) {
                if (!pending.isEmpty()) throw new IllegalStateException("前一事务未提交");
            } else if (sql.equalsIgnoreCase("COMMIT")) commit(next);
            else if (sql.equalsIgnoreCase("ROLLBACK")) { pending.clear(); commit(next); }
            else {
                // Fail closed even when the default database differs: a statement can reference source tables
                // through qualified identifiers, comments, RENAME, or cross-database SQL.
                throw new SyncFailure("DDL_UNSUPPORTED", "CAPTURE", false, "检测到 DDL 或非 ROW SQL，未跳过失败事务", null, null, 0, pendingGtid);
            }
        } else if (header.getEventType().name().equals("TRANSACTION_PAYLOAD")) {
            throw new IllegalStateException("首版不支持压缩 binlog 事务，请关闭 binlog_transaction_compression");
        }
        // Only published after all preceding commits have reached GaussDB.
        if (next.offset > 0) observed = next;
    }

    private Table table(long id) {
        TableMapEventData map = tableIds.get(id);
        if (map == null) throw new IllegalStateException("缺少 TABLE_MAP: " + id);
        return config.source.database.equals(map.getDatabase()) ? tables.get(map.getTable()) : null;
    }
    private void fullImage(Table table, BitSet included) {
        if (included.cardinality() != table.columns.size()) throw new IllegalStateException("binlog 不是完整行镜像: " + table.name);
    }
    private void add(Change change) {
        // Updates have two operations; the limit applies to buffered operations, not SQL affected-row counts.
        if (pending.size() >= config.maxTransactionRows) throw new IllegalStateException("单事务操作数超过 maxTransactionRows，未推进位点");
        pending.add(change);
    }
    private void commit(Position next) throws Exception {
        if (pendingGtid == null) throw new IllegalStateException("提交事件缺少 GTID，不能自动续传");
        com.github.shyiko.mysql.binlog.GtidSet updated = Gtids.parse(committedGtids); updated.add(pendingGtid);
        next.gtidSet = updated.toString(); next.sourceUuid = source.identity() == null ? "" : source.identity();
        Transaction transaction = new Transaction(); transaction.position = next; transaction.gtid = pendingGtid; transaction.changes.addAll(pending);
        store.append(transaction); committedGtids = next.gtidSet; pending.clear(); pendingGtid = null;
    }

    public void check() throws Exception {
        Throwable e = failure.get();
        if (e instanceof Exception) throw (Exception) e;
        if (e != null) throw new IllegalStateException("binlog 采集失败", e);
        if (!client.isConnected()) throw new IOException("binlog 未连接");
        if (System.nanoTime() - lastEventNanos > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(config.source.binlogIdleTimeoutMillis))
            throw new SyncFailure("SOURCE_CONNECTION_LOST", "CAPTURE", true, "未收到 binlog 心跳，从持久化 GTID 重新连接");
    }

    public boolean reached(Position boundary) throws Exception {
        check(); return Gtids.contains(committedGtids, boundary.gtidSet);
    }
    @Override public void close() throws IOException { closing = true; client.disconnect(); }
}
