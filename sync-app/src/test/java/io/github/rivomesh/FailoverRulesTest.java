package io.github.rivomesh;

import java.sql.SQLException;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FailoverRulesTest {
    private static final String A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    @Test void gtidUnionNeverDiscardsPreviouslyReadTransactions() {
        String combined = Gtids.union(A + ":1-3:7-9", A + ":2-8," + B + ":1-4");
        assertTrue(Gtids.contains(combined, A + ":1-9," + B + ":1-4"));
        assertTrue(Gtids.contains(A + ":1-9," + B + ":1-4", combined));
        assertFalse(Gtids.contains(A + ":1-8", A + ":9"));
        assertThrows(IllegalArgumentException.class, () -> Gtids.parse(A + ":tag:1"));
    }
    private SourceSelector.Probe primary(String uuid, String executed) {
        SourceSelector.Probe p = new SourceSelector.Probe(); p.uuid = uuid; p.endpoint = new Config.Endpoint("test",3306);
        p.writable = true; p.executed = executed; p.purged = ""; return p;
    }
    @Test void promotionRequiresCapturedAndSnapshotDependencyGtids() throws Exception {
        SourceSelector.Probe p = primary(B, A + ":1-5");
        SyncFailure lag = assertThrows(SyncFailure.class, () -> SourceSelector.choose(Collections.singletonList(p), "AUTO", A + ":1-4", A + ":1-6"));
        assertEquals("SOURCE_LAGGING", lag.code); assertTrue(lag.retryable);
        p.executed = A + ":1-6," + B + ":1";
        assertSame(p, SourceSelector.choose(Collections.singletonList(p), "AUTO", A + ":1-4", A + ":1-6"));
        p.purged = A + ":1-5";
        SyncFailure gap = assertThrows(SyncFailure.class, () -> SourceSelector.choose(Collections.singletonList(p), "AUTO", A + ":1-4", A + ":1-6"));
        assertEquals("SOURCE_LOG_GAP", gap.code); assertFalse(gap.retryable);
        // A new bootstrap skips past old purged history by establishing a fresh baseline.
        assertSame(p, SourceSelector.choose(Collections.singletonList(p), "AUTO", null, ""));
    }
    @Test void multipleWritersAndUnhealthyReplicationNeverGetSelected() {
        SourceSelector.Probe a = primary(A, ""), b = primary(B, "");
        assertEquals("SOURCE_MULTIPLE_WRITERS", assertThrows(SyncFailure.class, () -> SourceSelector.choose(Arrays.asList(a,b), "AUTO", "", "")).code);
        a.replicationHealthy = false;
        assertSame(b, assertDoesNotThrow(() -> SourceSelector.choose(Arrays.asList(a,b), "ASYNC", "", "")));
        b.groupMember = true; b.groupPrimary = true; b.groupOnline = false;
        assertThrows(SyncFailure.class, () -> SourceSelector.choose(Collections.singletonList(b), "GROUP_REPLICATION", "", ""));
        b.groupOnline = true;
        assertSame(b, assertDoesNotThrow(() -> SourceSelector.choose(Collections.singletonList(b), "GROUP_REPLICATION", "", "")));
        b.singlePrimary = false;
        assertThrows(SyncFailure.class, () -> SourceSelector.choose(Collections.singletonList(b), "AUTO", "", ""));
    }
    @Test void sqlErrorsAreClassifiedWithoutSkippingTransactions() {
        assertTrue(Sql.retryable(new SQLException("deadlock", "40001")));
        assertTrue(Sql.retryable(new SQLException("network", "08006")));
        assertTrue(Sql.retryable(new SQLException("local journal ownership transition","90020",90020)));
        assertFalse(Sql.retryable(new SQLException("bad row", "23514")));
        assertFalse(Sql.retryable(new SQLException("overflow", "22003")));
        assertFalse(Sql.retryable(new SQLException("syntax", "42601")));
        assertFalse(Sql.retryable(assertThrows(SQLException.class,()->JdbcConnections.open("missing.plugin.Driver","jdbc:missing:db","user","password"))));
        assertFalse(Sql.retryable(new IllegalStateException("decode", new java.io.IOException())));
        assertTrue(Sql.retryable(new Exception("source wrapper",new SyncFailure("SOURCE_REPLICATION_ERROR","SOURCE",true,"waiting for repair"))));
        assertFalse(Sql.retryable(new Exception("source wrapper",new SyncFailure("SOURCE_LOG_GAP","SOURCE",false,"logs missing"))));
        assertTrue(Sql.retryable(new com.github.shyiko.mysql.binlog.network.ServerException("Server shutdown in progress",1053,"08S01")));
        assertTrue(Sql.retryable(new com.github.shyiko.mysql.binlog.network.ServerException("Server is offline",3032,"HY000")));
        assertTrue(Sql.retryable(new com.github.shyiko.mysql.binlog.network.ServerException("A replica with the same server_uuid/server_id as this replica",1236,"HY000")));
        assertFalse(Sql.retryable(new com.github.shyiko.mysql.binlog.network.ServerException("Missing purged GTIDs",1236,"HY000")));
    }
}
