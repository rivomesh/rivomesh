package io.github.rivomesh;

/** No row payload or SQL literal is copied into error context. */
public final class SyncFailure extends Exception {
    public final String code;
    public final String stage;
    public final boolean retryable;
    public final String table;
    public final long sequence;
    public final String gtid;

    public SyncFailure(String code, String stage, boolean retryable, String message) {
        this(code, stage, retryable, message, null, null, 0, null);
    }
    public SyncFailure(String code, String stage, boolean retryable, String message, Throwable cause,
                       String table, long sequence, String gtid) {
        super(message, cause); this.code = code; this.stage = stage; this.retryable = retryable;
        this.table = table; this.sequence = sequence; this.gtid = gtid;
    }
}
