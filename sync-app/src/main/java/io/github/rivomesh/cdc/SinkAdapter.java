package io.github.rivomesh.cdc;

import java.util.*;

public interface SinkAdapter extends AutoCloseable {
    enum Consistency { TRANSACTIONAL,VERSIONED_MATERIALIZATION,BATCH_PUBLICATION,AT_LEAST_ONCE_EVENTS }
    final class Checkpoint {
        public long epoch,sequence;
        public String transactionId,transactionHash;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String skipOperation;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String skipHash;
    }
    final class SnapshotCheckpoint {
        public String snapshotId,state;
        public int nextPart;
        public long rows;
    }
    String id();
    default boolean includes(TableSchema table){return true;}
    Consistency consistency();
    void prepare(long epoch,TableSchema table)throws Exception;
    /** Acquire the sink's local fence and return its durable, possibly journal-ahead checkpoint. */
    Checkpoint activate(long epoch)throws Exception;
    Checkpoint checkpoint()throws Exception;
    default TargetRead validationRead(TableSchema table,Checkpoint expected,long maximumMillis,int maximumRowBytes)throws Exception{
        throw new UnsupportedOperationException("VALIDATION_TARGET_PROTOCOL_UNSUPPORTED");
    }
    /** Each implementation must resolve uncertain commits through its checkpoint/idempotency protocol. */
    void apply(long epoch,long sequence,SourceTransaction transaction)throws Exception;
    SnapshotCheckpoint beginSnapshot(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception;
    void snapshotPart(long epoch,TableSchema table,String snapshotId,Journal.SnapshotPart part)throws Exception;
    void finishSnapshot(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception;
    Map<String,Object> diagnostics()throws Exception;
    @Override void close()throws Exception;
}
