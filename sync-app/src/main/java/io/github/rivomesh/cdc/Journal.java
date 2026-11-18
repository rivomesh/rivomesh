package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.util.*;

public interface Journal extends AutoCloseable {
    final class Lease {
        public String owner;
        public long epoch;
        public Lease(){}
        public Lease(String owner,long epoch){this.owner=owner;this.epoch=epoch;}
    }
    final class Record {
        public long sequence;
        public SourceTransaction transaction;
    }
    final class Subscription {
        public String id,state;
        public long acknowledged,errorGeneration;
        public String errorCode;
    }
    final class PipelineState {
        public String state,errorCode;
        public long epoch,errorGeneration;
    }
    /** Internal, value-bearing snapshot. Not retained evidence or an execution credential. */
    final class FailureReview {
        public String pipelineId,generation,fingerprint,rawHash,normalizedHash;
        public long epoch,sequence,failureEpoch;
        public Subscription subscription;
        public SubscriberFailure failure;
        public SourceTransaction raw,normalized;
    }
    final class SnapshotState {
        public String scope,tableId,snapshotId,state;
        public int schemaVersion,nextPart,columnCount;
        public long rows;
        public boolean retained=true;
        public SourcePosition baseline;
    }
    final class SnapshotPart {
        public int part;
        public List<List<Cell>> rows;
    }
    void initialize(String fingerprint,String generation,List<String> subscribers)throws Exception;
    Lease claim(String owner,long leaseMillis)throws Exception;
    void heartbeat(Lease lease,long leaseMillis)throws Exception;
    void release(Lease lease)throws Exception;
    PipelineState pausePipeline()throws Exception;
    void resumePipeline(long expectedErrorGeneration)throws Exception;
    void failPipeline(Lease lease,String code)throws Exception;
    SourcePosition captured()throws Exception;
    void baseline(Lease lease,SourcePosition position)throws Exception;
    long append(Lease lease,SourceTransaction transaction)throws Exception;
    List<Record> read(String subscription,int maximumTransactions)throws Exception;
    void acknowledge(Lease lease,String subscription,long sequence)throws Exception;
    /** A normal nonempty SQL apply just returned after committing. Reconciliation uses acknowledge instead. */
    default void acknowledgeApplied(Lease lease,String subscription,long sequence)throws Exception{
        acknowledge(lease,subscription,sequence);
    }
    default void verifySkippedCheckpoint(String subscription,SinkAdapter.Checkpoint checkpoint)throws Exception{if(checkpoint.skipOperation!=null)throw new UnsupportedOperationException("SINK_SKIP_RECOVERY_REQUIRED");}
    List<Subscription> subscriptions()throws Exception;
    void pause(Lease lease,String subscription,String code)throws Exception;
    default void pauseFailure(Lease lease,String subscription,String code,SubscriberFailure failure)throws Exception{pause(lease,subscription,code);}
    default FailureReview inspectFailure(String subscription,long expectedErrorGeneration,long maximumBytes)throws Exception{throw new UnsupportedOperationException("FAILURE_REVIEW_UNSUPPORTED");}
    /** Optional retry diagnostics. Never changes run state, error generation, or a checkpoint. */
    default void retrying(Lease lease,String subscription,String code)throws Exception{}
    void resume(String subscription,long expectedErrorGeneration)throws Exception;
    long collect(Lease lease,int maximumTransactions)throws Exception;
    void saveSchema(Lease lease,TableSchema schema)throws Exception;
    List<TableSchema> schemas()throws Exception;
    SnapshotState beginSnapshot(Lease lease,String scope,TableSchema table,SourcePosition baseline)throws Exception;
    SnapshotState snapshot(String scope,String tableId)throws Exception;
    void appendSnapshot(Lease lease,SnapshotState snapshot,int part,List<List<Cell>> rows)throws Exception;
    void finishSnapshot(Lease lease,SnapshotState snapshot)throws Exception;
    void acknowledgeSnapshot(Lease lease,String subscription,SnapshotState snapshot)throws Exception;
    long collectSnapshots(Lease lease,int maximumParts)throws Exception;
    List<SnapshotPart> snapshotParts(String scope,String tableId,String snapshotId,int fromPart,int limit)throws Exception;
    List<SourcePosition> dependencies()throws Exception;
    Map<String,Object> status()throws Exception;
    @Override void close()throws Exception;
}
