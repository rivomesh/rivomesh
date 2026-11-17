package io.github.rivomesh.cdc;

/** Durable publication receipts. Broker confirmation always precedes these checkpoints. */
public interface PublicationStore {
    String pipelineId();
    String generation()throws Exception;
    void check(long epoch)throws Exception;
    void identity(long epoch,String identity)throws Exception;
    default String progress()throws Exception{return null;}
    default void progress(long epoch,String expected,String confirmed)throws Exception{throw new UnsupportedOperationException("PUBLICATION_TRANSPORT_PROGRESS_UNSUPPORTED");}
    SinkAdapter.Checkpoint activate(long epoch)throws Exception;
    SinkAdapter.Checkpoint checkpoint()throws Exception;
    boolean schemaPrepared(TableSchema table)throws Exception;
    void schemaPrepared(long epoch,TableSchema table)throws Exception;
    void committed(long epoch,long sequence,String transactionId,String contentHash)throws Exception;
    SinkAdapter.SnapshotCheckpoint snapshot(String tableId)throws Exception;
    void begin(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception;
    void part(long epoch,TableSchema table,String snapshotId,int part,int rows,String hash)throws Exception;
    void finish(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception;
}
