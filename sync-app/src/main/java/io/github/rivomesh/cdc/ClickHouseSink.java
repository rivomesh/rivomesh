package io.github.rivomesh.cdc;

import java.util.*;

/** Versioned materialization; publication receipts do not imply cross-row SQL transactions. */
public final class ClickHouseSink implements SinkAdapter {
    private final MessageSink delegate;
    public ClickHouseSink(ClickHouseMaterializer.Settings settings,PublicationStore store,int maximumMessageBytes)throws Exception{
        delegate=new MessageSink(settings.subscriptionId,settings.mappings,store,()->new ClickHouseMaterializer(settings),maximumMessageBytes);
    }
    @Override public String id(){return delegate.id();}
    @Override public boolean includes(TableSchema table){return delegate.includes(table);}
    @Override public Consistency consistency(){return Consistency.VERSIONED_MATERIALIZATION;}
    @Override public void prepare(long epoch,TableSchema table)throws Exception{delegate.prepare(epoch,table);}
    @Override public Checkpoint activate(long epoch)throws Exception{return delegate.activate(epoch);}
    @Override public Checkpoint checkpoint()throws Exception{return delegate.checkpoint();}
    @Override public void apply(long epoch,long sequence,SourceTransaction tx)throws Exception{delegate.apply(epoch,sequence,tx);}
    @Override public SnapshotCheckpoint beginSnapshot(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception{return delegate.beginSnapshot(epoch,table,snapshot);}
    @Override public void snapshotPart(long epoch,TableSchema table,String id,Journal.SnapshotPart part)throws Exception{delegate.snapshotPart(epoch,table,id,part);}
    @Override public void finishSnapshot(long epoch,TableSchema table,Journal.SnapshotState snapshot)throws Exception{delegate.finishSnapshot(epoch,table,snapshot);}
    @Override public Map<String,Object> diagnostics()throws Exception{Map<String,Object> result=delegate.diagnostics();result.put("consistency",consistency().name());return result;}
    @Override public void close(){delegate.close();}
}
