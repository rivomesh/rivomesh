package io.github.rivomesh.cdc;

import java.util.*;

/** Snapshot replay shares the destination checkpoint protocol and isolates journal failures. */
public final class SnapshotDispatcher {
    public interface Health {void check()throws Exception;}
    private SnapshotDispatcher(){}
    public static void prepare(Journal journal,Journal.Lease lease,SinkAdapter sink,Collection<TableSchema> schemas,
                               Map<String,Journal.SnapshotState> snapshots,Health health,SubscriberFailure diagnostic)throws Exception{
        diagnostic.sequence=0;diagnostic.transactionId=null;diagnostic.projectedHash=null;
        diagnostic.phase=SubscriberFailure.Phase.TARGET_ACTIVATE;sink.activate(lease.epoch);
        for(TableSchema table:schemas){
            Journal.SnapshotState snapshot=snapshots.get(table.id);
            if(snapshot==null)throw new FanoutDispatcher.JournalFailure(PipelineFailure.Phase.JOURNAL_STORAGE,new IllegalStateException("SNAPSHOT_MANIFEST_MISSING"));
            diagnostic.phase=SubscriberFailure.Phase.PROJECT;
            if(sink.includes(table)){
                health.check();diagnostic.phase=SubscriberFailure.Phase.TARGET_PREPARE;sink.prepare(lease.epoch,table);
                diagnostic.phase=SubscriberFailure.Phase.TARGET_SNAPSHOT;SinkAdapter.SnapshotCheckpoint cp=sink.beginSnapshot(lease.epoch,table,snapshot);
                if(cp==null||cp.nextPart<0||cp.nextPart>snapshot.nextPart)throw new IllegalStateException("SNAPSHOT_CHECKPOINT_INVALID");
                for(int part=cp.nextPart;part<snapshot.nextPart;part++){
                    health.check();Journal.SnapshotPart chunk;
                    diagnostic.phase=SubscriberFailure.Phase.JOURNAL_READ;
                    try{chunk=journal.snapshotParts("source",table.id,snapshot.snapshotId,part,1).get(0);}
                    catch(JdbcJournal.Fenced|InterruptedException global){throw global;}
                    catch(Exception storage){throw new FanoutDispatcher.JournalFailure(PipelineFailure.Phase.JOURNAL_STORAGE,storage);}
                    diagnostic.phase=SubscriberFailure.Phase.TARGET_SNAPSHOT;sink.snapshotPart(lease.epoch,table,snapshot.snapshotId,chunk);
                }diagnostic.phase=SubscriberFailure.Phase.TARGET_SNAPSHOT;sink.finishSnapshot(lease.epoch,table,snapshot);
            }
            diagnostic.phase=SubscriberFailure.Phase.JOURNAL_ACK;
            try{journal.acknowledgeSnapshot(lease,sink.id(),snapshot);}
            catch(JdbcJournal.Fenced|InterruptedException global){throw global;}
            catch(Exception storage){throw new FanoutDispatcher.JournalFailure(PipelineFailure.Phase.JOURNAL_ACK,storage);}
        }
    }
}
