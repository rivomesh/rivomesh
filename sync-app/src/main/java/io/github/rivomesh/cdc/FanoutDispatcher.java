package io.github.rivomesh.cdc;

import io.github.rivomesh.FailureDiagnostics;
import java.sql.SQLException;
import java.util.*;

/** Subscriber failures are isolated. A durable sink may lead its journal ACK by one uncertain commit. */
public final class FanoutDispatcher {
    public interface Transformer { SourceTransaction normalize(Journal.Record record)throws Exception; }
    private final Journal journal;
    private final Journal.Lease lease;
    private final List<SinkAdapter> sinks;
    private final Map<String,TableSchema> schemas;
    private final Transformer transformer;
    private final Map<String,Long> retryAfter=new HashMap<>();
    private final Map<String,String> states=new LinkedHashMap<>();
    public FanoutDispatcher(Journal journal,Journal.Lease lease,List<SinkAdapter> sinks,Map<String,TableSchema> schemas,Transformer transformer){
        this.journal=journal;this.lease=lease;this.sinks=new ArrayList<>(sinks);this.schemas=schemas;this.transformer=sourceTransformer(transformer);
    }
    private SourceTransaction project(SinkAdapter sink,SourceTransaction normalized){
        return TransactionProjection.project(normalized,change->schemas.get(change.tableId),sink::includes);
    }
    /** Bounded serial helper; the runner gives each destination its own dispatcher thread. */
    public int drain(int maximumPerSink)throws Exception{
        if(Thread.currentThread().isInterrupted())throw new InterruptedException("SUBSCRIBER_STOPPED");
        if(maximumPerSink<1||maximumPerSink>1000)throw new IllegalArgumentException("DRAIN_LIMIT_INVALID");int applied=0;
        Map<String,Journal.Subscription> subscriptions=new HashMap<>();for(Journal.Subscription sub:journal.subscriptions())subscriptions.put(sub.id,sub);
        for(SinkAdapter sink:sinks){
            Journal.Subscription sub=subscriptions.get(sink.id());if(sub==null)throw new IllegalStateException("SUBSCRIPTION_NOT_FOUND");
            if(!"RUNNING".equals(sub.state)){states.put(sink.id(),"PAUSED");continue;}
            if(retryAfter.containsKey(sink.id())&&System.nanoTime()-retryAfter.get(sink.id())<0){states.put(sink.id(),"RETRYING");continue;}
            SubscriberFailure diagnostic=new SubscriberFailure();diagnostic.phase=SubscriberFailure.Phase.TARGET_ACTIVATE;
            SourceTransaction projected=null;
            try{
                SinkAdapter.Checkpoint checkpoint=sink.activate(lease.epoch);
                diagnostic.phase=SubscriberFailure.Phase.RECONCILE;
                if(checkpoint.sequence<sub.acknowledged||checkpoint.sequence>Math.addExact(sub.acknowledged,1))throw new IllegalStateException("SINK_JOURNAL_CHECKPOINT_DIVERGED");
                if(checkpoint.skipOperation!=null&&checkpoint.sequence==sub.acknowledged)try{journal.verifySkippedCheckpoint(sink.id(),checkpoint);}catch(Exception failure){throw new JournalFailure(PipelineFailure.Phase.JOURNAL_STORAGE,failure);}
                diagnostic.phase=SubscriberFailure.Phase.JOURNAL_READ;
                List<Journal.Record> records=read(sink.id(),maximumPerSink);int start=0;
                if(checkpoint.sequence>sub.acknowledged){
                    diagnostic.phase=SubscriberFailure.Phase.RECONCILE;
                    if(checkpoint.skipOperation!=null)throw new IllegalStateException("SINK_SKIP_RECOVERY_REQUIRED");
                    diagnostic.phase=SubscriberFailure.Phase.RECONCILE;
                    if(records.isEmpty()||records.get(0).sequence!=checkpoint.sequence)throw new IllegalStateException("COMMITTED_SINK_HISTORY_MISSING");
                    projected=normalizeProject(sink,records.get(0),diagnostic);diagnostic.phase=SubscriberFailure.Phase.RECONCILE;
                    if(!projected.id.equals(checkpoint.transactionId)||!Codec.sha256(Codec.encode(projected)).equals(checkpoint.transactionHash))throw new IllegalStateException("COMMITTED_SINK_CONTENT_MISMATCH");
                    diagnostic.phase=SubscriberFailure.Phase.JOURNAL_ACK;
                    acknowledge(sink.id(),checkpoint.sequence);start=1;applied++;
                }
                for(int i=start;i<records.size();i++){
                    if(Thread.currentThread().isInterrupted())throw new InterruptedException("SUBSCRIBER_STOPPED");
                    Journal.Record record=records.get(i);projected=null;projected=normalizeProject(sink,record,diagnostic);
                    diagnostic.phase=SubscriberFailure.Phase.TARGET_APPLY;sink.apply(lease.epoch,record.sequence,projected);
                    diagnostic.phase=SubscriberFailure.Phase.JOURNAL_ACK;acknowledge(sink.id(),record.sequence);applied++;
                }
                if(sub.errorCode!=null||retryAfter.containsKey(sink.id())){
                    retrying(sink.id(),null);
                    FailureDiagnostics.targetRetryCleared(sink.id());
                }
                states.put(sink.id(),"RUNNING");retryAfter.remove(sink.id());
            }catch(JdbcJournal.Fenced failure){throw failure;}
            catch(InterruptedException stopped){throw stopped;}
            catch(Exception failure){
                // Source normalization faults stop the whole stream; target faults only pause that subscriber.
                // Transformer failures are tagged below and must never be mistaken for a target SQL failure.
                if(failure instanceof NormalizationFailure||failure instanceof JournalFailure)throw failure;
                String code=errorCode(failure);
                if(retryable(failure)){
                    if(code.equals("TARGET_APPLY_ERROR"))code="TARGET_CONNECTION_RETRY";
                    retrying(sink.id(),code);
                    if(!"RETRYING".equals(states.get(sink.id()))||!Objects.equals(sub.errorCode,code))
                        FailureDiagnostics.targetRetrying(sink.id(),code,failure);
                    retryAfter.put(sink.id(),System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(2));
                    states.put(sink.id(),"RETRYING");
                }else{
                    if(projected!=null)diagnostic.projectedHash=Codec.sha256(Codec.encode(projected));
                    journal.pauseFailure(lease,sink.id(),code,diagnostic);
                    states.put(sink.id(),"PAUSED");
                    FailureDiagnostics.targetPaused(sink.id(),code,failure);
                }
            }
        }
        return applied;
    }
    private SourceTransaction normalizeProject(SinkAdapter sink,Journal.Record record,SubscriberFailure diagnostic)throws Exception{
        diagnostic.sequence=record.sequence;diagnostic.transactionId=record.transaction.id;diagnostic.projectedHash=null;
        diagnostic.phase=SubscriberFailure.Phase.NORMALIZE;SourceTransaction normalized=transformer.normalize(record);
        diagnostic.phase=SubscriberFailure.Phase.PROJECT;return project(sink,normalized);
    }
    public static final class NormalizationFailure extends PipelineFailure {
        public NormalizationFailure(Exception cause){super(Phase.NORMALIZE,cause);}
    }
    public static final class JournalFailure extends PipelineFailure {
        public JournalFailure(Phase phase,Exception cause){super(phase,cause);}
    }
    private List<Journal.Record> read(String subscription,int limit)throws Exception{try{return journal.read(subscription,limit);}catch(JdbcJournal.Fenced failure){throw failure;}catch(Exception failure){throw new JournalFailure(PipelineFailure.Phase.JOURNAL_STORAGE,failure);}}
    private void acknowledge(String subscription,long sequence)throws Exception{try{journal.acknowledge(lease,subscription,sequence);}catch(JdbcJournal.Fenced failure){throw failure;}catch(Exception failure){throw new JournalFailure(PipelineFailure.Phase.JOURNAL_ACK,failure);}}
    private void retrying(String subscription,String code)throws Exception{try{journal.retrying(lease,subscription,code);}catch(JdbcJournal.Fenced failure){throw failure;}catch(Exception failure){throw new JournalFailure(PipelineFailure.Phase.JOURNAL_STORAGE,failure);}}
    public static Transformer sourceTransformer(Transformer transformer){return record->{try{return transformer.normalize(record);}catch(Exception failure){if(failure instanceof JdbcJournal.Fenced)throw failure;throw new NormalizationFailure(failure);}};}
    private static boolean retryable(Exception failure){return io.github.rivomesh.Sql.retryable(failure);}
    private static String errorCode(Exception failure){if(failure instanceof io.github.rivomesh.SyncFailure)return ((io.github.rivomesh.SyncFailure)failure).code;if(failure instanceof SQLException)return io.github.rivomesh.Sql.targetErrorCode((SQLException)failure);String message=failure.getMessage();return message!=null&&message.matches("[A-Z][A-Z0-9_]{1,120}")?message:"TARGET_APPLY_ERROR";}
    public Map<String,String> states(){return new LinkedHashMap<>(states);}
}
