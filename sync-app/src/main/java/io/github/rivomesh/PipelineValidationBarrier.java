package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.*;
import java.time.Instant;
import java.util.*;

/** Establish an explicit SQL validation cut. A barrier alone is not a data-validation result. */
public final class PipelineValidationBarrier {
    private PipelineValidationBarrier(){}
    public static final class Certificate {
        public String id,createdAt,fingerprint,generation;
        public long epoch,sequence,errorGeneration;
        public SourcePosition sourcePosition,capturedPosition;
        public Set<String> selectedTargets=new TreeSet<>();
        public Map<String,String> schemas=new TreeMap<>();
        public Map<String,SinkAdapter.Checkpoint> targetCheckpoints=new TreeMap<>();
    }
    public static final class Session implements AutoCloseable {
        private final PipelineConfig config;
        private final JdbcJournal journal;
        private final MysqlCdcSource source;
        private SourceAdapter.ConsistentRead view;
        private final List<TableSchema> tables;
        private final Certificate certificate=new Certificate();
        private boolean closed;
        Session(PipelineConfig config,Set<String> selected,long gateMillis,long readMillis,String reportId)throws Exception{
            this.config=config;Set<String> ids=new TreeSet<>();
            if(reportId!=null&&!UUID.fromString(reportId).toString().equals(reportId))throw new IllegalArgumentException("VALIDATION_ID_INVALID");
            if(selected==null||selected.isEmpty())throw new IllegalArgumentException("VALIDATION_TARGET_REQUIRED");
            for(PipelineConfig.Target target:config.targets)if(selected.contains(target.id)){
                if(target.product.messaging()||target.product.materialized())throw new IllegalArgumentException("VALIDATION_TARGET_PROTOCOL_UNSUPPORTED");ids.add(target.id);
            }
            if(!ids.equals(selected))throw new IllegalArgumentException("VALIDATION_TARGET_UNKNOWN");
            journal=new JdbcJournal(config.existingJournalSettings());MysqlCdcSource opened=null;
            try{
                journal.verifyIdentity(config.fingerprint(),config.subscribers());Map<String,Object> before=journal.status();
                if(!"RUNNING".equals(before.get("run_state")))throw error("VALIDATION_REQUIRES_RUNNING_PIPELINE");
                tables=journal.schemas();if(tables.isEmpty())throw error("VALIDATION_REQUIRES_COMPLETED_SNAPSHOT");
                Map<String,Object> snapshots=journal.snapshotSummary(1);
                if(((Number)snapshots.get("table_count")).longValue()!=tables.size()||((Number)snapshots.get("completed_tables")).longValue()!=tables.size())throw error("VALIDATION_REQUIRES_COMPLETED_SNAPSHOT");
                certificate.id=reportId==null?UUID.randomUUID().toString():reportId;certificate.createdAt=Instant.now().toString();certificate.fingerprint=config.fingerprint();certificate.generation=(String)before.get("generation");certificate.selectedTargets=ids;
                for(TableSchema table:tables)certificate.schemas.put(table.id,Codec.sha256(Codec.encode(table)));
                opened=new MysqlCdcSource(config.legacy(),new TreeSet<>(config.databases),certificate.generation);source=opened;
                source.validateFailover(journal.captured(),journal.dependencies());view=source.consistentRead(tables,gateMillis,readMillis);certificate.sourcePosition=view.baseline();
                long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(gateMillis);
                while(true){
                    view.verify();Map<String,Object> current=journal.status();
                    if(!certificate.generation.equals(current.get("generation"))||!Objects.equals(before.get("epoch"),current.get("epoch"))||!"RUNNING".equals(current.get("run_state")))throw error("VALIDATION_BARRIER_MOVED");
                    SourcePosition captured=journal.captured();long sequence=((Number)current.get("lastSequence")).longValue();
                    boolean caught=source.covers(captured,certificate.sourcePosition);
                    if(caught&&!source.covers(certificate.sourcePosition,captured))throw error("SOURCE_POSITION_ADVANCED_WHILE_GATE_HELD");
                    boolean ready=caught&&((Number)current.get("normalizedSequence")).longValue()==sequence;
                    for(Journal.Subscription sub:journal.subscriptions())if(ids.contains(sub.id)){
                        if(!"RUNNING".equals(sub.state))throw error("VALIDATION_TARGET_PAUSED");ready&=sub.acknowledged==sequence;
                    }
                    if(ready){
                        try{
                            Journal.PipelineState paused=journal.pauseDrained(certificate.generation,((Number)before.get("epoch")).longValue(),sequence,captured,ids,tables.size());
                            certificate.epoch=paused.epoch;certificate.errorGeneration=paused.errorGeneration;certificate.sequence=sequence;certificate.capturedPosition=captured;break;
                        }catch(IllegalStateException race){if(!"VALIDATION_BARRIER_MOVED".equals(race.getMessage()))throw race;}
                    }
                    if(System.nanoTime()-deadline>=0)throw error("VALIDATION_DRAIN_TIMED_OUT");Thread.sleep(Math.max(25,Math.min(200,config.pollIntervalMillis)));
                }
                for(PipelineConfig.Target target:config.targets)if(ids.contains(target.id))try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target,certificate.generation))){
                    SinkAdapter.Checkpoint checkpoint=sink.activate(certificate.epoch);
                    if(checkpoint.sequence!=certificate.sequence)throw error("VALIDATION_TARGET_CHECKPOINT_MISMATCH");certificate.targetCheckpoints.put(target.id,checkpoint);
                }
                view.verify();view.releaseWrites();verify();
            }catch(Exception failure){if(view!=null)view.close();if(opened!=null)opened.close();journal.close();throw failure;}
        }
        public Certificate certificate(){return Codec.decode(Codec.encode(certificate),Certificate.class);}
        public List<TableSchema> tables(){List<TableSchema> copy=new ArrayList<>();for(TableSchema table:tables)copy.add(Codec.decode(Codec.encode(table),TableSchema.class));return copy;}
        public SourceAdapter.Snapshot sourceRows(TableSchema table)throws Exception{if(closed)throw error("VALIDATION_SESSION_CLOSED");return view.table(table);}
        /** No automatic resume: concurrent control/configuration changes invalidate this cut. */
        public void verify()throws Exception{
            if(closed)throw error("VALIDATION_SESSION_CLOSED");view.verify();verifyCertificate(config,journal,certificate);
        }
        @Override public void close()throws Exception{if(closed)return;closed=true;try{if(view!=null)view.close();}finally{try{source.close();}finally{journal.close();}}}
    }
    public static Session open(PipelineConfig config,Set<String> targets,long gateMillis,long readMillis)throws Exception{return new Session(config,targets,gateMillis,readMillis,null);}
    public static Session open(PipelineConfig config,Set<String> targets,long gateMillis,long readMillis,String reportId)throws Exception{return new Session(config,targets,gateMillis,readMillis,reportId);}
    /** A sealed source may outlive its MVCC connection, but never its persisted paused cut. */
    public static void verifyCertificate(PipelineConfig config,Certificate certificate)throws Exception{
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){verifyCertificate(config,journal,certificate);}
    }
    private static void verifyCertificate(PipelineConfig config,JdbcJournal journal,Certificate certificate)throws Exception{
        if(certificate==null||!Objects.equals(config.fingerprint(),certificate.fingerprint)||certificate.selectedTargets==null||certificate.selectedTargets.isEmpty()||!certificate.targetCheckpoints.keySet().equals(certificate.selectedTargets))throw error("VALIDATION_CERTIFICATE_INVALID");
        journal.verifyIdentity(config.fingerprint(),config.subscribers());Map<String,Object> state=journal.status();
        if(!Objects.equals(certificate.generation,state.get("generation"))||!"PAUSED".equals(state.get("run_state"))||((Number)state.get("epoch")).longValue()!=certificate.epoch||((Number)state.get("errorGeneration")).longValue()!=certificate.errorGeneration||((Number)state.get("lastSequence")).longValue()!=certificate.sequence||((Number)state.get("normalizedSequence")).longValue()!=certificate.sequence||!Codec.encode(journal.captured()).equals(Codec.encode(certificate.capturedPosition)))throw error("VALIDATION_BARRIER_MOVED");
        Map<String,String> schemas=new TreeMap<>();for(TableSchema table:journal.schemas())schemas.put(table.id,Codec.sha256(Codec.encode(table)));if(!schemas.equals(certificate.schemas))throw error("VALIDATION_SCHEMA_MISMATCH");
        Set<String> found=new TreeSet<>();for(PipelineConfig.Target target:config.targets)if(certificate.selectedTargets.contains(target.id)){
            if(target.product.messaging()||target.product.materialized())throw error("VALIDATION_TARGET_PROTOCOL_UNSUPPORTED");found.add(target.id);
            try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target,certificate.generation))){SinkAdapter.Checkpoint current=sink.checkpoint(),expected=certificate.targetCheckpoints.get(target.id);
                if(expected==null||expected.epoch!=certificate.epoch||expected.sequence!=certificate.sequence||current.epoch!=expected.epoch||current.sequence!=expected.sequence||!Objects.equals(current.transactionId,expected.transactionId)||!Objects.equals(current.transactionHash,expected.transactionHash))throw error("VALIDATION_TARGET_CHECKPOINT_MISMATCH");
            }
        }if(!found.equals(certificate.selectedTargets))throw error("VALIDATION_CERTIFICATE_INVALID");
    }
    private static SyncFailure error(String code){return new SyncFailure(code,"VALIDATE",false,"Validation could not retain its declared cut. Inspect the task state before retrying.");}
}
