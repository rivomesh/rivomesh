package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.*;
import io.github.rivomesh.cdc.PipelineFailure.Phase;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** V3 runner: one source order, restartable snapshot artifacts and independent target transactions. */
public final class PipelineEngine implements AutoCloseable {
    private final PipelineConfig config;
    private volatile boolean stopped;
    private volatile SourceAdapter.Stream activeStream;
    private interface Health {void check()throws Exception;}
    private Health topology;
    private static final class SourceFailure extends Exception {SourceFailure(Exception cause){super("SOURCE_OR_JOURNAL_FAILED",cause);}}
    public PipelineEngine(PipelineConfig config){config.validate();this.config=config;}
    private void check(AtomicReference<Throwable> heartbeat,SourceAdapter.Stream capture)throws Exception{
        if(stopped)throw new InterruptedException("PIPELINE_STOPPED");
        Throwable failure=heartbeat.get();
        if(failure!=null)throw new SourceFailure(new PipelineFailure(Phase.JOURNAL_STORAGE,failure));
        try{capture.check();if(topology!=null)topology.check();}
        catch(Exception e){throw new SourceFailure(e);}
    }
    public void run()throws Exception{
        config.prepareLocalDirectory();JdbcJournal localKeeper=null;
        try{while(!stopped)try{
            // Keep a LOCAL automatic server alive across Busy/PAUSED/reconnect loops.
            // Repeatedly closing the owning JVM's last connection churns its lock file
            // and prevents same-host status/resume clients from completing a handoff.
            if("LOCAL".equals(config.journal.mode)&&localKeeper==null)localKeeper=new JdbcJournal(config.journalSettings());
            runOwner();
        }
        catch(InterruptedException end){if(!stopped)throw end;}
        catch(Exception failure){
            if(stopped)return;
            boolean leaseFailure=false;for(Throwable t=failure;t!=null;t=t.getCause())if(t instanceof JdbcJournal.Busy||t instanceof JdbcJournal.Fenced)leaseFailure=true;
            if(!leaseFailure&&!(failure instanceof JdbcJournal.PipelinePaused)){
                Phase phase=PipelineFailure.phase(failure,Phase.JOURNAL_STORAGE);
                System.err.println("PIPELINE_DIAGNOSTIC "+phase.name()+" "+sourceErrorCode(failure));
                if(!Sql.retryable(failure))throw failure;
            }
            System.err.println("PIPELINE_RETRY "+failure.getClass().getSimpleName());Thread.sleep(config.retryDelayMillis);
        }}finally{if(localKeeper!=null)localKeeper.close();}
    }
    private void runOwner()throws Exception{
        ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"v3-journal-heartbeat");t.setDaemon(true);return t;});
        try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
            JdbcJournal.Settings storage=config.journalSettings();
            // Check physical identity before any journal DDL can enter the source binlog.
            SourceAdapter probe;
            try{probe=PipelineSources.open(config,"probe");}
            catch(Exception e){throw new PipelineFailure(Phase.SOURCE_CONNECT,e);}
            try(SourceAdapter source=probe){
                source.requireSeparateDatabase(storage.driver,storage.url,storage.username,storage.password);
            }
            journal.initialize(config.fingerprint(),null,config.subscribers());Journal.Lease lease=journal.claim(config.nodeId.substring(0,Math.min(118,config.nodeId.length()))+"-"+UUID.randomUUID().toString().substring(0,8),config.leaseMillis);
            AtomicReference<Throwable> heartbeat=new AtomicReference<>();scheduler.scheduleAtFixedRate(()->{try{journal.heartbeat(lease,config.leaseMillis);}catch(Throwable e){heartbeat.compareAndSet(null,e);}},config.heartbeatMillis,config.heartbeatMillis,TimeUnit.MILLISECONDS);
            Phase phase=Phase.SOURCE_CONNECT;
            try(SourceAdapter source=PipelineSources.open(config,(String)journal.status().get("generation"))){
                phase=Phase.JOURNAL_STORAGE;
                SourcePosition captured=journal.captured();
                List<SourcePosition> dependencies=journal.dependencies();
                phase=Phase.SOURCE_CONNECT;
                source.validateFailover(captured,dependencies);
                long[] checked={System.nanoTime()};
                topology=()->{
                    if(System.nanoTime()-checked[0]<TimeUnit.MILLISECONDS.toNanos(config.source.healthCheckMillis))return;
                    SourcePosition latest;
                    List<SourcePosition> required;
                    try{latest=journal.captured();required=journal.dependencies();}
                    catch(Exception e){throw new PipelineFailure(Phase.JOURNAL_STORAGE,e);}
                    try{source.checkTopology(latest,required);}
                    catch(Exception e){throw new PipelineFailure(Phase.SOURCE_CONNECT,e);}
                    checked[0]=System.nanoTime();
                };
                if(captured==null){
                    phase=Phase.SOURCE_READ;
                    captured=source.position();
                    phase=Phase.JOURNAL_STORAGE;
                    journal.baseline(lease,captured);
                }
                phase=Phase.SOURCE_DISCOVERY;
                List<TableSchema> discovered=source.discover();Map<String,TableSchema> schemas=new LinkedHashMap<>();for(TableSchema table:discovered)schemas.put(table.id,table);
                phase=Phase.JOURNAL_STORAGE;
                List<TableSchema> existing=journal.schemas();
                phase=Phase.DDL;
                for(TableSchema table:existing)if(!schemas.containsKey(table.id)||!Codec.encode(table).equals(Codec.encode(schemas.get(table.id))))throw new IllegalStateException("SOURCE_SCHEMA_CHANGED_REQUIRES_PLAN");
                phase=Phase.JOURNAL_STORAGE;
                for(TableSchema table:schemas.values())journal.saveSchema(lease,table);
                phase=Phase.SOURCE_CONNECT;
                try(SourceAdapter.Stream capture=source.stream(captured,new SourceAdapter.Receiver(){
                    volatile boolean cancelled;
                    public void cancelled(){cancelled=true;}
                    public void committed(SourceTransaction transaction)throws Exception{
                        long reserve;
                        try{reserve=NormalizationBudget.reserve(transaction,schemas);}
                        catch(Exception failure){throw new PipelineFailure(Phase.NORMALIZE,failure);}
                        boolean waiting=false;
                        while(!cancelled&&!stopped){
                            try{journal.append(lease,transaction,reserve);if(waiting)System.err.println("JOURNAL_BACKPRESSURE_CLEARED");return;}
                            catch(JdbcJournal.Capacity full){
                                if(Math.addExact(Codec.encode(transaction).getBytes(java.nio.charset.StandardCharsets.UTF_8).length,reserve)>config.journal.maxBytes)
                                    throw new PipelineFailure(Phase.JOURNAL_STORAGE,new IllegalStateException("TRANSACTION_EXCEEDS_JOURNAL_QUOTA"));
                                if(!waiting){waiting=true;System.err.println("JOURNAL_BACKPRESSURE");}Thread.sleep(Math.max(250,config.pollIntervalMillis));
                            }
                            catch(JdbcJournal.Fenced fenced){throw fenced;}
                            catch(Exception failure){throw new PipelineFailure(Phase.JOURNAL_STORAGE,failure);}
                        }throw new InterruptedException("CAPTURE_DELIVERY_CANCELLED");
                    }
                })){
                    activeStream=capture;
                    check(heartbeat,capture);
                    System.err.println("PIPELINE_DIAGNOSTIC CLEAR");
                    Map<String,Journal.SnapshotState> snapshots=new LinkedHashMap<>();Map<String,SourcePosition> baselines=new LinkedHashMap<>();
                    for(TableSchema table:schemas.values()){
                        phase=Phase.JOURNAL_STORAGE;
                        check(heartbeat,capture);
                        Journal.SnapshotState snapshot=journal.snapshot("source",table.id);
                        if(snapshot==null||!"COMPLETE".equals(snapshot.state)){
                            phase=Phase.SOURCE_SNAPSHOT;
                            try(SourceAdapter.Snapshot read=source.snapshot(table)){
                                phase=Phase.JOURNAL_STORAGE;
                                snapshot=journal.beginSnapshot(lease,"source",table,read.baseline());
                                while(true){
                                    check(heartbeat,capture);
                                    phase=Phase.SOURCE_SNAPSHOT;
                                    List<List<Cell>> rows=read.next(config.chunkSize);
                                    phase=Phase.JOURNAL_STORAGE;
                                    if(rows.isEmpty())break;
                                    journal.appendSnapshot(lease,snapshot,snapshot.nextPart,rows);
                                }
                                journal.finishSnapshot(lease,snapshot);
                            }
                        }
                        while(!journal.seedIdentities(lease,table,snapshot))check(heartbeat,capture);
                        snapshots.put(table.id,snapshot);baselines.put(table.id,snapshot.baseline);
                    }
                    phase=Phase.JOURNAL_STORAGE;
                    String generation=(String)journal.status().get("generation");
                    try(SinkGroup subscribers=new SinkGroup(journal,lease,generation,source,heartbeat,capture,schemas,snapshots,baselines)){
                        while(!stopped){check(heartbeat,capture);subscribers.check();journal.collect(lease,1000);journal.collectSnapshots(lease,1000);Thread.sleep(config.pollIntervalMillis);}
                    }
                }finally{activeStream=null;topology=null;}
            }catch(Exception failure){
                if(failure instanceof InterruptedException||failure instanceof JdbcJournal.Fenced)throw failure;
                boolean capacity=hasCapacityCause(failure);
                Phase failedPhase=capacity?Phase.JOURNAL_STORAGE:PipelineFailure.phase(failure,phase);
                if(!stopped&&!Sql.retryable(failure)){
                    try{journal.failPipeline(lease,capacity?"JOURNAL_WORKSPACE_REQUIRES_QUOTA_PLAN":sourceErrorCode(failure),
                            failedPhase,OracleArchiveRead.failedDdl(failure));}catch(JdbcJournal.Fenced ignored){/* a newer owner/control already decided */}catch(Exception save){failure.addSuppressed(save);}
                }
                throw new PipelineFailure(failedPhase,failure);
            }finally{scheduler.shutdown();scheduler.awaitTermination(5,TimeUnit.SECONDS);journal.release(lease);}
        }finally{scheduler.shutdown();}
    }

    /** One bounded batch loop per destination; stalled target I/O cannot hold another destination's thread. */
    private final class SinkGroup implements AutoCloseable {
        final List<Thread> threads=new ArrayList<>();
        final AtomicReference<Throwable> failure=new AtomicReference<>();
        volatile boolean closing;
        SinkGroup(JdbcJournal journal,Journal.Lease lease,String generation,SourceAdapter source,
                  AtomicReference<Throwable> heartbeat,SourceAdapter.Stream capture,Map<String,TableSchema> schemas,
                  Map<String,Journal.SnapshotState> snapshots,Map<String,SourcePosition> baselines){
            try{
                for(PipelineConfig.Target target:config.targets){
                    Thread thread=new Thread(()->runTarget(target,journal,lease,generation,source,heartbeat,capture,schemas,snapshots,baselines),"rivomesh-sink-"+target.id);
                    thread.setDaemon(true);threads.add(thread);thread.start();
                }
            }catch(Throwable problem){close();throw problem;}
        }
        void check()throws Exception{Throwable error=failure.get();if(error instanceof Exception)throw (Exception)error;if(error!=null)throw new IllegalStateException("SUBSCRIBER_WORKER_FAILED",error);}
        boolean active(){return !closing&&!stopped;}
        void healthy(AtomicReference<Throwable> heartbeat,SourceAdapter.Stream capture)throws Exception{
            if(!active()||Thread.currentThread().isInterrupted())throw new InterruptedException("SUBSCRIBER_STOPPED");
            Throwable problem=heartbeat.get();if(problem!=null)throw new SourceFailure(new PipelineFailure(Phase.JOURNAL_STORAGE,problem));
            try{capture.check();}catch(Exception problemAtSource){throw new SourceFailure(problemAtSource);}
        }
        void runTarget(PipelineConfig.Target target,JdbcJournal journal,Journal.Lease lease,String generation,SourceAdapter source,
                       AtomicReference<Throwable> heartbeat,SourceAdapter.Stream capture,Map<String,TableSchema> schemas,
                       Map<String,Journal.SnapshotState> snapshots,Map<String,SourcePosition> baselines){
            SinkAdapter sink=null;FanoutDispatcher dispatcher=null;
            try{
                while(active()){
                    healthy(heartbeat,capture);
                    Journal.Subscription subscription=null;for(Journal.Subscription sub:journal.subscriptions())if(target.id.equals(sub.id))subscription=sub;
                    if(subscription==null)throw new IllegalStateException("SUBSCRIPTION_NOT_FOUND");
                    if(!"RUNNING".equals(subscription.state)){Thread.sleep(config.pollIntervalMillis);continue;}
                    SubscriberFailure preparation=new SubscriberFailure();preparation.phase=SubscriberFailure.Phase.TARGET_ACTIVATE;
                    if(dispatcher==null)try{
                        if(sink==null){if(!target.product.messaging()&&!target.product.materialized()){JdbcSink.Settings settings=config.targetSettings(target,generation);if(target.product!=TargetProduct.OCEANBASE)source.requireSeparateDatabase(settings.driver,settings.url,settings.username,settings.password);}sink=PipelineSinks.open(config,target,generation,journal,lease);}
                        SnapshotDispatcher.prepare(journal,lease,sink,schemas.values(),snapshots,()->healthy(heartbeat,capture),preparation);
                        if(subscription.errorCode!=null){
                            try{journal.retrying(lease,target.id,null);}catch(Exception storageFailure){throw new SourceFailure(storageFailure);}
                            FailureDiagnostics.targetRetryCleared(target.id);
                        }
                        dispatcher=new FanoutDispatcher(journal,lease,Collections.singletonList(sink),schemas,r->journal.normalize(lease,r,schemas,baselines,source::includes));
                    }catch(JdbcJournal.Fenced|SourceFailure|FanoutDispatcher.JournalFailure|InterruptedException global){throw global;}
                    catch(Exception targetFailure){
                        if(Sql.retryable(targetFailure)){
                            String code=errorCode(targetFailure);
                            if(code.equals("TARGET_PREPARE_ERROR"))code="TARGET_CONNECTION_RETRY";
                            journal.retrying(lease,target.id,code);
                            if(!Objects.equals(subscription.errorCode,code))FailureDiagnostics.targetRetrying(target.id,code,targetFailure);
                            Thread.sleep(config.retryDelayMillis);
                            continue;
                        }
                        String code=errorCode(targetFailure);journal.pauseFailure(lease,target.id,code,preparation);FailureDiagnostics.targetPaused(target.id,code,targetFailure);continue;
                    }
                    int applied=dispatcher.drain(100);if(applied==0)Thread.sleep(config.pollIntervalMillis);
                }
            }catch(Throwable problem){if(active()&&!(problem instanceof InterruptedException))failure.compareAndSet(null,problem);}
            finally{if(sink!=null)try{sink.close();}catch(Exception ignored){/* Source/sink checkpoints resolve any uncertain business commit. */}}
        }
        @Override public void close(){
            // H2 can execute embedded file I/O on these threads. Interrupting one can close a
            // shared FileChannel while another target is committing. Stop cooperatively;
            // epoch/checkpoint fences resolve a target that is still in network I/O.
            closing=true;long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            for(Thread thread:threads)try{long remaining=deadline-System.nanoTime();if(remaining>0)thread.join(Math.max(1,TimeUnit.NANOSECONDS.toMillis(remaining)));}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();break;}
        }
    }

    private static String errorCode(Exception failure){if(failure instanceof SyncFailure)return ((SyncFailure)failure).code;if(failure instanceof SQLException)return io.github.rivomesh.Sql.targetErrorCode((SQLException)failure);String message=failure.getMessage();return message!=null&&message.matches("[A-Z][A-Z0-9_]{1,120}")?message:"TARGET_PREPARE_ERROR";}
    private static boolean hasCapacityCause(Throwable failure){for(Throwable t=failure;t!=null;t=t.getCause())if(t instanceof JdbcJournal.Capacity)return true;return false;}
    static String sourceErrorCode(Throwable failure){String result="SOURCE_FAILED";for(Throwable t=failure;t!=null;t=t.getCause()){if(t instanceof SyncFailure)return ((SyncFailure)t).code;if(t.getMessage()!=null&&t.getMessage().matches("[A-Z][A-Z0-9_]{1,120}"))result=t.getMessage();}return result;}
    public static Map<String,Object> check(PipelineConfig config)throws Exception{
        Map<String,Object> result=new LinkedHashMap<>();try(SourceAdapter source=PipelineSources.open(config,"check")){List<TableSchema> tables=source.discover();result.put("sourceTables",tables.size());List<Map<String,Object>> plans=new ArrayList<>();for(PipelineConfig.Target target:config.targets)if(target.product==TargetProduct.CLICKHOUSE)for(TableSchema table:tables)if(target.mappings.containsKey(table.database)){ClickHouseIndexPlan plan=ClickHouseIndexPlan.plan(table,target.clickhouse.secondaryIndexes);Map<String,Object> entry=new LinkedHashMap<>();entry.put("target",target.id);entry.put("table",table.qualifiedName());entry.put("plan",plan);plans.add(entry);}if(!plans.isEmpty())result.put("clickhouseIndexPlans",plans);List<Map<String,Object>> dorisPlans=new ArrayList<>();for(PipelineConfig.Target target:config.targets)if(target.product==TargetProduct.DORIS)for(TableSchema table:tables)if(target.mappings.containsKey(table.database)){Map<String,Object> entry=new LinkedHashMap<>();entry.put("target",target.id);entry.put("table",table.qualifiedName());entry.put("plan",DorisIndexPlan.plan(table,target.doris.secondaryIndexes));dorisPlans.add(entry);}if(!dorisPlans.isEmpty())result.put("dorisIndexPlans",dorisPlans);
            List<Map<String,Object>> dmPlans=new ArrayList<>();for(PipelineConfig.Target target:config.targets)if(target.product==TargetProduct.DM)for(TableSchema table:tables)if(target.mappings.containsKey(table.database)){Map<String,Object> entry=new LinkedHashMap<>();entry.put("target",target.id);entry.put("table",table.qualifiedName());entry.put("columns",DmStorage.plan(table,target.dm.acknowledgeTextConversions));dmPlans.add(entry);}if(!dmPlans.isEmpty())result.put("dmStoragePlans",dmPlans);
            List<Map<String,Object>> hivePlans=new ArrayList<>();
            Map<String,Set<String>> hiveNames=new HashMap<>();
            for(PipelineConfig.Target target:config.targets)if(target.product==TargetProduct.HIVE)for(TableSchema table:tables)if(target.mappings.containsKey(table.database)){
                String destination=(target.mappings.get(table.database)+"."+table.name).toLowerCase(Locale.ROOT);if(!hiveNames.computeIfAbsent(target.id,key->new HashSet<>()).add(destination))throw new IllegalArgumentException("HIVE_IDENTIFIER_COLLISION");Map<String,String> storage=HiveRows.storage(table);
                Map<String,Object> entry=new LinkedHashMap<>();entry.put("target",target.id);entry.put("table",table.qualifiedName());entry.put("plan",HiveIndexPlan.plan(table,target.hive.acknowledgeIndexOmission));entry.put("columnStorage",storage);hivePlans.add(entry);
            }if(!hivePlans.isEmpty())result.put("hiveIndexPlans",hivePlans);
            Map<String,Object> gaussReview=GaussIndexReview.inspect(config.effectiveSourceType(),config.targets,tables);
            if(!gaussReview.isEmpty())result.put("gaussIndexReview",gaussReview);
        }
        List<String> products=new ArrayList<>();for(PipelineConfig.Target target:config.targets){if(target.product==TargetProduct.HIVE){products.add(target.id+": Hive "+HiveMaterializer.check(config.hiveSettings(target,"connection-check")));}else if(target.product==TargetProduct.DORIS){products.add(target.id+": Doris "+DorisMaterializer.check(config.dorisSettings(target,"connection-check")));}else if(target.product==TargetProduct.CLICKHOUSE){products.add(target.id+": ClickHouse "+ClickHouseMaterializer.check(PipelineConfig.clickHouseHttp(target)));}else if(target.product.messaging()){MessagePublishers.check(target);products.add(target.id+": "+target.product.name());}else try(Connection c=JdbcConnections.open(PipelineConfig.driver(target.product),target.url,target.username,target.password)){products.add(target.id+": "+(target.product==TargetProduct.OCEANBASE?OceanBaseProfile.check(c):target.product==TargetProduct.GBASE_8C?Gbase8cProfile.check(c):target.product==TargetProduct.KINGBASE?KingbaseProfile.check(c):target.product==TargetProduct.DM?DmProfile.check(c):c.getMetaData().getDatabaseProductName()));}}result.put("targets",products);return result;
    }
    @Override public void close(){stopped=true;SourceAdapter.Stream stream=activeStream;if(stream!=null)try{stream.close();}catch(Exception ignored){/* persisted source position remains unchanged */}}
}
