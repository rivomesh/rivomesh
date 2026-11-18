package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.validation.ValidationArchive;
import io.github.rivomesh.validation.ValidationRetention;
import java.io.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/** Durable operator intent, bounded asynchronous controls, and an isolated JVM for each configuration. */
public final class TaskService implements AutoCloseable {
    public static final class Conflict extends IllegalStateException { public Conflict(String code) { super(code); } }
    public static final class Operation {
        public String id, action, subscriptionId, status="PENDING", code, createdAt=Instant.now().toString(), finishedAt;
        public long errorCount=-1;
        public String actorId,actorRole;
        public Map<String,Object> result=new LinkedHashMap<>();
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public ValidationRequest validation;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public RepairRequest repair;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public CommitRequest commit;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public SkipRequest skip;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public GapVerificationRequest gap;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public CompensationRequest compensation;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public CorrectionRequest correction;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String gapPlanId;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String skipPlanId;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String failureEvidenceId;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String failureActorRevision;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String captureBoundaryHash;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public CaptureExclusionRequest captureExclusion;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public ValidationRetention.Request validationRetention;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public ValidationArchive.Request validationArchive;
        public boolean cancelRequested;
    }
    public static final class Control {
        public String revision=UUID.randomUUID().toString(), desired="STOPPED", fingerprint;
        public String createdBy,createdRole,createdAt;
        public String configurationVersion,configurationHash,configurationFile;
        public List<Operation> operations=new ArrayList<>();
    }
    public static final class Receipt {
        public String taskId, signature, actorId,actorRole;
        public Receipt() {}
        Receipt(String taskId,String signature) { this.taskId=taskId; this.signature=signature; }
    }
    public static final class Registry {
        public int version=1;
        public String auditIdentity;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String sharedAccessIdentity,sharedClusterId;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String globalTaskIdentity,globalNodeId,ownerDirectoryIdentity;
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public AccountDirectory.State accounts;
        public Map<String,Control> tasks=new LinkedHashMap<>();
        public Map<String,Receipt> requests=new LinkedHashMap<>();
        public List<AuditStore.Event> auditOutbox=new ArrayList<>();
    }
    private static final class Entry {
        final Path path,configurationFile; final TaskDefinition config;
        volatile boolean projectionPending; final AtomicBoolean busy=new AtomicBoolean();
        volatile TaskWorker worker;
        volatile TaskWorker.Diagnostic workerDiagnostic;
        volatile Map<String,Object> database=Collections.emptyMap();
        volatile String observedAt, problem, workerState="STOPPED";
        volatile boolean statusAvailable;
        volatile int exits; volatile long retryAt;
        long recoveryReceiptRetryAt;
        boolean permit;
        Entry(Path path,Path configurationFile,TaskDefinition config) { this.path=path;this.configurationFile=configurationFile; this.config=config; }
    }
    private final PlatformConfig config;
    private final TaskBackend backend;
    private final ConfigurationVersions versions;
    private AuditStore audit;
    private SharedAccessStore sharedAccess;
    private String auditProblem;
    private final ConfigurationDocuments documents=new ConfigurationDocuments();
    private final WorkerDiagnostics.ReadBudget diagnosticReads=new WorkerDiagnostics.ReadBudget();
    private final TaskWorker.Factory workers;
    private final Map<String,Entry> entries=new LinkedHashMap<>();
    private final Map<String,Thread> operationThreads=new ConcurrentHashMap<>();
    private final Map<String,AtomicBoolean> recoveryCancellations=new HashMap<>();
    private final Set<String> ownerRequests=new HashSet<>();
    private final Set<String> ownerStartBlocks=new HashSet<>();
    private final List<Map<String,String>> invalid=new ArrayList<>();
    private final Path statePath, logDirectory, workerDirectory;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService controls;
    private final ExecutorService validations;
    private final Semaphore slots;
    private volatile boolean closed;
    private boolean schedulingStarted;
    private Registry registry;
    private final AccountDirectory.State configuredAccounts;

    public TaskService(PlatformConfig c) throws Exception {
        this(c,new TaskBackend.Database(),(path,log,heap)->new TaskWorker.Jvm(path,log,heap,c.protectedEnvironment,c.workerSecretReferences));
    }
    public TaskService(PlatformConfig c,TaskBackend backend,TaskWorker.Factory workers) throws Exception {
        this(c,backend,workers,true);
    }
    private TaskService(PlatformConfig c,TaskBackend backend,TaskWorker.Factory workers,boolean online) throws Exception {
        c.validate();configuredAccounts=AccountDirectory.configured(c); this.config=c; this.backend=backend; this.workers=workers;slots=new Semaphore(c.maxRunningTasks);
        Path data=Paths.get(c.dataDirectory).toAbsolutePath().normalize();
        PrivateFiles.directory(data); PrivateFiles.directory(Paths.get(c.tasksDirectory));
        logDirectory=data.resolve("logs"); PrivateFiles.directory(logDirectory);
        workerDirectory=data.resolve("worker-configs");PrivateFiles.directory(workerDirectory);
        statePath=data.resolve("controller.json");
        versions=new ConfigurationVersions(data);
        lockChannel=FileChannel.open(data.resolve("controller.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired;
        try { acquired=lockChannel.tryLock(); }
        catch (OverlappingFileLockException e) { lockChannel.close(); throw new Conflict("CONTROLLER_ALREADY_RUNNING"); }
        if (acquired==null) { lockChannel.close(); throw new Conflict("CONTROLLER_ALREADY_RUNNING"); }
        lock=acquired;
        try {
            audit=new AuditStore(data,c.auditMaxEvents);
            if (Files.isSymbolicLink(statePath)) throw new IOException("SYMLINK_STATE");
            registry=Files.exists(statePath)?Model.JSON.readValue(Files.readAllBytes(statePath),Registry.class):new Registry();
            if ((registry.version!=1&&registry.version!=2&&registry.version!=3&&registry.version!=4) || registry.tasks==null || registry.requests==null || registry.auditOutbox==null) throw new IOException("CONTROL_STATE_INVALID");
            if(c.cluster!=null&&!c.cluster.executes()){
                boolean files;try(Stream<Path> paths=Files.list(Paths.get(c.tasksDirectory))){files=paths.anyMatch(p->p.getFileName().toString().matches(".*\\.ya?ml"));}
                if(!registry.tasks.isEmpty()||files)throw new Conflict("CONTROLLER_EXECUTION_DIRECTORY_NOT_EMPTY");
            }
            if(c.cluster!=null&&c.cluster.sharedTasks&&registry.version<4){
                boolean files;try(Stream<Path> paths=Files.list(Paths.get(c.tasksDirectory))){files=paths.anyMatch(p->p.getFileName().toString().matches(".*\\.ya?ml"));}
                if(!registry.tasks.isEmpty()||files)throw new Conflict("GLOBAL_TASK_IMPORT_REQUIRED");
            }
            if(registry.auditIdentity!=null&&!registry.auditIdentity.equals(audit.identity()))throw new IOException("AUDIT_STORE_IDENTITY_CHANGED");
            registry.auditIdentity=audit.identity();
            if(registry.version==2&&registry.accounts==null||registry.version==1&&registry.accounts!=null)throw new IOException("ACCOUNT_STATE_INVALID");
            if(registry.version>=3&&(registry.sharedAccessIdentity==null||!registry.sharedAccessIdentity.matches("[a-f0-9-]{36}")||registry.sharedClusterId==null||c.cluster==null||!c.cluster.sharedAccess||!registry.sharedClusterId.equals(c.cluster.id)))throw new IOException("SHARED_ACCESS_REQUIRED");
            if(registry.version<3&&(registry.sharedAccessIdentity!=null||registry.sharedClusterId!=null))throw new IOException("CONTROL_STATE_INVALID");
            if(registry.version==4&&(c.cluster==null||!c.cluster.sharedTasks||!Objects.equals(c.cluster.nodeId,registry.globalNodeId)||!uuid(registry.globalTaskIdentity)||!uuid(registry.ownerDirectoryIdentity)))throw new IOException("GLOBAL_TASK_AUTHORITY_REQUIRED");
            if(registry.version!=4&&(registry.globalTaskIdentity!=null||registry.globalNodeId!=null||registry.ownerDirectoryIdentity!=null))throw new IOException("CONTROL_STATE_INVALID");
            if(registry.accounts==null&&((Number)audit.accessAnchor().get("version")).longValue()>0)throw new IOException("ACCOUNT_STATE_REQUIRED");
            if(registry.accounts!=null)AccountDirectory.validate(registry.accounts);
            else if(c.accessMode==PlatformConfig.AccessMode.MANAGED&&(c.cluster==null||!c.cluster.sharedAccess)){
                if(((Number)audit.accessAnchor().get("version")).longValue()>0)throw new IOException("ACCOUNT_STATE_REQUIRED");
                AccountDirectory.State initial=Model.JSON.readValue(Model.JSON.writeValueAsBytes(configuredAccounts),AccountDirectory.State.class);initial.managed=true;initial.identity=UUID.randomUUID().toString();initial.revision=UUID.randomUUID().toString();initial.version=1;AccountDirectory.validate(initial);registry.version=2;registry.accounts=initial;registry.auditOutbox.add(AccountDirectory.event(initial,"ACTIVATE",null,null));
            }

            Set<Path> managed=new HashSet<>();
            for(Map.Entry<String,Control> registered:registry.tasks.entrySet())if(registered.getValue().configurationVersion!=null){Control state=registered.getValue();Path file=configurationFile(state.configurationFile);if(!managed.add(file))throw new IOException("CONTROL_STATE_INVALID");loadManaged(registered.getKey(),state,file);}
            try (Stream<Path> paths=Files.list(Paths.get(c.tasksDirectory))) {
                paths.filter(p -> !managed.contains(p.toAbsolutePath().normalize())&&p.getFileName().toString().matches(".*\\.ya?ml")).sorted().forEach(p -> load(p));
            }
            if(registry.version>=3){verifyAccessAnchor();bindSharedAccess();}
            persist(registry);
            flushAudit();verifyAccessAnchor();
            if(c.cluster!=null&&c.cluster.sharedAccess&&sharedAccess==null)bindSharedAccess();
            AuditStore.Event started=new AuditStore.Event();started.kind="AGENT".equals(nodeRole())?"AGENT_STARTED":"CONTROLLER_STARTED";started.status="READY";appendAudit(started);
        } catch (Exception e) { if(audit!=null)try{audit.close();}catch(IOException ignored){}lock.release(); lockChannel.close(); throw e; }
        controls=Executors.newFixedThreadPool(c.controlThreads,daemon("rivomesh-control"));
        validations=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(c.maxTasks),daemon("rivomesh-validation"),new ThreadPoolExecutor.AbortPolicy());
        scheduler=Executors.newSingleThreadScheduledExecutor(daemon("rivomesh-scheduler"));
        if(online&&(c.cluster==null||!c.cluster.sharedTasks))beginOwnerScheduling();
    }
    static TaskService deferredOwner(PlatformConfig config,TaskBackend backend,TaskWorker.Factory workers)throws Exception{
        if(config.cluster==null||!config.cluster.sharedAccess)throw new IllegalArgumentException("OWNER_SHARED_STORE_REQUIRED");return new TaskService(config,backend,workers,false);
    }
    synchronized void beginOwnerScheduling(){ensureOpen();if(config.cluster!=null&&!config.cluster.executes())throw new Conflict("OWNER_ROLE_REQUIRED");if(!schedulingStarted){scheduler.scheduleWithFixedDelay(this::schedule,0,config.pollMillis,TimeUnit.MILLISECONDS);schedulingStarted=true;}}
    synchronized void holdOwnerStart(String task,boolean hold){if(hold)ownerStartBlocks.add(task);else ownerStartBlocks.remove(task);}
    synchronized Map<String,String> ownerDocuments(){ensureOpen();if(!invalid.isEmpty()||!entries.keySet().equals(registry.tasks.keySet()))throw new Conflict("OWNER_LOCAL_CONFIGURATION_INVALID");Map<String,String> result=new LinkedHashMap<>();for(Map.Entry<String,Entry> item:entries.entrySet())result.put(item.getKey(),item.getValue().config.workerYaml);return result;}
    private void bindSharedAccess()throws Exception{
        Map<String,Object> localHead=audit.page(0,0,1),anchor=new LinkedHashMap<>();anchor.put("storeId",localHead.get("storeId"));anchor.put("lastSequence",localHead.get("lastSequence"));anchor.put("lastHash",localHead.get("lastHash"));
        sharedAccess=new SharedAccessStore(config,registry.accounts==null?configuredAccounts:registry.accounts,registry.sharedAccessIdentity,anchor);
        Registry bound=copy();bound.version=Math.max(3,bound.version);bound.sharedAccessIdentity=sharedAccess.identity;bound.sharedClusterId=config.cluster.id;writeRegistry(bound);
    }
    private static boolean uuid(String value){try{return value!=null&&UUID.fromString(value).toString().equals(value);}catch(IllegalArgumentException invalid){return false;}}
    SharedAccessStore sharedAccess(){if(sharedAccess==null)throw new Conflict("SHARED_ACCESS_REQUIRED");return sharedAccess;}
    synchronized String globalTaskIdentity(){return registry.globalTaskIdentity;}
    synchronized String ownerDirectoryIdentity()throws Exception{
        Path file=statePath.getParent().resolve("owner-directory-id");if(Files.isSymbolicLink(file))throw new IOException("OWNER_DIRECTORY_INVALID");String id;
        if(Files.exists(file))id=new String(PrivateFiles.read(file,64),StandardCharsets.US_ASCII).trim();
        else{if(registry.version==4)throw new IOException("OWNER_DIRECTORY_REQUIRED");id=UUID.randomUUID().toString();PrivateFiles.write(file,id.getBytes(StandardCharsets.US_ASCII));}
        if(!uuid(id)||registry.version==4&&!id.equals(registry.ownerDirectoryIdentity))throw new IOException("OWNER_DIRECTORY_INVALID");return id;
    }
    synchronized void bindGlobal(String identity,String directory)throws Exception{
        if(config.cluster==null||!config.cluster.sharedTasks||!uuid(identity)||!uuid(directory))throw new Conflict("GLOBAL_TASK_AUTHORITY_REQUIRED");
        if(registry.version==4&&(!identity.equals(registry.globalTaskIdentity)||!directory.equals(registry.ownerDirectoryIdentity)))throw new Conflict("GLOBAL_TASK_IDENTITY_CHANGED");
        if(registry.version<4&&!entries.isEmpty())throw new Conflict("GLOBAL_TASK_IMPORT_REQUIRED");Registry next=copy();next.version=4;next.globalTaskIdentity=identity;next.globalNodeId=config.cluster.nodeId;next.ownerDirectoryIdentity=directory;writeRegistry(next);
    }
    boolean accessReady(){return sharedAccess==null||sharedAccess.ready();}
    void checkAccessAuthority(){if(sharedAccess!=null)sharedAccess.accounts();}
    private static ThreadFactory daemon(String name) { return r -> { Thread t=new Thread(r,name);t.setDaemon(true);return t; }; }
    private void load(Path path) {
        try {
            if (Files.isSymbolicLink(path) || !Files.isRegularFile(path)) throw new Conflict("CONFIG_NOT_REGULAR");
            if (Files.size(path)>1024*1024) throw new Conflict("CONFIG_TOO_LARGE");
            byte[] yaml=PrivateFiles.read(path,1024*1024);
            TaskDefinition task=parseTask(new String(yaml,StandardCharsets.UTF_8),path.toAbsolutePath().getParent()); validateTask(task,null);
            Control state=registry.tasks.get(task.jobId);
            if (state==null) { state=new Control();state.desired=config.autoStart?"RUNNING":"STOPPED";registry.tasks.put(task.jobId,state); }
            if (state.fingerprint!=null && !state.fingerprint.equals(task.fingerprint())) throw new Conflict("CONFIG_IDENTITY_CHANGED");
            if (!Arrays.asList("STOPPED","RUNNING","PAUSED").contains(state.desired) || state.operations==null || state.revision==null)
                throw new Conflict("CONTROL_STATE_INVALID");
            state.fingerprint=task.fingerprint();Path snapshot=workerDirectory.resolve(task.jobId+".yml");
            PrivateFiles.write(snapshot,task.workerYaml.getBytes(StandardCharsets.UTF_8));entries.put(task.jobId,new Entry(snapshot,path.toAbsolutePath().normalize(),task));
        } catch (Exception e) {
            Map<String,String> row=new LinkedHashMap<>();row.put("file",path.getFileName().toString());row.put("code",safeCode(e));invalid.add(row);
        }
    }
    private Path configurationFile(String name)throws IOException{
        if(name==null||name.isEmpty()||!name.matches(".*\\.ya?ml")||!Paths.get(name).getFileName().toString().equals(name))throw new IOException("CONTROL_STATE_INVALID");Path base=Paths.get(config.tasksDirectory).toAbsolutePath().normalize(),file=base.resolve(name).normalize();if(!base.equals(file.getParent()))throw new IOException("CONTROL_STATE_INVALID");return file;
    }
    private void project(Entry entry){try{byte[] bytes=entry.config.workerYaml.getBytes(StandardCharsets.UTF_8);PrivateFiles.write(entry.configurationFile,bytes);Path snapshot=workerDirectory.resolve(entry.config.jobId+".yml");if(!snapshot.equals(entry.path))PrivateFiles.write(snapshot,bytes);entry.projectionPending=false;}catch(IOException failure){entry.projectionPending=true;}}
    private void loadManaged(String id,Control state,Path file){
        try{if(state.configurationHash==null||!state.configurationHash.matches("[a-f0-9]{64}"))throw new IOException("CONFIG_VERSION_CORRUPT");ConfigurationVersions.Stored stored=versions.read(id,state.configurationVersion,state.configurationHash);TaskDefinition task=parseTask(stored.yaml);if(!id.equals(task.jobId)||state.fingerprint==null||!state.fingerprint.equals(task.fingerprint()))throw new Conflict("CONFIG_IDENTITY_CHANGED");if(!Arrays.asList("STOPPED","RUNNING","PAUSED").contains(state.desired)||state.operations==null||state.revision==null)throw new Conflict("CONTROL_STATE_INVALID");validateTask(task,null);Entry entry=new Entry(stored.document,file,task);entries.put(id,entry);project(entry);}catch(Exception failure){Map<String,String> row=new LinkedHashMap<>();row.put("file",file.getFileName().toString());row.put("code",safeCode(failure));invalid.add(row);}
    }
    private TaskDefinition parseTask(String yaml)throws Exception{
        return parseTask(yaml,Paths.get(config.tasksDirectory));
    }
    private TaskDefinition parseTask(String yaml,Path directory)throws Exception{
        try{TaskWorker.Jvm.environment(System.getenv(),yaml,config.protectedEnvironment,config.workerSecretReferences);}
        catch(IllegalArgumentException denied){
            String code=denied.getMessage();
            throw new Conflict(code!=null&&code.matches("WORKER_SECRET_[A-Z_]{1,50}")?code:"WORKER_ENVIRONMENT_DENIED");
        }
        return TaskDefinition.parse(yaml,directory,Paths.get(config.dataDirectory));
    }
    private void validateTask(TaskDefinition task,String replacing) throws Exception {
        task.validate();
        if (!task.jobId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) throw new Conflict("TASK_ID_INVALID");
        if (entries.size()>=config.maxTasks && replacing==null) throw new Conflict("TASK_LIMIT");
        for (Entry entry:entries.values()) {
            TaskDefinition other=entry.config;
            if (other.jobId.equals(replacing)) continue;
            if (other.jobId.equals(task.jobId)) throw new Conflict("TASK_EXISTS");
            if (other.overlaps(task))
                throw new Conflict("DESTINATION_OVERLAP");
            if("MYSQL".equals(other.sourceType())&&"MYSQL".equals(task.sourceType())&&other.source.serverId==task.source.serverId
                    &&other.endpoints().stream().anyMatch(a->task.endpoints().stream().anyMatch(b->a.host.equalsIgnoreCase(b.host)&&a.port==b.port)))
                throw new Conflict("REPLICATION_SERVER_ID_COLLISION");
        }
    }
    private Registry copy() throws IOException { return Model.JSON.readValue(Model.JSON.writeValueAsBytes(registry),Registry.class); }
    private void persist(Registry next) throws IOException {
        if(registry!=null&&next!=registry)for(Map.Entry<String,Control> task:next.tasks.entrySet()){
            Control previous=registry.tasks.get(task.getKey());
            for(Operation operation:task.getValue().operations){
                Operation old=previous==null?null:previous.operations.stream().filter(item->Objects.equals(item.id,operation.id)).findFirst().orElse(null);
                if(old!=null&&Objects.equals(old.status,operation.status))continue;
                AuditStore.Event event=new AuditStore.Event();event.kind="OPERATION_COMMITTED";event.taskId=task.getKey();event.action=operation.action;event.status=operation.status;event.code=operation.code;event.actorId=operation.actorId;event.actorRole=operation.actorRole;event.operationHash=AuditStore.hash(operation.id);event.clientRequestHash=event.operationHash;next.auditOutbox.add(event);
                if(operation.captureExclusion!=null){event.subjectId=operation.id;event.operationHash=operation.captureExclusion.planHash;}
                else if(operation.validation!=null)event.subjectId=operation.validation.reportId;
                else if(operation.validationRetention!=null)event.subjectId=operation.validationRetention.reportId();
                else if(operation.validationArchive!=null){
                    event.subjectId=operation.validationArchive.reportId();
                    if(operation.result.get("hash") instanceof String hash)event.operationHash=hash;
                }
                else if(operation.repair!=null){event.subjectId=operation.repair.planId;if(operation.repair.approvedHash!=null)event.operationHash=operation.repair.approvedHash;else if(operation.result.get("hash") instanceof String)event.operationHash=(String)operation.result.get("hash");}
                else if(operation.gapPlanId!=null){event.subjectId=operation.gapPlanId;if(operation.result.get("hash") instanceof String)event.operationHash=(String)operation.result.get("hash");}
                else if(operation.skipPlanId!=null){event.subjectId=operation.skipPlanId;if(operation.result.get("hash") instanceof String)event.operationHash=(String)operation.result.get("hash");}
                else if(operation.failureEvidenceId!=null){event.subjectId=operation.failureEvidenceId;if(operation.result.get("hash") instanceof String)event.operationHash=(String)operation.result.get("hash");}
                else if(Arrays.asList("VALIDATE_CANCEL","REPAIR_CANCEL","GAP_VERIFY_CANCEL","RECOVERY_CANCEL").contains(operation.action))event.subjectId=(String)operation.result.get("subjectId");
            }
        }
        writeRegistry(next);
        try{flushAudit();}catch(AuditStore.Unavailable failure){auditProblem="AUDIT_UNAVAILABLE";}
    }
    private void writeRegistry(Registry next)throws IOException{PrivateFiles.write(statePath,Model.JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(next));registry=next;}
    private void appendAudit(AuditStore.Event event)throws AuditStore.Unavailable{audit.append(event);if(sharedAccess!=null)sharedAccess.append(event);}
    BrowserSessions.Store sessionStore(){return sharedAccess==null?new BrowserSessions.MemoryStore(config.sessions.maximum):sharedAccess.sessions();}
    private void flushAudit()throws AuditStore.Unavailable{
        try{while(!registry.auditOutbox.isEmpty()){
            appendAudit(registry.auditOutbox.get(0));Registry drained=copy();drained.auditOutbox.remove(0);writeRegistry(drained);
        }auditProblem=null;}catch(Exception failure){auditProblem="AUDIT_UNAVAILABLE";throw failure instanceof AuditStore.Unavailable?(AuditStore.Unavailable)failure:new AuditStore.Unavailable(failure);}
    }
    synchronized AuditStore.Event auditBegin(String action,String task,AccessControl.Principal actor,String peer,String request)throws AuditStore.Unavailable{
        flushAudit();AuditStore.Event event=new AuditStore.Event();event.kind="HTTP_BEGIN";event.correlation=event.id;event.action=action;event.taskId=entries.containsKey(task)?task:null;event.peer=peer;
        if(actor!=null){event.actorId=actor.id;event.actorRole=actor.role.name();}
        if(request!=null)event.clientRequestHash=AuditStore.hash(request);appendAudit(event);return event;
    }
    synchronized void auditComplete(AuditStore.Event begin,int status,String code)throws AuditStore.Unavailable{
        AuditStore.Event event=new AuditStore.Event();event.kind="HTTP_END";event.correlation=begin.correlation;event.action=begin.action;event.taskId=begin.taskId;event.peer=begin.peer;event.actorId=begin.actorId;event.actorRole=begin.actorRole;event.clientRequestHash=begin.clientRequestHash;event.httpStatus=status;event.status=status>=400?"REJECTED":"RESPONSE_PREPARED";event.code=code!=null&&code.matches("[A-Z0-9_]{1,120}")?code:null;appendAudit(event);
    }
    synchronized Map<String,Object> auditPage(long after,long through,int limit)throws AuditStore.Unavailable{
        flushAudit();Map<String,Object> page=sharedAccess==null?audit.page(after,through,limit):sharedAccess.auditPage(after,through,limit);page.put("pendingOperations",registry.auditOutbox.size());page.put("problem",auditProblem);return page;
    }
    private AccountDirectory.State accounts(){return sharedAccess!=null?sharedAccess.accounts():registry.accounts==null?configuredAccounts:registry.accounts;}
    private void verifyAccessAnchor()throws Exception{
        Map<String,Object> anchor=audit.accessAnchor();AccountDirectory.State state=registry.accounts;long version=((Number)anchor.get("version")).longValue();
        if(state==null){if(version!=0)throw new IOException("ACCOUNT_STATE_REQUIRED");}
        else if(version!=state.version||!Objects.equals(anchor.get("identity"),state.identity)||!Objects.equals(anchor.get("revision"),state.revision))throw new IOException("ACCOUNT_AUDIT_ANCHOR_MISMATCH");
    }
    synchronized AccessControl.Principal authenticate(String header){return AccountDirectory.authenticate(accounts(),header);}
    synchronized boolean sessionCurrent(AccessControl.Principal actor){
        AccountDirectory.State state=accounts();AccountDirectory.User user=state.users.get(actor.id);
        return user!=null&&user.enabled&&!user.deleted&&Objects.equals(actor.authorizationRevision,state.revision);
    }
    synchronized Map<String,Object> accountView(AccessControl.Principal actor){AccountDirectory.current(accounts(),actor);if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");Map<String,Object> view=AccountDirectory.view(accounts());view.put("authority",sharedAccess==null?"LOCAL":"SHARED");return view;}
    synchronized Map<String,Object> mutateAccounts(String action,String id,com.fasterxml.jackson.databind.JsonNode body,String request,AccessControl.Principal actor)throws Exception{
        flushAudit();if(sharedAccess!=null)return sharedAccess.mutate(action,id,body,request,actor);if(body.hasNonNull("nodeIds"))throw new IllegalArgumentException("ACCOUNT_NODE_SCOPE_SHARED_REQUIRED");verifyAccessAnchor();AccountDirectory.Change change=AccountDirectory.mutate(accounts(),action,id,body,request,actor);
        if(!change.replayed){Registry next=copy();next.version=2;next.accounts=change.state;next.auditOutbox.add(change.audit);persist(next);flushAudit();verifyAccessAnchor();}
        Map<String,Object> result=AccountDirectory.view(accounts());result.put("replayed",change.replayed);result.put("subjectId",change.userId);return result;
    }
    public static void recoverAccess(PlatformConfig config,String id)throws Exception{
        try(TaskService service=new TaskService(config,new TaskBackend.Database(),TaskWorker.Jvm::new,false)){service.recoverAccount(id);}
    }
    private synchronized void recoverAccount(String id)throws Exception{
        AccountDirectory.User seed=configuredAccounts.users.get(id);if(seed==null||seed.role!=PlatformConfig.Role.ADMIN)throw new IllegalArgumentException("ACCOUNT_RECOVERY_ADMIN_REQUIRED");
        if(sharedAccess!=null){sharedAccess.recover(id,seed);return;}
        flushAudit();verifyAccessAnchor();AccountDirectory.State current=accounts(),state=Model.JSON.readValue(Model.JSON.writeValueAsBytes(current),AccountDirectory.State.class);state.managed=true;if(state.identity==null)state.identity=UUID.randomUUID().toString();state.version=current.version+1;state.revision=UUID.randomUUID().toString();AccountDirectory.User restored=Model.JSON.readValue(Model.JSON.writeValueAsBytes(seed),AccountDirectory.User.class);restored.enabled=true;restored.deleted=false;restored.updatedAt=Instant.now().toString();state.users.put(id,restored);AccountDirectory.validate(state);
        Registry next=copy();next.version=2;next.accounts=state;AuditStore.Event event=AccountDirectory.event(state,"RECOVER",id,null);event.actorId="offline-recovery";next.auditOutbox.add(event);persist(next);flushAudit();verifyAccessAnchor();
    }
    String nodeId(){return config.cluster==null?null:config.cluster.nodeId;}
    boolean skipRecoveryEnabled(){return config.cluster==null||!config.cluster.sharedTasks||config.cluster.recoverySkip;}
    boolean commitRecoveryEnabled(){return config.cluster==null||!config.cluster.sharedTasks||config.cluster.recoveryCommit;}
    boolean failureReviewEnabled(){return config.cluster==null||!config.cluster.sharedTasks||config.cluster.recoveryReview;}
    String nodeRole(){return config.cluster==null?"CONTROLLER_WORKER":config.cluster.roleName();}
    boolean nodeAllowed(AccessControl.Principal actor){return actor==null||actor.node(nodeId());}
    synchronized AccessControl.Principal currentPrincipal(AccessControl.Principal supplied){
        if(supplied==null||supplied.authorizationRevision==null)throw new SecurityException("FORBIDDEN");
        AccountDirectory.State state=accounts();AccountDirectory.current(state,supplied);AccountDirectory.User user=state.users.get(supplied.id);
        if(user==null||!user.enabled||user.deleted)throw new SecurityException("FORBIDDEN");
        return new AccessControl.Principal(user.id,user.role,user.taskIds,state.revision,user.nodeIds);
    }
    private void currentActor(AccessControl.Principal actor){AccountDirectory.current(accounts(),actor);if(!nodeAllowed(actor))throw new SecurityException("FORBIDDEN");}
    private static String digest(String value) throws Exception {
        byte[] hash=MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));StringBuilder s=new StringBuilder();
        for(byte b:hash)s.append(String.format("%02x",b&255));return s.toString();
    }
    private static void requestId(String id) {
        if (id==null || !id.matches("[A-Za-z0-9_-]{8,128}")) throw new IllegalArgumentException("REQUEST_ID_REQUIRED");
    }
    private Receipt replay(String id,String signature,AccessControl.Principal actor) {
        Receipt receipt=registry.requests.get(id);
        if(receipt!=null && !receipt.signature.equals(signature))throw new Conflict("REQUEST_ID_REUSED");
        if(receipt!=null&&receipt.actorId!=null&&(actor==null||!receipt.actorId.equals(actor.id)||!Objects.equals(receipt.actorRole,actor.role.name())))throw new Conflict("REQUEST_IDENTITY_CHANGED");return receipt;
    }
    private void receipt(Registry next,String request,String task,String signature,AccessControl.Principal actor) {
        Receipt value=new Receipt(task,signature);if(actor!=null){value.actorId=actor.id;value.actorRole=actor.role.name();}next.requests.put(request,value);
        Iterator<String> requests=next.requests.keySet().iterator();while(next.requests.size()>500&&requests.hasNext()){String id=requests.next();if(!ownerRequests.contains(id))requests.remove();}
    }
    private static String actionSignature(String id,String action,String revision,long errorCount,String subscription,ValidationRequest validation,RepairRequest repair,CommitRequest commit,SkipRequest skip,GapVerificationRequest gap,CompensationRequest compensation,CorrectionRequest correction,String captureBoundaryHash,CaptureExclusionRequest captureExclusion)throws Exception{
        // Jackson may restore Set fields as HashSet; receipt inspection must match command admission's sorted sets.
        if(validation!=null){validation=Model.JSON.readValue(Model.JSON.writeValueAsBytes(validation),ValidationRequest.class);validation.validate("VALIDATE_RETRY".equals(action));}
        if(repair!=null){repair=Model.JSON.readValue(Model.JSON.writeValueAsBytes(repair),RepairRequest.class);repair.validate("REPAIR_EXECUTE".equals(action));}
        if(commit!=null)commit.validate();
        if(skip!=null)skip.validate(action);
        if(compensation!=null)compensation.validate(action);
        if(correction!=null)correction.validate(false,true);
        if(gap!=null)gap.validate("GAP_VERIFY_EXECUTE".equals(action));
        return digest(id+"\n"+action+"\n"+revision+"\n"+errorCount+(subscription==null?"":"\n"+subscription)+(validation==null?"":"\n"+io.github.rivomesh.cdc.Codec.encode(validation))+(repair==null?"":"\n"+io.github.rivomesh.cdc.Codec.encode(repair))+(commit==null?"":"\n"+io.github.rivomesh.cdc.Codec.encode(commit))+(skip==null?"":"\n"+io.github.rivomesh.cdc.Codec.encode(skip))+(gap==null?"":"\n"+io.github.rivomesh.cdc.Codec.encode(gap))+(compensation==null?"":"\n"+io.github.rivomesh.cdc.Codec.encode(compensation))+(correction==null?"":"\n"+io.github.rivomesh.cdc.Codec.encode(correction))+(captureBoundaryHash==null?"":"\n"+captureBoundaryHash)+(captureExclusion==null?"":"\n"+io.github.rivomesh.cdc.Codec.encode(captureExclusion)));
    }
    private static String configurationSignature(String id,String revision,com.fasterxml.jackson.databind.JsonNode document)throws Exception{return digest("EDIT_CONFIG\n"+id+"\n"+revision+"\n"+Model.JSON.writeValueAsString(document));}
    private static String restoreSignature(String id,String revision,String reference)throws Exception{return digest("RESTORE_CONFIG\n"+id+"\n"+revision+"\n"+reference);}
    static final class OwnerReceipt {
        final String phase,code;
        OwnerReceipt(String phase,String code){this.phase=phase;this.code=code;}
    }
    synchronized void pinOwnerRequest(String id){ensureOpen();requestId(id);if(!ownerRequests.contains(id)&&ownerRequests.size()>=config.maxTasks*2)throw new Conflict("OWNER_RECEIPT_CAPACITY");ownerRequests.add(id);}
    synchronized void releaseOwnerRequest(String id){ownerRequests.remove(id);}
    synchronized TaskRequest prepareOwnerRequest(TaskRequest supplied)throws Exception{
        ensureOpen();TaskRequest request=supplied.snapshot();if(!request.mutation())throw new IllegalArgumentException("OWNER_MUTATION_REQUIRED");
        if(request.kind!=TaskRequest.Kind.CREATE){require(request.taskId);request.revision=registry.tasks.get(request.taskId).revision;}return request;
    }
    synchronized TaskDefinition ownerResources(TaskRequest request)throws Exception{
        String yaml;if(request.kind==TaskRequest.Kind.CREATE)yaml=request.yaml;else if(request.kind==TaskRequest.Kind.CONFIG_UPDATE)yaml=new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.yaml.YAMLFactory()).writeValueAsString(request.document);else return null;
        TaskDefinition definition=parseTask(yaml);definition.validate();return definition;
    }
    synchronized OwnerReceipt ownerReceipt(TaskRequest supplied,String actorId,String actorRole)throws Exception{
        ensureOpen();TaskRequest request=supplied.snapshot();Receipt receipt=registry.requests.get(request.requestId);if(receipt==null)return null;
        String signature=ownerReceiptSignature(request);
        if(!Objects.equals(signature,receipt.signature)||!Objects.equals(request.taskId,receipt.taskId)||!Objects.equals(actorId,receipt.actorId)||!Objects.equals(actorRole,receipt.actorRole))throw new Conflict("OWNER_RECEIPT_MISMATCH");
        require(request.taskId);
        return ownerOutcome(request.taskId,request.requestId,request.kind);
    }
    static String ownerReceiptSignature(TaskRequest request)throws Exception{
        return switch(request.kind){
            case CREATE->digest("CREATE\n"+request.yaml);
            case CONFIG_UPDATE->configurationSignature(request.taskId,request.revision,request.document);
            case RESTORE->restoreSignature(request.taskId,request.revision,request.reference);
            case ACTION->{
                if(request.recoveryCancel())yield recoveryCancelSignature(request);
                if(request.validationMaintenance())yield validationMaintenanceSignature(request);
                yield actionSignature(request.taskId,request.action,request.revision,request.errorCount,request.subscriptionId,request.validation,request.repair,request.commit,request.skip,request.gap,request.compensation,request.correction,request.captureRetention()?request.reference:null,request.captureExclusion);
            }
            default->throw new IllegalArgumentException("OWNER_MUTATION_REQUIRED");
        };
    }
    private OwnerReceipt ownerOutcome(String task,String request,TaskRequest.Kind kind){
        if(kind==TaskRequest.Kind.CREATE)return new OwnerReceipt("SUCCEEDED","TASK_CREATED");
        if(kind==TaskRequest.Kind.CONFIG_UPDATE||kind==TaskRequest.Kind.RESTORE)return new OwnerReceipt("SUCCEEDED","CONFIG_UPDATED");
        for(Operation operation:registry.tasks.get(task).operations)if(request.equals(operation.id))return new OwnerReceipt("PENDING".equals(operation.status)?"ACCEPTED":operation.status,operation.code==null?"LOCAL_OPERATION_ACCEPTED":operation.code);
        return new OwnerReceipt("UNKNOWN","OWNER_OPERATION_EVIDENCE_REQUIRED");
    }
    public synchronized Map<String,Object> create(String yaml,String request) throws Exception {
        return create(yaml,request,null);
    }
    synchronized Map<String,Object> create(String yaml,String request,AccessControl.Principal actor) throws Exception {
        if(config.cluster!=null&&!config.cluster.executes())throw new Conflict("OWNER_ROLE_REQUIRED");
        currentActor(actor);
        ensureOpen();requestId(request);
        String signature=digest("CREATE\n"+yaml);Receipt previous=replay(request,signature,actor);
        if(previous!=null&&entries.containsKey(previous.taskId))return view(previous.taskId);
        TaskDefinition task=parseTask(yaml);validateTask(task,null);
        Path path=Paths.get(config.tasksDirectory).resolve(task.jobId+".yml").toAbsolutePath();
        if(Files.exists(path))throw new Conflict("CONFIG_FILE_EXISTS");
        Control state=new Control();state.fingerprint=task.fingerprint();if(actor!=null){state.createdBy=actor.id;state.createdRole=actor.role.name();state.createdAt=Instant.now().toString();}
        Registry next=copy();next.tasks.put(task.jobId,state);receipt(next,request,task.jobId,signature,actor);
        ConfigurationVersions.Stored stored=versions.create(task.jobId,task.workerYaml,"CREATE",request,actor,null,null);state.configurationVersion=stored.version.id;state.configurationHash=stored.hash;state.configurationFile=path.getFileName().toString();
        // The durable pointer is authoritative, including a crash before either projection is written.
        persist(next);Entry entry=new Entry(stored.document,path,task);entries.put(task.jobId,entry);project(entry);return view(task.jobId);
    }
    private com.fasterxml.jackson.databind.JsonNode document(TaskDefinition task)throws Exception{return new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.yaml.YAMLFactory()).readTree(task.workerYaml);}
    synchronized Map<String,Object> configuration(String id)throws Exception{Entry entry=require(id);Control state=registry.tasks.get(id);Map<String,Object> result=new LinkedHashMap<>();result.put("revision",state.revision);result.put("configurationVersion",state.configurationVersion);result.put("config",documents.redact(document(entry.config),id,state.revision));return result;}
    private TaskDefinition candidate(String id,com.fasterxml.jackson.databind.JsonNode supplied,String revision)throws Exception{
        Entry entry=require(id);Control state=registry.tasks.get(id);if(!state.revision.equals(revision))throw new Conflict("REVISION_CHANGED");com.fasterxml.jackson.databind.JsonNode restored=documents.restore(supplied,document(entry.config),id,revision);String yaml=new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.yaml.YAMLFactory()).writeValueAsString(restored);TaskDefinition candidate=parseTask(yaml);if(!id.equals(candidate.jobId))throw new Conflict("CONFIG_TASK_ID_CHANGED");validateTask(candidate,id);return candidate;
    }
    private Map<String,Object> comparison(Entry entry,TaskDefinition next)throws Exception{Map<String,Object> result=new LinkedHashMap<>();boolean identity=!entry.config.fingerprint().equals(next.fingerprint()),storage=!entry.config.stateIdentity().equals(next.stateIdentity());result.put("migrationRequired",identity||storage);result.put("stateStoreChanged",storage);result.put("changedPaths",ConfigurationDocuments.changes(document(entry.config),document(next)));return result;}
    synchronized Map<String,Object> previewConfiguration(String id,com.fasterxml.jackson.databind.JsonNode supplied,String revision)throws Exception{ensureOpen();TaskDefinition next=candidate(id,supplied,revision);return comparison(require(id),next);}
    synchronized Map<String,Object> updateConfiguration(String id,com.fasterxml.jackson.databind.JsonNode supplied,String revision,String request,AccessControl.Principal actor)throws Exception{
        currentActor(actor);
        ensureOpen();requestId(request);String signature=configurationSignature(id,revision,supplied);if(replay(request,signature,actor)!=null)return view(id);TaskDefinition next=candidate(id,supplied,revision);return commitConfiguration(id,next,revision,request,signature,actor,"EDIT_CONFIG");
    }
    private Map<String,Object> commitConfiguration(String id,TaskDefinition candidate,String revision,String request,String signature,AccessControl.Principal actor,String action)throws Exception{
        Entry entry=require(id);Control current=registry.tasks.get(id);if(!current.revision.equals(revision))throw new Conflict("REVISION_CHANGED");if(current.operations.stream().anyMatch(o->"PENDING".equals(o.status)))throw new Conflict("OPERATION_PENDING");if("RUNNING".equals(current.desired)||entry.worker!=null&&entry.worker.alive())throw new Conflict("CONFIG_STOP_REQUIRED");Map<String,Object> differences=comparison(entry,candidate);if(Boolean.TRUE.equals(differences.get("migrationRequired")))throw new Conflict("CONFIG_MIGRATION_REQUIRED");if(!entry.busy.compareAndSet(false,true))throw new Conflict("TASK_BUSY");
        try{if(current.configurationVersion!=null)versions.read(id,current.configurationVersion,current.configurationHash);stop(entry);entry.config.closeLocalJournal();String parent=current.configurationVersion,parentHash=current.configurationHash;if(parent==null){ConfigurationVersions.Stored baseline=versions.create(id,entry.config.workerYaml,"IMPORT_BASELINE",null,null,null,null);parent=baseline.version.id;parentHash=baseline.hash;}
            ConfigurationVersions.Stored stored=versions.create(id,candidate.workerYaml,action,request,actor,parent,parentHash);Registry next=copy();Control state=next.tasks.get(id);state.revision=UUID.randomUUID().toString();state.configurationVersion=stored.version.id;state.configurationHash=stored.hash;state.configurationFile=entry.configurationFile.getFileName().toString();Operation op=new Operation();op.id=request;op.action=action;op.status="SUCCEEDED";op.code="CONFIG_UPDATED";op.finishedAt=Instant.now().toString();op.result=differences;if(actor!=null){op.actorId=actor.id;op.actorRole=actor.role.name();}state.operations.add(op);while(state.operations.size()>40)state.operations.remove(0);receipt(next,request,id,signature,actor);persist(next);Entry replacement=new Entry(stored.document,entry.configurationFile,candidate);replacement.database=entry.database;replacement.observedAt=entry.observedAt;replacement.statusAvailable=entry.statusAvailable;replacement.problem=entry.problem;entries.put(id,replacement);project(replacement);return view(id);
        }finally{if(entries.get(id)==entry)entry.busy.set(false);}
    }
    private String reference(String id,Control state,ConfigurationVersions.Stored stored){return stored.version.id+"."+documents.signature("history\n"+id+"\n"+state.configurationVersion+"\n"+stored.version.id+"\n"+stored.hash);}
    private ConfigurationVersions.Stored resolveReference(String id,Control state,String reference)throws Exception{
        if(reference==null||reference.length()!=101||reference.charAt(36)!='.')throw new Conflict("CONFIG_HISTORY_CHANGED");ConfigurationVersions.Stored stored=versions.read(id,reference.substring(0,36),null);String expected=reference(id,state,stored);if(!MessageDigest.isEqual(reference.getBytes(StandardCharsets.US_ASCII),expected.getBytes(StandardCharsets.US_ASCII)))throw new Conflict("CONFIG_HISTORY_CHANGED");return stored;
    }
    synchronized Map<String,Object> configurationHistory(String id,String cursor)throws Exception{
        require(id);Control state=registry.tasks.get(id);Map<String,Object> result=new LinkedHashMap<>();List<Map<String,Object>> items=new ArrayList<>();result.put("versions",items);if(state.configurationVersion==null)return result;ConfigurationVersions.Stored stored=cursor==null?versions.read(id,state.configurationVersion,state.configurationHash):resolveReference(id,state,cursor);Set<String> seen=new HashSet<>();
        for(int i=0;stored!=null&&i<20;i++){if(!seen.add(stored.version.id))throw new IOException("CONFIG_VERSION_CORRUPT");ConfigurationVersions.Version v=stored.version;Map<String,Object> item=new LinkedHashMap<>();item.put("version",v.id);item.put("reference",reference(id,state,stored));item.put("createdAt",v.createdAt);item.put("action",v.action);item.put("actorId",v.actorId);item.put("actorRole",v.actorRole);items.add(item);stored=v.parent==null?null:versions.read(id,v.parent,v.parentHash);}
        if(stored!=null)result.put("nextCursor",reference(id,state,stored));return result;
    }
    synchronized Map<String,Object> previewRestoreConfiguration(String id,String selected,String revision)throws Exception{ensureOpen();Entry entry=require(id);Control state=registry.tasks.get(id);if(!state.revision.equals(revision))throw new Conflict("REVISION_CHANGED");ConfigurationVersions.Stored stored=resolveReference(id,state,selected);TaskDefinition candidate=parseTask(stored.yaml);validateTask(candidate,id);return comparison(entry,candidate);}
    synchronized Map<String,Object> restoreConfiguration(String id,String selected,String revision,String request,AccessControl.Principal actor)throws Exception{
        currentActor(actor);
        ensureOpen();requestId(request);String signature=restoreSignature(id,revision,selected);if(replay(request,signature,actor)!=null)return view(id);Entry entry=require(id);Control state=registry.tasks.get(id);if(!state.revision.equals(revision))throw new Conflict("REVISION_CHANGED");ConfigurationVersions.Stored stored=resolveReference(id,state,selected);TaskDefinition candidate=parseTask(stored.yaml);if(!id.equals(candidate.jobId))throw new Conflict("CONFIG_TASK_ID_CHANGED");validateTask(candidate,id);return commitConfiguration(id,candidate,revision,request,signature,actor,"RESTORE_CONFIG");
    }
    private static String recoveryCancelSignature(TaskRequest request)throws Exception{
        return digest("RECOVERY_CANCEL\n"+request.taskId+"\n"+request.revision+"\n"+request.reference);
    }
    synchronized Map<String,Object> cancelRecovery(TaskRequest supplied,AccessControl.Principal actor)throws Exception{
        ensureOpen();
        TaskRequest request=supplied.snapshot();
        if(!request.recoveryCancel())throw new IllegalArgumentException("ACTION_INVALID");
        currentActor(actor);
        if(actor==null||!actor.administers()||!actor.task(request.taskId))throw new SecurityException("FORBIDDEN");
        if(!skipRecoveryEnabled())throw new Conflict("SKIP_DISABLED");
        String signature=recoveryCancelSignature(request);
        if(replay(request.requestId,signature,actor)!=null)return view(request.taskId);
        require(request.taskId);
        Registry next=copy();
        Control task=next.tasks.get(request.taskId);
        if(!task.revision.equals(request.revision))throw new Conflict("REVISION_CHANGED");
        Operation parent=task.operations.stream().filter(op->request.reference.equals(op.id)&&TaskRequest.cancellableRecovery(op.action))
                .findFirst().orElseThrow(()->new Conflict("CANCEL_SCOPE_CHANGED"));
        if(parent.cancelRequested)throw new Conflict("CANCEL_ALREADY_REQUESTED");
        boolean running="PENDING".equals(parent.status);
        parent.cancelRequested=running;
        Operation cancel=new Operation();
        cancel.id=request.requestId;
        cancel.action=request.action;
        cancel.status="SUCCEEDED";
        cancel.code=running?"RECOVERY_CANCEL_REQUESTED":"PARENT_ALREADY_TERMINAL";
        cancel.actorId=actor.id;
        cancel.actorRole=actor.role.name();
        cancel.finishedAt=Instant.now().toString();
        cancel.result.put("subjectId",parent.id);
        task.operations.add(cancel);
        while(task.operations.size()>40)task.operations.remove(0);
        task.revision=UUID.randomUUID().toString();
        receipt(next,request.requestId,request.taskId,signature,actor);
        persist(next);
        AtomicBoolean cancellation=recoveryCancellations.get(request.taskId);
        if(running&&cancellation!=null)cancellation.set(true);
        return view(request.taskId);
    }
    public synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount) throws Exception {
        return command(id,action,revision,request,errorCount,null);
    }
    public synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,AccessControl.Principal actor) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,actor,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,AccessControl.Principal actor,ValidationRequest validation) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,actor,validation,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,AccessControl.Principal actor,ValidationRequest validation,RepairRequest repair) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,actor,validation,repair,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,AccessControl.Principal actor,ValidationRequest validation,RepairRequest repair,CommitRequest commit) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,actor,validation,repair,commit,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,AccessControl.Principal actor,ValidationRequest validation,RepairRequest repair,CommitRequest commit,SkipRequest skip) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,actor,validation,repair,commit,skip,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,AccessControl.Principal actor,ValidationRequest validation,RepairRequest repair,CommitRequest commit,SkipRequest skip,GapVerificationRequest gap) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,actor,validation,repair,commit,skip,gap,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,AccessControl.Principal actor,ValidationRequest validation,RepairRequest repair,CommitRequest commit,SkipRequest skip,GapVerificationRequest gap,CompensationRequest compensation) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,actor,validation,repair,commit,skip,gap,compensation,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,AccessControl.Principal actor,ValidationRequest validation,RepairRequest repair,CommitRequest commit,SkipRequest skip,GapVerificationRequest gap,CompensationRequest compensation,CorrectionRequest correction) throws Exception {
        return command(id,action,revision,request,errorCount,subscription,actor,validation,repair,commit,skip,gap,compensation,correction,null,null);
    }
    synchronized Map<String,Object> command(String id,String action,String revision,String request,long errorCount,String subscription,
                                            AccessControl.Principal actor,ValidationRequest validation,RepairRequest repair,CommitRequest commit,
                                            SkipRequest skip,GapVerificationRequest gap,CompensationRequest compensation,CorrectionRequest correction,
                                            String captureBoundaryHash,CaptureExclusionRequest captureExclusion) throws Exception {
        currentActor(actor);
        ensureOpen();requestId(request);
        boolean correcting="CORRECTION_EXECUTE".equals(action);
        if(correcting){
            if(!skipRecoveryEnabled())throw new Conflict("SKIP_DISABLED");
            if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");
            if(correction==null)throw new IllegalArgumentException("CORRECTION_APPROVAL_REQUIRED");
            correction=Model.JSON.readValue(Model.JSON.writeValueAsBytes(correction),CorrectionRequest.class);
            correction.validate(false,true);
        }else if(correction!=null)throw new IllegalArgumentException("CORRECTION_ACTION_REQUIRED");
        boolean compensating=CompensationRequest.handles(action);
        if (compensating) {
            if (!skipRecoveryEnabled()) throw new Conflict("SKIP_DISABLED");
            if (actor==null||!actor.administers()||!actor.task(id)) throw new SecurityException("FORBIDDEN");
            if (compensation==null) throw new IllegalArgumentException("COMPENSATION_APPROVAL_INVALID");
            compensation=Model.JSON.readValue(Model.JSON.writeValueAsBytes(compensation),CompensationRequest.class);
            compensation.validate(action);
        } else if (compensation!=null) throw new IllegalArgumentException("COMPENSATION_ACTION_REQUIRED");
        boolean gapping=Arrays.asList("GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE").contains(action);
        if(gapping||"GAP_VERIFY_CANCEL".equals(action)){if(!skipRecoveryEnabled())throw new Conflict("SKIP_DISABLED");if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");}
        if(gapping){if(gap==null)throw new IllegalArgumentException("GAP_VERIFICATION_REQUEST_INVALID");gap=Model.JSON.readValue(Model.JSON.writeValueAsBytes(gap),GapVerificationRequest.class);gap.validate("GAP_VERIFY_EXECUTE".equals(action));}else if(gap!=null)throw new IllegalArgumentException("GAP_VERIFICATION_ACTION_REQUIRED");
        boolean retaining="CAPTURE_RETAIN".equals(action);
        if(retaining){
            if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");
            if(errorCount<0||subscription!=null||captureBoundaryHash==null||!captureBoundaryHash.matches("[0-9a-f]{64}"))
                throw new IllegalArgumentException("CAPTURE_EVIDENCE_REQUEST_INVALID");
        }
        boolean excluding="CAPTURE_EXCLUDE".equals(action);
        if(excluding){
            if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");
            if(errorCount<0||subscription!=null||captureExclusion==null)
                throw new IllegalArgumentException("CAPTURE_EXCLUSION_APPROVAL_REQUIRED");
            captureExclusion.validate();
        }else if(captureExclusion!=null)throw new IllegalArgumentException("CAPTURE_EXCLUSION_ACTION_REQUIRED");
        boolean reviewing="FAILURE_REVIEW".equals(action);
        boolean confirming="CONFIRM_COMMIT".equals(action);
        boolean skipping=Arrays.asList("SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL").contains(action);
        if(skipping){if(!skipRecoveryEnabled())throw new Conflict("SKIP_DISABLED");if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");if(skip==null)throw new IllegalArgumentException("SKIP_REQUEST_INVALID");skip=Model.JSON.readValue(Model.JSON.writeValueAsBytes(skip),SkipRequest.class);skip.validate(action);}
        else if(skip!=null)throw new IllegalArgumentException("SKIP_ACTION_REQUIRED");
        if(confirming){if(!commitRecoveryEnabled())throw new Conflict("FAILURE_ACK_DISABLED");if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");if(commit==null)throw new IllegalArgumentException("FAILURE_ACK_REQUEST_INVALID");commit=Model.JSON.readValue(Model.JSON.writeValueAsBytes(commit),CommitRequest.class);commit.validate();}
        else if(commit!=null)throw new IllegalArgumentException("FAILURE_ACK_ACTION_REQUIRED");
        if(reviewing){if(!failureReviewEnabled())throw new Conflict("FAILURE_REVIEW_DISABLED");if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");}
        boolean repairing=Arrays.asList("REPAIR_PLAN","REPAIR_EXECUTE").contains(action);
        if(repairing||"REPAIR_CANCEL".equals(action)){if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");}
        if(repairing){if(repair==null)throw new IllegalArgumentException("REPAIR_OPTIONS_REQUIRED");repair=Model.JSON.readValue(Model.JSON.writeValueAsBytes(repair),RepairRequest.class);repair.validate("REPAIR_EXECUTE".equals(action));}
        else if(repair!=null)throw new IllegalArgumentException("REPAIR_ACTION_REQUIRED");
        boolean validating=Arrays.asList("VALIDATE","VALIDATE_RETRY").contains(action);
        if(!correcting&&!compensating&&!Arrays.asList("START","STOP","PAUSE","RESUME","CHECK","RESUME_TARGET","VALIDATE","VALIDATE_RETRY","VALIDATE_CANCEL","REPAIR_PLAN","REPAIR_EXECUTE","REPAIR_CANCEL","FAILURE_REVIEW","CAPTURE_RETAIN","CAPTURE_EXCLUDE","CONFIRM_COMMIT","SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL","GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE","GAP_VERIFY_CANCEL").contains(action))throw new IllegalArgumentException("ACTION_INVALID");
        if("VALIDATE_CANCEL".equals(action)&&(actor==null||!actor.administers()||!actor.task(id)))throw new SecurityException("FORBIDDEN");
        if(validating){if(actor==null||!actor.administers()||!actor.task(id))throw new SecurityException("FORBIDDEN");if(validation==null)throw new IllegalArgumentException("VALIDATION_OPTIONS_REQUIRED");validation=Model.JSON.readValue(Model.JSON.writeValueAsBytes(validation),ValidationRequest.class);validation.validate("VALIDATE_RETRY".equals(action));}
        else if(validation!=null)throw new IllegalArgumentException("VALIDATION_ACTION_REQUIRED");
        String signature=actionSignature(id,action,revision,errorCount,subscription,validation,repair,commit,skip,gap,compensation,correction,captureBoundaryHash,captureExclusion);
        if(replay(request,signature,actor)!=null)return view(id);
        Entry e=require(id);Control current=registry.tasks.get(id);
        if(!current.revision.equals(revision))throw new Conflict("REVISION_CHANGED");
        if("GAP_VERIFY_CANCEL".equals(action)){
            Registry next=copy();Control state=next.tasks.get(id);Operation pending=state.operations.stream().filter(o->"PENDING".equals(o.status)&&o.gap!=null).findFirst().orElseThrow(()->new Conflict("GAP_VERIFICATION_NOT_RUNNING"));if(pending.cancelRequested)throw new Conflict("GAP_VERIFICATION_CANCEL_ALREADY_REQUESTED");pending.cancelRequested=true;state.revision=UUID.randomUUID().toString();
            Operation cancel=new Operation();cancel.id=request;cancel.action=action;cancel.status="SUCCEEDED";cancel.code="GAP_VERIFICATION_CANCEL_REQUESTED";cancel.finishedAt=Instant.now().toString();cancel.actorId=actor.id;cancel.actorRole=actor.role.name();cancel.result.put("subjectId",pending.gapPlanId);state.operations.add(cancel);receipt(next,request,id,signature,actor);persist(next);Thread thread=operationThreads.get(id);if(thread!=null)thread.interrupt();return view(id);
        }
        if("REPAIR_CANCEL".equals(action)){
            Registry next=copy();Control state=next.tasks.get(id);Operation pending=state.operations.stream().filter(o->"PENDING".equals(o.status)&&o.repair!=null).findFirst().orElseThrow(()->new Conflict("REPAIR_NOT_RUNNING"));if(pending.cancelRequested)throw new Conflict("REPAIR_CANCEL_ALREADY_REQUESTED");pending.cancelRequested=true;state.revision=UUID.randomUUID().toString();
            Operation cancel=new Operation();cancel.id=request;cancel.action=action;cancel.status="SUCCEEDED";cancel.code="REPAIR_CANCEL_REQUESTED";cancel.finishedAt=Instant.now().toString();cancel.actorId=actor.id;cancel.actorRole=actor.role.name();cancel.result.put("subjectId",pending.repair.planId);state.operations.add(cancel);receipt(next,request,id,signature,actor);persist(next);Thread thread=operationThreads.get(id);if(thread!=null)thread.interrupt();return view(id);
        }
        if("VALIDATE_CANCEL".equals(action)){
            Registry next=copy();Control state=next.tasks.get(id);Operation pending=state.operations.stream().filter(o->"PENDING".equals(o.status)&&o.validation!=null).findFirst().orElseThrow(()->new Conflict("VALIDATION_NOT_RUNNING"));if(pending.cancelRequested)throw new Conflict("VALIDATION_CANCEL_ALREADY_REQUESTED");pending.cancelRequested=true;state.revision=UUID.randomUUID().toString();
            Operation cancel=new Operation();cancel.id=request;cancel.action=action;cancel.status="SUCCEEDED";cancel.code="VALIDATION_CANCEL_REQUESTED";cancel.finishedAt=Instant.now().toString();cancel.actorId=actor.id;cancel.actorRole=actor.role.name();cancel.result.put("reportId",pending.validation.reportId);cancel.result.put("subjectId",pending.validation.reportId);state.operations.add(cancel);receipt(next,request,id,signature,actor);persist(next);Thread thread=operationThreads.get(id);if(thread!=null)thread.interrupt();return view(id);
        }
        if(current.operations.stream().anyMatch(o -> "PENDING".equals(o.status)))throw new Conflict("OPERATION_PENDING");
        if(validating){
            if(e.config.pipeline==null)throw new Conflict("VALIDATION_V3_REQUIRED");
            if("VALIDATE".equals(action)){
                Set<String> allowed=new HashSet<>();for(io.github.rivomesh.PipelineConfig.Target target:e.config.pipeline.targets)if(!target.product.messaging()&&!target.product.materialized())allowed.add(target.id);
                if(!allowed.containsAll(validation.targets))throw new Conflict("VALIDATION_TARGET_PROTOCOL_UNSUPPORTED");
                if(e.worker==null||!e.worker.alive()||!"RUNNING".equals(current.desired))throw new Conflict("VALIDATION_WORKER_REQUIRED");validation.reportId=UUID.randomUUID().toString();
            }
        }
        if(repairing){if(e.config.pipeline==null)throw new Conflict("REPAIR_V3_REQUIRED");if("REPAIR_EXECUTE".equals(action)&&!"PAUSED".equals(current.desired))throw new Conflict("REPAIR_REQUIRES_PAUSED");if("REPAIR_PLAN".equals(action))repair.planId=UUID.randomUUID().toString();}
        if((reviewing||confirming||skipping||gapping||compensating||correcting||Arrays.asList("RESUME","RESUME_TARGET").contains(action))&&errorCount<0)throw new IllegalArgumentException("ERROR_GENERATION_REQUIRED");
        if((reviewing||confirming||skipping||gapping||compensating||correcting||"RESUME_TARGET".equals(action))&&(e.config.pipeline==null||subscription==null||e.config.pipeline.targets.stream().noneMatch(t->t.id.equals(subscription))))throw new IllegalArgumentException("SUBSCRIPTION_REQUIRED");
        if(confirming&&!"PAUSED".equals(current.desired))throw new Conflict("FAILURE_ACK_REQUIRES_PIPELINE_PAUSED");
        if(skipping&&!"PAUSED".equals(current.desired))throw new Conflict("SKIP_REQUIRES_PIPELINE_PAUSED");
        if(correcting&&!"PAUSED".equals(current.desired))throw new Conflict("CORRECTION_REQUIRES_PIPELINE_PAUSED");
        if(compensating&&!"PAUSED".equals(current.desired))throw new Conflict("COMPENSATION_REQUIRES_PIPELINE_PAUSED");
        if(gapping&&!"PAUSED".equals(current.desired))throw new Conflict("SKIP_REQUIRES_PIPELINE_PAUSED");
        Registry next=copy();Control state=next.tasks.get(id);state.revision=UUID.randomUUID().toString();
        Operation op=new Operation();op.id=request;op.action=action;op.errorCount=errorCount;op.subscriptionId=subscription;op.validation=validation;op.repair=repair;op.commit=commit;op.skip=skip;op.gap=gap;op.compensation=compensation;op.correction=correction;if(actor!=null){op.actorId=actor.id;op.actorRole=actor.role.name();}state.operations.add(op);
        if(retaining||excluding){
            if(e.config.pipeline==null||!"MYSQL".equals(e.config.pipeline.effectiveSourceType()))throw new Conflict("MYSQL_BINLOG_CONTENT_REVIEW_UNAVAILABLE");
            op.captureBoundaryHash=captureBoundaryHash;op.captureExclusion=captureExclusion;op.failureActorRevision=accounts().revision;
        }
        if(reviewing){op.failureEvidenceId=UUID.randomUUID().toString();op.failureActorRevision=accounts().revision;op.result.put("configurationRevision",revision);}
        if(correcting){op.failureEvidenceId=correction.evidenceId;op.failureActorRevision=accounts().revision;}
        if(compensating){op.failureEvidenceId=compensation.evidenceId;op.failureActorRevision=accounts().revision;}
        if(confirming){op.failureEvidenceId=commit.evidenceId;op.failureActorRevision=accounts().revision;}
        if(gapping){op.gapPlanId="GAP_VERIFY_PLAN".equals(action)?request:gap.planId;op.failureActorRevision=accounts().revision;}
        if(skipping){op.skipPlanId="SKIP_TRANSACTION".equals(action)?request:skip.planId;op.failureActorRevision=accounts().revision;if("SKIP_TRANSACTION".equals(action))op.failureEvidenceId=skip.evidenceId;}
        while(state.operations.size()>40)state.operations.remove(0);
        if("STOP".equals(action))state.desired="STOPPED";
        if("PAUSE".equals(action))state.desired="PAUSED";
        receipt(next,request,id,signature,actor);persist(next);
        if(Arrays.asList("START","RESUME","RESUME_TARGET").contains(action)){e.exits=0;e.retryAt=0;}
        return view(id);
    }
    private void schedule() {
        List<Map.Entry<String,Entry>> tasks;
        synchronized(this){if(closed)return;try{flushAudit();}catch(AuditStore.Unavailable ignored){}tasks=new ArrayList<>(entries.entrySet());}
        for(Map.Entry<String,Entry> task:tasks)if(task.getValue().busy.compareAndSet(false,true)) {
            try {
                boolean validation; synchronized(this){validation=registry.tasks.get(task.getKey()).operations.stream().anyMatch(o->"PENDING".equals(o.status)&&Arrays.asList("VALIDATE","VALIDATE_RETRY","REPAIR_PLAN","REPAIR_EXECUTE","FAILURE_REVIEW","CAPTURE_RETAIN","CAPTURE_EXCLUDE","CONFIRM_COMMIT","SKIP_TRANSACTION","SKIP_RETRY","SKIP_RECOVER","SKIP_CANCEL","GAP_VERIFY_PLAN","GAP_VERIFY_EXECUTE","COMPENSATION_PLAN","COMPENSATION_EXECUTE","CORRECTION_EXECUTE",ValidationArchive.COPY,ValidationArchive.RESTORE).contains(o.action));}
                (validation?validations:controls).execute(() -> {
                try { reconcile(task.getKey(),task.getValue()); }
                catch(Exception e){task.getValue().problem=safeCode(e);}
                finally{task.getValue().busy.set(false);}
            }); } catch(RejectedExecutionException e){task.getValue().busy.set(false);}
        }
    }
    private void reconcile(String id,Entry entry) throws Exception {
        // Paused/status/CLI access must not repeatedly replace H2 AUTO_SERVER endpoints.
        synchronized(this){if(closed)return;}
        entry.config.holdExistingLocalJournal();
        if(entry.projectionPending)project(entry);
        Operation pending;String desired;
        synchronized(this){Control c=registry.tasks.get(id);desired=c.desired;pending=c.operations.stream().filter(o -> "PENDING".equals(o.status)).findFirst().orElse(null);}
        if(pending!=null) {
            String code="OK";Map<String,Object> result=new LinkedHashMap<>();Exception failure=null;
            boolean cancellableRecovery=TaskRequest.cancellableRecovery(pending.action);
            boolean recoveryCancelled=false;
            AtomicBoolean cancellation=new AtomicBoolean();
            if(cancellableRecovery){
                synchronized(this){
                    if(closed)return;
                    recoveryCancelled=registry.tasks.get(id).operations.stream()
                            .anyMatch(op->pending.id.equals(op.id)&&op.cancelRequested);
                    if(recoveryCancelled&&System.currentTimeMillis()<entry.recoveryReceiptRetryAt)return;
                    recoveryCancellations.put(id,cancellation);
                }
            }
            try {
                if(recoveryCancelled){
                    result="COMPENSATION_PLAN".equals(pending.action)?null:backend.recoveryReceipt(entry.config,
                            failureEvidenceDirectory(id),failureEvidenceScope(id),pending);
                    code=result==null?"RECOVERY_CANCELLED":String.valueOf(result.get("code"));
                }else switch(pending.action) {
                    case "CHECK":result=backend.check(entry.config);break;
                    case "CAPTURE_RETAIN":
                        if(entry.config.pipeline==null||pending.actorId==null||pending.captureBoundaryHash==null)
                            throw new Conflict("CAPTURE_EVIDENCE_REQUEST_INVALID");
                        result=CaptureRecoveryReview.retain(entry.config.pipeline,captureEvidenceDirectory(id),
                                pending.errorCount,pending.captureBoundaryHash,()->authorizeCapture(pending));
                        code="CAPTURE_EVIDENCE_RETAINED";break;
                    case "CAPTURE_EXCLUDE":
                        if(entry.config.pipeline==null||pending.actorId==null||pending.captureExclusion==null)
                            throw new Conflict("CAPTURE_EXCLUSION_APPROVAL_REQUIRED");
                        stop(entry);
                        CaptureExclusionRequest approval=pending.captureExclusion;
                        result=CaptureRecoveryReview.exclude(entry.config.pipeline,captureEvidenceDirectory(id),
                                pending.errorCount,approval.boundaryHash,approval.archiveSha256,approval.planHash,
                                pending.id,pending.actorId,approval.reason,()->authorizeCapture(pending));
                        code="CAPTURE_TRANSACTION_EXCLUDED";break;
                    case ValidationRetention.ACTION:
                        synchronized(this){
                            authorizeValidationMaintenance(pending);
                            ValidationRetention.review(reports(id),pending.validationRetention,pending.id,pending.actorId,
                                    config.validationRetentionMinimumDays);
                        }
                        code="VALIDATION_RETENTION_REVIEWED";break;
                    case ValidationArchive.COPY:case ValidationArchive.RESTORE:
                        ValidationArchive.Scope scope;
                        synchronized(this){
                            authorizeValidationMaintenance(pending);
                            scope=validationArchiveScope(id,pending.validationArchive.reportId(),ValidationArchive.RESTORE.equals(pending.action));
                        }
                        ValidationArchive.Seal archived=ValidationArchive.execute(validationArchiveRoot(),reports(id),scope,pending.validationArchive,
                                pending.action,pending.id,pending.actorId);
                        result=Map.of("archiveId",archived.archiveId(),"hash",digest(Model.JSON.writeValueAsString(archived)));
                        code=ValidationArchive.COPY.equals(pending.action)?"VALIDATION_ARCHIVE_SEALED":"VALIDATION_ARCHIVE_RESTORED";
                        break;
                    case "VALIDATE":case "VALIDATE_RETRY":
                        synchronized(this){if(registry.tasks.get(id).operations.stream().anyMatch(o->o.id.equals(pending.id)&&o.cancelRequested))throw new Conflict("VALIDATION_CANCELLED");operationThreads.put(id,Thread.currentThread());}
                        if(pending.validation==null)throw new Conflict("VALIDATION_OPTIONS_REQUIRED");
                        if("VALIDATE".equals(pending.action)&&!Files.exists(reports(id).resolve(pending.validation.reportId))&&(entry.worker==null||!entry.worker.alive()))throw new Conflict("VALIDATION_WORKER_REQUIRED");
                        result=backend.validateRows(entry.config,reports(id),pending.validation,"VALIDATE_RETRY".equals(pending.action));code=String.valueOf(result.get("code"));break;
                    case "REPAIR_PLAN":case "REPAIR_EXECUTE":
                        synchronized(this){if(registry.tasks.get(id).operations.stream().anyMatch(o->o.id.equals(pending.id)&&o.cancelRequested))throw new Conflict("REPAIR_CANCELLED");operationThreads.put(id,Thread.currentThread());}
                        if(pending.repair==null||pending.actorId==null)throw new Conflict("REPAIR_OPTIONS_REQUIRED");
                        result=backend.repair(entry.config,reports(id),pending.repair,"REPAIR_EXECUTE".equals(pending.action),pending.actorId,pending.id,pending.createdAt);code=String.valueOf(result.get("code"));break;
                    case "GAP_VERIFY_PLAN":case "GAP_VERIFY_EXECUTE":
                        if(pending.gap==null||pending.actorId==null||pending.gapPlanId==null)throw new Conflict("GAP_VERIFICATION_REQUEST_INVALID");
                        synchronized(this){AccountDirectory.State access=accounts();AccountDirectory.User user=access.users.get(pending.actorId);if(!Objects.equals(pending.failureActorRevision,access.revision)||user==null||!user.enabled||user.deleted||user.role!=PlatformConfig.Role.ADMIN)throw new SecurityException("FORBIDDEN");if(!skipRecoveryEnabled())throw new Conflict("SKIP_DISABLED");if(registry.tasks.get(id).operations.stream().anyMatch(o->o.id.equals(pending.id)&&o.cancelRequested))throw new Conflict("GAP_VERIFICATION_CANCELLED_CHECK_RECEIPT");operationThreads.put(id,Thread.currentThread());}
                        result=backend.gapVerification(entry.config,reports(id),pending.gap,"GAP_VERIFY_EXECUTE".equals(pending.action),pending.id,pending.actorId,pending.createdAt,pending.subscriptionId,pending.errorCount);code=String.valueOf(result.get("code"));break;
                    case "SKIP_TRANSACTION":case "SKIP_RETRY":case "SKIP_RECOVER":case "SKIP_CANCEL":
                        if(pending.skip==null||pending.actorId==null||pending.skipPlanId==null)throw new Conflict("SKIP_REQUEST_INVALID");
                        synchronized(this){AccountDirectory.State access=accounts();AccountDirectory.User user=access.users.get(pending.actorId);if(!Objects.equals(pending.failureActorRevision,access.revision)||user==null||!user.enabled||user.deleted||user.role!=PlatformConfig.Role.ADMIN)throw new SecurityException("FORBIDDEN");if(!skipRecoveryEnabled())throw new Conflict("SKIP_DISABLED");}
                        result=backend.skip(entry.config,failureEvidenceDirectory(id),failureEvidenceScope(id),pending.action,pending.skip,pending.id,pending.actorId,pending.subscriptionId,pending.errorCount);code=String.valueOf(result.get("code"));break;
                    case "CORRECTION_EXECUTE":
                        if(pending.correction==null||pending.actorId==null)throw new Conflict("CORRECTION_APPROVAL_REQUIRED");
                        synchronized(this){
                            AccountDirectory.State access=accounts();
                            AccountDirectory.User user=access.users.get(pending.actorId);
                            if(!Objects.equals(pending.failureActorRevision,access.revision)||user==null||!user.enabled||user.deleted||user.role!=PlatformConfig.Role.ADMIN)
                                throw new SecurityException("FORBIDDEN");
                            if(!skipRecoveryEnabled())throw new Conflict("SKIP_DISABLED");
                        }
                        result=backend.correction(entry.config,failureEvidenceDirectory(id),failureEvidenceScope(id),
                                pending.correction,false,pending.id,pending.actorId,pending.subscriptionId,pending.errorCount,cancellation::get);
                        code=String.valueOf(result.get("code"));break;
                    case "COMPENSATION_PLAN":case "COMPENSATION_EXECUTE":
                        if (pending.compensation==null||pending.actorId==null) throw new Conflict("COMPENSATION_APPROVAL_INVALID");
                        synchronized(this){
                            AccountDirectory.State access=accounts();
                            AccountDirectory.User user=access.users.get(pending.actorId);
                            if (!Objects.equals(pending.failureActorRevision,access.revision)||user==null||!user.enabled||user.deleted||user.role!=PlatformConfig.Role.ADMIN)
                                throw new SecurityException("FORBIDDEN");
                            if (!skipRecoveryEnabled()) throw new Conflict("SKIP_DISABLED");
                        }
                        result=backend.compensate(entry.config,failureEvidenceDirectory(id),failureEvidenceScope(id),pending.action,pending.compensation,pending.id,pending.actorId,pending.subscriptionId,pending.errorCount,cancellation::get);
                        code=String.valueOf(result.get("code"));break;
                    case "CONFIRM_COMMIT":
                        if(pending.commit==null||pending.actorId==null)throw new Conflict("FAILURE_ACK_REQUEST_INVALID");
                        synchronized(this){AccountDirectory.State access=accounts();AccountDirectory.User user=access.users.get(pending.actorId);if(!Objects.equals(pending.failureActorRevision,access.revision)||user==null||!user.enabled||user.deleted||user.role!=PlatformConfig.Role.ADMIN)throw new SecurityException("FORBIDDEN");if(!commitRecoveryEnabled())throw new Conflict("FAILURE_ACK_DISABLED");}
                        result=backend.confirmCommit(entry.config,failureEvidenceDirectory(id),failureEvidenceScope(id),pending.commit,pending.id,pending.actorId,pending.subscriptionId,pending.errorCount);code="FAILURE_ACK_CONFIRMED";break;
                    case "FAILURE_REVIEW":
                        if(pending.failureEvidenceId==null||pending.actorId==null)throw new Conflict("FAILURE_REVIEW_OPTIONS_REQUIRED");
                        synchronized(this){AccountDirectory.State access=accounts();AccountDirectory.User user=access.users.get(pending.actorId);if(!Objects.equals(pending.failureActorRevision,access.revision)||user==null||!user.enabled||user.deleted||user.role!=PlatformConfig.Role.ADMIN)throw new SecurityException("FORBIDDEN");}
                        result=backend.reviewFailure(entry.config,failureEvidenceDirectory(id),failureEvidenceScope(id),pending.failureEvidenceId,pending.id,String.valueOf(pending.result.get("configurationRevision")),pending.actorId,pending.subscriptionId,pending.errorCount);code="FAILURE_EVIDENCE_READY";break;
                    case "STOP":stop(entry);code="LOCAL_STOPPED";break;
                    case "PAUSE":
                        stop(entry);
                        if(backend.status(entry.config).isEmpty())code="LOCAL_PAUSED_NOT_INITIALIZED";
                        else {result=backend.pause(entry.config);code="SHARED_PAUSED";if(result.containsKey("targetFences")&&((Map<?,?>)result.get("targetFences")).values().stream().anyMatch(v->String.valueOf(v).startsWith("UNCONFIRMED")))code="PAUSE_PENDING_TARGET_FENCE";}break;
                    case "RESUME":backend.resume(entry.config,pending.errorCount);code="RESUME_REQUESTED";break;
                    case "RESUME_TARGET":backend.subscription(entry.config,pending.subscriptionId,pending.action,pending.errorCount);result.put("pipelineState",backend.status(entry.config).get("run_state"));code="TARGET_RESUME_REQUESTED";break;
                    case "START":
                        if(Arrays.asList("PAUSED","ERROR").contains(backend.status(entry.config).get("run_state")))throw new Conflict("TASK_PAUSED_USE_RESUME");
                        code="START_REQUESTED";break;
                    default:throw new Conflict("ACTION_INVALID");
                }
            }catch(Exception e){failure=e;code=safeCode(e);}
            finally{if(pending.validation!=null||pending.repair!=null||pending.gap!=null){operationThreads.remove(id,Thread.currentThread());Thread.interrupted();}}
            synchronized(this) {
                if(cancellableRecovery)recoveryCancellations.remove(id);
                if(closed&&failure!=null&&(cancellableRecovery||Arrays.asList("CAPTURE_RETAIN","CAPTURE_EXCLUDE",
                        ValidationRetention.ACTION,ValidationArchive.COPY,ValidationArchive.RESTORE).contains(pending.action)))return;
                Registry next=copy();
                Control state=next.tasks.get(id);
                Operation op=state.operations.stream().filter(o -> o.id.equals(pending.id)).findFirst().get();
                if(cancellableRecovery&&op.cancelRequested&&failure!=null){
                    if(recoveryCancelled)entry.recoveryReceiptRetryAt=System.currentTimeMillis()+5000;
                    if(!"RECOVERY_CANCEL_RESOLVING".equals(op.code)){
                        op.code="RECOVERY_CANCEL_RESOLVING";
                        persist(next);
                    }
                    return;
                }
                op.status=failure==null?"SUCCEEDED":"FAILED";op.correction=null;op.code=code;
                op.result=result==null?new LinkedHashMap<>():result;op.finishedAt=Instant.now().toString();
                if(cancellableRecovery){
                    entry.recoveryReceiptRetryAt=0;
                    if(op.cancelRequested&&(result==null||"COMPENSATION_PLAN".equals(op.action))){
                        op.status="CANCELLED";
                        op.code="RECOVERY_CANCELLED";
                        op.result=new LinkedHashMap<>();
                    }
                }else if(op.cancelRequested){op.status="FAILED";op.code=op.gap!=null?"GAP_VERIFICATION_CANCELLED_CHECK_RECEIPT":op.repair==null?"VALIDATION_CANCELLED":"REPAIR_CANCELLED";}
                if(Arrays.asList("VALIDATE","VALIDATE_RETRY","REPAIR_EXECUTE").contains(op.action))state.desired="PAUSED";
                if(failure==null&&"CAPTURE_EXCLUDE".equals(op.action))state.desired="PAUSED";
                if(failure==null&&Arrays.asList("START","RESUME").contains(op.action))state.desired="RUNNING";
                if(failure==null&&"RESUME_TARGET".equals(op.action)&&"RUNNING".equals(result.get("pipelineState"))&&!"PAUSED".equals(state.desired))state.desired="RUNNING";
                persist(next);desired=state.desired;
            }
        }
        if(!"RUNNING".equals(desired)){stop(entry);entry.workerState="PAUSED".equals(desired)?"PAUSED":"STOPPED";}
        else if(!closed) {
            synchronized(entry){if(entry.worker!=null&&!entry.worker.alive()) {
                entry.problem="WORKER_EXIT_"+entry.worker.exitCode();entry.workerDiagnostic=entry.worker.diagnostic();entry.worker=null;
                if(entry.permit){slots.release();entry.permit=false;}
                entry.retryAt=System.currentTimeMillis()+Math.min(60000,1000L<<Math.min(entry.exits++,6));
            }}
            if(entry.worker==null&&entry.exits<5&&System.currentTimeMillis()>=entry.retryAt) {
                try {synchronized(this){if(!closed&&!ownerStartBlocks.contains(id)&&"RUNNING".equals(registry.tasks.get(id).desired))synchronized(entry){
                    if(slots.tryAcquire()){
                        entry.permit=true;
                        try{Control committed=registry.tasks.get(id);if(committed.configurationVersion!=null)versions.read(id,committed.configurationVersion,committed.configurationHash);else PrivateFiles.write(entry.path,entry.config.workerYaml.getBytes(StandardCharsets.UTF_8));entry.config.holdLocalJournal();entry.worker=workers.start(entry.path,logDirectory.resolve(id+".log"),config.workerHeapMiB);entry.workerState="RUNNING";}
                        catch(Exception e){slots.release();entry.permit=false;try{entry.config.closeLocalJournal();}catch(Exception close){e.addSuppressed(close);}throw e;}
                    }
                }}}
                catch(Exception e){entry.exits++;entry.retryAt=System.currentTimeMillis()+5000;entry.problem=safeCode(e);}
            }
            if(entry.worker==null){synchronized(this){entry.workerState=ownerStartBlocks.contains(id)?"OWNER_COMMAND_PENDING":entry.exits>=5?"CRASH_LOOP":System.currentTimeMillis()<entry.retryAt?"BACKOFF":"QUEUED";}}
        }
        try {
            Map<String,Object> database=publicStatus(backend.status(entry.config),entry.config);
            TaskWorker current=entry.worker;
            if(current!=null)entry.workerDiagnostic=current.diagnostic();
            TaskWorker.Diagnostic diagnostic=entry.workerDiagnostic;
            if(diagnostic!=null&&"RUNNING".equals(desired)&&!database.containsKey("error_stage")){
                database.put("worker_error_stage",diagnostic.phase.name());
                database.put("worker_error_code",diagnostic.code);
            }
            entry.database=database;entry.observedAt=Instant.now().toString();entry.statusAvailable=true;
            if(entry.database.isEmpty()&&!Arrays.asList("CRASH_LOOP","BACKOFF").contains(entry.workerState))entry.problem="NOT_INITIALIZED";
            else if(!Arrays.asList("CRASH_LOOP","BACKOFF").contains(entry.workerState))entry.problem=null;
        }catch(Exception e){
            entry.problem=safeCode(e);entry.statusAvailable=false;
            TaskWorker current=entry.worker;
            if(current!=null)entry.workerDiagnostic=current.diagnostic();
        }
    }
    private void stop(Entry e) throws Exception {synchronized(e){if(e.worker!=null){e.worker.stop();if(e.worker.alive())throw new Conflict("WORKER_STOP_UNCONFIRMED");e.worker=null;}if(e.permit){slots.release();e.permit=false;}e.workerState="STOPPED";}}
    private static Map<String,Object> publicStatus(Map<String,Object> db,TaskDefinition config) {
        Map<String,Object> out=new LinkedHashMap<>();
        for(String key:Arrays.asList("phase","run_state","node_id","source_address","capture_gtid","capture_lsn","capture_scn","required_gtid","applied_sequence","covered_sequence","captured_sequence","updated_at","error_code","error_stage","error_sqlstate","error_vendor","error_table","error_sequence","error_gtid","error_count","pending_transactions","table_count","epoch","lastSequence","pendingBytes","snapshotBytes","identityBytes","maxBytes","completed_tables","sourcePosition","dataFormat","hasGaps","gapCount","gapCountLimited","openGapCount","verifiedGapCount","hasOpenGaps","hasSourceExclusions"))
            if(db.containsKey(key))out.put(key,db.get(key));
        Object captureLease=db.get("captureLeaseState");
        if(config.pipeline!=null&&Arrays.asList("CLAIMED","EXPIRED","UNCLAIMED").contains(captureLease))
            out.put("captureLeaseState",captureLease);
        if(config.pipeline!=null&&db.get("normalizedSequence") instanceof Number sequence)
            out.put("normalized_sequence",Long.toString(sequence.longValue()));
        if(config.pipeline!=null&&db.get("gapsBySubscription") instanceof Map){
            Map<?,?> observed=(Map<?,?>)db.get("gapsBySubscription");Map<String,Long> counts=new LinkedHashMap<>();
            for(io.github.rivomesh.PipelineConfig.Target target:config.pipeline.targets){Object count=observed.get(target.id);if(count instanceof Number&&((Number)count).longValue()>=0&&((Number)count).longValue()<=1001)counts.put(target.id,((Number)count).longValue());}
            out.put("gapsBySubscription",counts);
        }
        if(db.containsKey("tables")) {
            List<Map<String,Object>> rows=new ArrayList<>();
            for(Object item:(List<?>)db.get("tables")) {
                Map<?,?> row=(Map<?,?>)item;Map<String,Object> safe=new LinkedHashMap<>();
                for(String key:Arrays.asList("table","copiedRows","done"))safe.put(key,row.get(key));rows.add(safe);
            }out.put("tables",rows);
        }
        if(db.containsKey("subscriptions"))out.put("subscriptions",Model.JSON.valueToTree(db.get("subscriptions")));
        if(!db.isEmpty()&&!out.containsKey("sourcePosition")) {
            Map<String,String> payload=new LinkedHashMap<>();
            payload.put("capturedGtid",String.valueOf(db.get("capture_gtid")));payload.put("requiredGtid",String.valueOf(db.get("required_gtid")));
            out.put("sourcePosition",new SourcePosition(1,"MYSQL",config.source.clusterId,payload));
        }return out;
    }
    synchronized List<ControlStore.Task> nodeObservations(){
        List<ControlStore.Task> result=new ArrayList<>();
        for(Map.Entry<String,Entry> item:entries.entrySet()){
            Control control=registry.tasks.get(item.getKey());ControlStore.Task task=new ControlStore.Task();task.id=item.getKey();task.revision=control.revision;task.desired=control.desired;task.workerState=item.getValue().workerState;task.observedAt=item.getValue().observedAt;result.add(task);
        }return result;
    }
    public synchronized Map<String,Object> list() {return list(null);}
    synchronized Map<String,Object> list(AccessControl.Principal actor) {
        Map<String,Object> result=new LinkedHashMap<>();List<Map<String,Object>> tasks=new ArrayList<>();
        for(String id:entries.keySet())if(nodeAllowed(actor)&&(actor==null||actor.task(id)))tasks.add(view(id));result.put("tasks",tasks);result.put("invalidConfigs",actor!=null&&(actor.scoped()||!nodeAllowed(actor))?Collections.emptyList():new ArrayList<>(invalid));return result;
    }
    public synchronized Map<String,Object> view(String id) {
        Entry e=require(id);Control c=registry.tasks.get(id);Map<String,Object> row=new LinkedHashMap<>();
        row.put("id",id);row.put("revision",c.revision);row.put("configurationVersion",c.configurationVersion);row.put("configurationProjectionPending",e.projectionPending);row.put("desired",c.desired);row.put("workerState",e.workerState);
        if(c.createdBy!=null){row.put("createdBy",c.createdBy);row.put("createdRole",c.createdRole);row.put("createdAt",c.createdAt);}
        row.put("sourceDatabase",e.config.databases());row.put("sourceCluster",e.config.source.clusterId);row.put("formatVersion",e.config.pipeline==null?2:3);
        row.put("targetSchema",e.config.target.schema);row.put("targetType",e.config.target.type);row.put("status",e.database);row.put("observedAt",e.observedAt);row.put("problem",e.problem);
        if(e.config.pipeline!=null){row.put("targetCount",e.config.pipeline.targets.size());row.put("destinations",e.config.destinations((String)e.database.get("generation")));}
        row.put("statusAvailable",e.statusAvailable);
        if(e.config.pipeline!=null)
            row.put("captureLeaseState",e.statusAvailable?e.database.getOrDefault("captureLeaseState","UNKNOWN"):"UNKNOWN");
        row.put("sourceType",e.config.sourceType());
        // Copy operation views: serialization must never race a later request mutating controller state.
        com.fasterxml.jackson.databind.node.ArrayNode operations=Model.JSON.valueToTree(c.operations);
        for(com.fasterxml.jackson.databind.JsonNode operation:operations)
            ((com.fasterxml.jackson.databind.node.ObjectNode)operation).remove(List.of("correction","validationRetention","validationArchive"));
        TaskWorker.Diagnostic startup=e.workerDiagnostic;
        if(startup!=null&&!e.statusAvailable&&"RUNNING".equals(c.desired)){
            row.put("workerErrorStage",startup.phase.name());
            row.put("workerErrorCode",startup.code);
        }
        row.put("operations",operations);return row;
    }
    private Entry require(String id){Entry e=entries.get(id);if(e==null)throw new NoSuchElementException("TASK_NOT_FOUND");return e;}
    Map<String,Object> handoffProbe(TaskRequest request,AccessControl.Principal actor)throws Exception{
        Map<String,Object> originalEvidence=null;
        SharedHandoffEvidence.ReceiptPlan closedReceipts=null;
        synchronized(this){
            ensureOpen();currentActor(actor);
            if(!actor.administers()||!actor.task(request.taskId)||!nodeAllowed(actor))throw new SecurityException("FORBIDDEN");
            if(config.cluster==null||!config.cluster.sharedTasks)throw new Conflict("GLOBAL_HANDOFF_REQUIRED");
            if(request.kind==TaskRequest.Kind.HANDOFF_EVIDENCE_PROBE){
                Entry entry=require(request.taskId);Control task=registry.tasks.get(request.taskId);
                if(!"STOPPED".equals(task.desired)||!"STOPPED".equals(entry.workerState)
                        ||task.operations.stream().anyMatch(op->"PENDING".equals(op.status)))
                    throw new Conflict("HANDOFF_ORIGINAL_WORKER_CHANGED");
                Map<String,String> inventory=Map.of(
                        "validationReports",privateDirectoryState(reports(request.taskId)),
                        "failureEvidence",privateDirectoryState(failureEvidenceDirectory(request.taskId)),
                        "sourceEvidence",privateDirectoryState(captureEvidenceDirectory(request.taskId)));
                long receipts=registry.requests.values().stream().filter(receipt->request.taskId.equals(receipt.taskId)).count();
                originalEvidence=new LinkedHashMap<>(Map.of("state","REFUSED","code","ORIGINAL_PRIVATE_EVIDENCE_CUSTODY_UNPROVEN",
                        "originInventory",inventory,"retainedReceipts",receipts));
                if(config.cluster.handoffEvidenceRoot!=null){
                    verifyHandoffEvidenceScope(request,true);
                    closedReceipts=collectClosedHandoffReceipts(request.taskId,request.handoffClosedReceipts);
                }
            }else{
                if(entries.containsKey(request.taskId))throw new Conflict("CANDIDATE_TASK_ALREADY_PRESENT");
                if(request.kind==TaskRequest.Kind.HANDOFF_CUSTODY_PROBE)verifyHandoffEvidenceScope(request,false);
            }
        }
        Path archiveRoot=config.cluster.handoffEvidenceRoot==null?null:Path.of(config.cluster.handoffEvidenceRoot);
        if(originalEvidence!=null){
            if(closedReceipts!=null){
                if(closedReceipts.refusal()!=null)originalEvidence.put("closedReceipts",Map.of("state","REFUSED","code",closedReceipts.refusal()));
                try{
                    SharedHandoffEvidence.Archive archive=SharedHandoffEvidence.export(archiveRoot,request.handoffEvidenceScope,
                            reports(request.taskId),closedReceipts.receipts());
                    if(archive.reports()>0)originalEvidence.put("shareableReports",Map.of("state","ARCHIVED","archive",archive));
                    if(archive.closedReceipts()>0)originalEvidence.put("closedReceipts",Map.of("state","ARCHIVED","archive",archive));
                }catch(Exception failed){
                    originalEvidence.put("shareableReports",archiveRefusal(failed));
                    if(!closedReceipts.receipts().isEmpty())originalEvidence.put("closedReceipts",archiveRefusal(failed));
                }
            }
            return originalEvidence;
        }
        if(request.kind==TaskRequest.Kind.HANDOFF_CUSTODY_PROBE){
            try{
                SharedHandoffEvidence.verify(archiveRoot,request.handoffEvidenceScope,request.handoffArchive);
                return Map.of("state","VERIFIED","archive",request.handoffArchive);
            }catch(Exception failed){return archiveRefusal(failed);}
        }
        if(request.kind!=TaskRequest.Kind.HANDOFF_CANDIDATE_PROBE||request.yaml==null||request.handoffCut==null)
            throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
        try{
            TaskDefinition definition=parseTask(request.yaml);
            if(!request.taskId.equals(definition.jobId)||definition.pipeline==null)
                throw new Conflict("HANDOFF_CONFIGURATION_CHANGED");
            return Map.of("state","VERIFIED","cut",definition.pipeline.verifyHandoffCut(request.handoffCut));
        }catch(io.github.rivomesh.cdc.JdbcJournal.Busy active){
            return Map.of("state","REFUSED","code","HANDOFF_JOURNAL_LEASE_ACTIVE");
        }catch(Exception failure){
            String code=safeCode(failure);
            if(code==null||!code.matches("(HANDOFF|WRITER_ASSIGNMENT|JOURNAL|SOURCE|SINK)_[A-Z0-9_]{1,90}"))
                code="HANDOFF_CANDIDATE_ACCESS_UNVERIFIED";
            return Map.of("state","REFUSED","code",code);
        }
    }
    private SharedHandoffEvidence.ReceiptPlan collectClosedHandoffReceipts(String task,SharedHandoffEvidence.ReceiptPlan plan){
        if(plan==null)return new SharedHandoffEvidence.ReceiptPlan(List.of(),"HANDOFF_CLOSED_RECEIPT_UNPROVEN");
        if(plan.refusal()!=null)return plan;
        Set<String> verified=new HashSet<>();
        for(SharedHandoffEvidence.ClosedReceipt expected:plan.receipts()){
            Receipt local=registry.requests.get(expected.request());
            OwnerReceipt actual=ownerOutcome(task,expected.request(),expected.kind());
            if(local==null||!task.equals(local.taskId)||!expected.signature().equals(local.signature)
                    ||!expected.actor().equals(local.actorId)||!expected.role().equals(local.actorRole)
                    ||!expected.phase().equals(actual.phase)||!expected.code().equals(actual.code)||!verified.add(expected.request()))
                return new SharedHandoffEvidence.ReceiptPlan(List.of(),"HANDOFF_CLOSED_RECEIPT_UNPROVEN");
        }
        long actual=registry.requests.entrySet().stream().filter(row->task.equals(row.getValue().taskId)).count();
        return actual==verified.size()?plan:new SharedHandoffEvidence.ReceiptPlan(List.of(),"HANDOFF_CLOSED_RECEIPT_UNPROVEN");
    }
    private void verifyHandoffEvidenceScope(TaskRequest request,boolean original){
        SharedHandoffEvidence.Scope scope=request.handoffEvidenceScope;
        if(scope==null||!config.cluster.id.equals(scope.cluster())||!request.taskId.equals(scope.task())
                ||!request.reference.equals(scope.assessment())||!request.revision.equals(scope.revision())
                ||!Objects.equals(registry.ownerDirectoryIdentity,original?scope.originalDirectory():scope.candidateDirectory()))
            throw new Conflict("HANDOFF_ARCHIVE_SCOPE_CHANGED");
    }
    private static Map<String,Object> archiveRefusal(Exception failure){
        String code=safeCode(failure);
        if(code==null||!code.matches("HANDOFF_[A-Z0-9_]{1,90}"))code="HANDOFF_ARCHIVE_UNAVAILABLE";
        return Map.of("state","REFUSED","code",code);
    }
    private static String privateDirectoryState(Path directory){
        for(Path path=directory;path!=null;path=path.getParent())if(Files.isSymbolicLink(path))return "UNSAFE";
        if(Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS))return "PRESENT";
        return Files.notExists(directory,LinkOption.NOFOLLOW_LINKS)?"ABSENT":"UNKNOWN";
    }
    synchronized Map<String,Object> workerDiagnostics(String id,String cursor,int limit,String subscription,AccessControl.Principal actor)throws Exception{
        ensureOpen();
        currentActor(actor);
        if(!actor.administers())throw new SecurityException("FORBIDDEN");
        if(!actor.task(id)||!nodeAllowed(actor))throw new NoSuchElementException("TASK_NOT_FOUND");
        Entry entry=require(id);
        if(subscription!=null&&(entry.config.pipeline==null
                ||entry.config.pipeline.targets.stream().noneMatch(target->subscription.equals(target.id))))
            throw new Conflict("DIAGNOSTICS_TARGET_UNKNOWN");
        if(!diagnosticReads.take(System.nanoTime()))throw new Conflict("DIAGNOSTICS_RATE_LIMITED");
        Map<String,Object> result=WorkerDiagnostics.page(logDirectory.resolve(id+".log"),cursor,limit,subscription);
        result.put("ownerNodeId",nodeId());
        return result;
    }

    Map<String,Object> captureRecovery(String id,AccessControl.Principal actor)throws Exception{
        TaskDefinition definition;String revision;
        synchronized(this){
            ensureOpen();currentActor(actor);if(!actor.administers())throw new SecurityException("FORBIDDEN");
            if(!actor.task(id))throw new NoSuchElementException("TASK_NOT_FOUND");
            definition=require(id).config;revision=registry.tasks.get(id).revision;
        }
        if(definition.pipeline==null)throw new Conflict("SOURCE_REVIEW_V3_REQUIRED");
        Map<String,Object> result;
        try{result=CaptureRecoveryReview.inspect(definition.pipeline);}
        catch(IllegalStateException failure){
            String code=failure.getMessage();
            if("SOURCE_REVIEW_REQUIRES_CAPTURE_ERROR".equals(code)||"SOURCE_REVIEW_STALE".equals(code))throw new Conflict(code);
            throw failure;
        }
        synchronized(this){
            ensureOpen();currentActor(actor);
            if(definition!=require(id).config||!revision.equals(registry.tasks.get(id).revision))
                throw new Conflict("SOURCE_REVIEW_STALE");
        }
        return result;
    }
    Map<String,Object> captureExclusions(String id,String after,int limit,AccessControl.Principal actor)throws Exception{
        TaskDefinition definition;
        String revision;
        synchronized(this){
            ensureOpen();
            currentActor(actor);
            if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");
            if(!actor.task(id)||!nodeAllowed(actor))throw new NoSuchElementException("TASK_NOT_FOUND");
            definition=require(id).config;
            revision=registry.tasks.get(id).revision;
        }
        if(definition.pipeline==null)throw new Conflict("SOURCE_REVIEW_V3_REQUIRED");
        Map<String,Object> page;
        try(io.github.rivomesh.cdc.JdbcJournal journal=new io.github.rivomesh.cdc.JdbcJournal(definition.pipeline.existingJournalSettings())){
            journal.verifyIdentity(definition.pipeline.fingerprint(),definition.pipeline.subscribers());
            page=journal.captureExclusions(after,limit);
        }
        synchronized(this){
            ensureOpen();
            currentActor(actor);
            if(!revision.equals(registry.tasks.get(id).revision))throw new Conflict("SOURCE_REVIEW_STALE");
        }
        return page;
    }
    private synchronized Void authorizeCapture(Operation pending) {
        ensureOpen();
        AccountDirectory.State directory=accounts();
        AccountDirectory.User user=directory.users.get(pending.actorId);
        if(!Objects.equals(directory.revision,pending.failureActorRevision)||user==null
                ||!user.enabled||user.deleted||user.role!=PlatformConfig.Role.ADMIN)
            throw new SecurityException("FORBIDDEN");
        return null;
    }
    private Path captureEvidenceDirectory(String id){
        return Paths.get(config.dataDirectory).toAbsolutePath().normalize().resolve("capture-evidence").resolve(id);
    }
    private Path reports(String id){return Paths.get(config.dataDirectory).toAbsolutePath().normalize().resolve("validations").resolve(id);}
    private Path failureEvidenceDirectory(String id){return Paths.get(config.dataDirectory).toAbsolutePath().normalize().resolve("failure-evidence").resolve(id);}
    private io.github.rivomesh.recovery.FailureEvidence.Scope failureEvidenceScope(String id)throws Exception{
        io.github.rivomesh.recovery.FailureEvidence.Scope scope=new io.github.rivomesh.recovery.FailureEvidence.Scope();scope.taskId=id;scope.ownerNodeId=nodeId()==null?"local":nodeId();scope.ownerDirectoryId=ownerDirectoryIdentity();return scope;
    }
    synchronized Map<String,Object> failureEvidence(String id,String evidence,String view,int from,int limit,AccessControl.Principal actor)throws Exception{
        currentActor(actor);if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");if(!actor.task(id)||!nodeAllowed(actor))throw new NoSuchElementException("TASK_NOT_FOUND");require(id);
        if(!failureReviewEnabled())throw new Conflict("FAILURE_REVIEW_DISABLED");io.github.rivomesh.recovery.FailureEvidence.uuid(evidence);
        if(view!=null&&!Arrays.asList("RAW","NORMALIZED").contains(view))throw new IllegalArgumentException("FAILURE_EVIDENCE_PAGE_INVALID");
        Map<String,Object> result=backend.failureEvidence(failureEvidenceDirectory(id),failureEvidenceScope(id),evidence,view,from,limit);
        AuditStore.Event event=new AuditStore.Event();event.kind=view==null?"FAILURE_EVIDENCE_SUMMARY_READ":"FAILURE_EVIDENCE_EVENTS_READ";event.action=view==null?"READ_FAILURE_SUMMARY":"READ_FAILURE_EVENTS";event.taskId=id;event.subjectId=evidence;event.operationHash=String.valueOf(result.get("hash"));event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";appendAudit(event);return result;
    }
    private Entry recoveryRead(String id,AccessControl.Principal actor)throws Exception{
        ensureOpen();currentActor(actor);if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");if(!actor.task(id)||!nodeAllowed(actor))throw new NoSuchElementException("TASK_NOT_FOUND");if(!skipRecoveryEnabled())throw new Conflict("SKIP_DISABLED");return require(id);
    }
    Map<String,Object> correctionPreview(TaskRequest request,AccessControl.Principal actor)throws Exception{
        String id=request.taskId,authority;
        TaskDefinition definition;
        io.github.rivomesh.recovery.FailureEvidence.Scope scope;
        synchronized(this){
            definition=recoveryRead(id,actor).config;
            Control task=registry.tasks.get(id);
            if(!task.revision.equals(request.revision))throw new Conflict("REVISION_CHANGED");
            if(!"PAUSED".equals(task.desired))throw new Conflict("CORRECTION_REQUIRES_PIPELINE_PAUSED");
            if(task.operations.stream().anyMatch(o->"PENDING".equals(o.status)))throw new Conflict("OPERATION_PENDING");
            authority=accounts().revision;
            scope=failureEvidenceScope(id);
        }
        boolean schema=request.kind==TaskRequest.Kind.CORRECTION_SCHEMA;
        Map<String,Object> result;
        try{
            result=backend.correction(definition,failureEvidenceDirectory(id),scope,request.correction,
                    schema,null,actor.id,request.subscriptionId,request.errorCount,()->false);
        }catch(IllegalArgumentException|IllegalStateException failure){
            String safe=safeCode(failure);
            if(safe.startsWith("CORRECTION_")||safe.startsWith("COMPENSATION_")||safe.startsWith("FAILURE_")
                    ||safe.startsWith("REPAIR_")||safe.startsWith("SINK_")||"ERROR_GENERATION_CHANGED".equals(safe))
                throw new Conflict(safe);
            throw failure;
        }
        synchronized(this){
            if(recoveryRead(id,actor).config!=definition||!registry.tasks.get(id).revision.equals(request.revision))
                throw new Conflict("REVISION_CHANGED");
            if(!Objects.equals(authority,accounts().revision))throw new SecurityException("FORBIDDEN");
            AuditStore.Event event=new AuditStore.Event();
            event.kind="CORRECTION_REVIEW";
            event.action=schema?"READ_CORRECTION_SCHEMA":"READ_CORRECTION_PREVIEW";
            event.taskId=id;event.subjectId=request.correction.evidenceId;
            event.operationHash=String.valueOf(result.getOrDefault("hash",request.correction.evidenceHash));
            event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";
            appendAudit(event);return result;
        }
    }
    Map<String,Object> skipPlan(String id,String plan,boolean nativeProof,AccessControl.Principal actor)throws Exception{
        TaskDefinition definition;String authority;
        synchronized(this){definition=recoveryRead(id,actor).config;authority=accounts().revision;}
        Map<String,Object> result=backend.skipPlan(definition,plan,nativeProof);
        synchronized(this){if(recoveryRead(id,actor).config!=definition)throw new Conflict("REVISION_CHANGED");if(!Objects.equals(authority,accounts().revision))throw new SecurityException("FORBIDDEN");AuditStore.Event event=new AuditStore.Event();event.kind="SKIP_PLAN_READ";event.action=nativeProof?"READ_SKIP_NATIVE_PROOF":"READ_SKIP_PLAN";event.taskId=id;event.subjectId=plan;event.operationHash=String.valueOf(result.get("planHash"));event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";appendAudit(event);return result;}
    }
    Map<String,Object> gapVerificationPlan(String id,String plan,AccessControl.Principal actor)throws Exception{
        TaskDefinition definition;String authority;
        synchronized(this){definition=recoveryRead(id,actor).config;authority=accounts().revision;}
        Map<String,Object> result=backend.gapVerificationPlan(definition,plan);
        synchronized(this){if(recoveryRead(id,actor).config!=definition)throw new Conflict("REVISION_CHANGED");if(!Objects.equals(authority,accounts().revision))throw new SecurityException("FORBIDDEN");AuditStore.Event event=new AuditStore.Event();event.kind="GAP_VERIFICATION_READ";event.action="READ_GAP_VERIFICATION";event.taskId=id;event.subjectId=plan;event.operationHash=String.valueOf(result.get("hash"));event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";appendAudit(event);return result;}
    }
    Map<String,Object> recoveryPage(String id,String subscription,String cursor,int limit,boolean gaps,AccessControl.Principal actor)throws Exception{
        TaskDefinition definition;String authority;
        synchronized(this){definition=recoveryRead(id,actor).config;authority=accounts().revision;}
        Map<String,Object> result=backend.recoveryPage(definition,subscription,cursor,limit,gaps);
        synchronized(this){if(recoveryRead(id,actor).config!=definition)throw new Conflict("REVISION_CHANGED");if(!Objects.equals(authority,accounts().revision))throw new SecurityException("FORBIDDEN");AuditStore.Event event=new AuditStore.Event();event.kind=gaps?"DATA_GAPS_READ":"SKIP_PLANS_READ";event.action=gaps?"READ_DATA_GAPS":"READ_SKIP_PLANS";event.taskId=id;event.subjectId=subscription;event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";appendAudit(event);return result;}
    }
    Map<String,Object> validationReports(String id,String after,int limit,AccessControl.Principal actor)throws Exception{
        TaskDefinition definition;
        synchronized(this){
            ensureOpen();
            currentActor(actor);
            if(actor==null||!actor.task(id)||!nodeAllowed(actor))throw new NoSuchElementException("TASK_NOT_FOUND");
            definition=require(id).config;
        }
        if(definition.pipeline==null)throw new Conflict("VALIDATION_V3_REQUIRED");
        Map<String,Object> page=io.github.rivomesh.validation.ValidationStore.catalog(reports(id),after,limit);
        synchronized(this){
            ensureOpen();
            currentActor(actor);
            if(definition!=require(id).config)throw new Conflict("REVISION_CHANGED");
            AuditStore.Event event=new AuditStore.Event();
            event.kind="VALIDATION_CATALOG_READ";event.action="READ_VALIDATION_CATALOG";event.taskId=id;
            event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";
            appendAudit(event);
        }
        return page;
    }
    synchronized Map<String,Object> validationReport(String id,String reportId,AccessControl.Principal actor)throws Exception{
        currentActor(actor);if(actor==null||!actor.task(id))throw new NoSuchElementException("TASK_NOT_FOUND");Entry entry=require(id);
        if(reportId==null||!UUID.fromString(reportId).toString().equals(reportId))throw new IllegalArgumentException("VALIDATION_ID_INVALID");Map<String,Object> result=backend.validationReport(entry.config,reports(id),reportId);
        AuditStore.Event event=new AuditStore.Event();event.kind="VALIDATION_REPORT_READ";event.action="READ_VALIDATION_REPORT";event.taskId=id;event.subjectId=reportId;event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";appendAudit(event);return result;
    }
    synchronized Map<String,Object> validationStorage(String id,String reportId,AccessControl.Principal actor)throws Exception{
        ensureOpen();currentActor(actor);
        if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");
        if(!actor.task(id)||!nodeAllowed(actor))throw new NoSuchElementException("TASK_NOT_FOUND");
        if(require(id).config.pipeline==null)throw new Conflict("VALIDATION_V3_REQUIRED");
        Map<String,Object> result;
        try{
            result=io.github.rivomesh.validation.ValidationStore.storage(reports(id),reportId);
            ValidationRetention.Review retained=ValidationRetention.observe(reports(id),reportId);
            if(config.validationRetentionMinimumDays>0||retained!=null)result.put("retentionMinimumDays",config.validationRetentionMinimumDays);
            if(retained!=null)result.put("retention",retained);
            if(validationArchiveRoot()!=null)result.put("archiveEnabled",true);
            ValidationArchive.Seal archive=ValidationArchive.observe(reports(id).resolve(reportId),reportId);
            if(archive!=null){
                ValidationArchive.Scope scope=validationArchiveScope(id,reportId,true);
                if(!scope.equals(archive.scope()))throw new Conflict("VALIDATION_ARCHIVE_SCOPE_INVALID");
                result.put("archive",Map.of("id",archive.archiveId(),"archivedAt",archive.archivedAt(),
                        "actorId",archive.actorId(),"bytes",archive.files().values().stream().mapToLong(ValidationArchive.FileSeal::bytes).sum()));
            }
        }
        catch(io.github.rivomesh.SyncFailure unavailable){throw new Conflict(unavailable.code);}
        AuditStore.Event event=new AuditStore.Event();
        event.kind="VALIDATION_STORAGE_READ";event.action="READ_VALIDATION_STORAGE";event.taskId=id;event.subjectId=reportId;
        event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";appendAudit(event);
        return result;
    }
    private Path validationArchiveRoot(){return config.validationArchiveDirectory==null?null:Paths.get(config.validationArchiveDirectory);}
    private ValidationArchive.Scope validationArchiveScope(String task,String report,boolean existing)throws Exception{
        if(existing&&!Files.isRegularFile(statePath.getParent().resolve("owner-directory-id"),LinkOption.NOFOLLOW_LINKS))
            throw new Conflict("OWNER_DIRECTORY_REQUIRED");
        return new ValidationArchive.Scope(task,ownerDirectoryIdentity(),report);
    }
    private void authorizeValidationMaintenance(Operation pending){
        AccountDirectory.State access=accounts();AccountDirectory.User user=access.users.get(pending.actorId);
        if(!Objects.equals(pending.failureActorRevision,access.revision)||user==null||!user.enabled
                ||user.deleted||user.role!=PlatformConfig.Role.ADMIN)throw new Conflict("FORBIDDEN");
    }
    private static String validationMaintenanceSignature(TaskRequest request)throws Exception{
        return digest(request.action+"\n"+request.taskId+"\n"+request.revision+"\n"
                +Model.JSON.writeValueAsString(request.reviewValidationRetention()?request.validationRetention:request.validationArchive));
    }
    synchronized Map<String,Object> validationMaintenance(TaskRequest request,AccessControl.Principal actor)throws Exception{
        ensureOpen();currentActor(actor);
        if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");
        if(!actor.task(request.taskId))throw new NoSuchElementException("TASK_NOT_FOUND");
        String signature=validationMaintenanceSignature(request);
        if(replay(request.requestId,signature,actor)!=null)return view(request.taskId);
        if(request.reviewValidationRetention()){
            if(config.validationRetentionMinimumDays==0)throw new Conflict("VALIDATION_RETENTION_REVIEW_DISABLED");
            if(request.validationRetention.minimumDays()<config.validationRetentionMinimumDays)
                throw new Conflict("VALIDATION_RETENTION_MINIMUM_NOT_MET");
        }else if(validationArchiveRoot()==null)throw new Conflict("VALIDATION_ARCHIVE_DISABLED");
        if(require(request.taskId).config.pipeline==null)throw new Conflict("VALIDATION_V3_REQUIRED");
        Control current=registry.tasks.get(request.taskId);
        if(!Objects.equals(current.revision,request.revision))throw new Conflict("REVISION_CHANGED");
        if(current.operations.stream().anyMatch(operation->"PENDING".equals(operation.status)))throw new Conflict("OPERATION_PENDING");
        Registry next=copy();Control state=next.tasks.get(request.taskId);
        Operation operation=new Operation();operation.id=request.requestId;operation.action=request.action;
        operation.actorId=actor.id;operation.actorRole=actor.role.name();operation.failureActorRevision=accounts().revision;
        operation.validationRetention=request.validationRetention;operation.validationArchive=request.validationArchive;
        state.operations.add(operation);
        while(state.operations.size()>40)state.operations.remove(0);
        state.revision=UUID.randomUUID().toString();
        receipt(next,request.requestId,request.taskId,signature,actor);
        persist(next);
        return view(request.taskId);
    }
    synchronized Map<String,Object> validationDifferences(String id,String reportId,String target,String table,String after,int limit,String identity,AccessControl.Principal actor)throws Exception{
        currentActor(actor);if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");if(!actor.task(id))throw new NoSuchElementException("TASK_NOT_FOUND");Entry entry=require(id);
        if(reportId==null||!UUID.fromString(reportId).toString().equals(reportId)||target==null||table==null)throw new IllegalArgumentException("VALIDATION_DIFF_SCOPE_INVALID");
        Map<String,Object> result=backend.validationDifferences(entry.config,reports(id),reportId,target,table,after,limit,identity);
        AuditStore.Event event=new AuditStore.Event();event.kind="VALIDATION_DIFFERENCE_READ";event.action=identity==null?"READ_VALIDATION_DIFFERENCES":"READ_VALIDATION_VALUES";event.taskId=id;event.subjectId=reportId;event.operationHash=AuditStore.hash(target+"\n"+table+"\n"+(identity==null?after:identity));event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";appendAudit(event);return result;
    }
    synchronized Map<String,Object> repairPlan(String id,String report,String plan,AccessControl.Principal actor)throws Exception{
        currentActor(actor);if(actor==null||!actor.administers())throw new SecurityException("FORBIDDEN");if(!actor.task(id))throw new NoSuchElementException("TASK_NOT_FOUND");
        if(report==null||plan==null||!UUID.fromString(report).toString().equals(report)||!UUID.fromString(plan).toString().equals(plan))throw new IllegalArgumentException("REPAIR_REQUEST_INVALID");Entry entry=require(id);Map<String,Object> result=backend.repairPlan(entry.config,reports(id),report,plan);
        AuditStore.Event event=new AuditStore.Event();event.kind="REPAIR_PLAN_READ";event.action="READ_REPAIR_PLAN";event.taskId=id;event.subjectId=plan;event.operationHash=String.valueOf(result.get("hash"));event.actorId=actor.id;event.actorRole=actor.role.name();event.status="RESPONSE_PREPARED";appendAudit(event);return result;
    }
    private void ensureOpen(){if(closed)throw new Conflict("CONTROLLER_STOPPING");}
    public static String safeCode(Throwable error) {
        if(error instanceof Conflict)return error.getMessage();
        if(error instanceof SyncFailure)return ((SyncFailure)error).code;
        if((error instanceof IllegalStateException||error instanceof IllegalArgumentException)&&error.getMessage()!=null&&error.getMessage().matches("(?:VALIDATION|REPAIR|FAILURE|SKIP|SINK|GAP|COMPENSATION|CORRECTION)_[A-Z0-9_]+"))return error.getMessage();
        if("ERROR_GENERATION_CHANGED".equals(error.getMessage()))return "ERROR_GENERATION_CHANGED";
        String message=error.getMessage();
        if(message!=null&&(error instanceof IllegalStateException||error instanceof IOException)
                &&("SOURCE_REVIEW_STALE".equals(message)||message.matches("CAPTURE_EXCLUSION_[A-Z0-9_]+")||message.matches("MYSQL_BINLOG_(?:CONTENT_REVIEW_UNAVAILABLE|EVIDENCE_[A-Z0-9_]+)")))
            return message;
        if(Arrays.asList("CONFIG_VERSION_CORRUPT","CONFIG_VERSION_INVALID","JOURNAL_IDENTITY_MISMATCH","SUBSCRIPTION_MIGRATION_REQUIRED","DESTINATION_ALREADY_OWNED","WORKER_ENVIRONMENT_DENIED").contains(error.getMessage()))return error.getMessage();
        for(Throwable e=error;e!=null;e=e.getCause())if(e instanceof SQLException) {
            SQLException sql=(SQLException)e;return "SQL_"+(sql.getSQLState()==null?"UNKNOWN":sql.getSQLState())+"_"+sql.getErrorCode();
        }
        return error instanceof IllegalArgumentException?"CONFIG_INVALID":error instanceof IOException?"STORAGE_ERROR":"CONTROL_FAILED";
    }
    @Override public void close() throws Exception {
        boolean recovering;
        synchronized(this){
            if(closed)return;
            closed=true;
            recovering=!recoveryCancellations.isEmpty();
            recoveryCancellations.values().forEach(cancellation->cancellation.set(true));
        }
        for(Entry e:entries.values())synchronized(e){if(e.worker!=null)e.worker.requestStop();}
        scheduler.shutdownNow();controls.shutdown();
        // The recovery thread holds a journal fence, possibly on an embedded H2 FileChannel.
        if(recovering)validations.shutdown();else validations.shutdownNow();
        boolean terminated=controls.awaitTermination(20,TimeUnit.SECONDS);
        if(!terminated){controls.shutdownNow();terminated=controls.awaitTermination(10,TimeUnit.SECONDS);}
        terminated=validations.awaitTermination(10,TimeUnit.SECONDS)&&terminated;
        Exception failure=null;
        for(Entry e:entries.values())try{stop(e);}catch(Exception ex){failure=ex;}
        for(Entry e:entries.values())try{e.config.closeLocalJournal();}catch(Exception ex){failure=ex;}
        if(!terminated)throw new IOException("CONTROL_SHUTDOWN_INCOMPLETE");
        try{flushAudit();AuditStore.Event stopped=new AuditStore.Event();stopped.kind="AGENT".equals(nodeRole())?"AGENT_STOPPED":"CONTROLLER_STOPPED";stopped.status=failure==null?"STOPPED":"STOP_FAILED";appendAudit(stopped);}catch(IOException ex){failure=ex;}
        try{audit.close();}catch(IOException ex){failure=ex;}
        lock.release();lockChannel.close();if(failure!=null)throw failure;
    }
}
