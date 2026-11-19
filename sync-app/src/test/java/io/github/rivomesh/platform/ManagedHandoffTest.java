package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class ManagedHandoffTest {
    private GlobalTaskStoreTest fixture;
    private JdbcControlStore aNodes,bNodes;
    private AgentDirectory aAgents,bAgents;
    private GlobalTaskViews views;
    private DurableCommands commands;
    private GlobalTaskPlane a,b;
    private ManagedHandoff aHandoff;
    private OwnerQueries aQueries,bQueries;
    private ControlStore.Lease aLease,bLease;
    private ClusterConfig bConfig;
    private String revision;
    private final String candidateDirectory=UUID.randomUUID().toString();
    private static final String YAML="""
            formatVersion: 3
            pipelineId: orders
            source:
              password: private-source-password
            journal:
              mode: EXTERNAL
              product: POSTGRESQL
              password: private-journal-password
            targets:
            - product: POSTGRESQL
            """;

    @BeforeEach void open()throws Exception{
        fixture=new GlobalTaskStoreTest();fixture.initialize();
        ClusterConfig config=fixture.config.cluster;
        bConfig=Model.JSON.readValue(Model.JSON.writeValueAsBytes(config),ClusterConfig.class);bConfig.nodeId="b";
        aNodes=new JdbcControlStore(config);bNodes=new JdbcControlStore(bConfig);
        aAgents=new AgentDirectory(config,fixture.access,fixture.a);bAgents=new AgentDirectory(bConfig,fixture.access,fixture.b);
        aLease=aNodes.register(ControlStoreTest.node("a"),List.of());aAgents.register(aLease,fixture.directory);
        bLease=bNodes.register(ControlStoreTest.node("b"),List.of());bAgents.register(bLease,candidateDirectory);
        commands=new DurableCommands(config,fixture.access,fixture.a,40);
        views=new GlobalTaskViews(config,fixture.a,aAgents,300000);
        a=entry(config,fixture.a,aAgents,commands,views);
        b=entryB();
        GlobalTaskStore.Record task=fixture.a.create("orders","a",fixture.directory,YAML,"create_managed_orders",fixture.admin);
        revision=task.revision;
        fixture.a.service((c,actors)->{fixture.a.applied(c,task,task.configurationVersion);return null;});
        publish("STOPPED","UNCLAIMED","GTID-history");
    }

    @AfterEach void close(){
        if(aNodes!=null)aNodes.close();
        if(bNodes!=null)bNodes.close();
    }

    private GlobalTaskPlane entryB()throws Exception{
        return entry(bConfig,fixture.b,bAgents,new DurableCommands(bConfig,fixture.access,fixture.b,40),
                new GlobalTaskViews(bConfig,fixture.b,bAgents,300000));
    }

    private GlobalTaskPlane entry(ClusterConfig config,GlobalTaskStore store,AgentDirectory agents,
                                  DurableCommands commands,GlobalTaskViews views)throws Exception{
        ManagedHandoff handoff=new ManagedHandoff(config,fixture.access,store,commands,agents,views);
        OwnerQueries transport=new OwnerQueries(config,store,agents,10,30000,handoff);
        if(config.nodeId.equals("a")){aQueries=transport;aHandoff=handoff;}else bQueries=transport;
        return new GlobalTaskPlane(store,commands,agents,views,transport,
                config.nodeId,false,handoff);
    }

    private void publish(String worker,String lease,String checkpoint)throws Exception{
        publish("orders",worker,lease,checkpoint);
    }
    private void publish(String task,String worker,String lease,String checkpoint)throws Exception{
        Map<String,Object> status=Map.of("phase","STREAMING","run_state","RUNNING","hasOpenGaps",false,
                "table_count",1,"epoch",9,"captured_sequence","2","normalized_sequence","2",
                "sourcePosition",Map.of("gtidSet",checkpoint));
        views.publish(aLease,fixture.directory,List.of(Map.of("id",task,"desired",worker,"workerState",worker,
                "statusAvailable",true,"captureLeaseState",lease,"observedAt",Instant.now().toString(),"status",status)));
    }

    private TaskRequest assess(String id,String previous){
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.HANDOFF_ASSESS,"orders");
        request.ownerNodeId="b";request.revision=revision;request.requestId=id;request.reference=previous;return request;
    }

    private Map<String,Object> latest(GlobalTaskPlane plane,AccessControl.Principal actor)throws Exception{
        return plane.invoke(TaskRequest.of(TaskRequest.Kind.HANDOFF_LATEST,"orders"),actor);
    }

    private TaskRequest probe(String assessment){
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.HANDOFF_PROBE,"orders");
        request.reference=assessment;return request;
    }

    @Test void routesTheActiveCutToTheCandidateAndThePrivateInventoryOnlyToTheOriginal()throws Exception{
        Map<String,Object> initial=a.invoke(assess("handoff_probe_request",null),fixture.admin);
        try(ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor()){
            Future<Map<String,Object>> result=executor.submit(()->b.invoke(probe((String)initial.get("id")),fixture.admin));
            PlatformTest.await(()->!aQueries.pending(aLease,fixture.directory,10).isEmpty()
                    &&!bQueries.pending(bLease,candidateDirectory,10).isEmpty());
            String candidate=bQueries.pending(bLease,candidateDirectory,10).getFirst();
            String evidence=aQueries.pending(aLease,fixture.directory,10).getFirst();
            TaskControlPlane shouldNotRun=(request,actor)->{throw new AssertionError("wrong node ran probe");};
            assertThrows(ControlStore.Fenced.class,()->aQueries.serve(candidate,aLease,fixture.directory,shouldNotRun));
            assertThrows(ControlStore.Fenced.class,()->bQueries.serve(evidence,bLease,candidateDirectory,shouldNotRun));
            bQueries.serve(candidate,bLease,candidateDirectory,(request,actor)->{
                assertEquals(TaskRequest.Kind.HANDOFF_CANDIDATE_PROBE,request.kind);
                assertEquals(YAML,request.yaml);
                assertEquals(9,request.handoffCut.epoch());
                assertEquals(2,request.handoffCut.sequence());
                return Map.of("state","VERIFIED");
            });
            aQueries.serve(evidence,aLease,fixture.directory,(request,actor)->{
                assertEquals(TaskRequest.Kind.HANDOFF_EVIDENCE_PROBE,request.kind);
                assertNull(request.yaml);
                assertNull(request.handoffCut);
                assertEquals(fixture.directory,request.handoffEvidenceScope.originalDirectory());
                var archive=new SharedHandoffEvidence.Archive("a".repeat(64),10,1,0,1);
                return Map.of("state","REFUSED","code","ORIGINAL_PRIVATE_EVIDENCE_CUSTODY_UNPROVEN",
                        "shareableReports",Map.of("state","ARCHIVED","archive",archive),
                        "historicalFailureEvidence",Map.of("state","ARCHIVED","archive",archive),
                        "rawCaptureEvidence",Map.of("state","ARCHIVED","archive",
                                new SharedHandoffEvidence.Archive("b".repeat(64),30,0,0,0,1)));
            });
            PlatformTest.await(()->!bQueries.pending(bLease,candidateDirectory,10).isEmpty());
            String archive=bQueries.pending(bLease,candidateDirectory,10).getFirst();
            bQueries.serve(archive,bLease,candidateDirectory,(request,actor)->{
                assertEquals(TaskRequest.Kind.HANDOFF_CUSTODY_PROBE,request.kind);
                assertEquals(candidateDirectory,request.handoffEvidenceScope.candidateDirectory());
                assertEquals(revision,request.handoffEvidenceScope.revision());
                assertEquals("a".repeat(64),request.handoffArchive.sha256());
                assertEquals(1,request.handoffArchive.failureRecords());
                assertNull(request.yaml);
                return Map.of("state","VERIFIED");
            });
            PlatformTest.await(()->!bQueries.pending(bLease,candidateDirectory,10).isEmpty());
            String rawArchive=bQueries.pending(bLease,candidateDirectory,10).getFirst();
            bQueries.serve(rawArchive,bLease,candidateDirectory,(request,actor)->{
                assertEquals(TaskRequest.Kind.HANDOFF_CUSTODY_PROBE,request.kind);
                assertEquals(candidateDirectory,request.handoffEvidenceScope.candidateDirectory());
                assertEquals("b".repeat(64),request.handoffArchive.sha256());
                assertEquals(1,request.handoffArchive.rawCaptures());
                assertEquals(0,request.handoffArchive.failureRecords());
                assertNull(request.yaml);
                return Map.of("state","REFUSED","code","HANDOFF_ARCHIVE_MISSING");
            });
            Map<String,Object> certified=result.get(10,TimeUnit.SECONDS);
            assertEquals(List.of("ORIGINAL_RECOVERY_EVIDENCE_UNVERIFIED"),certified.get("blockers"));
            assertEquals(false,certified.get("executionAuthorized"));
            assertEquals(Map.of("state","VERIFIED"),((Map<?,?>)certified.get("probe")).get("sharedReportCustody"));
            assertEquals(Map.of("state","VERIFIED"),((Map<?,?>)certified.get("probe")).get("sharedFailureEvidenceCustody"));
            assertEquals(Map.of("state","REFUSED","code","HANDOFF_ARCHIVE_MISSING"),
                    ((Map<?,?>)certified.get("probe")).get("sharedRawCaptureCustody"));
            assertEquals(certified,latest(entryB(),fixture.admin));
            assertFalse(Model.JSON.writeValueAsString(certified).contains("private-"));
            assertEquals("a",fixture.b.get("orders",fixture.admin).owner);
        }
    }

    @Test void changedTaskRevisionRefusesQueuedProbeBeforeTheCandidateCanReadConfiguration()throws Exception{
        a.invoke(assess("handoff_probe_request",null),fixture.admin);
        OwnerQueries.HandoffQueries queued=aQueries.submitHandoff(probe("handoff_probe_request"),fixture.admin);
        fixture.a.update("orders",revision,YAML,"handoff_probe_revision",fixture.admin);
        bQueries.serve(queued.candidate(),bLease,candidateDirectory,(request,actor)->{
            throw new AssertionError("changed revision reached candidate callback");
        });
        assertEquals("REVISION_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->aQueries.await(queued.candidate(),fixture.admin)).getMessage());
    }

    @Test void oneReceiptAcrossEntriesDoesNotChangeOwnerOrExposePrivateEvidence()throws Exception{
        TaskRequest request=assess("handoff_first_request",null);
        Map<String,Object> observed=a.invoke(request,fixture.admin);
        assertEquals("EVIDENCE_REQUIRED",observed.get("state"));
        assertEquals(false,observed.get("executionAuthorized"));
        assertEquals(20,observed.get("candidateTaskCapacity"));
        assertEquals(observed,latest(entryB(),fixture.admin));
        assertEquals(observed,b.invoke(request,fixture.admin));
        assertFalse(Model.JSON.writeValueAsString(observed).contains("private-"));
        assertFalse(Model.JSON.writeValueAsString(observed).contains("GTID-history"));
        assertEquals("a",fixture.b.get("orders",fixture.admin).owner);
        assertEquals(revision,fixture.b.get("orders",fixture.admin).revision);
        try(Connection c=fixture.connection();Statement s=c.createStatement();ResultSet r=s.executeQuery(
                "SELECT COUNT(*) FROM rivomesh_control.rmc_audit_event WHERE payload LIKE '%MANAGED_HANDOFF_ASSESSED%'")){
            r.next();assertEquals(1,r.getInt(1));
        }
    }

    @Test void changedObservationCandidateLeaseOrRevisionInvalidatesTheRecordedAssessment()throws Exception{
        a.invoke(assess("handoff_first_request",null),fixture.admin);
        publish("RUNNING","CLAIMED","next-GTID");
        assertEquals("STALE",latest(b,fixture.admin).get("state"));
        assertEquals("HANDOFF_ASSESSMENT_CHANGED",assertThrows(TaskService.Conflict.class,
                ()->b.invoke(assess("handoff_second_request",null),fixture.admin)).getMessage());
        Map<String,Object> refreshed=b.invoke(assess("handoff_second_request","handoff_first_request"),fixture.admin);
        assertEquals("BLOCKED",refreshed.get("state"));
        assertTrue(((List<?>)refreshed.get("blockers")).contains("ORIGINAL_WORKER_NOT_DRAINED"));
        bNodes.stopped(bLease);
        ControlStore.Lease next=bNodes.register(ControlStoreTest.node("b"),List.of());
        bAgents.register(next,candidateDirectory);
        assertEquals("STALE",latest(a,fixture.admin).get("state"));
        fixture.a.update("orders",revision,YAML,"handoff_revision_change",fixture.admin);
        assertEquals("REVISION_CHANGED",assertThrows(TaskService.Conflict.class,()->a.invoke(
                assess("handoff_third_request","handoff_second_request"),fixture.admin)).getMessage());
    }

    @Test void deniesScopedCandidateAndMarksCapacityAndPendingCommands()throws Exception{
        String accounts=fixture.access.accounts().revision;
        String token="scoped-handoff-admin-token-32-characters";
        fixture.access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+accounts
                +"\",\"id\":\"origin-only\",\"role\":\"OPERATOR\",\"enabled\":true,\"nodeIds\":[\"a\"],"
                +"\"taskIds\":[\"orders\"],\"token\":\""+token+"\"}"),"create_handoff_scoped",fixture.admin);
        AccessControl.Principal scoped=AccountDirectory.authenticate(fixture.access.accounts(),"Bearer "+token);
        AccessControl.Principal admin=AccountDirectory.authenticate(fixture.access.accounts(),"Bearer "+GlobalTaskStoreTest.TOKEN);
        assertThrows(SecurityException.class,()->a.invoke(assess("handoff_scoped_request",null),scoped));
        TaskRequest stop=TaskRequest.of(TaskRequest.Kind.ACTION,"orders");
        stop.action="STOP";stop.revision=revision;stop.requestId="handoff_pending_stop";
        commands.enqueue(stop,admin);revision=fixture.a.get("orders",admin).revision;
        ControlStore.Node restricted=ControlStoreTest.node("b");restricted.incarnation=bLease.incarnation;
        restricted.taskCapacity=1;bNodes.heartbeat(bLease,restricted,List.of());
        fixture.b.create("occupied","b",candidateDirectory,YAML,"create_occupied_slot",admin);
        Map<String,Object> observed=a.invoke(assess("handoff_admin_request",null),admin);
        assertTrue(((List<?>)observed.get("blockers")).containsAll(
                List.of("CANDIDATE_TASK_CAPACITY_UNAVAILABLE","TASK_COMMANDS_UNRESOLVED")));
        assertThrows(SecurityException.class,()->latest(b,scoped));
    }

    private Map<String,Object> verifiedAssessment(String id,AccessControl.Principal actor)throws Exception{
        return verifiedAssessment(assess(id,null),actor,null);
    }

    private Map<String,Object> verifiedAssessment(TaskRequest assessment,AccessControl.Principal actor,String writerAssignment)throws Exception{
        Map<String,Object> before=a.invoke(assessment,actor);
        TaskRequest probe=TaskRequest.of(TaskRequest.Kind.HANDOFF_PROBE,assessment.taskId);
        probe.reference=assessment.requestId;
        SharedHandoffEvidence.Archive archive=new SharedHandoffEvidence.Archive("b".repeat(64),20,1,2);
        try(ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor()){
            Future<Map<String,Object>> result=executor.submit(()->b.invoke(probe,actor));
            PlatformTest.await(()->!aQueries.pending(aLease,fixture.directory,10).isEmpty()
                    &&!bQueries.pending(bLease,candidateDirectory,10).isEmpty());
            bQueries.serve(bQueries.pending(bLease,candidateDirectory,10).getFirst(),bLease,candidateDirectory,(request,principal)->{
                if(writerAssignment==null)return Map.of("state","VERIFIED");
                assertEquals("a",request.ownerNodeId);
                assertEquals(9,request.handoffCut.epoch());
                String checkpoint=Model.JSON.valueToTree(before.get("originalCut")).path("checkpointHash").asText();
                return Map.of("state","VERIFIED","cut",new io.github.rivomesh.cdc.JournalWriterAssignment.CutProof(
                        "c".repeat(64),"d".repeat(64),List.of("pg"),true,
                        new io.github.rivomesh.cdc.JournalWriterAssignment.ManagedCut("a",AuditStore.hash(writerAssignment),9,2,checkpoint)));
            });
            aQueries.serve(aQueries.pending(aLease,fixture.directory,10).getFirst(),aLease,fixture.directory,
                    (request,principal)->Map.of("state","REFUSED","originInventory",Map.of("validationReports","PRESENT",
                                    "sourceEvidence","ABSENT","failureEvidence","ABSENT"),
                            "closedReceipts",Map.of("state","ARCHIVED","archive",archive),
                            "shareableReports",Map.of("state","ARCHIVED","archive",archive)));
            PlatformTest.await(()->!bQueries.pending(bLease,candidateDirectory,10).isEmpty());
            bQueries.serve(bQueries.pending(bLease,candidateDirectory,10).getFirst(),bLease,candidateDirectory,
                    (request,principal)->Map.of("state","VERIFIED","archive",request.handoffArchive));
            return result.get(10,TimeUnit.SECONDS);
        }
    }
    private TaskRequest prepare(String id,Map<String,Object> assessment){
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.HANDOFF_PREPARE,"orders");
        request.requestId=id;request.reference=(String)assessment.get("id");request.revision=revision;
        request.identityHash=(String)assessment.get("preparationEvidenceSha256");return request;
    }
    private TaskRequest release(String id,String plan){
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.HANDOFF_RELEASE,"orders");
        request.requestId=id;request.reference=plan;return request;
    }
    private TaskRequest currentPreparation(){return TaskRequest.of(TaskRequest.Kind.HANDOFF_PREPARATION,"orders");}
    private TaskRequest stop(String id){
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.ACTION,"orders");
        request.requestId=id;request.revision=revision;request.action="STOP";return request;
    }
    private AccessControl.Principal currentAdmin()throws Exception{
        return AccountDirectory.authenticate(fixture.access.accounts(),"Bearer "+GlobalTaskStoreTest.TOKEN);
    }
    private void capacity(ControlStore.Lease lease,JdbcControlStore store,int tasks,int workers)throws Exception{
        ControlStore.Node current=ControlStoreTest.node(lease.nodeId);current.incarnation=lease.incarnation;
        current.taskCapacity=tasks;current.runningCapacity=workers;store.heartbeat(lease,current,List.of());
    }
    private static void conflict(String code,org.junit.jupiter.api.function.Executable operation){
        assertEquals(code,assertThrows(TaskService.Conflict.class,operation).getMessage());
    }

    @Test void preparationReservesCapacityAcrossRestartAndReleasePreservesSameRequestReplay()throws Exception{
        capacity(aLease,aNodes,1,1);capacity(bLease,bNodes,2,1);
        Map<String,Object> proved=verifiedAssessment("prepare_verified_assessment",fixture.admin);
        assertEquals(proved,latest(entryB(),fixture.admin));
        TaskRequest prepare=prepare("prepare_same_request",proved);
        assertNotNull(prepare.identityHash);
        String good=prepare.identityHash;prepare.identityHash="a".repeat(64);
        conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED",()->a.invoke(prepare,fixture.admin));
        prepare.identityHash=good;
        Map<String,Object> prepared;
        try(ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor()){
            var first=executor.submit(()->a.invoke(prepare,fixture.admin));
            var second=executor.submit(()->b.invoke(prepare,fixture.admin));
            prepared=first.get(10,TimeUnit.SECONDS);
            assertEquals(prepared,second.get(10,TimeUnit.SECONDS));
        }
        assertEquals("PREPARED",prepared.get("state"));
        assertEquals(1,prepared.get("reservedTaskSlots"));assertEquals(1,prepared.get("reservedWorkerSlots"));
        assertEquals(false,prepared.get("executionAuthorized"));
        assertEquals(prepared,entryB().invoke(currentPreparation(),fixture.admin));
        assertEquals("a",fixture.b.get("orders",fixture.admin).owner);
        assertEquals(revision,fixture.b.get("orders",fixture.admin).revision);
        assertFalse(Model.JSON.writeValueAsString(prepared).contains(candidateDirectory));
        conflict("HANDOFF_PREPARATION_ACTIVE",()->commands.enqueue(stop("prepare_frozen_stop"),fixture.admin));
        conflict("HANDOFF_PREPARATION_ACTIVE",()->fixture.b.update("orders",revision,YAML,"prepare_frozen_edit",fixture.admin));
        prepare.identityHash="a".repeat(64);
        conflict("HANDOFF_PREPARATION_REQUEST_REUSED",()->a.invoke(prepare,fixture.admin));prepare.identityHash=good;
        try(ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor()){
            var automatic=executor.submit(()->conflict("AGENT_PLACEMENT_UNAVAILABLE",()->fixture.a.allocate("automatic","yaml",
                    "prepare_auto_allocation",fixture.admin,aAgents,null)));
            var explicit=executor.submit(()->conflict("AGENT_HANDOFF_CAPACITY_RESERVED",()->fixture.b.create("explicit","b",
                    candidateDirectory,"yaml","prepare_explicit_allocation",fixture.admin)));
            automatic.get(10,TimeUnit.SECONDS);explicit.get(10,TimeUnit.SECONDS);
        }
        conflict("HANDOFF_PREPARATION_CHANGED",()->a.invoke(release("prepare_wrong_release","another_preparation"),fixture.admin));
        TaskRequest release=release("prepare_release_request",prepare.requestId);
        Map<String,Object> released=b.invoke(release,fixture.admin);
        assertEquals("RELEASED",released.get("state"));assertEquals(0,released.get("reservedWorkerSlots"));
        assertEquals(released,entryB().invoke(release,fixture.admin));
        assertEquals(released,a.invoke(prepare,fixture.admin));
        assertEquals("b",fixture.a.allocate("automatic","yaml","prepare_auto_allocation",fixture.admin,aAgents,null).owner);
        assertEquals("PENDING",commands.enqueue(stop("prepare_unfrozen_stop"),fixture.admin).get("status"));
        assertEquals(released,a.invoke(prepare,fixture.admin));
    }

    @Test void candidateStagingRequiresAnActiveExactPreparationAtAdmissionAndDispatch()throws Exception{
        TaskRequest preparation=prepare("stage_prepared_fixture",verifiedAssessment("stage_assessment_fixture",fixture.admin));
        TaskRequest staging=TaskRequest.of(TaskRequest.Kind.HANDOFF_STAGE,"orders");
        staging.requestId="stage_candidate_fixture";staging.reference=preparation.requestId;staging.revision=revision;
        staging.identityHash=preparation.identityHash;
        conflict("HANDOFF_PREPARATION_CHANGED",()->aQueries.submitHandoffStaging(staging,fixture.admin));
        Map<String,Object> active=a.invoke(preparation,fixture.admin);
        assertEquals("PREPARED",active.get("state"));
        String proof=staging.identityHash;staging.identityHash="0".repeat(64);
        conflict("HANDOFF_PREPARATION_EVIDENCE_CHANGED",()->aQueries.submitHandoffStaging(staging,fixture.admin));
        staging.identityHash=proof;
        String query=aQueries.submitHandoffStaging(staging,fixture.admin);
        assertFalse(aQueries.pending(aLease,fixture.a.get("orders",fixture.admin).directory,10).contains(query));
        assertTrue(bQueries.pending(bLease,candidateDirectory,10).contains(query));
        bQueries.serve(query,bLease,candidateDirectory,(request,actor)->{
            assertEquals(TaskRequest.Kind.HANDOFF_STAGING_RPC,request.kind);
            assertEquals(preparation.requestId,request.handoffStaging.preparation());
            assertEquals(proof,request.handoffStaging.evidence());
            assertEquals(candidateDirectory,request.handoffStaging.candidate().directory());
            assertEquals(active.get("custodySha256"),request.handoffStaging.archive().sha256());
            return Map.of("state","STAGED","executionAuthorized",false);
        });
        assertEquals(Map.of("state","STAGED","executionAuthorized",false),aQueries.await(query,fixture.admin));
        assertEquals("a",fixture.a.get("orders",fixture.admin).owner);
        staging.requestId="stage_after_release_fixture";
        String pending=bQueries.submitHandoffStaging(staging,fixture.admin);
        a.invoke(release("stage_release_fixture",preparation.requestId),fixture.admin);
        bQueries.serve(pending,bLease,candidateDirectory,(request,actor)->fail("Released preparation reached the Agent"));
        conflict("HANDOFF_PREPARATION_CHANGED",()->bQueries.poll(pending,fixture.admin));
    }


    @Test void readinessTransportDerivesPrivateInputAndFencesReleasedOrMalformedResults()throws Exception{
        Map<String,Object> reviewed=verifiedAssessment("handoff_readiness_assessment",fixture.admin);
        TaskRequest request=TaskRequest.of(TaskRequest.Kind.HANDOFF_COMMIT_OWNER,"orders");
        request.requestId="handoff_readiness_lookup";request.reference="handoff_readiness_plan";request.revision=revision;
        request.identityHash=(String)reviewed.get("preparationEvidenceSha256");
        conflict("HANDOFF_PREPARATION_CHANGED",()->aQueries.candidateReadiness(request,fixture.admin));
        var original=new HandoffPreparations.Binding("a",fixture.directory,aLease.incarnation,aLease.epoch);
        var candidate=new HandoffPreparations.Binding("b",candidateDirectory,bLease.incarnation,bLease.epoch);
        var cut=Model.JSON.valueToTree(reviewed.get("originalCut"));
        fixture.a.transaction("orders",fixture.admin,(c,task,actor)->{
            // Native writer assignment is verified separately. This fixture seals its exact assessment for the transport.
            var proof=new HandoffPreparations.Evidence((String)reviewed.get("id"),revision,task.activeConfigurationVersion,
                    original,candidate,new HandoffPreparations.JournalCut(cut.path("journalEpoch").asText(),
                    cut.path("sequence").asText(),cut.path("checkpointHash").asText()),request.identityHash,"b".repeat(64));
            var activation=new HandoffPreparations.WriterActivation("a".repeat(64),UUID.randomUUID().toString(),
                    "c".repeat(64),"d".repeat(64),true);
            return fixture.a.preparations().prepare(c,task,request.reference,actor,proof,activation,aNodes.now(c));
        });
        request.yaml="caller supplied configuration must never be sent";
        assertEquals("HANDOFF_REQUEST_INVALID",assertThrows(IllegalArgumentException.class,
                ()->aQueries.candidateReadiness(request,fixture.admin)).getMessage());
        request.yaml=null;
        conflict("HANDOFF_EXECUTION_REQUEST_CHANGED",()->aQueries.candidateReadiness(request,fixture.admin));
        assertNull(aHandoff.bindOrReplayExecution(request,fixture.admin));
        HandoffPreparations.Plan plan=fixture.b.transaction("orders",fixture.admin,(c,task,actor)->
                fixture.b.preparations().byRequest(c,request.reference,aNodes.now(c)));
        assertEquals(request.requestId,plan.executionRequest());
        var ready=new ManagedHandoff.CandidateAdmission(plan.id(),candidate,"STOPPED","e".repeat(64),"f".repeat(64));
        Map<String,Object> masked=Model.JSON.convertValue(ready,Map.class);
        try(ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor()){
            var result=executor.submit(()->aQueries.candidateReadiness(request,fixture.admin));
            PlatformTest.await(()->!bQueries.pending(bLease,candidateDirectory,10).isEmpty());
            String queued=bQueries.pending(bLease,candidateDirectory,10).getFirst();
            assertThrows(ControlStore.Fenced.class,()->aQueries.serve(queued,aLease,fixture.directory,
                    (ignored,actor)->fail("original Agent received candidate configuration")));
            bQueries.serve(queued,bLease,candidateDirectory,(scoped,actor)->{
                assertEquals(TaskRequest.Kind.HANDOFF_READINESS_RPC,scoped.kind);
                assertEquals(YAML,scoped.yaml);
                assertEquals(plan,scoped.handoffPreparation);
                assertEquals(candidate,scoped.handoffStaging.candidate());
                assertEquals(fixture.admin.id,actor.id);
                return masked;
            });
            assertEquals(ready,result.get(10,TimeUnit.SECONDS));
            assertFalse(Model.JSON.writeValueAsString(aQueries.poll(queued,fixture.admin)).contains("private-"));
            assertEquals(queued,bQueries.nextHandoffResume(bLease,candidateDirectory,""));
            assertNull(aQueries.nextHandoffResume(aLease,fixture.directory,""));
            assertThrows(ControlStore.Fenced.class,()->aQueries.resumeHandoff(queued,aLease,fixture.directory,
                    (scoped,actor,admission)->fail("predecessor must not consume candidate readiness")));
            conflict("HANDOFF_JOURNAL_LEASE_ACTIVE",()->bQueries.resumeHandoff(queued,bLease,candidateDirectory,
                    (scoped,actor,admission)->{
                        assertEquals(plan,scoped.handoffPreparation);
                        assertEquals(ready,admission);
                        throw new TaskService.Conflict("HANDOFF_JOURNAL_LEASE_ACTIVE");
                    }));
            assertEquals(queued,bQueries.nextHandoffResume(bLease,candidateDirectory,""));
            assertEquals("a",fixture.a.get("orders",fixture.admin).owner);

            var malformed=executor.submit(()->aQueries.candidateReadiness(request,fixture.admin));
            PlatformTest.await(()->!bQueries.pending(bLease,candidateDirectory,10).isEmpty());
            String malformedId=bQueries.pending(bLease,candidateDirectory,10).getFirst();
            bQueries.serve(malformedId,bLease,candidateDirectory,(scoped,actor)->{
                Map<String,Object> leaked=new HashMap<>(masked);leaked.put("yaml",scoped.yaml);return leaked;
            });
            assertEquals("HANDOFF_CANDIDATE_ADMISSION_CHANGED",assertThrows(ExecutionException.class,
                    ()->malformed.get(10,TimeUnit.SECONDS)).getCause().getMessage());
            assertFalse(aQueries.poll(malformedId,fixture.admin).containsKey("result"));

            var released=executor.submit(()->aQueries.candidateReadiness(request,fixture.admin));
            PlatformTest.await(()->!bQueries.pending(bLease,candidateDirectory,10).isEmpty());
            bQueries.serve(bQueries.pending(bLease,candidateDirectory,10).getFirst(),bLease,candidateDirectory,(scoped,actor)->{
                a.invoke(release("handoff_readiness_release",plan.id()),fixture.admin);
                return masked;
            });
            assertEquals("HANDOFF_PREPARATION_CHANGED",assertThrows(ExecutionException.class,
                    ()->released.get(10,TimeUnit.SECONDS)).getCause().getMessage());
            bQueries.resumeHandoff(queued,bLease,candidateDirectory,
                    (scoped,actor,admission)->fail("released approval must not repeat native checks"));
            assertNull(bQueries.nextHandoffResume(bLease,candidateDirectory,""));
        }
        assertEquals("a",fixture.a.get("orders",fixture.admin).owner);
    }

    @Test void preparationAndNewPlacementRaceForOneWorkerSlot()throws Exception{
        capacity(aLease,aNodes,1,1);capacity(bLease,bNodes,1,1);
        TaskRequest request=prepare("prepare_allocation_race",verifiedAssessment("prepare_race_assessment",fixture.admin));
        CountDownLatch start=new CountDownLatch(1);
        try(ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor()){
            var preparation=executor.submit(()->{
                start.await();
                try{return a.invoke(request,fixture.admin).get("state");}catch(TaskService.Conflict conflict){return conflict.getMessage();}
            });
            var placement=executor.submit(()->{
                start.await();
                try{return fixture.b.allocate("raced","yaml","prepare_raced_new_job",fixture.admin,bAgents,null).owner;}
                catch(TaskService.Conflict conflict){return conflict.getMessage();}
            });
            start.countDown();
            Object first=preparation.get(10,TimeUnit.SECONDS),second=placement.get(10,TimeUnit.SECONDS);
            assertTrue("PREPARED".equals(first)&&"AGENT_PLACEMENT_UNAVAILABLE".equals(second)
                    ||"HANDOFF_ASSESSMENT_CHANGED".equals(first)&&"b".equals(second),()->first+" / "+second);
            fixture.a.service((c,accounts)->{
                AgentDirectory.HandoffCapacity capacity=aAgents.handoffCapacity(c,"b");
                assertEquals(1,capacity.assignedTasks()+capacity.reservedTasks());
                return null;
            });
        }
    }

    @Test void revokedActorCannotReplayAndAnotherCurrentAdminCanRelease()throws Exception{
        String token="temporary-preparation-admin-test-token";
        fixture.access.mutate("CREATE",null,Model.JSON.readTree("{\"expectedRevision\":\""+fixture.access.accounts().revision
                +"\",\"id\":\"temporary\",\"role\":\"ADMIN\",\"enabled\":true,\"token\":\""+token+"\"}"),"prepare_create_actor",fixture.admin);
        AccessControl.Principal temporary=AccountDirectory.authenticate(fixture.access.accounts(),"Bearer "+token);
        Map<String,Object> evidence=verifiedAssessment("prepare_for_temporary_actor",temporary);
        TaskRequest prepare=prepare("prepare_temporary_request",evidence);
        a.invoke(prepare,temporary);
        conflict("HANDOFF_PREPARATION_REQUEST_REUSED",()->b.invoke(prepare,currentAdmin()));
        fixture.access.mutate("UPDATE","temporary",Model.JSON.readTree("{\"expectedRevision\":\""+fixture.access.accounts().revision
                +"\",\"role\":\"ADMIN\",\"enabled\":false}"),"prepare_disable_actor",currentAdmin());
        conflict("AUTHORIZATION_CHANGED",()->a.invoke(prepare,temporary));
        assertEquals("RELEASED",b.invoke(release("prepare_release_revoked",prepare.requestId),currentAdmin()).get("state"));
    }

    @Test void candidateReenrollmentRejectsStaleEvidence()throws Exception{
        Map<String,Object> evidence=verifiedAssessment("prepare_with_bound_candidate",fixture.admin);
        bNodes.stopped(bLease);bLease=bNodes.register(ControlStoreTest.node("b"),List.of());bAgents.register(bLease,candidateDirectory);
        conflict("HANDOFF_ASSESSMENT_CHANGED",()->b.invoke(prepare("prepare_stale_candidate",evidence),fixture.admin));
    }

    @Test void expiryUsesThePersistedDeadlineAndCrossEntryReplaysRemainTerminal()throws Exception{
        Map<String,Object> evidence=verifiedAssessment("prepare_with_deadline",fixture.admin);
        TaskRequest prepare=prepare("prepare_expires_request",evidence);
        Map<String,Object> plan=a.invoke(prepare,fixture.admin);
        long deadline=Instant.parse((String)plan.get("expiresAt")).toEpochMilli();
        fixture.b.service((c,accounts)->{
            assertEquals("PREPARED",fixture.b.preparations().latest(c,"orders",deadline-1).phase());
            assertEquals("EXPIRED",fixture.b.preparations().latest(c,"orders",deadline).phase());
            return null;
        });
        assertEquals("EXPIRED",entryB().invoke(prepare,fixture.admin).get("state"));
        conflict("HANDOFF_PREPARATION_CHANGED",()->b.invoke(release("prepare_expired_release",prepare.requestId),fixture.admin));
        assertEquals("PENDING",commands.enqueue(stop("prepare_expired_stop"),fixture.admin).get("status"));
    }

    @Test void exactManagedAssignmentBindsActivationToTheReservationWithoutMovingTheOwner()throws Exception{
        String assignment=UUID.randomUUID().toString();
        String managed=YAML.replace("pipelineId: orders","pipelineId: managed-orders\nnodeId: a")
                .replace("  product: POSTGRESQL", "  product: POSTGRESQL\n  url: jdbc:postgresql://journal/shared\n  managedWriter: true\n  writerAssignment: "+assignment);
        GlobalTaskStore.Record task=fixture.a.create("managed-orders","a",fixture.directory,managed,"create_managed_orders_mode",fixture.admin);
        fixture.a.service((c,actors)->{fixture.a.applied(c,task,task.configurationVersion);return null;});
        publish("managed-orders","STOPPED","UNCLAIMED","GTID-history");
        TaskRequest assessment=assess("managed_writer_assessment",null);assessment.taskId=task.id;assessment.revision=task.revision;
        Map<String,Object> evidence=verifiedAssessment(assessment,fixture.admin,UUID.randomUUID().toString());
        TaskRequest preparation=prepare("managed_writer_preparation",evidence);
        preparation.taskId=task.id;preparation.revision=task.revision;
        conflict("HANDOFF_MANAGED_WRITER_PROOF_REQUIRED",()->a.invoke(preparation,fixture.admin));
        assessment.reference=assessment.requestId;assessment.requestId="managed_writer_assessment_retry";
        evidence=verifiedAssessment(assessment,fixture.admin,assignment);
        preparation.reference=assessment.requestId;preparation.identityHash=(String)evidence.get("preparationEvidenceSha256");
        Map<String,Object> plan=a.invoke(preparation,fixture.admin);
        Map<?,?> activation=(Map<?,?>)plan.get("writerActivation");
        assertEquals(AuditStore.hash(assignment),activation.get("originalAssignmentSha256"));
        assertEquals(false,activation.get("checkedInControlTransaction"));
        assertEquals(false,plan.get("executionAuthorized"));
        assertEquals(plan,entryB().invoke(preparation,fixture.admin));
        assertFalse(Model.JSON.writeValueAsString(plan).contains(assignment));
        TaskRequest release=release("managed_writer_release",preparation.requestId);release.taskId=task.id;
        assertEquals(activation,b.invoke(release,fixture.admin).get("writerActivation"));
        assertEquals("a",fixture.b.get(task.id,fixture.admin).owner);
    }

    @Test void evidenceStageAloneDoesNotGrantInternalCommitAndSeparateStoresStayFenced()throws Exception{
        Map<String,Object> evidence=verifiedAssessment("handoff_guarded_commit_assessment",fixture.admin);
        TaskRequest prepared=prepare("handoff_guarded_commit",evidence);
        a.invoke(prepared,fixture.admin);
        HandoffPreparations.Binding binding=new HandoffPreparations.Binding("b",candidateDirectory,bLease.incarnation,bLease.epoch);
        String configuration="c".repeat(64),stage="d".repeat(64);
        conflict("HANDOFF_CANDIDATE_ADMISSION_REQUIRED",()->aHandoff.commitAdmittedWriter("orders",
                new ManagedHandoff.CandidateAdmission(prepared.requestId,binding,"STAGED",configuration,stage),"handoff_guarded_execute",fixture.admin));
        conflict("HANDOFF_MANAGED_CONTROL_CUT_REQUIRED",()->aHandoff.commitAdmittedWriter("orders",
                new ManagedHandoff.CandidateAdmission(prepared.requestId,binding,"STOPPED",configuration,stage),"handoff_guarded_execute",fixture.admin));
        HandoffPreparations.Binding stranger=new HandoffPreparations.Binding("b",candidateDirectory,bLease.incarnation,bLease.epoch+1);
        conflict("HANDOFF_CANDIDATE_ADMISSION_CHANGED",()->aHandoff.commitAdmittedWriter("orders",
                new ManagedHandoff.CandidateAdmission(prepared.requestId,stranger,"STOPPED",configuration,stage),"handoff_guarded_execute",fixture.admin));
        assertEquals("a",fixture.a.get("orders",fixture.admin).owner);
        assertEquals("PREPARED",entryB().invoke(currentPreparation(),fixture.admin).get("state"));
    }

}
