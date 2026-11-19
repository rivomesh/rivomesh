package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.platform.ClusterConfig;
import io.github.rivomesh.platform.ConsoleServer;
import io.github.rivomesh.platform.PlatformConfig;
import io.github.rivomesh.platform.TaskBackend;
import io.github.rivomesh.platform.TaskService;
import io.github.rivomesh.platform.TaskWorker;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import static io.github.rivomesh.cdc.KafkaPhysicalOwnershipAcceptance.*;
import static org.apache.kafka.common.acl.AclOperation.*;

/** Real managed roles and native Kafka readback. The fixture injects synthetic transactions after Worker admission. */
public final class KafkaManagedPhysicalReceiptAcceptance {
    private static final String TASK="managed-physical",SUBSCRIPTION="kafka",OWNER="receipt-agent";
    private static final String TOPIC="managed.receipt.business",GENERATION="native-managed-receipt";
    private static final String PEER="peer",PEER_TOPIC="managed.receipt.peer",CONTROL="managed.receipt.control";
    private static final String TASK_ROUTE="/api/tasks/"+TASK,RECEIPTS=TASK_ROUTE+"/recovery-receipts/";
    private static final String PHYSICAL=TASK_ROUTE+"/kafka-physical-receipt";
    private static final String REVIEW="managed_physical_predecessor";
    private static final String PLAN=TASK_ROUTE+"/kafka-notice-plan",APPROVALS=TASK_ROUTE+"/kafka-notice-approvals";
    private static final String APPROVAL_ID="managed_sender_approval";
    private static final String PREFLIGHT=APPROVALS+"/"+APPROVAL_ID+"/preflight";
    private static final TopicPartition PARTITION=new TopicPartition(TOPIC,0);
    private static final TopicPartition PEER_PARTITION=new TopicPartition(PEER_TOPIC,0),CONTROL_PARTITION=new TopicPartition(CONTROL,0);
    private static final TableSchema TABLE=schema();

    public static void main(String[] args)throws Exception{
        String secret=System.getenv("RIVOMESH_KAFKA_FIXTURE_SECRET");
        if(secret==null||!secret.matches("[a-f0-9]{48}"))throw new IllegalArgumentException("Fixture credentials are required");
        boolean notice=args.length==3&&"--notice-preflight".equals(args[2]);
        Path home=Path.of(args[1]);
        if("--control-preflight".equals(args[0])){
            String endpoint="127.0.0.1:1";
            var business=target(endpoint,secret,TOPIC);
            var profile=notice?configureNotice(business,peerTarget(endpoint,secret),List.of(endpoint),secret,
                    Codec.sha256("fixture control cluster"),Uuid.randomUuid()):null;
            runManagement(home,secret,business,null,profile);
            return;
        }
        List<String> bootstrap=List.of(args[0].split(","));
        require(bootstrap.size()==(notice?3:1),"Kafka fixture has incorrect broker count");
        var destination=target(bootstrap.getFirst(),secret,TOPIC);
        try(Admin broker=Admin.create(adminProperties(destination,secret))){
            await(()->broker.describeCluster().nodes().get(5,TimeUnit.SECONDS),nodes->nodes.size()==bootstrap.size());
            createTopic(broker);
            var acls=new ArrayList<>(List.of(acl(ResourceType.TOPIC,TOPIC,READ),acl(ResourceType.TOPIC,TOPIC,WRITE),
                    acl(ResourceType.TOPIC,TOPIC,DESCRIBE_CONFIGS),acl(ResourceType.CLUSTER,Resource.CLUSTER_NAME,DESCRIBE_CONFIGS),
                    acl(ResourceType.CLUSTER,Resource.CLUSTER_NAME,IDEMPOTENT_WRITE)));
            NoticeProfile profile=null;
            if(notice){
                var peer=peerTarget(bootstrap.getFirst(),secret);
                var configs=new LinkedHashMap<>(KAFKA_CONFIG);
                configs.put("max.message.bytes",Integer.toString(peer.messaging.maxMessageBytes+4096));
                broker.createTopics(List.of(new NewTopic(PEER_TOPIC,1,(short)1).configs(configs))).all().get(15,TimeUnit.SECONDS);
                createControl(broker);
                var control=await(()->describe(broker,CONTROL),d->d.partitions().getFirst().isr().size()==3);
                String cluster=broker.describeCluster().clusterId().get(10,TimeUnit.SECONDS);
                profile=configureNotice(destination,peer,bootstrap,secret,Codec.sha256("KAFKA:"+cluster),control.topicId());
                acls.addAll(List.of(acl(ResourceType.TOPIC,PEER_TOPIC,READ),acl(ResourceType.TOPIC,PEER_TOPIC,WRITE),
                        acl(ResourceType.TOPIC,PEER_TOPIC,DESCRIBE_CONFIGS),acl(ResourceType.TOPIC,CONTROL,READ),
                        acl(ResourceType.TOPIC,CONTROL,DESCRIBE_CONFIGS),writerAcl(CONTROL,WRITE),writerAcl(TOPIC,DESCRIBE)));
            }
            broker.createAcls(acls).all().get(15,TimeUnit.SECONDS);
            awaitBusinessAccess(destination);
            if(profile!=null)awaitBusinessAccess(profile.peer());
            runManagement(home,secret,destination,broker,profile);
        }
    }

    private record NoticeProfile(PipelineConfig.Target peer,PlatformConfig.KafkaNoticeWriter writer){}
    private static NoticeProfile configureNotice(PipelineConfig.Target business,PipelineConfig.Target peer,List<String> bootstrap,
                                                   String secret,String cluster,Uuid topic){
        var declared=new PipelineConfig.KafkaNotice();declared.reservationId=java.util.UUID.randomUUID().toString();
        declared.controlTopic=CONTROL;declared.clusterSha256=cluster;declared.topicId=topic.toString();
        business.messaging.recoveryNotice=declared;
        var writer=new PlatformConfig.KafkaNoticeWriter();writer.taskId=TASK;writer.targetId=SUBSCRIPTION;
        writer.bootstrapServers=bootstrap;writer.username="fixture-writer";writer.password=secret+"-writer";
        writer.clusterSha256=cluster;writer.topicId=topic.toString();
        return new NoticeProfile(peer,writer);
    }
    private static PipelineConfig.Target peerTarget(String endpoint,String secret){
        var peer=target(endpoint,secret,PEER_TOPIC);peer.id=PEER;peer.messaging.maxMessageBytes=128*1024;return peer;
    }

    private static void runManagement(Path home,String secret,PipelineConfig.Target destination,Admin broker,
                                      NoticeProfile notice)throws Exception{
        String token=secret+"-controller",operator=secret+"-operator",store="jdbc:h2:mem:managed-"+secret+";DB_CLOSE_DELAY=-1";
        byte[] controlSecret=new byte[32];new SecureRandom().nextBytes(controlSecret);
        String key=Base64.getEncoder().encodeToString(controlSecret);
        var a=management(home,"controller-a",ClusterConfig.Role.CONTROLLER,store,key,token);
        var b=management(home,"controller-b",ClusterConfig.Role.CONTROLLER,store,key,token);
        var owner=management(home,OWNER,ClusterConfig.Role.AGENT,store,key,token);
        owner.kafkaPhysicalOwnershipEvidence=true;
        if(notice!=null)owner.kafkaNoticeWriters=List.of(notice.writer());
        var worker=new SyntheticWorkerFactory();
        try(TaskService aService=new TaskService(a);TaskService bService=new TaskService(b);
            TaskService agentService=new TaskService(owner,new TaskBackend.Database(),worker);
            ConsoleServer one=new ConsoleServer(a,aService);ConsoleServer two=new ConsoleServer(b,bService);
            ConsoleServer agent=new ConsoleServer(owner,agentService)){
            one.start();two.start();agent.start();
            var first=new Management(one,token);var second=new Management(two,token);
            require("CONTROLLER".equals(first.read("/api/session").path("nodeRole").asText())
                    &&"CONTROLLER".equals(second.read("/api/session").path("nodeRole").asText()),"Controllers must use separate execution");
            new Management(agent,token).exchange("GET","/api/tasks",null,null,404);
            first.createOperator(operator);
            first.create(notice==null?List.of(destination):List.of(destination,notice.peer()));
            first.action("START","start_physical_fixture");
            var launched=worker.worker.poll(12,TimeUnit.SECONDS);
            require(launched!=null&&OWNER.equals(launched.grant().member().member().node()),"Agent did not issue its real member grant");
            require(first.task().path("activeConfigurationVersion").asText().equals(launched.grant().scope().configurationVersion()),
                    "Worker grant used a different active configuration");
            if(broker==null){
                if(notice!=null){
                    var missing=Codec.JSON.createObjectNode().put("expectedRevision",first.task().path("revision").asText())
                            .put("approvalScopeSha256","0".repeat(64));
                    require("RECOVERY_RECEIPT_NOT_FOUND".equals(first.exchange("POST",PREFLIGHT,null,missing,404).path("code").asText()),
                            "Exact application does not expose the durable approval preflight");
                }
                first.action("STOP","stop_control_preflight");
                System.out.println("PASS actual Controller/Controller/Agent HTTP routes, shared authority and Worker grant before broker startup.");
                return;
            }

            PipelineConfig workerConfig=PipelineConfig.load(launched.configuration());
            var settings=workerConfig.existingJournalSettings();settings.kafkaPhysicalGrant=launched.grant();
            try(JdbcJournal journal=new JdbcJournal(settings)){
                var physical=seedFailure(journal,workerConfig,launched.grant());
                var topic=topicId(broker);
                require(physical.receipt().observation().topic().topicId().equals(topic.toString()),"Retained receipt does not identify the business Topic");
                first.action("PAUSE","pause_physical_fixture");
                var previous=first.review();
                String revision=first.task().path("revision").asText();
                var lookup=Codec.JSON.createObjectNode().put("expectedRevision",revision).put("predecessorRequestId",REVIEW);
                new Management(two,operator).exchange("POST",PHYSICAL,null,lookup,403);
                long before=offset(broker);
                JsonNode checked=first.lookup(lookup,200);
                require(checked.equals(second.lookup(lookup,200))&&checked.equals(first.lookup(lookup,200)),
                        "Replaying the read through either Controller changed its receipt");
                require("VERIFIED".equals(checked.path("status").asText())
                        &&"1".equals(checked.path("acknowledgedSequence").asText())
                        &&launched.grant().scope().configurationVersion().equals(checked.path("configurationVersion").asText())
                        &&Codec.sha256(Codec.encode(physical)).equals(checked.path("receiptSha256").asText()),
                        "Controller did not verify the exact retained Agent receipt");
                require(!Instant.parse(checked.path("acknowledgedAt").asText()).isBefore(Instant.parse(checked.path("publishedAt").asText()))
                        &&!checked.has("signature")&&!checked.has("nativeReceipts")&&!checked.has("executionAuthorized")
                        &&!checked.toString().contains(secret),"Receipt response is not redacted and read-only");
                require(before==offset(broker)&&broker.listConsumerGroups().all().get(10,TimeUnit.SECONDS).isEmpty()
                        &&journal.subscriptions().getFirst().acknowledged==1,"Query advanced a Kafka or journal checkpoint");
                if(notice!=null){
                    verifyNoticePreflight(first,second,new Management(two,operator),journal,broker,physical,previous,revision,secret);
                    return;
                }

                broker.deleteTopics(List.of(TOPIC)).all().get(15,TimeUnit.SECONDS);
                await(()->broker.listTopics().names().get(5,TimeUnit.SECONDS),topics->!topics.contains(TOPIC));
                createTopic(broker);
                awaitBusinessAccess(destination);
                Uuid replacement=await(()->topicId(broker),id->!topic.equals(id));
                require(!topic.equals(replacement)&&offset(broker)==0,"Replacement Topic was not independently recreated");
                var missing=second.lookup(lookup,200);
                require("MISSING".equals(missing.path("status").asText())
                        &&"KAFKA_PHYSICAL_RECEIPT_TOPIC_CHANGED".equals(missing.path("code").asText())
                        &&!missing.has("receiptSha256")&&missing.equals(first.lookup(lookup,200)),"Replacement Topic received old evidence");
                require(previous.equals(second.read(RECEIPTS+REVIEW))
                        &&physical.equals(new JdbcPublicationStore(journal,SUBSCRIPTION,null).kafkaPhysicalReceipt(GENERATION,launched.grant())),
                        "The lookup changed either retained receipt");

                journal.resumePipeline(1);
                require("KAFKA_PHYSICAL_RECEIPT_FAILURE_CHANGED".equals(second.lookup(lookup,409).path("code").asText()),
                        "A resumed journal used the old retained failure scope");
                first.action("STOP","physical_new_revision_stop");
                require(!revision.equals(first.task().path("revision").asText()),"The fixture did not change the task revision");
                require("REVISION_CHANGED".equals(first.lookup(lookup,409).path("code").asText()),"The old task revision was accepted");
                require(offset(broker)==0&&broker.listConsumerGroups().all().get(10,TimeUnit.SECONDS).isEmpty()
                        &&journal.subscriptions().getFirst().acknowledged==1,"Recovery query moved data after Topic replacement");
                System.out.println("PASS two Controllers and one Agent verified the exact TLS/SCRAM receipt; replay, changed Topic, stale failure/revision and RBAC passed.");
            }
        }
    }

    private static KafkaPhysicalReceipt.Signed seedFailure(JdbcJournal journal,PipelineConfig config,
                                                           KafkaPhysicalReceipt.Grant grant)throws Exception{
        journal.initialize(config.fingerprint(),GENERATION,config.subscribers());
        var lease=journal.claim("synthetic-source",120000);
        var store=new JdbcPublicationStore(journal,SUBSCRIPTION,lease);
        var target=config.targets.getFirst();
        var peer=config.targets.size()==2?config.targets.get(1):null;
        try(var sink=nativeSink(target,store,grant);
            var healthy=peer==null?null:nativeSink(peer,new JdbcPublicationStore(journal,PEER,lease),grant)){
            sink.activate(lease.epoch);
            journal.saveSchema(lease,TABLE);
            var snapshot=journal.beginSnapshot(lease,"source",TABLE,position(0));
            journal.finishSnapshot(lease,snapshot);
            publishSnapshot(sink,lease.epoch,snapshot);
            if(healthy!=null){
                healthy.activate(lease.epoch);
                publishSnapshot(healthy,lease.epoch,snapshot);
            }
            for(int sequence=1;sequence<=2;sequence++){
                var input=transaction(sequence);
                journal.append(lease,input);
                var normalized=journal.normalize(lease,journal.read(SUBSCRIPTION,1).getFirst(),
                        Map.of(TABLE.id,TABLE),Map.of(TABLE.id,position(0)),(position,id)->false);
                if(sequence==1){
                    sink.apply(lease.epoch,sequence,normalized);
                    journal.acknowledge(lease,SUBSCRIPTION,sequence);
                }else{
                    try{sink.apply(lease.epoch,sequence,normalized);throw new AssertionError("Expected oversize publication refusal");}
                    catch(IllegalStateException refused){require("MESSAGE_TOO_LARGE".equals(refused.getMessage()),"Wrong native publication refusal");}
                    var diagnostic=new SubscriberFailure();diagnostic.phase=SubscriberFailure.Phase.TARGET_APPLY;
                    diagnostic.sequence=2;diagnostic.transactionId=input.id;diagnostic.projectedHash=Codec.sha256(Codec.encode(normalized));
                    journal.pauseFailure(lease,SUBSCRIPTION,"MESSAGE_TOO_LARGE",diagnostic);
                }
                if(healthy!=null){
                    healthy.apply(lease.epoch,sequence,normalized);
                    journal.acknowledge(lease,PEER,sequence);
                }
            }
            var proof=store.kafkaPhysicalReceipt(GENERATION,grant);
            require(proof!=null&&grant.verifies(proof)&&proof.receipt().sequence()==1,"The publication did not retain a signed exact receipt");
            return proof;
        }finally{journal.release(lease);}
    }

    private static MessageSink nativeSink(PipelineConfig.Target target,JdbcPublicationStore store,KafkaPhysicalReceipt.Grant grant)throws Exception{
        return new MessageSink(target.id,target.mappings,store,()->new KafkaMessagePublisher(target),target.messaging.maxMessageBytes,grant);
    }

    private static void publishSnapshot(MessageSink sink,long epoch,Journal.SnapshotState snapshot)throws Exception{
        sink.prepare(epoch,TABLE);
        sink.beginSnapshot(epoch,TABLE,snapshot);
        sink.finishSnapshot(epoch,TABLE,snapshot);
    }

    private static void verifyNoticePreflight(Management first,Management second,Management operator,JdbcJournal journal,
                                              Admin broker,KafkaPhysicalReceipt.Signed physical,JsonNode predecessor,
                                              String revision,String secret)throws Exception{
        var original=describe(broker,CONTROL).topicId();
        var before=offsets(broker);
        require(before.get(CONTROL_PARTITION)==0L&&broker.listConsumerGroups().all().get(10,TimeUnit.SECONDS).isEmpty(),
                "Preflight fixture control Topic or consumer group already has progress");
        var planInput=Codec.JSON.createObjectNode().put("expectedRevision",revision).put("predecessorRequestId",REVIEW);
        var plan=first.exchange("POST",PLAN,null,planInput,200);
        require("KAFKA_NOTICE_PLAN_REVIEWED".equals(plan.path("status").asText())
                &&"VERIFIED_AT_REVIEW".equals(plan.path("writerEntitlement").path("status").asText())
                &&plan.path("writerEntitlement").path("evidenceSha256").asText().matches("[0-9a-f]{64}")
                &&"VERIFIED_AT_REVIEW".equals(plan.path("catalogCoverage").path("status").asText()),
                "Expected real distinct TLS/SCRAM writer entitlement and peer catalog coverage");
        String hash=plan.path("approvalScopeSha256").asText();
        var approval=first.exchange("POST",APPROVALS,APPROVAL_ID,planInput.deepCopy().put("approvalScopeSha256",hash),200);
        require("RECORDED_BLOCKED".equals(approval.path("status").asText())&&hash.equals(approval.path("approvalScopeSha256").asText())
                &&approval.equals(second.read(RECEIPTS+APPROVAL_ID))&&!approval.path("executionAuthorized").asBoolean(true),
                "Actual conditions did not produce an immutable blocked approval");
        var input=Codec.JSON.createObjectNode().put("expectedRevision",revision).put("approvalScopeSha256",hash);
        operator.exchange("POST",PREFLIGHT,null,input,403);
        var verified=first.exchange("POST",PREFLIGHT,null,input,200);
        require(verified.equals(second.exchange("POST",PREFLIGHT,null,input,200)),"Native sender recheck differed across Controllers");
        require("SENDER_BLOCKED".equals(verified.path("status").asText())&&!verified.path("executionAuthorized").asBoolean(true)
                &&approval.path("approvalScopeSha256").equals(verified.path("approvalScopeSha256"))
                &&approval.path("plan").path("failure").equals(verified.path("failure"))
                &&original.toString().equals(verified.path("controlTopicId").asText())
                &&"VERIFIED".equals(verified.path("precedingPhysicalReceipt").path("status").asText())
                &&Codec.sha256(Codec.encode(physical)).equals(verified.path("precedingPhysicalReceipt").path("receiptSha256").asText())
                &&List.of("KAFKA_NOTICE_HISTORICAL_OWNERSHIP_UNVERIFIED","KAFKA_NOTICE_TOPIC_ID_PRODUCE_REQUIRED")
                    .equals(Codec.JSON.convertValue(verified.path("blockers"),List.class))
                &&!verified.toString().contains(secret)&&!verified.toString().contains("fixture-writer"),
                "Sender prerequisite report lost its scope, redaction or historical blocker");
        assertNativeProgress(journal,broker,before);
        System.out.println("PASS RF3 TLS/SCRAM approval and both Controller preflight reads match exact original failure, topic and actor.");

        broker.deleteTopics(List.of(CONTROL)).all().get(15,TimeUnit.SECONDS);
        await(()->broker.listTopics().names().get(5,TimeUnit.SECONDS),topics->!topics.contains(CONTROL));
        createControl(broker);
        var replacement=await(()->describe(broker,CONTROL),d->!original.equals(d.topicId())&&d.partitions().getFirst().isr().size()==3);
        require(!original.equals(replacement.topicId()),"The control Topic replacement did not use a new UUID");
        for(Management entry:List.of(first,second))
            require("KAFKA_NOTICE_CONTROL_TOPIC_CHANGED".equals(entry.exchange("POST",PREFLIGHT,null,input,409).path("code").asText()),
                    "Preflight accepted an old control Topic UUID");
        assertNativeProgress(journal,broker,before);
        System.out.println("PASS both Controllers reject same-name replacement of the RF3 control Topic.");

        var oldVersion=first.task().path("activeConfigurationVersion").asText();
        second.changeConfiguration();
        var current=first.task();
        require(!oldVersion.equals(current.path("activeConfigurationVersion").asText())&&!revision.equals(current.path("revision").asText()),
                "The fixture did not activate a distinct task configuration");
        require("REVISION_CHANGED".equals(first.exchange("POST",PREFLIGHT,null,input,409).path("code").asText())
                &&"REVISION_CHANGED".equals(second.exchange("POST",PREFLIGHT,null,
                        input.deepCopy().put("expectedRevision",current.path("revision").asText()),409).path("code").asText()),
                "The original approval was accepted across a real configuration change");
        require(approval.equals(first.read(RECEIPTS+APPROVAL_ID))&&predecessor.equals(second.read(RECEIPTS+REVIEW)),
                "Refusing sender preflight changed a historical receipt");
        assertNativeProgress(journal,broker,before);
        System.out.println("PASS updated configuration is rejected; control and business offsets, groups and both journal ACKs are unchanged.");
    }

    private static void assertNativeProgress(JdbcJournal journal,Admin broker,Map<TopicPartition,Long> baseline)throws Exception{
        var acknowledgments=new LinkedHashMap<String,Long>();
        for(var sub:journal.subscriptions())acknowledgments.put(sub.id,sub.acknowledged);
        require(baseline.equals(offsets(broker))&&broker.listConsumerGroups().all().get(10,TimeUnit.SECONDS).isEmpty()
                &&acknowledgments.equals(Map.of(SUBSCRIPTION,1L,PEER,2L)),"A preflight read advanced a Kafka or journal position");
    }

    private static PlatformConfig management(Path base,String id,ClusterConfig.Role role,String store,String key,String token){
        var config=new PlatformConfig();config.port=0;config.pollMillis=100;config.authToken=token;
        config.tasksDirectory=base.resolve(id).resolve("tasks").toString();config.dataDirectory=base.resolve(id).resolve("data").toString();
        var cluster=new ClusterConfig();cluster.id="kafka-physical-query";cluster.nodeId=id;cluster.driver="org.h2.Driver";cluster.url=store;
        cluster.username="sa";cluster.password="";cluster.taskKey=key;cluster.sharedAccess=true;cluster.sharedTasks=true;
        cluster.recoveryReview=true;cluster.separateExecution=true;cluster.role=role;cluster.heartbeatMillis=250;cluster.leaseMillis=10000;
        config.cluster=cluster;return config;
    }

    private static final class SyntheticWorkerFactory implements TaskWorker.Factory {
        record Launch(Path configuration,KafkaPhysicalReceipt.Grant grant){}
        final ArrayBlockingQueue<Launch> worker=new ArrayBlockingQueue<>(2);
        @Override public TaskWorker start(Path config,Path log,int heap){throw new AssertionError("Agent did not provide an ownership grant");}
        @Override public TaskWorker start(Path config,Path log,int heap,KafkaPhysicalReceipt.Grant supplied)throws Exception{
            var grant=KafkaPhysicalReceipt.Grant.open(supplied.encode(),TASK,Files.readAllBytes(config));
            worker.add(new Launch(config,grant));
            return new TaskWorker(){
                private volatile boolean active=true;
                @Override public boolean alive(){return active;}
                @Override public int exitCode(){return 0;}
                @Override public void stop(){active=false;}
                @Override public void requestStop(){active=false;}
            };
        }
    }

    private static final class Management {
        private final HttpClient client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        private final String url,token;
        Management(ConsoleServer console,String token){url="http://127.0.0.1:"+console.port();this.token=token;}
        JsonNode exchange(String method,String route,String id,JsonNode body,int status)throws Exception{
            var builder=HttpRequest.newBuilder(URI.create(url+route)).timeout(Duration.ofSeconds(35)).header("Authorization","Bearer "+token);
            if(id!=null)builder.header("X-Request-ID",id);
            if(body!=null)builder.header("Content-Type","application/json");
            var reply=client.send(builder.method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(body.toString()))
                    .build(),HttpResponse.BodyHandlers.ofString());
            var json=Codec.JSON.readTree(reply.body());
            require(reply.statusCode()==status,"Unexpected "+method+" "+route+" status "+reply.statusCode()+" code "+json.path("code").asText());
            return json;
        }
        JsonNode read(String route)throws Exception{return exchange("GET",route,null,null,200);}
        JsonNode task()throws Exception{return read(TASK_ROUTE);}
        void createOperator(String operator)throws Exception{
            var body=Codec.JSON.createObjectNode().put("id","fixture-reader").put("role","OPERATOR").put("enabled",true)
                    .put("token",operator).put("expectedRevision",read("/api/accounts").path("revision").asText());
            body.putArray("taskIds").add(TASK);body.putArray("nodeIds").add(OWNER);
            exchange("POST","/api/accounts","create_fixture_operator",body,201);
        }
        void create(List<PipelineConfig.Target> targets)throws Exception{
            var config=new PipelineConfig();config.pipelineId=TASK;config.source.username="fixture-source";config.databases=List.of("sales");
            config.targets.addAll(targets);
            ObjectNode document=Codec.JSON.valueToTree(config);((ObjectNode)document.path("journal")).remove("path");
            ObjectNode body=Codec.JSON.createObjectNode().put("ownerNodeId",OWNER);body.set("config",document);
            exchange("POST","/api/tasks","create_physical_fixture",body,202);
            operation("create_physical_fixture");
            require(OWNER.equals(task().path("ownerNodeId").asText())&&!task().path("configurationPending").asBoolean(true),"Agent configuration was not applied");
        }
        void changeConfiguration()throws Exception{
            String route=TASK_ROUTE+"/configuration",request="rotate_preflight_config";
            var saved=read(route);
            var config=((ObjectNode)saved.path("config")).deepCopy();
            ((ObjectNode)config.path("source")).put("password","synthetic-config-transition");
            var body=Codec.JSON.createObjectNode().put("expectedRevision",saved.path("revision").asText());
            body.set("config",config);
            exchange("PUT",route,request,body,202);
            operation(request);
            await(this::task,task->!task.path("configurationPending").asBoolean(true));
        }
        void action(String action,String id)throws Exception{
            exchange("POST",TASK_ROUTE+"/actions",id,actionBody(action),202);operation(id);
        }
        JsonNode review()throws Exception{
            var body=actionBody("REVIEW_KAFKA_RECEIPTS").put("subscriptionId",SUBSCRIPTION).put("errorCount",1);
            exchange("POST",TASK_ROUTE+"/actions",REVIEW,body,202);operation(REVIEW);
            JsonNode receipt=read(RECEIPTS+REVIEW);
            require(KafkaRecoveryReview.VERIFIED.equals(receipt.path("code").asText())&&OWNER.equals(receipt.path("ownerNodeId").asText()),
                    "Actual Agent native predecessor verification failed");
            return receipt;
        }
        private ObjectNode actionBody(String action)throws Exception{
            return Codec.JSON.createObjectNode().put("action",action).put("expectedRevision",task().path("revision").asText());
        }
        JsonNode lookup(JsonNode body,int status)throws Exception{return exchange("POST",PHYSICAL,null,body,status);}
        private void operation(String id)throws Exception{
            JsonNode operation=await(()->{
                for(JsonNode item:task().path("operations"))if(id.equals(item.path("id").asText()))return item;
                return Codec.JSON.createObjectNode();
            },item->Set.of("SUCCEEDED","FAILED","REJECTED").contains(item.path("status").asText()));
            require("SUCCEEDED".equals(operation.path("status").asText()),"Fixture operation failed "+id+" code "+operation.path("code").asText());
        }
    }

    private static void createTopic(Admin broker)throws Exception{
        broker.createTopics(List.of(new NewTopic(TOPIC,1,(short)1).configs(KAFKA_CONFIG))).all().get(15,TimeUnit.SECONDS);
    }
    private static void createControl(Admin broker)throws Exception{
        var policy=new LinkedHashMap<>(KAFKA_CONFIG);
        policy.put("max.message.bytes","65536");policy.put("min.insync.replicas","2");
        broker.createTopics(List.of(new NewTopic(CONTROL,1,(short)3).configs(policy))).all().get(15,TimeUnit.SECONDS);
    }
    private static org.apache.kafka.clients.admin.TopicDescription describe(Admin broker,String name)throws Exception{
        return broker.describeTopics(List.of(name)).allTopicNames().get(5,TimeUnit.SECONDS).get(name);
    }
    private static AclBinding writerAcl(String topic,AclOperation action){
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC,topic,PatternType.LITERAL),
                new AccessControlEntry("User:fixture-writer","*",action,AclPermissionType.ALLOW));
    }
    private static Uuid topicId(Admin broker)throws Exception{
        return describe(broker,TOPIC).topicId();
    }
    private static long offset(Admin broker)throws Exception{
        return broker.listOffsets(Map.of(PARTITION,OffsetSpec.latest())).all().get(10,TimeUnit.SECONDS).get(PARTITION).offset();
    }
    private static Map<TopicPartition,Long> offsets(Admin broker)throws Exception{
        var nativeResult=broker.listOffsets(Map.of(PARTITION,OffsetSpec.latest(),PEER_PARTITION,OffsetSpec.latest(),
                CONTROL_PARTITION,OffsetSpec.latest())).all().get(10,TimeUnit.SECONDS);
        var offsets=new LinkedHashMap<TopicPartition,Long>();
        nativeResult.forEach((partition,result)->offsets.put(partition,result.offset()));
        return offsets;
    }
    private static void awaitBusinessAccess(PipelineConfig.Target target)throws Exception{
        try(Admin business=Admin.create(KafkaMessagePublisher.connectionProperties(target))){
            String topic=target.messaging.topic;
            await(()->{
                try{return business.describeTopics(List.of(topic),new DescribeTopicsOptions().includeAuthorizedOperations(true))
                        .allTopicNames().get(5,TimeUnit.SECONDS).get(topic).authorizedOperations();}
                catch(java.util.concurrent.ExecutionException waiting){
                    if(waiting.getCause() instanceof org.apache.kafka.common.errors.TopicAuthorizationException)return Set.<AclOperation>of();
                    throw waiting;
                }
            },allowed->allowed!=null&&allowed.containsAll(Set.of(READ,WRITE)));
        }
    }
    private static TableSchema schema(){
        var table=new TableSchema();table.id=TableSchema.identity("sales","","orders");table.database="sales";table.name="orders";
        var column=new TableSchema.Column();column.name="id";column.type=TableSchema.Type.STRING;table.columns.add(column);
        var payload=new TableSchema.Column();payload.name="payload";payload.type=TableSchema.Type.STRING;table.columns.add(payload);
        table.primaryKey.add("id");return table;
    }
    private static SourceTransaction transaction(int sequence){
        var tx=new SourceTransaction();tx.id="fixture-tx-"+sequence;tx.streamGeneration=GENERATION;tx.position=position(sequence);
        var row=new Change();row.eventId=tx.id+"-insert";row.tableId=TABLE.id;row.operation=Change.Operation.INSERT;
        row.after=List.of(Cell.value(Integer.toString(sequence)),Cell.value(sequence==1?"retained-fixture":"x".repeat(66536)));
        tx.changes.add(row);return tx;
    }
    private static SourcePosition position(int sequence){
        String uuid="25b043c4-e42d-41ad-960f-53a7f0958a73";
        String gtid=sequence<=1?uuid+":1":uuid+":1-"+sequence;
        return new SourcePosition(1,"MYSQL","fixture-source",Map.of("gtidSet",gtid,"serverUuid",uuid,
                "file","binlog.000001","offset",Integer.toString(104+sequence)));
    }
    @FunctionalInterface interface Read<T>{T get()throws Exception;}
    private static <T>T await(Read<T> read,Predicate<T> condition)throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(18);
        while(System.nanoTime()<until){
            try{
                T value=read.get();if(condition.test(value))return value;
            }catch(java.util.concurrent.ExecutionException retry){
                if(!(retry.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException))throw retry;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Fixture did not reach the expected state");
    }
    private static void require(boolean valid,String message){if(!valid)throw new AssertionError(message);}
}
