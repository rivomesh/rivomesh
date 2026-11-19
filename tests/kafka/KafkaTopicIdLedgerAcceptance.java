package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

/** Scoped native proof that the application ledger can use the isolated 4.1.2 sender. */
public final class KafkaTopicIdLedgerAcceptance {
    private static final String CONTROL = "uuid-notice-control", BUSINESS = "uuid-notice-business", PEER = "uuid-notice-peer";
    private static final String TARGET = "blocked";
    private static final Map<String, String> POLICY = Map.of("cleanup.policy","delete","retention.ms","-1",
            "retention.bytes","-1","unclean.leader.election.enable","false","min.insync.replicas","2","max.message.bytes","65536");
    private static final KafkaRecoveryNotice.Authority APPROVED_FIXTURE = () -> {};

    public static void main(String[] args) throws Exception {
        String secret = System.getenv("RIVOMESH_KAFKA_FIXTURE_SECRET");
        check(secret != null && secret.matches("[a-f0-9]{48}"), "Fixture credentials required");
        List<String> addresses = List.of(args[0].split(","));
        PipelineConfig.Target reviewer = KafkaPhysicalOwnershipAcceptance.target(addresses.getFirst(), secret, BUSINESS);
        reviewer.id = TARGET;
        reviewer.messaging.endpoints = addresses.stream().skip(1).map(endpoint -> "kafka://" + endpoint).toList();
        var reviewerProperties = KafkaMessagePublisher.connectionProperties(reviewer);
        try (Admin admin = Admin.create(KafkaPhysicalOwnershipAcceptance.adminProperties(reviewer, secret))) {
            awaitCluster(admin,addresses.size());
            admin.createTopics(List.of(topic(BUSINESS), topic(PEER), topic(CONTROL))).all().get(20, TimeUnit.SECONDS);
            Uuid original = awaitTopic(admin, null);
            String cluster = Codec.sha256("KAFKA:" + admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS));
            var controlTopic = new KafkaRecoveryNotice.Topic(CONTROL, cluster, original.toString());
            JdbcJournal.Settings store = new JdbcJournal.Settings();
            store.driver = "org.h2.Driver";
            store.url = "jdbc:h2:file:" + Path.of(args[1]).toRealPath().resolve("uuid-notice-ledger") + ";WRITE_DELAY=0";
            store.pipelineId = "notice-fixture";
            KafkaRecoveryNotice.Plan plan;
            long confirmedOffset;
            String rawHash;
            String normalizedHash;
            var broker = KafkaNoticeBroker.physicallyAddressed(reviewer, addresses, "fixture-writer", secret + "-writer");

            try (JdbcJournal journal = new JdbcJournal(store)) {
                KafkaRecoveryNoticeAcceptance.seed(journal);
                var ledger = new JdbcKafkaNoticeLedger(journal);
                ledger.provision();
                plan = ledger.prepare(TARGET, 1, controlTopic, Codec.sha256("reviewed predecessor"), APPROVED_FIXTURE);
                var cut = journal.inspectFailure(TARGET, 1, 100_000);
                rawHash = cut.rawHash;
                normalizedHash = cut.normalizedHash;
                admin.createAcls(List.of(
                        acl("reviewer", ResourceType.CLUSTER, Resource.CLUSTER_NAME, AclOperation.DESCRIBE_CONFIGS),
                        acl("reviewer", ResourceType.TOPIC, CONTROL, AclOperation.READ),
                        acl("reviewer", ResourceType.TOPIC, CONTROL, AclOperation.DESCRIBE_CONFIGS),
                        acl("writer", ResourceType.TOPIC, CONTROL, AclOperation.WRITE),
                        acl("writer", ResourceType.TRANSACTIONAL_ID, plan.transactionalId(), AclOperation.WRITE)))
                        .all().get(15, TimeUnit.SECONDS);
                try (Admin authorized = Admin.create(reviewerProperties)) {
                    awaitReader(authorized);
                }
                broker.inspectControlTopic(plan);
                var protocol = new KafkaRecoveryNotice(ledger, loseCommitReply(broker));
                try {
                    protocol.publish(plan, plan.hash(), APPROVED_FIXTURE);
                    throw new AssertionError("Expected lost commit reply");
                } catch (IOException lost) {
                    check("fixture lost commit reply".equals(lost.getMessage()), "Unexpected IOException");
                }
                var pending = ledger.existing(plan);
                check(pending.hasCommitIntent() && !pending.verified(), "Lost reply must remain pending until readback");
                confirmedOffset = pending.offset();
            }

            try (JdbcJournal reopened = new JdbcJournal(store)) {
                var ledger = new JdbcKafkaNoticeLedger(reopened);
                var replay = new KafkaRecoveryNotice(ledger, readOnly(broker));
                var receipt = replay.publish(plan, plan.hash(), APPROVED_FIXTURE);
                check(receipt != null && receipt.offset() == confirmedOffset && receipt.topic().topicId().equals(original.toString())
                        && receipt.payloadSha256().equals(plan.payloadSha256()) && ledger.existing(plan).verified(), "Exact native receipt missing");
                check(receipt.equals(replay.publish(plan, plan.hash(), APPROVED_FIXTURE)), "Replay changed the native receipt");
                checkSameCut(reopened, rawHash, normalizedHash);
                System.out.println("PASS the packaged application's UUID transport recovers one native notice after a lost Kafka commit reply.");

                admin.deleteTopics(List.of(CONTROL)).all().get(15, TimeUnit.SECONDS);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (admin.listTopics().names().get(5, TimeUnit.SECONDS).contains(CONTROL) && System.nanoTime() < deadline)
                    Thread.sleep(100);
                admin.createTopics(List.of(topic(CONTROL))).all().get(18, TimeUnit.SECONDS);
                awaitTopic(admin, original);
                try {
                    replay.publish(plan, plan.hash(), APPROVED_FIXTURE);
                    throw new AssertionError("A same-name replacement reused an accepted notice");
                } catch (IllegalStateException changed) {
                    check("KAFKA_NOTICE_CONTROL_TOPIC_CHANGED".equals(changed.getMessage()), "Wrong reason for replacement");
                }
                check(ledger.existing(plan).verified() && ledger.existing(plan).offset() == confirmedOffset,
                        "Replacement changed the prior verified ledger entry");
                checkSameCut(reopened, rawHash, normalizedHash);
                check(offset(admin, CONTROL) == 0 && offset(admin, BUSINESS) == 0 && offset(admin, PEER) == 0,
                        "Notice or replay touched unrelated native progress");
                check(admin.listConsumerGroups().all().get(8, TimeUnit.SECONDS).isEmpty(), "Notice created a consumer group");
                System.out.println("PASS both journal ACKs and source hashes stay fixed. Same-name replacement cannot replay the old receipt.");
            }
            verifyAbortedAttempt(admin,reviewer,addresses,secret,Path.of(args[1]).toRealPath(),cluster);
        }
    }

    private static void verifyAbortedAttempt(Admin admin,PipelineConfig.Target target,List<String> addresses,
                                            String secret,Path path,String cluster)throws Exception{
        String control="uuid-notice-abort";
        admin.createTopics(List.of(topic(control))).all().get(18,TimeUnit.SECONDS);
        var id=admin.describeTopics(List.of(control)).allTopicNames().get(8,TimeUnit.SECONDS).get(control).topicId();
        var address=new KafkaRecoveryNotice.Topic(control,cluster,id.toString());
        var settings=new JdbcJournal.Settings();
        settings.driver="org.h2.Driver";
        settings.url="jdbc:h2:file:"+path.resolve("uuid-notice-abort-ledger")+";WRITE_DELAY=0";
        settings.pipelineId="notice-abort-fixture";
        var broker=KafkaNoticeBroker.physicallyAddressed(target,addresses,"fixture-writer",secret+"-writer");
        KafkaRecoveryNotice.Plan plan;
        try(var journal=new JdbcJournal(settings)){
            KafkaRecoveryNoticeAcceptance.seed(journal);
            var ledger=new JdbcKafkaNoticeLedger(journal);
            ledger.provision();
            plan=ledger.prepare(TARGET,1,address,Codec.sha256("same predecessor for interrupted attempt"),APPROVED_FIXTURE);
            admin.createAcls(List.of(
                    acl("reviewer",ResourceType.TOPIC,control,AclOperation.READ),
                    acl("reviewer",ResourceType.TOPIC,control,AclOperation.DESCRIBE_CONFIGS),
                    acl("writer",ResourceType.TOPIC,control,AclOperation.WRITE),
                    acl("writer",ResourceType.TRANSACTIONAL_ID,plan.transactionalId(),AclOperation.WRITE)))
                    .all().get(15,TimeUnit.SECONDS);
            var interrupted=new KafkaRecoveryNotice(ledger,interruptedBeforeCommit(broker));
            try{
                interrupted.publish(plan,plan.hash(),APPROVED_FIXTURE);
                throw new AssertionError("Expected an interruption before committing the Kafka transaction");
            }catch(IOException expected){
                check("fixture interrupted before Kafka commit".equals(expected.getMessage()),"Unexpected interruption");
            }
            var pending=ledger.existing(plan);
            check(pending.hasCommitIntent()&&pending.offset()==0&&!pending.verified(),"Interrupted attempt must retain native offset zero");
            check(broker.read(plan,pending.offset())==null,"A transaction interrupted before commit became visible");
        }
        try(var reopened=new JdbcJournal(settings)){
            var ledger=new JdbcKafkaNoticeLedger(reopened);
            var recovery=new KafkaRecoveryNotice(ledger,broker);
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(80);
            while(ledger.existing(plan).hasCommitIntent()&&System.nanoTime()<deadline){
                check(recovery.publish(plan,plan.hash(),APPROVED_FIXTURE)==null,
                        "The interrupted attempt unexpectedly committed");
                Thread.sleep(600);
            }
            check(!ledger.existing(plan).hasCommitIntent()&&ledger.existing(plan).number()==1,
                    "Only Kafka's terminal abort may retire the original intent");
            var receipt=recovery.publish(plan,plan.hash(),APPROVED_FIXTURE);
            check(receipt!=null&&receipt.offset()==2&&ledger.existing(plan).number()==2&&ledger.existing(plan).verified()
                    &&broker.read(plan,0L)==null&&offset(admin,control)==4,"Retry must append exactly one committed notice after the abort marker");
            check(receipt.equals(recovery.publish(plan,plan.hash(),APPROVED_FIXTURE))&&offset(admin,control)==4,
                    "Repeat delivery reused neither the verified receipt nor exact native offset");
            check(reopened.subscriptions().stream().allMatch(row->row.acknowledged==(TARGET.equals(row.id)?1:2)),
                    "Retry advanced a business acknowledgement");
        }
        System.out.println("PASS an interrupted native Kafka transaction remains uncertain until broker abort, then the exact notice retries once.");
    }

    private static KafkaRecoveryNotice.Broker interruptedBeforeCommit(KafkaNoticeBroker broker){
        return new KafkaRecoveryNotice.Broker(){
            @Override public KafkaRecoveryNotice.Receipt read(KafkaRecoveryNotice.Plan plan,Long offset)throws Exception{
                return broker.read(plan,offset);
            }
            @Override public KafkaRecoveryNotice.Attempt begin(KafkaRecoveryNotice.Plan plan)throws Exception{
                var nativeAttempt=broker.begin(plan);
                return new KafkaRecoveryNotice.Attempt(){
                    private long offset;
                    @Override public long send(byte[] payload)throws Exception{
                        offset=nativeAttempt.send(payload);
                        return offset;
                    }
                    @Override public void commit()throws Exception{
                        check(broker.read(plan,offset)==null&&!broker.confirmedAborted(plan,offset),
                                "A still-open native transaction must remain uncertain");
                        throw new IOException("fixture interrupted before Kafka commit");
                    }
                    @Override public void close(){nativeAttempt.close();}
                };
            }
        };
    }

    private static void checkSameCut(JdbcJournal journal,String rawHash,String normalizedHash) throws Exception {
        var cut = journal.inspectFailure(TARGET, 1, 100_000);
        long peer = journal.subscriptions().stream().filter(row -> row.id.equals("peer")).findFirst().orElseThrow().acknowledged;
        check(cut.subscription.acknowledged == 1 && peer == 2 && cut.rawHash.equals(rawHash)
                && cut.normalizedHash.equals(normalizedHash), "Native notice changed a subscription ACK or source transaction");
    }

    private static KafkaRecoveryNotice.Broker loseCommitReply(KafkaNoticeBroker broker) {
        return new KafkaRecoveryNotice.Broker() {
            @Override public KafkaRecoveryNotice.Receipt read(KafkaRecoveryNotice.Plan plan, Long offset) throws Exception {
                return broker.read(plan, offset);
            }
            @Override public KafkaRecoveryNotice.Attempt begin(KafkaRecoveryNotice.Plan plan) throws Exception {
                var real = broker.begin(plan);
                return new KafkaRecoveryNotice.Attempt() {
                    @Override public long send(byte[] payload) throws Exception { return real.send(payload); }
                    @Override public void commit() throws Exception { real.commit(); throw new IOException("fixture lost commit reply"); }
                    @Override public void close() { real.close(); }
                };
            }
        };
    }

    private static KafkaRecoveryNotice.Broker readOnly(KafkaNoticeBroker broker) {
        return new KafkaRecoveryNotice.Broker() {
            @Override public KafkaRecoveryNotice.Receipt read(KafkaRecoveryNotice.Plan plan, Long offset) throws Exception {
                return broker.read(plan, offset);
            }
            @Override public KafkaRecoveryNotice.Attempt begin(KafkaRecoveryNotice.Plan plan) {
                throw new AssertionError("Replay started another Kafka producer");
            }
        };
    }

    private static AclBinding acl(String principal,ResourceType type,String resource,AclOperation operation) {
        return new AclBinding(new ResourcePattern(type,resource,PatternType.LITERAL),
                new AccessControlEntry("User:fixture-" + principal,"*",operation,AclPermissionType.ALLOW));
    }
    private static NewTopic topic(String name) {
        return new NewTopic(name, 1, (short)3).configs(POLICY);
    }
    private static long offset(Admin admin,String topic) throws Exception {
        TopicPartition partition = new TopicPartition(topic,0);
        return admin.listOffsets(Map.of(partition, OffsetSpec.latest())).partitionResult(partition).get(8,TimeUnit.SECONDS).offset();
    }

    private static Uuid awaitTopic(Admin admin,Uuid previous) throws Exception {
        var limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < limit) {
            try {
                var description = admin.describeTopics(List.of(CONTROL)).allTopicNames().get(5,TimeUnit.SECONDS).get(CONTROL);
                if (!description.topicId().equals(previous) && !description.topicId().equals(Uuid.ZERO_UUID)
                        && description.partitions().getFirst().isr().size() == 3) return description.topicId();
            } catch (java.util.concurrent.ExecutionException transition) {
                if (!(transition.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) throw transition;
                // A broker may briefly report the just-deleted catalog before installing the new Topic.
            }
            Thread.sleep(150);
        }
        throw new AssertionError("Expected physical control Topic and three live replicas");
    }

    private static void awaitCluster(Admin admin,int count)throws Exception{
        var deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(40);
        Exception last=null;
        while(System.nanoTime()<deadline){
            try{
                var described=admin.describeCluster(new org.apache.kafka.clients.admin.DescribeClusterOptions().timeoutMs(2500));
                if(described.nodes().get(3,TimeUnit.SECONDS).size()==count&&described.controller().get(3,TimeUnit.SECONDS)!=null)
                    return;
            }catch(java.util.concurrent.ExecutionException|java.util.concurrent.TimeoutException unavailable){
                last=unavailable;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("The fixture Kafka caller cannot reach the complete TLS/SCRAM cluster",last);
    }

    private static void awaitReader(Admin reviewer) throws Exception {
        var limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < limit) {
            try {
                var description = reviewer.describeTopics(List.of(CONTROL),
                        new DescribeTopicsOptions().includeAuthorizedOperations(true)).allTopicNames().get(5,TimeUnit.SECONDS).get(CONTROL);
                if (description.authorizedOperations().containsAll(Set.of(AclOperation.READ,AclOperation.DESCRIBE_CONFIGS))) return;
            } catch (java.util.concurrent.ExecutionException transition) {
                // The three brokers may be applying ACL metadata independently.
            }
            Thread.sleep(150);
        }
        throw new AssertionError("The read-only control Topic identity never gained its ACL");
    }

    private static void check(boolean valid,String problem) {
        if (!valid) throw new AssertionError(problem);
    }
}
