package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.common.errors.InterruptException;

/** Redacted ownership evidence for the exact acknowledged predecessor of a current paused failure. */
public final class KafkaPhysicalReceiptReview {
    private record Snapshot(KafkaRecoveryReview.Cut cut,KafkaPhysicalReceipt.Signed proof){}
    @FunctionalInterface interface TopicRead {
        KafkaPhysicalReceipt.Topic read(PipelineConfig.Target target,KafkaRecoveryReview.Observation predecessor)throws Exception;
    }

    private KafkaPhysicalReceiptReview(){}

    public static Map<String,Object> inspect(PipelineConfig config,KafkaPhysicalReceipt.Scope scope,
            KafkaNoticePlanning.Predecessor predecessor,String key,Predicate<KafkaPhysicalReceipt.MemberWitness> verifyMember)throws Exception{
        return inspect(config,scope,predecessor,key,verifyMember,KafkaRecoveryReview::matchingPhysicalTopic);
    }

    static Map<String,Object> inspect(PipelineConfig config,KafkaPhysicalReceipt.Scope scope,
            KafkaNoticePlanning.Predecessor predecessor,String key,Predicate<KafkaPhysicalReceipt.MemberWitness> verifyMember,
            TopicRead topics)throws Exception{
        if(!scope.task().equals(config.pipelineId)||!scope.task().equals(predecessor.taskId())
                ||!scope.configurationVersion().equals(predecessor.configurationVersion())
                ||!scope.ownerDirectory().equals(predecessor.ownerDirectoryId())||predecessor.observation().originalFailure()==null)
            throw rejected("KAFKA_PHYSICAL_RECEIPT_SCOPE_CHANGED");
        var observed=predecessor.observation();
        var target=config.targets.stream().filter(t->t.id.equals(observed.target())&&t.product==TargetProduct.KAFKA)
                .findFirst().orElseThrow(()->rejected("KAFKA_PHYSICAL_RECEIPT_TARGET_UNSUPPORTED"));
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            Snapshot first=snapshot(journal,observed,scope.configurationVersion());
            if(!verified(first,scope,predecessor,key,verifyMember))return result("MISSING","KAFKA_PHYSICAL_RECEIPT_UNVERIFIED",scope,observed);
            KafkaPhysicalReceipt.Topic topic;
            try{topic=topics.read(target,observed);}
            catch(InterruptException stopped){Thread.currentThread().interrupt();throw new InterruptedException();}
            catch(InterruptedException stopped){Thread.currentThread().interrupt();throw stopped;}
            catch(Exception unavailable){return result("UNREACHABLE","KAFKA_PHYSICAL_RECEIPT_BROKER_UNAVAILABLE",scope,observed);}
            if(!first.equals(snapshot(journal,observed,scope.configurationVersion())))
                throw rejected("KAFKA_PHYSICAL_RECEIPT_FAILURE_CHANGED");
            var receipt=first.proof().receipt();
            if(!receipt.observation().topic().equals(topic))return result("MISSING","KAFKA_PHYSICAL_RECEIPT_TOPIC_CHANGED",scope,observed);
            var response=result("VERIFIED","KAFKA_PHYSICAL_RECEIPT_VERIFIED",scope,observed);
            response.put("receiptSha256",Codec.sha256(Codec.encode(first.proof())));
            response.put("publishedAt",Instant.ofEpochMilli(receipt.journalTime().publishedUtcMillis()).toString());
            response.put("acknowledgedAt",Instant.ofEpochMilli(receipt.journalTime().acknowledgedUtcMillis()).toString());
            return response;
        }
    }

    private static Snapshot snapshot(JdbcJournal journal,KafkaRecoveryReview.Observation observed,String version)throws Exception{
        try(Connection c=journal.connect();RepairConnectionGuard guard=new RepairConnectionGuard(c,10000)){
            c.setAutoCommit(false);
            KafkaRecoveryReview.Cut cut;
            try{cut=KafkaRecoveryReview.cut(journal,c,observed.target(),Long.parseLong(observed.errorGeneration()));}
            catch(IllegalStateException failure){throw rejected("KAFKA_PHYSICAL_RECEIPT_FAILURE_CHANGED");}
            if(!observed.originalFailure().equals(cut.originalFailure())||!observed.epoch().equals(Long.toString(cut.epoch()))
                    ||!observed.acknowledgedSequence().equals(Long.toString(cut.acknowledged()))
                    ||!observed.failedSequence().equals(Long.toString(cut.failedSequence())))
                throw rejected("KAFKA_PHYSICAL_RECEIPT_FAILURE_CHANGED");
            var proof=read(journal,c,observed.target(),cut.generation(),version);
            guard.verify();c.rollback();return new Snapshot(cut,proof);
        }
    }

    private static KafkaPhysicalReceipt.Signed read(JdbcJournal journal,Connection c,String target,String generation,String version)throws Exception{
        var point=c.setSavepoint();
        try{return JdbcKafkaPhysicalHistory.attested(journal,c,target,generation,version);}
        catch(IllegalArgumentException|IllegalStateException invalid){return null;}
        catch(SQLException missing){
            String state=missing.getSQLState();
            if(!"42S02".equals(state)&&!"42S04".equals(state)&&!"42P01".equals(state))throw missing;
            c.rollback(point);return null;
        }finally{c.releaseSavepoint(point);}
    }

    private static boolean verified(Snapshot candidate,KafkaPhysicalReceipt.Scope scope,KafkaNoticePlanning.Predecessor predecessor,
                                  String key,Predicate<KafkaPhysicalReceipt.MemberWitness> verifyMember){
        if(candidate.proof()==null)return false;
        var receipt=candidate.proof().receipt();var cut=candidate.cut();var time=receipt.journalTime();var member=receipt.member();
        if(member==null||time==null||time.acknowledgementEpoch()==null||time.acknowledgementEpoch()>cut.epoch()
                ||!scope.equals(receipt.scope())||!predecessor.ownerNodeId().equals(member.member().node())
                ||!predecessor.observation().target().equals(receipt.target())||!cut.generation().equals(receipt.generation())
                ||receipt.sequence()!=cut.acknowledged()||!cut.transactionHash().equals(receipt.transactionSha256())
                ||!cut.clusterHash().equals(receipt.observation().topic().clusterSha256())
                ||!new KafkaPhysicalReceipt.Grant(scope,key,member).verifies(candidate.proof())||!verifyMember.test(member))return false;
        var wires=receipt.observation().commits().stream().map(wire->new KafkaRecoveryReview.Receipt(
                wire.namespace(),Long.toString(wire.offset()),wire.payloadSha256())).toList();
        return new HashSet<>(wires).equals(new HashSet<>(predecessor.observation().nativeReceipts()));
    }

    private static Map<String,Object> result(String status,String code,KafkaPhysicalReceipt.Scope scope,KafkaRecoveryReview.Observation observed){
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("status",status);result.put("code",code);result.put("configurationVersion",scope.configurationVersion());
        result.put("targetId",observed.target());result.put("acknowledgedSequence",observed.acknowledgedSequence());
        return result;
    }

    private static IllegalStateException rejected(String code){return new IllegalStateException(code);}
}
