package io.github.rivomesh.cdc;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.rivomesh.recovery.FailureEvidence;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kafka.common.Uuid;

/** Evidence of one actual Kafka business COMMIT at a managed configuration version. */
public final class KafkaPhysicalReceipt {
    public static final String GRANT_ENV="RIVOMESH_KAFKA_PHYSICAL_GRANT";
    static final int MAX_WIRE=16;
    static final int MAX_BYTES=6144;

    public record Scope(String authorityId,String task,String ownerDirectory,String configurationVersion,
                        String workerProjectionSha256) {
        public Scope {
            if(!uuid(authorityId)||!id(task)||!uuid(ownerDirectory)||!uuid(configurationVersion)
                    ||!FailureEvidence.hash(workerProjectionSha256))throw invalid();
        }
        public String keyDomain(){return "kafka-physical-receipt-key-v1\n"+Codec.encode(this);}
    }

    /** Control-store UTC and lease boundaries describe the grant issuance only. */
    public record Member(Scope scope,String node,String incarnation,long epoch,long observedUtcMillis,long expiresUtcMillis) {
        public Member {
            if(scope==null||node==null||!node.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")||!uuid(incarnation)
                    ||epoch<1||observedUtcMillis<1||expiresUtcMillis<=observedUtcMillis)throw invalid();
        }
        public String signatureDomain(){return "kafka-physical-member-v1\n"+Codec.encode(this);}
    }

    public record MemberWitness(Member member,String signature) {
        public MemberWitness {
            if(member==null||!validSignature(signature))throw invalid();
        }
    }

    record Topic(String clusterSha256,String topicId) {
        Topic {
            if(!FailureEvidence.hash(clusterSha256)||topicId==null||!topicId.matches("[A-Za-z0-9_-]{22}")
                    ||Uuid.ZERO_UUID.equals(Uuid.fromString(topicId)))throw invalid();
        }
    }

    record Wire(String namespace,long offset,String payloadSha256) {
        Wire {
            if(namespace==null||!namespace.matches("[A-Za-z][A-Za-z0-9_]{0,62}")||offset<0||!FailureEvidence.hash(payloadSha256))
                throw invalid();
        }
    }

    record Observation(Topic topic,List<Wire> commits) {
        Observation {
            if(topic==null||commits==null||commits.isEmpty()||commits.size()>MAX_WIRE)throw invalid();
            commits=commits.stream().sorted(Comparator.comparing(Wire::namespace)).toList();
            var namespaces=new HashSet<String>();var offsets=new HashSet<Long>();
            for(Wire item:commits)if(!namespaces.add(item.namespace())||!offsets.add(item.offset()))throw invalid();
        }
    }

    record JournalTime(long publishedUtcMillis,Long acknowledgedUtcMillis,Long acknowledgementEpoch) {
        JournalTime {
            if(publishedUtcMillis<1||(acknowledgedUtcMillis==null)!=(acknowledgementEpoch==null)
                    ||acknowledgedUtcMillis!=null&&(acknowledgedUtcMillis<publishedUtcMillis||acknowledgementEpoch<1))
                throw invalid();
        }
    }

    record Receipt(Scope scope,String target,String generation,long epoch,long sequence,String transactionSha256,
                   Observation observation,@JsonInclude(JsonInclude.Include.NON_NULL) MemberWitness member,
                   @JsonInclude(JsonInclude.Include.NON_NULL) JournalTime journalTime) {
        Receipt {
            if(scope==null||!id(target)||generation==null||generation.isBlank()||generation.length()>128||epoch<1||sequence<1
                    ||!FailureEvidence.hash(transactionSha256)||observation==null)throw invalid();
            if(member!=null&&!scope.equals(member.member().scope())||journalTime!=null&&(member==null
                    ||journalTime.acknowledgementEpoch()!=null&&journalTime.acknowledgementEpoch()<epoch))throw invalid();
        }
        Receipt withTime(JournalTime time){
            return new Receipt(scope,target,generation,epoch,sequence,transactionSha256,observation,member,time);
        }
    }

    record Signed(Receipt receipt,String signature) {
        Signed {
            if(receipt==null||!validSignature(signature))throw invalid();
        }
    }

    private record Envelope(Scope scope,String key,@JsonInclude(JsonInclude.Include.NON_NULL) MemberWitness member,
            @JsonInclude(JsonInclude.Include.NON_NULL) KafkaHistoryOrigin historyOrigin,
            @JsonInclude(JsonInclude.Include.NON_NULL) String historyIntentKey){}

    public static final class Grant {
        private final Scope scope;
        private final String key;
        private final MemberWitness member;
        private final KafkaHistoryOrigin historyOrigin;
        private final String historyIntentKey;
        Grant(Scope scope,String key){this(scope,key,null);}
        public Grant(Scope scope,String key,MemberWitness member) {this(scope,key,member,null,null);}
        public Grant(Scope scope,String key,MemberWitness member,KafkaHistoryOrigin historyOrigin,String historyIntentKey) {
            if(scope==null||!validSignature(key)||Base64.getUrlDecoder().decode(key).length!=32
                    ||member!=null&&!scope.equals(member.member().scope())
                    ||(historyOrigin==null)!=(historyIntentKey==null)
                    ||historyOrigin!=null&&(member==null||!historyOrigin.belongsTo(scope)||!validSignature(historyIntentKey)))throw invalid();
            this.scope=scope;this.key=key;this.member=member;this.historyOrigin=historyOrigin;this.historyIntentKey=historyIntentKey;
        }
        public Scope scope(){return scope;}
        public MemberWitness member(){return member;}
        public KafkaHistoryOrigin historyOrigin(){return historyOrigin;}
        public String encode(){return Base64.getUrlEncoder().withoutPadding().encodeToString(
                Codec.encode(new Envelope(scope,key,member,historyOrigin,historyIntentKey)).getBytes(StandardCharsets.UTF_8));}
        public static Grant open(String encoded,String task,byte[] workerProjection)throws Exception{
            if(encoded==null)return null;
            if(encoded.length()>4096)throw invalid();
            Envelope envelope;
            try{envelope=Codec.decode(new String(Base64.getUrlDecoder().decode(encoded),StandardCharsets.UTF_8),Envelope.class);}
            catch(Exception invalid){throw invalid();}
            var grant=new Grant(envelope.scope(),envelope.key(),envelope.member(),envelope.historyOrigin(),envelope.historyIntentKey());
            if(!task.equals(grant.scope.task())||!Codec.sha256(workerProjection).equals(grant.scope.workerProjectionSha256()))throw invalid();
            return grant;
        }
        Signed sign(String target,String generation,long epoch,long sequence,String transactionSha256,Observation observation){
            var receipt=new Receipt(scope,target,generation,epoch,sequence,transactionSha256,observation,member,null);
            return new Signed(receipt,authenticate(receipt));
        }
        Signed published(Signed value,long utcMillis){
            var receipt=value.receipt();
            if(member==null||!member.equals(receipt.member())||receipt.journalTime()!=null||!verifies(value))throw invalid();
            var timed=receipt.withTime(new JournalTime(utcMillis,null,null));
            return new Signed(timed,authenticate(timed));
        }
        Signed acknowledged(Signed value,long utcMillis,long epoch){
            var receipt=value.receipt();var time=receipt.journalTime();
            if(member==null||time==null||time.acknowledgementEpoch()!=null||epoch<receipt.epoch()||!verifies(value))throw invalid();
            if(utcMillis<time.publishedUtcMillis())return null;
            var completed=receipt.withTime(new JournalTime(time.publishedUtcMillis(),utcMillis,epoch));
            return new Signed(completed,authenticate(completed));
        }
        boolean verifies(Signed value){
            return scope.equals(value.receipt().scope())&&MessageDigest.isEqual(authenticate(value.receipt()).getBytes(StandardCharsets.US_ASCII),
                    value.signature().getBytes(StandardCharsets.US_ASCII));
        }
        String signHistoryIntent(String payload){
            if(historyOrigin==null)throw invalid();
            return authenticate(historyIntentKey,"kafka-history-first-intent-v1\n"+historyOrigin.signature()+"\n"+payload);
        }
        boolean verifiesHistoryIntent(String payload,String signature){
            return signature!=null&&MessageDigest.isEqual(signHistoryIntent(payload).getBytes(StandardCharsets.US_ASCII),
                    signature.getBytes(StandardCharsets.US_ASCII));
        }
        private String authenticate(Receipt receipt){return authenticate(key,"kafka-physical-ack-v1\n"+Codec.encode(receipt));}
        private static String authenticate(String key,String payload){
            try{
                Mac mac=Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(Base64.getUrlDecoder().decode(key),"HmacSHA256"));
                return Base64.getUrlEncoder().withoutPadding().encodeToString(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
            }catch(GeneralSecurityException impossible){throw new IllegalStateException("KAFKA_PHYSICAL_CRYPTO_UNAVAILABLE");}
        }
    }

    private KafkaPhysicalReceipt(){}
    private static boolean validSignature(String signature){return signature!=null&&signature.matches("[A-Za-z0-9_-]{43}");}
    private static boolean id(String value){return value!=null&&value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");}
    private static boolean uuid(String value){
        try{return value!=null&&UUID.fromString(value).toString().equals(value);}
        catch(IllegalArgumentException invalid){return false;}
    }
    private static IllegalArgumentException invalid(){return new IllegalArgumentException("KAFKA_PHYSICAL_RECEIPT_INVALID");}
}
