package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.recovery.FailureEvidence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/** A read-only proof of the last acknowledged materialization receipt before a paused failed head. */
public final class MaterializedRecoveryReview {
    private record Cut(String generation,long epoch,long acknowledged,long failedSequence,String failedRawHash,
                       String failedNormalizedHash,String precedingTransactionId,String precedingTransactionHash,
                       String progress,String nativeIdentityHash){}

    private MaterializedRecoveryReview(){}

    public static Map<String,Object> inspect(PipelineConfig config,String targetId,long expectedErrorGeneration)throws Exception{
        if(config==null)throw new IllegalArgumentException("MATERIALIZED_RECOVERY_V3_REQUIRED");
        PipelineConfig.Target target=config.targets.stream().filter(item->item.id.equals(targetId)).findFirst()
                .orElseThrow(()->new IllegalArgumentException("MATERIALIZED_RECOVERY_TARGET_UNKNOWN"));
        if(target.product==null||!target.product.materialized())throw new IllegalArgumentException("MATERIALIZED_RECOVERY_TARGET_UNSUPPORTED");
        String code=target.product.name()+"_RECOVERY_";
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            Cut before=cut(journal,targetId,expectedErrorGeneration,code);
            String namespace=Collections.max(target.mappings.values());
            String expectedEvent=MessageSink.eventId(config.pipelineId,before.generation(),targetId,
                    MessageSink.transactionEventKey(before.precedingTransactionId(),namespace,"COMMIT"));
            verifyNative(config,target,before,expectedEvent);
            if(!before.equals(cut(journal,targetId,expectedErrorGeneration,code)))
                throw new IllegalStateException(code+"CUT_CHANGED");
            return Map.of("status","PRECEDING_RECEIPT_VERIFIED","target",targetId,"product",target.product.name(),"observedAt",Instant.now().toString(),
                    "epoch",Long.toString(before.epoch()),"errorGeneration",Long.toString(expectedErrorGeneration),
                    "acknowledgedSequence",Long.toString(before.acknowledged()),"failedSequence",Long.toString(before.failedSequence()),
                    "journalPrecedingTransactionSha256",before.precedingTransactionHash(),"nativeReceiptSha256",Codec.sha256(before.progress()));
        }
    }

    private static void verifyNative(PipelineConfig config,PipelineConfig.Target target,Cut cut,String event)throws Exception{
        switch(target.product){
            case CLICKHOUSE -> {
                var settings=config.clickHouseSettings(target,cut.generation());
                settings.http.connectTimeoutMillis=Math.min(settings.http.connectTimeoutMillis,3000);
                settings.http.requestTimeoutMillis=Math.min(settings.http.requestTimeoutMillis,3000);
                try(var materializer=new ClickHouseMaterializer(settings)){
                    materializer.verifyPrecedingCommit(cut.progress(),cut.nativeIdentityHash(),event);
                }
            }
            case DORIS -> {
                var settings=config.dorisSettings(target,cut.generation());
                settings.http.connectTimeoutMillis=Math.min(settings.http.connectTimeoutMillis,3000);
                settings.http.requestTimeoutMillis=Math.min(settings.http.requestTimeoutMillis,3000);
                try(var materializer=new DorisMaterializer(settings)){
                    materializer.verifyPrecedingCommit(cut.progress(),cut.nativeIdentityHash(),event);
                }
            }
            case HIVE -> {
                var settings=config.hiveSettings(target,cut.generation());
                settings.queryTimeoutSeconds=Math.min(settings.queryTimeoutSeconds,4);
                try(var materializer=new HiveMaterializer(settings)){
                    materializer.verifyPrecedingCommit(cut.progress(),cut.nativeIdentityHash(),event);
                }
            }
            default -> throw new IllegalArgumentException("MATERIALIZED_RECOVERY_TARGET_UNSUPPORTED");
        }
    }

    private static Cut cut(JdbcJournal journal,String target,long errorGeneration,String code)throws Exception{
        try(Connection c=journal.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,10000)){
            c.setAutoCommit(false);
            Map<String,Object> job=journal.locked(c);
            if(!"PAUSED".equals(job.get("state"))||job.get("owner")!=null)
                throw new IllegalStateException(code+"REQUIRES_PIPELINE_PAUSED");
            Journal.FailureReview failure=new JdbcFailureReview(journal).inspect(c,target,errorGeneration,JdbcFailureReview.MAXIMUM_BYTES);
            if(failure.failure.phase!=SubscriberFailure.Phase.TARGET_APPLY||failure.failure.sequence!=failure.sequence
                    ||failure.normalizedHash==null)
                throw new IllegalStateException(code+"REQUIRES_COMPLETE_APPLY_FAILURE");
            String publication=journal.table("publication"),transport=journal.table("publication_transport"),schema=journal.table("publication_schema");
            String sql="SELECT p.generation,p.epoch,p.stream_seq,p.transaction_id,p.transaction_hash,t.generation,t.payload,s.payload_hash FROM "+publication+" p"
                    +" JOIN "+transport+" t ON t.pipeline_id=p.pipeline_id AND t.subscription_id=p.subscription_id"
                    +" JOIN "+schema+" s ON s.pipeline_id=p.pipeline_id AND s.subscription_id=p.subscription_id AND s.table_id='_transport' AND s.schema_version=0"
                    +" WHERE p.pipeline_id=? AND p.subscription_id=?";
            Cut result;
            try(PreparedStatement query=c.prepareStatement(sql)){
                query.setString(1,journal.settings.pipelineId);query.setString(2,target);
                try(ResultSet rows=query.executeQuery()){
                    if(!rows.next())throw new IllegalStateException(code+"PRECEDING_RECEIPT_UNAVAILABLE");
                    long sequence=rows.getLong(3);
                    String transaction=rows.getString(4),hash=rows.getString(5),progress=rows.getString(7),identity=rows.getString(8);
                    if(!failure.generation.equals(rows.getString(1))||!failure.generation.equals(rows.getString(6))
                            ||rows.getLong(2)!=failure.epoch||sequence<1||sequence!=failure.subscription.acknowledged
                            ||transaction==null||transaction.isBlank()||!FailureEvidence.hash(hash)||!FailureEvidence.hash(identity)
                            ||progress==null||progress.length()>2048)
                        throw new IllegalStateException(code+"PRECEDING_RECEIPT_UNAVAILABLE");
                    result=new Cut(failure.generation,failure.epoch,sequence,failure.sequence,failure.rawHash,
                            failure.normalizedHash,transaction,hash,progress,identity);
                }
            }
            deadline.verify();c.rollback();return result;
        }
    }
}
