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
public final class ClickHouseRecoveryReview {
    private record Cut(String generation,long epoch,long acknowledged,long failedSequence,String failedRawHash,
                       String failedNormalizedHash,String precedingTransactionId,String precedingTransactionHash,
                       String progress,String nativeIdentityHash){}

    private ClickHouseRecoveryReview(){}

    public static Map<String,Object> inspect(PipelineConfig config,String targetId,long expectedErrorGeneration)throws Exception{
        PipelineConfig.Target target=config.targets.stream().filter(item->item.id.equals(targetId)).findFirst()
                .orElseThrow(()->new IllegalArgumentException("CLICKHOUSE_RECOVERY_TARGET_UNKNOWN"));
        if(target.product!=TargetProduct.CLICKHOUSE)throw new IllegalArgumentException("CLICKHOUSE_RECOVERY_TARGET_UNSUPPORTED");
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            Cut before=cut(journal,targetId,expectedErrorGeneration);
            String namespace=Collections.max(target.mappings.values());
            String expectedEvent=MessageSink.eventId(config.pipelineId,before.generation(),targetId,
                    MessageSink.transactionEventKey(before.precedingTransactionId(),namespace,"COMMIT"));
            ClickHouseMaterializer.Settings settings=config.clickHouseSettings(target,before.generation());
            settings.http.connectTimeoutMillis=Math.min(settings.http.connectTimeoutMillis,3000);
            settings.http.requestTimeoutMillis=Math.min(settings.http.requestTimeoutMillis,3000);
            try(ClickHouseMaterializer materializer=new ClickHouseMaterializer(settings)){
                materializer.verifyPrecedingCommit(before.progress(),before.nativeIdentityHash(),expectedEvent);
            }
            if(!before.equals(cut(journal,targetId,expectedErrorGeneration)))
                throw new IllegalStateException("CLICKHOUSE_RECOVERY_CUT_CHANGED");
            return Map.of("status","PRECEDING_RECEIPT_VERIFIED","target",targetId,"observedAt",Instant.now().toString(),
                    "epoch",Long.toString(before.epoch()),"errorGeneration",Long.toString(expectedErrorGeneration),
                    "acknowledgedSequence",Long.toString(before.acknowledged()),"failedSequence",Long.toString(before.failedSequence()),
                    "journalPrecedingTransactionSha256",before.precedingTransactionHash(),"nativeReceiptSha256",Codec.sha256(before.progress()));
        }
    }

    private static Cut cut(JdbcJournal journal,String target,long errorGeneration)throws Exception{
        try(Connection c=journal.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,10000)){
            c.setAutoCommit(false);
            Map<String,Object> job=journal.locked(c);
            if(!"PAUSED".equals(job.get("state"))||job.get("owner")!=null)
                throw new IllegalStateException("CLICKHOUSE_RECOVERY_REQUIRES_PIPELINE_PAUSED");
            Journal.FailureReview failure=new JdbcFailureReview(journal).inspect(c,target,errorGeneration,JdbcFailureReview.MAXIMUM_BYTES);
            if(failure.failure.phase!=SubscriberFailure.Phase.TARGET_APPLY||failure.failure.sequence!=failure.sequence
                    ||failure.normalizedHash==null)
                throw new IllegalStateException("CLICKHOUSE_RECOVERY_REQUIRES_COMPLETE_APPLY_FAILURE");
            String publication=journal.table("publication"),transport=journal.table("publication_transport"),schema=journal.table("publication_schema");
            String sql="SELECT p.generation,p.epoch,p.stream_seq,p.transaction_id,p.transaction_hash,t.generation,t.payload,s.payload_hash FROM "+publication+" p"
                    +" JOIN "+transport+" t ON t.pipeline_id=p.pipeline_id AND t.subscription_id=p.subscription_id"
                    +" JOIN "+schema+" s ON s.pipeline_id=p.pipeline_id AND s.subscription_id=p.subscription_id AND s.table_id='_transport' AND s.schema_version=0"
                    +" WHERE p.pipeline_id=? AND p.subscription_id=?";
            Cut result;
            try(PreparedStatement query=c.prepareStatement(sql)){
                query.setString(1,journal.settings.pipelineId);query.setString(2,target);
                try(ResultSet rows=query.executeQuery()){
                    if(!rows.next())throw new IllegalStateException("CLICKHOUSE_RECOVERY_PRECEDING_RECEIPT_UNAVAILABLE");
                    long sequence=rows.getLong(3);
                    String transaction=rows.getString(4),hash=rows.getString(5),progress=rows.getString(7),identity=rows.getString(8);
                    if(!failure.generation.equals(rows.getString(1))||!failure.generation.equals(rows.getString(6))
                            ||rows.getLong(2)!=failure.epoch||sequence<1||sequence!=failure.subscription.acknowledged
                            ||transaction==null||transaction.isBlank()||!FailureEvidence.hash(hash)||!FailureEvidence.hash(identity)
                            ||progress==null||progress.length()>2048)
                        throw new IllegalStateException("CLICKHOUSE_RECOVERY_PRECEDING_RECEIPT_UNAVAILABLE");
                    result=new Cut(failure.generation,failure.epoch,sequence,failure.sequence,failure.rawHash,
                            failure.normalizedHash,transaction,hash,progress,identity);
                }
            }
            deadline.verify();c.rollback();return result;
        }
    }
}
