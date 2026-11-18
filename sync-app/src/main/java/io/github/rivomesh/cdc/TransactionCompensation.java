package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.recovery.FailureEvidence;
import io.github.rivomesh.recovery.FailureEvidenceStore;
import java.sql.Connection;
import java.util.*;

/** Restore reviewed identities to the state before the next unapplied source transaction. */
public final class TransactionCompensation {
    private TransactionCompensation() {}

    public static Map<String,Object> run(PipelineConfig config, FailureEvidenceStore.Envelope retained,
            String approvedHash, String operation, String actor, String reason) throws Exception {
        return run(config,retained,null,approvedHash,operation,actor,reason);
    }

    public static Map<String,Object> run(PipelineConfig config, FailureEvidenceStore.Envelope retained,
            CorrectionBatch correction, String approvedHash, String operation, String actor, String reason) throws Exception {
        configTargetCheck(config, retained.value.review);
        PipelineConfig.Target target = config.targets.stream()
                .filter(candidate -> candidate.id.equals(retained.value.review.subscription.id)).findFirst()
                .orElseThrow(() -> refused("COMPENSATION_TARGET_UNKNOWN"));
        if (target.product != TargetProduct.MYSQL && target.product != TargetProduct.POSTGRESQL)
            throw refused("COMPENSATION_TARGET_UNSUPPORTED");
        try (JdbcJournal journal = new JdbcJournal(config.existingJournalSettings())) {
            journal.verifyIdentity(config.fingerprint(), config.subscribers());
            return run(journal, config.targetSettings(target, retained.value.review.generation),
                    retained, correction, approvedHash, operation, actor, reason);
        }
    }

    private static void configTargetCheck(PipelineConfig config, Journal.FailureReview review) {
        if (!config.pipelineId.equals(review.pipelineId) || !config.fingerprint().equals(review.fingerprint))
            throw refused("COMPENSATION_CONFIGURATION_CHANGED");
    }

    static Map<String,Object> run(JdbcJournal journal, JdbcSink.Settings target, FailureEvidenceStore.Envelope retained,
            String approvedHash, String operation, String actor, String reason) throws Exception {
        return run(journal,target,retained,null,approvedHash,operation,actor,reason);
    }

    static Map<String,Object> run(JdbcJournal journal, JdbcSink.Settings target, FailureEvidenceStore.Envelope retained,
            CorrectionBatch correction, String approvedHash, String operation, String actor, String reason) throws Exception {
        FailureEvidence evidence = retained.value;
        evidence.validate();
        Journal.FailureReview review = evidence.review;
        if (!FailureEvidence.hash(retained.hash) || !retained.hash.equals(Codec.sha256(Codec.encode(evidence))))
            throw refused("FAILURE_EVIDENCE_CONTENT_CHANGED");
        if (!journal.settings.pipelineId.equals(review.pipelineId) || !target.pipelineId.equals(review.pipelineId)
                || !target.id.equals(review.subscription.id) || !target.fingerprint.equals(review.fingerprint)
                || !target.generation.equals(review.generation)) throw refused("COMPENSATION_CONFIGURATION_CHANGED");
        if (review.failure.phase != SubscriberFailure.Phase.TARGET_APPLY || review.failure.sequence != review.sequence
                || review.raw.kind != SourceTransaction.Kind.DATA || review.normalized == null
                || review.normalized.kind != SourceTransaction.Kind.DATA) throw refused("COMPENSATION_COMPLETE_APPLY_FAILURE_REQUIRED");
        SourceTransaction projected = FailureCheckpointInspection.projected(evidence, target);
        if (!Codec.sha256(Codec.encode(projected)).equals(review.failure.projectedHash))
            throw refused("COMPENSATION_PROJECTION_CHANGED");
        Collection<CompensationRows.Row> rows = correction == null
                ? CompensationRows.reconstruct(projected,evidence.schemas) : correction.bind(projectedTables(evidence,projected));
        String requestHash = null;
        if (approvedHash != null) {
            if (!FailureEvidence.hash(approvedHash) || operation == null || !operation.matches("[A-Za-z0-9_-]{8,128}")
                    || actor == null || actor.isEmpty() || actor.length() > 128
                    || reason == null || reason.trim().length() < 8 || reason.length() > 1000)
                throw new IllegalArgumentException("COMPENSATION_APPROVAL_INVALID");
            requestHash = Codec.sha256(Codec.encode(correction==null
                    ? Arrays.asList(approvedHash,retained.hash,actor,reason)
                    : Arrays.asList("CORRECTION_V1",approvedHash,retained.hash,actor,reason,correction)));
        }
        if (requestHash != null) try (JdbcSink sink = JdbcSink.existing(target)) {
            Map<String,Object> already = sink.compensationReceipt(operation, requestHash);
            if (already != null) return already;
        }
        try (Connection source = journal.connect()) {
            Map<String,Object> job;
            try (RepairConnectionGuard wait = new RepairConnectionGuard(source, 30000)) {
                source.setAutoCommit(false);
                job = journal.locked(source);
                if (!"PAUSED".equals(job.get("state"))) throw refused("COMPENSATION_REQUIRES_PIPELINE_PAUSED");
                if ((int)job.get("format") == 4) JdbcSkipJournal.requireNoPending(journal, source, target.id);
                FailureCheckpointInspection.verify(review, new JdbcFailureReview(journal)
                        .inspect(source, target.id, review.subscription.errorGeneration, 4L * 1024 * 1024));
                wait.verify();
            }
            try (JdbcSink sink = JdbcSink.existing(target)) {
                return sink.compensation(rows, retained, correction!=null, approvedHash, operation, requestHash, actor, reason, checkpoint -> {
                    if (checkpoint.skipOperation != null) JdbcSkipJournal.verifyCheckpoint(journal, source, job, target.id, checkpoint);
                    return null;
                });
            }
        }
    }

    private static Map<String,TableSchema> projectedTables(FailureEvidence evidence, SourceTransaction projected) {
        Map<String,TableSchema> tables=new TreeMap<>();
        for(Change event:projected.changes){
            if(event.operation==Change.Operation.BARRIER)continue;
            TableSchema table=evidence.schemas.get(event.tableId+"/"+event.schemaVersion);
            TableSchema old=tables.putIfAbsent(table.id,table);
            if(old!=null&&old.version!=table.version)throw refused("CORRECTION_SCHEMA_CHANGED");
        }
        return tables;
    }

    /** Administrative shape lookup. Returns column contracts from immutable failure evidence. */
    public static Map<String,Object> correctionTables(PipelineConfig config, FailureEvidenceStore.Envelope retained) {
        FailureEvidence evidence=retained.value;
        evidence.validate();
        configTargetCheck(config,evidence.review);
        PipelineConfig.Target target=config.targets.stream()
                .filter(candidate->candidate.id.equals(evidence.review.subscription.id)).findFirst()
                .orElseThrow(()->refused("CORRECTION_TARGET_UNKNOWN"));
        if(target.product!=TargetProduct.POSTGRESQL&&target.product!=TargetProduct.MYSQL)
            throw refused("CORRECTION_TARGET_UNSUPPORTED");
        if(evidence.review.failure.phase!=SubscriberFailure.Phase.TARGET_APPLY)
            throw refused("CORRECTION_APPLY_FAILURE_REQUIRED");
        SourceTransaction projected=FailureCheckpointInspection.projected(evidence,config.targetSettings(target,evidence.review.generation));
        List<Map<String,Object>> result=new ArrayList<>();
        for(TableSchema table:projectedTables(evidence,projected).values()){
            Map<String,Object> item=new LinkedHashMap<>();
            item.put("id",table.id);
            item.put("name",table.qualifiedName());
            item.put("keys",CorrectionBatch.keys(table));
            List<Map<String,Object>> fields=new ArrayList<>();
            for(TableSchema.Column column:table.columns){
                Map<String,Object> field=new LinkedHashMap<>();
                field.put("name",column.name);
                field.put("type",column.type.name());
                field.put("nullable",column.nullable);
                field.put("generated",column.generationKind!=null);
                fields.add(field);
            }
            item.put("columns",fields);
            result.add(item);
        }
        Map<String,Object> response=Collections.singletonMap("tables",result);
        if(Codec.encode(response).getBytes(java.nio.charset.StandardCharsets.UTF_8).length>CorrectionBatch.MAX_BYTES)
            throw refused("CORRECTION_SCHEMA_LIMIT");
        return response;
    }

    private static IllegalStateException refused(String code) {
        return new IllegalStateException(code);
    }
}
