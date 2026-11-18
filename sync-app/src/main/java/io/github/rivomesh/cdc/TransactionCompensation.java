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
        configTargetCheck(config, retained.value.review);
        PipelineConfig.Target target = config.targets.stream()
                .filter(candidate -> candidate.id.equals(retained.value.review.subscription.id)).findFirst()
                .orElseThrow(() -> refused("COMPENSATION_TARGET_UNKNOWN"));
        if (target.product != TargetProduct.MYSQL && target.product != TargetProduct.POSTGRESQL)
            throw refused("COMPENSATION_TARGET_UNSUPPORTED");
        try (JdbcJournal journal = new JdbcJournal(config.existingJournalSettings())) {
            journal.verifyIdentity(config.fingerprint(), config.subscribers());
            return run(journal, config.targetSettings(target, retained.value.review.generation),
                    retained, approvedHash, operation, actor, reason);
        }
    }

    private static void configTargetCheck(PipelineConfig config, Journal.FailureReview review) {
        if (!config.pipelineId.equals(review.pipelineId) || !config.fingerprint().equals(review.fingerprint))
            throw refused("COMPENSATION_CONFIGURATION_CHANGED");
    }

    static Map<String,Object> run(JdbcJournal journal, JdbcSink.Settings target, FailureEvidenceStore.Envelope retained,
            String approvedHash, String operation, String actor, String reason) throws Exception {
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
        Collection<CompensationRows.Row> rows = CompensationRows.reconstruct(projected, evidence.schemas);
        String requestHash = null;
        if (approvedHash != null) {
            if (!FailureEvidence.hash(approvedHash) || operation == null || !operation.matches("[A-Za-z0-9_-]{8,128}")
                    || actor == null || actor.isEmpty() || actor.length() > 128
                    || reason == null || reason.trim().length() < 8 || reason.length() > 1000)
                throw new IllegalArgumentException("COMPENSATION_APPROVAL_INVALID");
            requestHash = Codec.sha256(Codec.encode(Arrays.asList(approvedHash, retained.hash, actor, reason)));
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
                return sink.compensation(rows, retained, approvedHash, operation, requestHash, actor, reason, checkpoint -> {
                    if (checkpoint.skipOperation != null) JdbcSkipJournal.verifyCheckpoint(journal, source, job, target.id, checkpoint);
                    return null;
                });
            }
        }
    }

    private static IllegalStateException refused(String code) {
        return new IllegalStateException(code);
    }
}
