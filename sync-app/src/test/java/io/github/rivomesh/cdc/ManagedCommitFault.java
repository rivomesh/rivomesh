package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import java.nio.file.Paths;
import java.util.*;

/** Test-only lost-ACK injection: commit the real paused binlog head, deliberately withhold ACK. */
public final class ManagedCommitFault {
    public static void main(String[] args)throws Exception{
        if(args.length!=2)throw new IllegalArgumentException("FAULT_ARGUMENTS_INVALID");
        PipelineConfig config=PipelineConfig.load(Paths.get(args[0]));String subscription=args[1];
        PipelineConfig.Target target=config.targets.stream().filter(t->t.id.equals(subscription)).findFirst().orElseThrow(()->new IllegalArgumentException("FAULT_TARGET_MISSING"));
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());Journal.Subscription sub=journal.subscriptions().stream().filter(s->s.id.equals(subscription)).findFirst().get();
            Journal.FailureReview review=journal.inspectFailure(subscription,sub.errorGeneration,4L*1024*1024);if(review.normalized==null)throw new IllegalStateException("FAULT_NORMALIZATION_MISSING");
            Set<String> references=new TreeSet<>();for(Change change:review.normalized.changes)if(change.operation!=Change.Operation.BARRIER)references.add(change.tableId+"/"+change.schemaVersion);
            Map<String,TableSchema> schemas=journal.historicalSchemas(references,2L*1024*1024);
            try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target,review.generation))){
                sink.activate(review.epoch);for(TableSchema table:schemas.values())if(sink.includes(table))sink.prepare(review.epoch,table);
                SourceTransaction projected=TransactionProjection.project(review.normalized,c->schemas.get(c.tableId+"/"+c.schemaVersion),sink::includes);
                sink.apply(review.epoch,review.sequence,projected);
                if(journal.subscriptions().stream().filter(s->s.id.equals(subscription)).findFirst().get().acknowledged!=sub.acknowledged)throw new IllegalStateException("FAULT_ACK_MOVED");
                System.out.println("COMMITTED_WITHOUT_ACK sequence="+review.sequence);
            }
        }
    }
}
