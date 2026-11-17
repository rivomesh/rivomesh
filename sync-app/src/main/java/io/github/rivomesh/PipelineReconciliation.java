package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.Path;
import java.util.*;

/** Source sealing and resumable SQL target comparison. Every operation retains the durable pause. */
public final class PipelineReconciliation {
    private PipelineReconciliation(){}
    public static String stage(PipelineConfig config,Set<String> targets,Path reports,ValidationStore.Limits limits,long gateMillis,long sourceMillis)throws Exception{
        return stage(config,targets,reports,limits,gateMillis,sourceMillis,null);
    }
    public static String stage(PipelineConfig config,Set<String> targets,Path reports,ValidationStore.Limits limits,long gateMillis,long sourceMillis,String reportId)throws Exception{
        try(PipelineValidationBarrier.Session cut=PipelineValidationBarrier.open(config,targets,gateMillis,sourceMillis,reportId)){
            PipelineValidationBarrier.Certificate certificate=cut.certificate();List<TableSchema> tables=cut.tables();Map<String,Set<String>> scope=new TreeMap<>();Set<String> sourceTables=new TreeSet<>();
            for(PipelineConfig.Target target:config.targets)if(targets.contains(target.id)){
                Set<String> selected=new TreeSet<>();for(TableSchema table:tables)if(target.mappings.containsKey(table.database))selected.add(table.id);scope.put(target.id,selected);sourceTables.addAll(selected);
            }
            try(ValidationStore store=ValidationStore.create(reports,certificate.id,certificate,tables,scope,limits);JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
                try{
                    for(TableSchema table:tables)if(sourceTables.contains(table.id)){
                        cut.verify();if(table.keyless()){
                            String after="";while(true){List<JdbcJournal.IdentityRow> page=journal.identities(certificate.generation,certificate.epoch,certificate.sequence,table.id,after,128,limits.maximumRowBytes);if(page.isEmpty())break;for(JdbcJournal.IdentityRow identity:page){store.identity(table.id,identity);after=identity.rowId;}}
                        }
                        try(SourceAdapter.Snapshot cursor=cut.sourceRows(table)){for(List<List<Cell>> batch;!(batch=cursor.next(1)).isEmpty();)for(List<Cell> row:batch)store.source(table.id,row);}
                        cut.verify();store.finishSourceTable(table.id);
                    }
                    cut.verify();store.completeSource();return certificate.id;
                }catch(Exception failure){try{store.failed(code(failure));}catch(Exception saved){failure.addSuppressed(saved);}throw failure;}
            }
        }
    }
    /** Reuses only a complete source seal. Partial target scans are discarded and safely reread. */
    public static Map<String,Object> compare(PipelineConfig config,Path reports,String id,long targetMillis)throws Exception{
        try(ValidationStore store=ValidationStore.open(reports,id)){
            ValidationStore.Manifest manifest=store.manifest();
            if(!manifest.sourceComplete)throw new IllegalStateException("VALIDATION_NEW_SOURCE_VIEW_REQUIRED");
            PipelineValidationBarrier.verifyCertificate(config,manifest.certificate);
            if("COMPLETE".equals(manifest.state))return store.summary();
            try{
                store.verifySource();store.restartTargets();
                for(PipelineConfig.Target target:config.targets)if(manifest.targetTables.containsKey(target.id)){
                    try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target,manifest.certificate.generation))){
                        for(String tableId:manifest.targetTables.get(target.id)){
                            PipelineValidationBarrier.verifyCertificate(config,manifest.certificate);
                            try(TargetRead reader=sink.validationRead(manifest.schemas.get(tableId),manifest.certificate.targetCheckpoints.get(target.id),targetMillis,manifest.limits.maximumRowBytes)){
                                for(List<TargetRead.Row> batch;!(batch=reader.next(128)).isEmpty();)for(TargetRead.Row row:batch)store.target(target.id,tableId,row.rowId,row.values);reader.verify();
                            }
                            PipelineValidationBarrier.verifyCertificate(config,manifest.certificate);store.finishTarget(target.id,tableId);
                        }
                    }
                }
                PipelineValidationBarrier.verifyCertificate(config,manifest.certificate);store.complete();return store.summary();
            }catch(Exception failure){try{store.failed(code(failure));}catch(Exception saved){failure.addSuppressed(saved);}throw failure;}
        }
    }
    private static String code(Exception failure){return failure instanceof SyncFailure?((SyncFailure)failure).code:"VALIDATION_SCAN_FAILED";}
}
