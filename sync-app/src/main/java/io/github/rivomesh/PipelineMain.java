package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.*;
import io.github.rivomesh.validation.ValidationStore;

/** CLI dispatch kept separate so legacy v2 commands retain their exact semantics. */
final class PipelineMain {
    private PipelineMain(){}
    static void execute(String[] args)throws Exception{
        var path=Paths.get(args[1]).toAbsolutePath().normalize();
        String ownership=args.length==2&&"run".equals(args[0])?System.getenv(KafkaPhysicalReceipt.GRANT_ENV):null;
        PipelineConfig config;
        if(ownership==null)config=PipelineConfig.load(path);
        else{
            byte[] projected=Files.readAllBytes(path);
            config=PipelineConfig.parse(new String(projected,StandardCharsets.UTF_8),path.getParent());
            config.kafkaPhysicalGrant=KafkaPhysicalReceipt.Grant.open(ownership,config.pipelineId,projected);
        }
        if(args.length==2&&args[0].equals("run")){
            try(PipelineEngine engine=new PipelineEngine(config)){
                Thread hook=new Thread(engine::close,"v3-shutdown");Runtime.getRuntime().addShutdownHook(hook);
                try{engine.run();}finally{try{Runtime.getRuntime().removeShutdownHook(hook);}catch(IllegalStateException ignored){/* shutdown */}}
            }
        }else if(args.length==2&&args[0].equals("check"))System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineEngine.check(config)));
        else if(args.length==2&&args[0].equals("init-writer"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(JournalWriterAssignment.initialize(
                    config,generation->PipelineSources.open(config,generation))));
        else if(args.length==5&&args[0].equals("transfer-writer"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(JournalWriterAssignment.transfer(
                    config,args[2],args[3],Long.parseLong(args[4]),generation->PipelineSources.open(config,generation))));
        else if(args[0].equals("review-source")&&(args.length==2||args.length==3&&args[2].equals("--verify-contents")))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(CaptureRecoveryReview.inspect(config,args.length==3,null)));
        else if(args.length==4&&args[0].equals("review-source")&&args[2].equals("--retain-contents"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(CaptureRecoveryReview.inspect(config,true,Paths.get(args[3]))));
        else if(args.length==4&&args[0].equals("review-clickhouse-recovery"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(ClickHouseRecoveryReview.inspect(config,args[2],Long.parseLong(args[3]))));
        else if(args.length==4&&args[0].equals("review-materialized-recovery"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(MaterializedRecoveryReview.inspect(config,args[2],Long.parseLong(args[3]))));
        else if(args.length==4&&args[0].equals("review-kafka-recovery"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(KafkaRecoveryReview.inspect(config,args[2],Long.parseLong(args[3]))));
        else if(args.length==2&&args[0].equals("pause"))System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineControls.pause(config)));
        else if(args.length==2&&args[0].equals("status"))try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){journal.verifyIdentity(config.fingerprint(),config.subscribers());System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(journal.status()));}
        else if(args.length==3&&args[0].equals("resume-pipeline"))try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){journal.verifyIdentity(config.fingerprint(),config.subscribers());journal.resumePipeline(Long.parseLong(args[2]));System.out.println("PIPELINE_RESUMED_ORIGINAL_POSITION");}
        else if(args.length==4&&args[0].equals("resume"))try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){journal.verifyIdentity(config.fingerprint(),config.subscribers());journal.resume(args[2],Long.parseLong(args[3]));System.out.println("SUBSCRIPTION_RESUMED_ORIGINAL_TRANSACTION");}
        else if(args.length==4&&(args[0].equals("validate-stage")||args[0].equals("validate-data"))){
            Set<String> targets=new TreeSet<>(Arrays.asList(args[3].split(",",-1)));if(targets.contains("")||targets.size()!=args[3].split(",",-1).length)throw new IllegalArgumentException("VALIDATION_TARGET_INVALID");
            String id=PipelineReconciliation.stage(config,targets,Paths.get(args[2]),new ValidationStore.Limits(),30000,3600000);
            System.out.println("VALIDATION_SOURCE_SEALED "+id);
            if(args[0].equals("validate-data"))System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineReconciliation.compare(config,Paths.get(args[2]),id,600000)));
        }
        else if(args.length==4&&args[0].equals("validate-compare"))System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineReconciliation.compare(config,Paths.get(args[2]),args[3],600000)));
        else if(args.length==4&&args[0].equals("validation-report"))try(ValidationStore store=ValidationStore.open(Paths.get(args[2]),args[3])){
            if(!config.fingerprint().equals(store.manifest().certificate.fingerprint))throw new IllegalArgumentException("VALIDATION_CONFIG_MISMATCH");System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(store.summary()));
        }
        else if(args.length==6&&args[0].equals("repair-plan")){
            Set<String> scope=new TreeSet<>(Arrays.asList(args[4].split(",",-1)));if(scope.contains("")||scope.size()!=args[4].split(",",-1).length)throw new IllegalArgumentException("REPAIR_PLAN_SCOPE_INVALID");
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineRepair.plan(config,Paths.get(args[2]),args[3],scope,args[5])));
        }
        else if(args.length==8&&args[0].equals("repair-execute"))System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineRepair.execute(config,Paths.get(args[2]),args[3],args[4],args[5],args[6],args[7])));
        else if(args.length==3&&args[0].equals("preview-adoption"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineAdoptionPreview.inspect(config,args[2])));
        else if(args[0].equals("plan-adoption")&&(args.length==3||args.length==5&&args[3].equals("--expect-sha256")))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(
                    PipelineAdoptionPlan.inspect(config,args[2],args.length==5?args[4]:null)));
        else if(args.length==8&&args[0].equals("adopt-existing"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(
                    PipelineAdoptionPlan.execute(config,args[2],args[3],args[4],args[5],args[6],args[7].equals("--acknowledge-exclusive-freeze"))));
        else if(args.length==10&&args[0].equals("restore-adopted-unique"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(
                    PipelineAdoptionPlan.restoreUnique(config,args[2],new PostgresExistingAdoption.Approval(args[3],args[4],args[5],args[6]),
                            new PostgresAdoptedUnique.Approval(args[7],args[8]),args[9].equals("--acknowledge-exclusive-freeze"))));
        else if(args.length==11&&args[0].equals("prepare-target-writer"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(
                    PipelineAdoptionPlan.prepareWriter(config,args[2],new PostgresExistingAdoption.Approval(args[3],args[4],args[5],args[6]),
                            new PostgresWriterReadiness.Approval(args[7],args[8],args[9]),args[10].equals("--acknowledge-exclusive-freeze"))));
        else if(args.length==11&&(args[0].equals("activate-target-writer")||args[0].equals("failback-target-writer")))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(
                    PipelineAdoptionPlan.transitionWriter(config,args[2],new PostgresExistingAdoption.Approval(args[3],args[4],args[5],args[6]),
                            args[7],new PostgresWriterHandoff.Approval(args[8],args[9]),args[10].equals("--acknowledge-exclusive-freeze"),
                            args[0].equals("failback-target-writer"))));
        else if(args.length==5&&args[0].equals("review-unique"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineConstraintReview.inspect(config,Paths.get(args[2]),args[3],args[4])));
        else if(args[0].equals("plan-unique-restore")&&(args.length==7||args.length==9&&args[7].equals("--expect-sha256")))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineUniqueRestoration.plan(config,Paths.get(args[2]),args[3],args[4],args[5],args[6],args.length==9?args[8]:null)));
        else if(args.length==11&&args[0].equals("restore-unique"))
            System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineUniqueRestoration.execute(
                    config,Paths.get(args[2]),args[3],args[4],args[5],args[6],args[7],args[8],args[9],args[10].equals("--acknowledge-exclusive-freeze"))));
        else throw new IllegalArgumentException("V3_USAGE: run|check|status|pause|init-writer config.yml; transfer-writer candidate.yml previousAssignment journalGeneration expectedEpoch; review-source config.yml [--verify-contents | --retain-contents privateDirectory]; resume config.yml subscriptionId expectedErrorGeneration; resume-pipeline config.yml expectedErrorGeneration; validate-data|validate-stage config.yml privateReportsDirectory target1,target2; validate-compare|validation-report config.yml privateReportsDirectory reportId; repair-plan config.yml reports reportId target/tableId,... actor; repair-execute config.yml reports reportId planId approvedHash actor requestUUID; preview-adoption config.yml targetId; plan-adoption config.yml targetId [--expect-sha256 digest]; adopt-existing config.yml targetId tableName digest requestUUID actor --acknowledge-exclusive-freeze; restore-adopted-unique config.yml targetId tableName adoptionDigest adoptionRequest adoptionActor restoreRequest actor --acknowledge-exclusive-freeze; prepare-target-writer config.yml targetId tableName adoptionDigest adoptionRequest adoptionActor writerRole reviewRequest reviewActor --acknowledge-exclusive-freeze; activate-target-writer|failback-target-writer config.yml targetId tableName adoptionDigest adoptionRequest adoptionActor readinessReceiptSha256 requestUUID actor --acknowledge-exclusive-freeze; review-unique config.yml privateReportsDirectory reportId targetId; plan-unique-restore config.yml privateReportsDirectory reportId targetId tableId sourceIndex [--expect-sha256 digest]; restore-unique config.yml privateReportsDirectory reportId targetId tableId sourceIndex approvedDigest requestUUID actor --acknowledge-exclusive-freeze");
    }
}
