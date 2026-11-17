package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import java.nio.file.Paths;
import java.util.*;
import io.github.rivomesh.validation.ValidationStore;

/** CLI dispatch kept separate so legacy v2 commands retain their exact semantics. */
final class PipelineMain {
    private PipelineMain(){}
    static void execute(String[] args)throws Exception{
        PipelineConfig config=PipelineConfig.load(Paths.get(args[1]));
        if(args.length==2&&args[0].equals("run")){
            try(PipelineEngine engine=new PipelineEngine(config)){
                Thread hook=new Thread(engine::close,"v3-shutdown");Runtime.getRuntime().addShutdownHook(hook);
                try{engine.run();}finally{try{Runtime.getRuntime().removeShutdownHook(hook);}catch(IllegalStateException ignored){/* shutdown */}}
            }
        }else if(args.length==2&&args[0].equals("check"))System.out.println(Codec.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(PipelineEngine.check(config)));
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
        else throw new IllegalArgumentException("V3_USAGE: run|check|status|pause config.yml; resume config.yml subscriptionId expectedErrorGeneration; resume-pipeline config.yml expectedErrorGeneration; validate-data|validate-stage config.yml privateReportsDirectory target1,target2; validate-compare|validation-report config.yml privateReportsDirectory reportId; repair-plan config.yml reports reportId target/tableId,... actor; repair-execute config.yml reports reportId planId approvedHash actor requestUUID");
    }
}
