package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import java.util.*;

/** Shared intent precedes target-local fence confirmation; unreachable targets remain explicitly unconfirmed. */
public final class PipelineControls {
    private PipelineControls(){}
    public static Map<String,Object> pause(PipelineConfig config)throws Exception{
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            Journal.PipelineState state=journal.pausePipeline();String generation=(String)journal.status().get("generation");Map<String,String> fences=new LinkedHashMap<>();
            for(PipelineConfig.Target target:config.targets){
                if(target.product.materialized()){try{new JdbcPublicationStore(journal,target.id,null).activate(state.epoch);fences.put(target.id,"UNCONFIRMED_IN_FLIGHT_MATERIALIZATION");}catch(Exception failure){fences.put(target.id,"UNCONFIRMED");}continue;}
                // A journal fence cannot revoke an already in-flight broker producer.
                if(target.product.messaging()){try{new JdbcPublicationStore(journal,target.id,null).activate(state.epoch);fences.put(target.id,"UNCONFIRMED_IN_FLIGHT_PUBLICATION");}catch(Exception failure){fences.put(target.id,"UNCONFIRMED");}continue;}
                try(JdbcSink sink=new JdbcSink(config.targetSettings(target,generation))){sink.activate(state.epoch);fences.put(target.id,"FENCED");}
                catch(Exception failure){fences.put(target.id,"UNCONFIRMED");}
            }
            Map<String,Object> result=new LinkedHashMap<>();result.put("pipeline",state);result.put("targetFences",fences);return result;
        }
    }
}
