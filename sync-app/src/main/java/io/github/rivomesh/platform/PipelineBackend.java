package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.*;
import java.sql.SQLException;
import java.util.*;

final class PipelineBackend {
    private PipelineBackend(){}
    static Map<String,Object> status(PipelineConfig c)throws Exception{
        try(JdbcJournal journal=new JdbcJournal(c.existingJournalSettings())){
            try{journal.verifyIdentity(c.fingerprint(),c.subscribers());}
            catch(SQLException missing){
                // The connection succeeded; a qualified metadata read can still report an absent MySQL database.
                // Do not swallow connection/default-catalog errors or the broad 42000 SQLState.
                if("EXTERNAL".equals(c.journal.mode)&&c.journal.product==TargetProduct.MYSQL&&missing.getErrorCode()==1049&&"42000".equals(missing.getSQLState()))return Collections.emptyMap();throw missing;
            }
            Map<String,Object> status=journal.status(),preview=journal.snapshotSummary(100);status.putAll(preview);
            List<Map<String,Object>> subscriptions=journal.subscriptionDiagnostics();long minimum=((Number)status.get("lastSequence")).longValue();for(Map<String,Object> sub:subscriptions)minimum=Math.min(minimum,((Number)sub.get("acknowledged")).longValue());
            status.put("pending_transactions",((Number)status.get("lastSequence")).longValue()-minimum);status.put("applied_sequence",minimum);status.put("node_id",status.get("owner"));status.put("error_code",status.get("errorCode"));status.put("error_count",status.get("errorGeneration"));
            long totalSnapshots=((Number)preview.get("table_count")).longValue();@SuppressWarnings("unchecked")Map<String,Long> snapshotAcks=(Map<String,Long>)preview.get("snapshotAcknowledgments");boolean targetsReady=true;for(Map<String,Object> sub:subscriptions)if(snapshotAcks.getOrDefault((String)sub.get("id"),0L)<totalSnapshots)targetsReady=false;
            status.put("phase",totalSnapshots==0?"DISCOVERY":!preview.get("table_count").equals(preview.get("completed_tables"))?"SNAPSHOT":!targetsReady?"TARGET_SNAPSHOT":((Number)status.get("pending_transactions")).longValue()>0?"CATCHUP":"STREAMING");
            SourcePosition position=journal.captured();if(position!=null){status.put("sourcePosition",position);status.put("capture_gtid",position.getPayload().get("gtidSet"));}
            status.put("subscriptions",subscriptions);status.put("maxBytes",c.journal.maxBytes);return status;
        }catch(IllegalStateException e){if("JOURNAL_NOT_INITIALIZED".equals(e.getMessage()))return Collections.emptyMap();throw e;}
        catch(SQLException e){if(Arrays.asList("42P01","3F000","3F001","42S02","90079").contains(e.getSQLState()))return Collections.emptyMap();throw e;}
    }
    static Map<String,Object> pause(PipelineConfig c)throws Exception{return PipelineControls.pause(c);}
    static void resume(PipelineConfig c,long generation)throws Exception{try(JdbcJournal journal=new JdbcJournal(c.existingJournalSettings())){journal.verifyIdentity(c.fingerprint(),c.subscribers());journal.resumePipeline(generation);}}
    static void subscription(PipelineConfig c,String id,String action,long generation)throws Exception{
        if(c.targets.stream().noneMatch(target->target.id.equals(id)))throw new IllegalArgumentException("SUBSCRIPTION_NOT_FOUND");
        try(JdbcJournal journal=new JdbcJournal(c.existingJournalSettings())){
            journal.verifyIdentity(c.fingerprint(),c.subscribers());
            if("RESUME_TARGET".equals(action))journal.resume(id,generation);
            else throw new IllegalArgumentException("SUBSCRIPTION_ACTION_INVALID");
        }
    }
}
