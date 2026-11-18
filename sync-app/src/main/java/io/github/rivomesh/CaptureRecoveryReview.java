package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.JdbcJournal;
import io.github.rivomesh.cdc.SourceAdapter;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Checks source admission at a failed capture checkpoint without changing the journal or source. */
public final class CaptureRecoveryReview {
    private CaptureRecoveryReview() {}

    public static Map<String,Object> inspect(PipelineConfig config)throws Exception{
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());
            Map<String,Object> before=journal.status();
            String phase=(String)before.get("errorStage");
            if(!"ERROR".equals(before.get("run_state"))||phase==null||
                    !Arrays.asList("SOURCE_CONNECT","SOURCE_READ","SOURCE_DECODE","SOURCE_SNAPSHOT","SOURCE_DISCOVERY","DDL","NORMALIZE").contains(phase))
                throw new IllegalStateException("SOURCE_REVIEW_REQUIRES_CAPTURE_ERROR");
            SourcePosition checkpoint=journal.captured();
            Map<String,Object> nativeEvidence=new LinkedHashMap<>();
            String admission=checkpoint==null?"SOURCE_POSITION_UNAVAILABLE":
                    probe(config,(String)before.get("generation"),checkpoint,journal,phase,nativeEvidence);

            Map<String,Object> after=journal.status();
            for(String key:Arrays.asList("generation","epoch","run_state","errorGeneration","errorCode","lastSequence")){
                if(!Objects.equals(before.get(key),after.get(key)))
                    throw new IllegalStateException("SOURCE_REVIEW_STALE");
            }
            if(!Objects.equals(checkpoint,journal.captured()))throw new IllegalStateException("SOURCE_REVIEW_STALE");

            Map<String,Object> report=new LinkedHashMap<>();
            report.put("taskId",config.pipelineId);
            report.put("sourceType",config.effectiveSourceType());
            report.put("errorGeneration",before.get("errorGeneration"));
            report.put("phase",phase);
            report.put("failureCode",before.get("errorCode"));
            report.put("lastCapturedSequence",before.get("lastSequence").toString());
            if(checkpoint!=null)report.put("checkpointHash",Codec.sha256(Codec.encode(checkpoint)));
            report.put("sourceAdmission",admission);
            report.put("affectedSubscriptions",config.subscribers());
            report.put("scope","UNPROVEN_SOURCE_RANGE");
            report.put("actions",actions(admission,phase));
            report.putAll(nativeEvidence);
            return report;
        }
    }

    private static String probe(PipelineConfig config,String generation,SourcePosition checkpoint,JdbcJournal journal,
                                String phase,Map<String,Object> nativeEvidence)throws Exception{
        // Read journal dependencies before the source probe so storage errors cannot look like source failures.
        List<SourcePosition> dependencies=journal.dependencies();
        try(SourceAdapter source=PipelineSources.open(config,generation)){
            source.validateFailover(checkpoint,dependencies);
            if(source instanceof MysqlCdcSource&&"SOURCE_DECODE".equals(phase)){
                try{
                    Map<String,Object> boundary=((MysqlCdcSource)source).inspectDecodeBoundary(checkpoint);
                    if(boundary!=null)nativeEvidence.put("nextTransactionHeaders",boundary);
                }catch(java.sql.SQLException unavailable){
                    // A missing, rotated or unreadable source file leaves only the admission result.
                }
            }
            return "SOURCE_CHECKPOINT_AVAILABLE";
        }catch(InterruptedException interrupted){
            Thread.currentThread().interrupt();
            throw interrupted;
        }catch(Exception failure){
            return PipelineEngine.sourceErrorCode(failure);
        }
    }

    private static List<String> actions(String admission,String phase){
        if("SOURCE_POSITION_UNAVAILABLE".equals(admission))
            return Arrays.asList("PRESERVE_SOURCE_LOGS","REPAIR_CAPTURE_CAUSE","REVIEW_NEW_BASELINE");
        if("SOURCE_CHECKPOINT_AVAILABLE".equals(admission)){
            if("SOURCE_CONNECT".equals(phase)||"SOURCE_READ".equals(phase))
                return Arrays.asList("PRESERVE_SOURCE_LOGS","RETRY_ORIGINAL_CHECKPOINT");
            return Arrays.asList("PRESERVE_SOURCE_LOGS","REPAIR_CAPTURE_CAUSE","RETRY_ORIGINAL_CHECKPOINT");
        }
        return Arrays.asList("PRESERVE_SOURCE_LOGS","RESTORE_LOGS_OR_SELECT_ELIGIBLE_SOURCE","REVIEW_NEW_BASELINE");
    }
}
