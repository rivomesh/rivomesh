package io.github.rivomesh.recovery;

import io.github.rivomesh.PipelineValidationBarrier;
import io.github.rivomesh.cdc.*;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/** Review scope only. Historical MATCH results never close a gap without a fresh fenced scan. */
public final class GapVerificationPlan {
    public int version=1;
    public String action="VERIFY_GAPS_AT_COMMON_CUT",id,createdAt,createdBy,reason;
    public String pipelineId,subscriptionId,fingerprint,generation,sourceType,sourceId;
    public String reportId,certificateHash,sourceSeal,checkpointHash;
    public long epoch,sequence,errorGeneration;
    public long maximumMillis=300000;
    public Map<String,Gap> gaps=new TreeMap<>();
    public Map<String,Table> tables=new TreeMap<>();
    public static final class Gap {public String planHash,receiptHash;public long sequence;}
    public static final class Table {public String database,name,schemaHash,sourceRoot;public int schemaVersion;public long rows;}

    public void validate(){
        if(version!=1||!"VERIFY_GAPS_AT_COMMON_CUT".equals(action)||id==null||!id.matches("[A-Za-z0-9_-]{8,128}")||createdBy==null||createdBy.isEmpty()||createdBy.length()>128||reason==null||reason.trim().length()<8||reason.length()>1000)throw invalid();
        try{Instant.parse(createdAt);FailureEvidence.uuid(reportId);}catch(Exception malformed){throw invalid();}
        if(pipelineId==null||!pipelineId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||subscriptionId==null||!subscriptionId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||generation==null||generation.isEmpty()||sourceType==null||sourceId==null||sourceId.isEmpty()||epoch<1||sequence<1||errorGeneration<0||maximumMillis<1000||maximumMillis>86400000)throw invalid();
        for(String hash:Arrays.asList(fingerprint,certificateHash,sourceSeal,checkpointHash))if(!FailureEvidence.hash(hash))throw invalid();
        if(gaps==null||gaps.isEmpty()||gaps.size()>100||tables==null||tables.isEmpty()||tables.size()>1000)throw invalid();
        Set<Long> sequences=new HashSet<>();
        for(Map.Entry<String,Gap> entry:gaps.entrySet()){Gap gap=entry.getValue();if(entry.getKey()==null||!entry.getKey().matches("[A-Za-z0-9_-]{8,128}")||gap==null||!FailureEvidence.hash(gap.planHash)||!FailureEvidence.hash(gap.receiptHash)||gap.sequence<1||gap.sequence>sequence||!sequences.add(gap.sequence))throw invalid();}
        for(Map.Entry<String,Table> entry:tables.entrySet()){Table table=entry.getValue();if(!FailureEvidence.hash(entry.getKey())||table==null||!FailureEvidence.hash(table.schemaHash)||!FailureEvidence.hash(table.sourceRoot)||table.schemaVersion<1||table.rows<0)throw invalid();}
        if(Codec.encode(this).getBytes(StandardCharsets.UTF_8).length>512*1024)throw new IllegalArgumentException("GAP_VERIFICATION_SCOPE_TOO_LARGE");
    }
    public String hash(){validate();return Codec.sha256(Codec.encode(this));}

    public static GapVerificationPlan create(ValidationStore store,List<SkipPlan> originals,Map<String,SkipReceipt> receipts,String target,String request,String actor,String reason)throws Exception{
        ValidationStore.Manifest manifest=store.manifest();PipelineValidationBarrier.Certificate cut=manifest.certificate;
        if(cut==null||!manifest.sourceComplete||!"COMPLETE".equals(manifest.state)||!"VALIDATION_MATCH".equals(manifest.code)||manifest.targetTables==null||!manifest.targetTables.containsKey(target)||cut.selectedTargets==null||!cut.selectedTargets.contains(target)||cut.capturedPosition==null)throw new IllegalStateException("GAP_COMPLETE_MATCH_REQUIRED");
        if(originals==null||originals.isEmpty()||originals.size()>100||receipts==null||receipts.size()!=originals.size())throw new IllegalArgumentException("GAP_VERIFICATION_SCOPE_INVALID");
        SinkAdapter.Checkpoint checkpoint=cut.targetCheckpoints.get(target);
        if(checkpoint==null||checkpoint.epoch!=cut.epoch||checkpoint.sequence!=cut.sequence)throw new IllegalStateException("GAP_REPORT_CHECKPOINT_INVALID");
        GapVerificationPlan plan=new GapVerificationPlan();plan.id=request;plan.createdAt=Instant.now().toString();plan.createdBy=actor;plan.reason=reason;plan.subscriptionId=target;
        plan.reportId=cut.id;plan.certificateHash=Codec.sha256(Codec.encode(cut));plan.sourceSeal=manifest.sourceSeal;plan.checkpointHash=Codec.sha256(Codec.encode(checkpoint));plan.fingerprint=cut.fingerprint;plan.generation=cut.generation;plan.epoch=cut.epoch;plan.sequence=cut.sequence;plan.errorGeneration=cut.errorGeneration;plan.sourceType=cut.capturedPosition.getSourceType();plan.sourceId=cut.capturedPosition.getSourceId();
        store.verifySource();
        for(SkipPlan original:originals){
            original.validate();SkipReceipt receipt=receipts.get(original.id);if(receipt==null)throw new IllegalStateException("GAP_NATIVE_RECEIPT_REQUIRED");receipt.validate(original);
            if(receipt.executionRequest==null)throw new IllegalStateException("GAP_NATIVE_RECEIPT_REQUIRED");
            if(!target.equals(original.subscriptionId)||!plan.fingerprint.equals(original.fingerprint)||!plan.generation.equals(original.generation)||original.epoch>cut.epoch||original.sequence>cut.sequence||!plan.sourceType.equals(original.sourcePosition.getSourceType())||!plan.sourceId.equals(original.sourcePosition.getSourceId()))throw new IllegalStateException("GAP_REPORT_SCOPE_CHANGED");
            if(plan.pipelineId==null)plan.pipelineId=original.pipelineId;else if(!plan.pipelineId.equals(original.pipelineId))throw new IllegalStateException("GAP_REPORT_SCOPE_CHANGED");
            Gap gap=new Gap();gap.planHash=original.hash();gap.receiptHash=receipt.hash();gap.sequence=original.sequence;if(plan.gaps.put(original.id,gap)!=null)throw new IllegalArgumentException("GAP_VERIFICATION_SCOPE_INVALID");
            for(String reference:original.schemaVersions){
                String tableId=reference.substring(0,64);TableSchema schema=manifest.schemas.get(tableId);
                if(schema==null||!reference.equals(tableId+"/"+schema.version)||!manifest.targetTables.get(target).contains(tableId)||!Objects.equals(cut.schemas.get(tableId),Codec.sha256(Codec.encode(schema))))throw new IllegalStateException("GAP_REPORT_SCHEMA_CHANGED");
                ValidationStore.TableResult result=manifest.results.get(target+"/"+tableId);
                if(result==null||!result.matches()||!target.equals(result.target)||!tableId.equals(result.tableId)||result.sourceRows!=result.targetRows||!Objects.equals(result.sourceRoot,result.targetRoot)||!Objects.equals(result.sourceRoot,manifest.sourceRoots.get(tableId)))throw new IllegalStateException("GAP_COMPLETE_MATCH_REQUIRED");
                store.verifyTarget(target,tableId);Table table=new Table();table.database=schema.database;table.name=schema.name;table.schemaHash=cut.schemas.get(tableId);table.schemaVersion=schema.version;table.sourceRoot=result.sourceRoot;table.rows=result.sourceRows;plan.tables.put(tableId,table);
            }
        }
        plan.validate();return plan;
    }
    private static IllegalArgumentException invalid(){return new IllegalArgumentException("GAP_VERIFICATION_PLAN_INVALID");}
}
