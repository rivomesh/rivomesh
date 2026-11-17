package io.github.rivomesh.repair;

import io.github.rivomesh.PipelineValidationBarrier;
import io.github.rivomesh.cdc.Codec;
import java.util.*;

/** Immutable review content; execution progress and target receipts are stored separately. */
public final class RepairPlan {
    public static final class Table {
        public String target,tableId,schemaHash,beforeRoot,afterRoot;
        public long beforeRows,afterRows,missing,extra,changed;
        public long deletes(){return Math.addExact(extra,changed);}
        public long inserts(){return Math.addExact(missing,changed);}
    }
    public int version=1;
    public String id,reportId,createdAt,createdBy,sourceSeal,hash;
    public PipelineValidationBarrier.Certificate certificate;
    public Map<String,Table> tables=new TreeMap<>();
    public long maximumAffectedRows=100000,maximumProbeBytes=1024L*1024*1024,maximumMillis=600000;
    public int maximumRowBytes=4*1024*1024;
    public String contentHash(){
        RepairPlan copy=Codec.decode(Codec.encode(this),RepairPlan.class);copy.hash=null;if(copy.certificate!=null)copy.certificate.selectedTargets=new TreeSet<>(copy.certificate.selectedTargets);return Codec.sha256(Codec.encode(copy));
    }
    public void validate(){
        if(version!=1||id==null||!UUID.fromString(id).toString().equals(id)||reportId==null||!UUID.fromString(reportId).toString().equals(reportId)||createdBy==null||createdBy.isEmpty()||createdBy.length()>128||createdAt==null||certificate==null||certificate.epoch<1||certificate.sequence<0||certificate.errorGeneration<0||certificate.fingerprint==null||!certificate.fingerprint.matches("[0-9a-f]{64}")||certificate.generation==null||certificate.generation.isEmpty()||certificate.selectedTargets==null||certificate.schemas==null||certificate.targetCheckpoints==null||!certificate.targetCheckpoints.keySet().equals(certificate.selectedTargets)||!reportId.equals(certificate.id)||sourceSeal==null||!sourceSeal.matches("[0-9a-f]{64}")||tables==null||tables.isEmpty()||maximumAffectedRows<1||maximumProbeBytes<1024*1024||maximumMillis<1000||maximumMillis>86400000||maximumRowBytes<1024||maximumRowBytes>32*1024*1024||maximumRowBytes>maximumProbeBytes/4)throw new IllegalArgumentException("REPAIR_PLAN_INVALID");
        java.time.Instant.parse(createdAt);
        long affected=0;for(Map.Entry<String,Table> entry:tables.entrySet()){
            Table table=entry.getValue();if(table==null||!entry.getKey().equals(table.target+"/"+table.tableId)||!certificate.selectedTargets.contains(table.target)||!Objects.equals(certificate.schemas.get(table.tableId),table.schemaHash)||table.beforeRoot==null||!table.beforeRoot.matches("[0-9a-f]{64}")||table.afterRoot==null||!table.afterRoot.matches("[0-9a-f]{64}")||table.missing<0||table.extra<0||table.changed<0||table.beforeRows<0||table.afterRows<0||table.extra>table.beforeRows||table.missing>table.afterRows||table.changed>table.beforeRows-table.extra||table.changed>table.afterRows-table.missing||certificate.targetCheckpoints.get(table.target)==null||certificate.targetCheckpoints.get(table.target).epoch!=certificate.epoch||certificate.targetCheckpoints.get(table.target).sequence!=certificate.sequence)throw new IllegalArgumentException("REPAIR_PLAN_INVALID");
            if(Math.addExact(table.beforeRows,Math.subtractExact(table.missing,table.extra))!=table.afterRows)throw new IllegalArgumentException("REPAIR_PLAN_COUNTS_INVALID");affected=Math.addExact(affected,Math.addExact(Math.addExact(table.missing,table.extra),table.changed));
        }
        if(affected<1||affected>maximumAffectedRows||hash==null||!hash.equals(contentHash()))throw new IllegalArgumentException("REPAIR_PLAN_HASH_OR_LIMIT_INVALID");
    }
}
