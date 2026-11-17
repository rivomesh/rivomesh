package io.github.rivomesh.repair;

import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.validation.ValidationStore;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.*;

/** Plans contain hashes/counts, never raw business values. Caller holds the report writer lock. */
public final class RepairPlans {
    private RepairPlans(){}
    public static RepairPlan create(ValidationStore store,Set<String> selected,String actor)throws Exception{
        return create(store,selected,actor,UUID.randomUUID().toString(),Instant.now().toString());
    }
    public static RepairPlan create(ValidationStore store,Set<String> selected,String actor,String id,String createdAt)throws Exception{
        ValidationStore.Manifest report=store.manifest();
        if(!"COMPLETE".equals(report.state)||selected==null||selected.isEmpty()||actor==null||actor.isEmpty()||actor.length()>128)throw new IllegalArgumentException("REPAIR_PLAN_SCOPE_INVALID");
        store.verifySource();RepairPlan plan=new RepairPlan();plan.id=id;plan.reportId=report.certificate.id;plan.createdAt=createdAt;plan.createdBy=actor;plan.sourceSeal=report.sourceSeal;plan.certificate=report.certificate;
        for(String key:new TreeSet<>(selected)){
            ValidationStore.TableResult result=report.results.get(key);if(result==null||result.matches())throw new IllegalArgumentException("REPAIR_PLAN_SCOPE_INVALID");store.verifyTarget(result.target,result.tableId);
            RepairPlan.Table table=new RepairPlan.Table();table.target=result.target;table.tableId=result.tableId;table.schemaHash=report.certificate.schemas.get(result.tableId);table.beforeRoot=result.targetRoot;table.afterRoot=result.sourceRoot;table.beforeRows=result.targetRows;table.afterRows=result.sourceRows;table.missing=result.missing;table.extra=result.extra;table.changed=result.changed;plan.tables.put(key,table);
        }
        plan.hash=plan.contentHash();plan.validate();return plan;
    }
    public static void verifyReport(RepairPlan plan,ValidationStore store)throws Exception{
        plan.validate();ValidationStore.Manifest report=store.manifest();
        if(!"COMPLETE".equals(report.state)||!Objects.equals(plan.sourceSeal,report.sourceSeal)||!certificateHash(plan.certificate).equals(certificateHash(report.certificate)))throw new IllegalStateException("REPAIR_REPORT_CHANGED");
        store.verifySource();for(RepairPlan.Table table:plan.tables.values()){
            ValidationStore.TableResult result=report.results.get(table.target+"/"+table.tableId);
            if(result==null||!Objects.equals(result.sourceRoot,table.afterRoot)||!Objects.equals(result.targetRoot,table.beforeRoot)||result.sourceRows!=table.afterRows||result.targetRows!=table.beforeRows||result.missing!=table.missing||result.extra!=table.extra||result.changed!=table.changed)throw new IllegalStateException("REPAIR_REPORT_CHANGED");
            store.verifyTarget(table.target,table.tableId);
        }
    }
    private static String certificateHash(io.github.rivomesh.PipelineValidationBarrier.Certificate cut){
        io.github.rivomesh.PipelineValidationBarrier.Certificate copy=Codec.decode(Codec.encode(cut),io.github.rivomesh.PipelineValidationBarrier.Certificate.class);copy.selectedTargets=new TreeSet<>(copy.selectedTargets);return Codec.sha256(Codec.encode(copy));
    }
    private static Path directory(Path reports,String reportId)throws Exception{
        if(!UUID.fromString(reportId).toString().equals(reportId))throw new IllegalArgumentException("REPAIR_REPORT_ID_INVALID");
        Path report=reports.toAbsolutePath().normalize().resolve(reportId);if(Files.isSymbolicLink(report)||!Files.isDirectory(report))throw new IllegalArgumentException("REPAIR_REPORT_PATH_INVALID");
        Path dir=report.resolve("repair-plans");if(Files.isSymbolicLink(dir))throw new IllegalArgumentException("REPAIR_PLAN_PATH_INVALID");Files.createDirectories(dir);try{Files.setPosixFilePermissions(dir,PosixFilePermissions.fromString("rwx------"));}catch(UnsupportedOperationException ignored){}return dir;
    }
    public static void save(Path reports,RepairPlan plan)throws Exception{
        plan.validate();byte[] content=Codec.encode(plan).getBytes(java.nio.charset.StandardCharsets.UTF_8);if(content.length>8*1024*1024)throw new IllegalArgumentException("REPAIR_PLAN_TOO_LARGE");
        Path dir=directory(reports,plan.reportId),file=dir.resolve(plan.id+".json");
        if(Files.exists(file)){RepairPlan old=load(reports,plan.reportId,plan.id);if(!old.hash.equals(plan.hash))throw new IllegalStateException("REPAIR_PLAN_CONFLICT");return;}
        Path temp=Files.createTempFile(dir,".plan-",".tmp");try{
            try{Files.setPosixFilePermissions(temp,PosixFilePermissions.fromString("rw-------"));}catch(UnsupportedOperationException ignored){}
            try(java.nio.channels.FileChannel channel=java.nio.channels.FileChannel.open(temp,StandardOpenOption.WRITE)){java.nio.ByteBuffer buffer=java.nio.ByteBuffer.wrap(content);while(buffer.hasRemaining())channel.write(buffer);channel.force(true);}
            Files.move(temp,file,StandardCopyOption.ATOMIC_MOVE);
            try(java.nio.channels.FileChannel channel=java.nio.channels.FileChannel.open(dir,StandardOpenOption.READ)){channel.force(true);}
        }finally{Files.deleteIfExists(temp);}
    }
    public static RepairPlan load(Path reports,String reportId,String id)throws Exception{
        if(!UUID.fromString(id).toString().equals(id))throw new IllegalArgumentException("REPAIR_PLAN_ID_INVALID");Path file=directory(reports,reportId).resolve(id+".json");
        if(Files.isSymbolicLink(file)||Files.size(file)>8*1024*1024)throw new IllegalArgumentException("REPAIR_PLAN_PATH_OR_SIZE_INVALID");RepairPlan plan=Codec.decode(new String(Files.readAllBytes(file),java.nio.charset.StandardCharsets.UTF_8),RepairPlan.class);plan.validate();if(!id.equals(plan.id)||!reportId.equals(plan.reportId))throw new IllegalStateException("REPAIR_PLAN_ID_MISMATCH");return plan;
    }
}
