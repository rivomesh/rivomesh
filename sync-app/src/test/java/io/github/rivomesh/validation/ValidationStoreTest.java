package io.github.rivomesh.validation;

import io.github.rivomesh.*;
import io.github.rivomesh.cdc.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ValidationStoreTest {
    @TempDir Path root;
    TableSchema table(boolean keyless){TableSchema t=new TableSchema();t.id=TableSchema.identity("source","","rows");t.database="source";t.name="rows";for(String name:Arrays.asList("id","note","json")){TableSchema.Column c=new TableSchema.Column();c.name=name;c.type=name.equals("id")?TableSchema.Type.INT64:name.equals("json")?TableSchema.Type.JSON:TableSchema.Type.STRING;c.nullable=!name.equals("id");t.columns.add(c);}if(!keyless)t.primaryKey.add("id");return t;}
    List<Cell> row(int id,String note,String json){return Arrays.asList(Cell.value(""+id),Cell.value(note),Cell.value(json));}
    PipelineValidationBarrier.Certificate certificate(TableSchema t){PipelineValidationBarrier.Certificate c=new PipelineValidationBarrier.Certificate();c.id=UUID.randomUUID().toString();c.generation="generation";c.fingerprint=Codec.sha256("config");c.epoch=2;c.sequence=5;c.selectedTargets.add("target");c.schemas.put(t.id,Codec.sha256(Codec.encode(t)));return c;}
    ValidationStore create(TableSchema t,PipelineValidationBarrier.Certificate c,ValidationStore.Limits limits)throws Exception{return ValidationStore.create(root,c.id,c,Collections.singletonList(t),Collections.singletonMap("target",Collections.singleton(t.id)),limits);}
    JdbcJournal.IdentityRow identity(String id,TableSchema t,List<Cell> data){JdbcJournal.IdentityRow r=new JdbcJournal.IdentityRow();r.rowId=Codec.sha256(id);r.payload=RowValues.canonical(t,data);r.hash=Codec.sha256(r.payload);return r;}
    @Test void boundedDetailsRequireAnActualDifferenceAndRejectForeignScopes()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate c=certificate(t);
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){
            store.source(t.id,row(1,"same",null));store.source(t.id,row(2,"before",null));store.finishSourceTable(t.id);store.completeSource();store.target("target",t.id,null,row(1,"same",null));store.target("target",t.id,null,row(2,"after",null));store.finishTarget("target",t.id);store.complete();
            String changed=store.differences("target",t.id,"",1).get(0).identityHash;ValidationStore.RowPair pair=store.differenceValues("target",t.id,changed,1024);assertEquals("before",pair.expected.get(1).value);assertEquals("after",pair.actual.get(1).value);
            String unchanged=Codec.sha256(RowValues.identity(t,row(1,"same",null),null));assertEquals("VALIDATION_DIFFERENCE_NOT_FOUND",assertThrows(SyncFailure.class,()->store.differenceValues("target",t.id,unchanged,1024)).code);
            assertEquals("VALIDATION_DIFF_PAGE_INVALID",assertThrows(SyncFailure.class,()->store.differences("foreign",t.id,"",1)).code);assertEquals("VALIDATION_DIFF_PAGE_INVALID",assertThrows(SyncFailure.class,()->store.differences("target",t.id,"invalid",1)).code);
        }
    }
    @Test void largeRawRowsAreRefusedBeforeDetailsAreReturned()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate c=certificate(t);String large=String.join("",Collections.nCopies(4000,"甲"));
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){
            store.source(t.id,row(1,large,null));store.finishSourceTable(t.id);store.completeSource();store.target("target",t.id,null,row(1,"changed",null));store.finishTarget("target",t.id);store.complete();String key=store.differences("target",t.id,"",1).get(0).identityHash;
            assertEquals("VALIDATION_DETAIL_TOO_LARGE",assertThrows(SyncFailure.class,()->store.differenceValues("target",t.id,key,1024)).code);assertEquals(large,store.differenceValues("target",t.id,key,32768).expected.get(1).value);
        }
    }
    @Test void corruptDifferenceHashesCannotReleaseRawRows()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate c=certificate(t);String key;
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){store.source(t.id,row(1,"before",null));store.finishSourceTable(t.id);store.completeSource();store.target("target",t.id,null,row(1,"after",null));store.finishTarget("target",t.id);store.complete();key=store.differences("target",t.id,"",1).get(0).identityHash;}
        try(Connection db=JdbcConnections.open("org.h2.Driver","jdbc:h2:file:"+root.resolve(c.id).resolve("rows")+";IFEXISTS=TRUE","","");Statement sql=db.createStatement()){sql.executeUpdate("UPDATE rmv_diff SET expected_hash='corrupt'");}
        try(ValidationStore store=ValidationStore.open(root,c.id)){assertEquals("VALIDATION_DIFF_CORRUPT",assertThrows(SyncFailure.class,()->store.differenceValues("target",t.id,key,4096)).code);}
    }
    @Test void persistedDifferencesAreKeysetPagedAndRetainBothBeforeAndExpectedRows()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate cert=certificate(t);
        try(ValidationStore store=create(t,cert,new ValidationStore.Limits())){
            store.source(t.id,row(1,"same","{\"a\":1.00,\"b\":2}"));store.source(t.id,row(2,"missing",null));store.source(t.id,row(3,"before",null));store.finishSourceTable(t.id);store.completeSource();
            store.target("target",t.id,null,row(1,"same","{\"b\":2,\"a\":1}"));store.target("target",t.id,null,row(3,"after",null));store.target("target",t.id,null,row(4,"extra",null));
            ValidationStore.TableResult result=store.finishTarget("target",t.id);assertEquals(1,result.missing);assertEquals(1,result.extra);assertEquals(1,result.changed);assertFalse(result.matches());store.complete();
            assertFalse(Codec.encode(store.summary()).contains("before"));assertFalse(Codec.encode(store.summary()).contains("\":\"missing\""));
        }
        try(ValidationStore store=ValidationStore.open(root,cert.id)){
            assertEquals("COMPLETE",store.manifest().state);assertEquals("VALIDATION_DIFFERENCES",store.manifest().code);Set<String> kinds=new HashSet<>();String after="";
            while(true){List<ValidationStore.Difference> page=store.differences("target",t.id,after,1);if(page.isEmpty())break;assertEquals(1,page.size());ValidationStore.Difference diff=page.get(0);assertTrue(diff.identityHash.compareTo(after)>0);after=diff.identityHash;kinds.add(diff.kind);ValidationStore.RowPair values=store.values("target",t.id,diff.identityHash);
                if(diff.kind.equals("MISSING")){assertNotNull(values.expected);assertNull(values.actual);}else if(diff.kind.equals("EXTRA")){assertNull(values.expected);assertNotNull(values.actual);}else{assertEquals("before",values.expected.get(1).value);assertEquals("after",values.actual.get(1).value);}
            }
            assertEquals(new HashSet<>(Arrays.asList("MISSING","EXTRA","CHANGED")),kinds);
        }
    }
    @Test void sameMultisetWithDifferentOccurrenceIdsIsNotAValidKeylessReplica()throws Exception{
        TableSchema t=table(true);List<Cell> data=row(7,"same","{\"x\":true}");PipelineValidationBarrier.Certificate cert=certificate(t);JdbcJournal.IdentityRow a=identity("a",t,data),b=identity("b",t,data);
        try(ValidationStore store=create(t,cert,new ValidationStore.Limits())){
            store.identity(t.id,a);store.identity(t.id,b);store.source(t.id,data);store.source(t.id,data);store.finishSourceTable(t.id);store.completeSource();
            store.target("target",t.id,a.rowId,data);store.target("target",t.id,Codec.sha256("wrong-id"),data);ValidationStore.TableResult r=store.finishTarget("target",t.id);assertEquals(2,r.sourceRows);assertEquals(2,r.targetRows);assertEquals(1,r.missing);assertEquals(1,r.extra);store.complete();assertEquals("VALIDATION_DIFFERENCES",store.manifest().code);
        }
    }
    @Test void incompleteSourceCannotBeContinuedAsANewMvccView()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate cert=certificate(t);
        try(ValidationStore store=create(t,cert,new ValidationStore.Limits())){store.source(t.id,row(1,"partial",null));store.flush();}
        try(ValidationStore store=ValidationStore.open(root,cert.id)){
            assertFalse(store.manifest().sourceComplete);assertEquals("INTERRUPTED_SOURCE",store.manifest().state);
            assertEquals("VALIDATION_SOURCE_STATE_INVALID",assertThrows(SyncFailure.class,()->store.source(t.id,row(2,"new-view",null))).code);
            assertEquals("VALIDATION_SOURCE_SCAN_INCOMPLETE",assertThrows(SyncFailure.class,store::completeSource).code);
        }
    }
    @Test void aSealedSourceSurvivesRestartAndDoesNotNeedALiveSourceConnection()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate cert=certificate(t);
        try(ValidationStore store=create(t,cert,new ValidationStore.Limits())){store.source(t.id,row(1,"raw-source","{\"x\":1.00}"));store.finishSourceTable(t.id);store.completeSource();}
        try(ValidationStore store=ValidationStore.open(root,cert.id)){assertTrue(store.manifest().sourceComplete);assertEquals("SOURCE_READY",store.manifest().state);store.target("target",t.id,null,row(1,"raw-source","{\"x\":1}"));assertTrue(store.finishTarget("target",t.id).matches());store.complete();assertEquals("VALIDATION_MATCH",store.manifest().code);}
    }
    @Test void missingJournalOccurrenceAndCorruptPayloadAreRejected()throws Exception{
        TableSchema t=table(true);List<Cell> data=row(7,"same",null);PipelineValidationBarrier.Certificate c=certificate(t);
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){assertEquals("VALIDATION_SOURCE_IDENTITY_MISMATCH",assertThrows(SyncFailure.class,()->store.source(t.id,data)).code);}
        PipelineValidationBarrier.Certificate other=certificate(t);try(ValidationStore store=create(t,other,new ValidationStore.Limits())){JdbcJournal.IdentityRow row=identity("a",t,data);row.hash=Codec.sha256("corrupt");assertEquals("ROW_IDENTITY_CHECKSUM_MISMATCH",assertThrows(SyncFailure.class,()->store.identity(t.id,row)).code);}
    }
    @Test void quotaFailureCannotPublishACompleteReport()throws Exception{
        TableSchema t=table(false);ValidationStore.Limits limits=new ValidationStore.Limits();limits.maximumRows=1;PipelineValidationBarrier.Certificate cert=certificate(t);
        try(ValidationStore store=create(t,cert,limits)){store.source(t.id,row(1,"allowed",null));assertEquals("VALIDATION_STORAGE_QUOTA_EXCEEDED",assertThrows(SyncFailure.class,()->store.source(t.id,row(2,"over-budget",null))).code);store.failed("VALIDATION_STORAGE_QUOTA_EXCEEDED");}
        try(ValidationStore store=ValidationStore.open(root,cert.id)){assertEquals("FAILED",store.manifest().state);assertEquals(0,store.manifest().rows);assertThrows(SyncFailure.class,store::complete);}
    }
    @Test void anActiveReportRejectsASecondWriter()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate cert=certificate(t);try(ValidationStore store=create(t,cert,new ValidationStore.Limits())){assertEquals("VALIDATION_REPORT_BUSY",assertThrows(SyncFailure.class,()->ValidationStore.open(root,cert.id)).code);}
    }
    @Test void retainedReportCatalogPagesWithoutOpeningTheReportDatabases()throws Exception{
        TableSchema table=table(false);
        PipelineValidationBarrier.Certificate first=certificate(table),second=certificate(table);
        try(ValidationStore open=create(table,first,new ValidationStore.Limits())){
            try(ValidationStore other=create(table,second,new ValidationStore.Limits())){
                Map<String,Object> a=ValidationStore.catalog(root,"",1);
                String idA=Codec.JSON.valueToTree(a).path("reports").get(0).path("id").asText();
                assertNotNull(a.get("nextCursor"));
                Map<String,Object> b=ValidationStore.catalog(root,(String)a.get("nextCursor"),1);
                String idB=Codec.JSON.valueToTree(b).path("reports").get(0).path("id").asText();
                assertEquals(Set.of(first.id,second.id),Set.of(idA,idB));
                assertNull(b.get("nextCursor"));
                assertEquals("SCANNING_SOURCE",open.manifest().state);
                assertEquals("SCANNING_SOURCE",other.manifest().state);
            }
        }
        assertThrows(IllegalArgumentException.class,()->ValidationStore.catalog(root,"../reports",20));
    }
    @Test void reopeningAReportRemovesAnInterruptedProjection()throws Exception{
        TableSchema table=table(false);
        PipelineValidationBarrier.Certificate certificate=certificate(table);
        try(ValidationStore report=create(table,certificate,new ValidationStore.Limits())){
            report.source(table.id,row(1,"present",null));
            report.finishSourceTable(table.id);
            report.completeSource();
        }
        Path directory=root.resolve(certificate.id);
        Path abandoned=Files.writeString(directory.resolve("summary-123.tmp"),"interrupted write");
        Path other=Files.writeString(directory.resolve("operator-notes.tmp"),"keep");
        try(ValidationStore report=ValidationStore.open(root,certificate.id)){
            assertTrue(report.manifest().sourceComplete);
            assertFalse(Files.exists(abandoned));
            assertEquals("keep",Files.readString(other));
        }
    }
    @Test void targetRetryDiscardsPartialTargetsAndRetainsTheSealedSource()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate c=certificate(t);String seal;
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){store.source(t.id,row(1,"expected",null));store.finishSourceTable(t.id);store.completeSource();seal=store.manifest().sourceSeal;store.target("target",t.id,null,row(1,"partial-target",null));store.flush();}
        try(ValidationStore store=ValidationStore.open(root,c.id)){store.restartTargets();assertEquals(seal,store.manifest().sourceSeal);assertEquals(1,store.manifest().rows);store.target("target",t.id,null,row(1,"expected",null));assertTrue(store.finishTarget("target",t.id).matches());store.complete();assertThrows(SyncFailure.class,store::restartTargets);}
    }
    @Test void completedSourceTablesCannotImportAdditionalOccurrenceIds()throws Exception{
        TableSchema t=table(true);List<Cell> data=row(7,"same",null);PipelineValidationBarrier.Certificate c=certificate(t);JdbcJournal.IdentityRow identity=identity("a",t,data);
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){store.identity(t.id,identity);store.source(t.id,data);store.finishSourceTable(t.id);assertEquals("VALIDATION_SOURCE_STATE_INVALID",assertThrows(SyncFailure.class,()->store.identity(t.id,identity)).code);store.completeSource();}
    }
    @Test void reportPrivacyDoesNotChangeAnExistingParentDirectoryPermissions()throws Exception{
        if(!Files.getFileStore(root).supportsFileAttributeView("posix"))return;
        java.util.Set<java.nio.file.attribute.PosixFilePermission> previous=java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x");Files.setPosixFilePermissions(root,previous);
        TableSchema t=table(false);PipelineValidationBarrier.Certificate c=certificate(t);try(ValidationStore store=create(t,c,new ValidationStore.Limits())){assertEquals(previous,Files.getPosixFilePermissions(root));assertEquals(java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"),Files.getPosixFilePermissions(root.resolve(c.id)));}
    }
    @Test void rawPayloadCorruptionIsDetectedBeforeReturningRepairValues()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate c=certificate(t);String key;
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){store.source(t.id,row(1,"before",null));store.finishSourceTable(t.id);store.completeSource();store.target("target",t.id,null,row(1,"after",null));store.finishTarget("target",t.id);store.complete();key=store.differences("target",t.id,"",1).get(0).identityHash;}
        try(Connection db=JdbcConnections.open("org.h2.Driver","jdbc:h2:file:"+root.resolve(c.id).resolve("rows")+";IFEXISTS=TRUE","","");Statement sql=db.createStatement()){sql.executeUpdate("UPDATE rmv_row SET raw_data='corrupt' WHERE side='_source'");}
        try(ValidationStore store=ValidationStore.open(root,c.id)){assertEquals("VALIDATION_ROW_CHECKSUM_MISMATCH",assertThrows(SyncFailure.class,()->store.values("target",t.id,key)).code);}
    }

    @Test void missingSummaryProjectionIsRecoveredFromTheDurableManifest()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate c=certificate(t);
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){store.finishSourceTable(t.id);store.completeSource();}
        Path projection=root.resolve(c.id).resolve("summary.json");Files.delete(projection);
        try(ValidationStore store=ValidationStore.open(root,c.id)){assertEquals("SOURCE_READY",Codec.JSON.readTree(Files.readAllBytes(projection)).get("state").asText());}
    }
    @Test void sourceSealAndRowChecksumsAreVerifiedBeforeComparison()throws Exception{
        TableSchema t=table(false);PipelineValidationBarrier.Certificate c=certificate(t);
        try(ValidationStore store=create(t,c,new ValidationStore.Limits())){store.source(t.id,row(1,"before",null));store.finishSourceTable(t.id);store.completeSource();store.verifySource();}
        try(Connection db=JdbcConnections.open("org.h2.Driver","jdbc:h2:file:"+root.resolve(c.id).resolve("rows")+";IFEXISTS=TRUE","","");Statement sql=db.createStatement()){sql.executeUpdate("UPDATE rmv_row SET canonical_data='corrupt' WHERE side='_source'");}
        try(ValidationStore store=ValidationStore.open(root,c.id)){assertEquals("VALIDATION_ROW_CHECKSUM_MISMATCH",assertThrows(SyncFailure.class,store::verifySource).code);store.target("target",t.id,null,row(1,"before",null));assertEquals("VALIDATION_ROW_CHECKSUM_MISMATCH",assertThrows(SyncFailure.class,()->store.finishTarget("target",t.id)).code);}
    }
    @Test void sourceSealIsIndependentOfSetIterationOrderAfterDeserialization()throws Exception{
        String[] ids={"e7127dfb2a490cf9962c637c07c055aa0b18c06f8c43e81ed6c2b14a8022387e","217c42cd2cfb6c871c993f88b40a3497708cbca1fbc1dc769c7645e500351673","785719ae77587319f80e9d009017b0e11c134136b58fa81f68b04789f27d4c92"};
        List<TableSchema> tables=new ArrayList<>();for(int i=0;i<ids.length;i++){TableSchema t=table(false);t.id=ids[i];t.name="table"+i;tables.add(t);}
        PipelineValidationBarrier.Certificate c=certificate(tables.get(0));c.schemas.clear();for(TableSchema t:tables)c.schemas.put(t.id,Codec.sha256(Codec.encode(t)));c.selectedTargets=new LinkedHashSet<>(Arrays.asList("z-last","target"));
        Map<String,Set<String>> targets=new TreeMap<>();targets.put("target",new TreeSet<>(Arrays.asList(ids)));targets.put("z-last",new LinkedHashSet<>(Arrays.asList(ids[2],ids[0],ids[1])));
        try(ValidationStore store=ValidationStore.create(root,c.id,c,tables,targets,new ValidationStore.Limits())){for(TableSchema t:tables){store.source(t.id,row(1,"source",null));store.finishSourceTable(t.id);}store.completeSource();store.verifySource();}
        try(ValidationStore store=ValidationStore.open(root,c.id)){store.verifySource();for(String target:targets.keySet())for(TableSchema t:tables){store.target(target,t.id,null,row(1,"source",null));assertTrue(store.finishTarget(target,t.id).matches());}store.complete();assertEquals("VALIDATION_MATCH",store.manifest().code);}
    }

}
