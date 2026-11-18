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
    @Test void storageInspectionObservesProtectionWithoutOpeningOrCleaningUpAReport()throws Exception{
        TableSchema table=table(false);
        PipelineValidationBarrier.Certificate certificate=certificate(table);
        Path abandoned;
        try(ValidationStore report=create(table,certificate,new ValidationStore.Limits())){
            Path directory=root.resolve(certificate.id);
            abandoned=Files.writeString(directory.resolve("summary-127.tmp"),"retained inspection fixture");
            Map<String,Object> active=ValidationStore.storage(root,certificate.id);
            assertEquals(certificate.id,active.get("id"));
            assertEquals("PROTECTED_ACTIVE",active.get("activity"));
            Map<?,?> files=(Map<?,?>)active.get("files");
            assertEquals(Files.size(directory.resolve("rows.mv.db")),files.get("rows.mv.db"));
            assertFalse(files.containsKey(abandoned.getFileName().toString()));
            assertTrue(Files.exists(abandoned));
            assertEquals("SCANNING_SOURCE",report.manifest().state);
        }
        assertEquals("ACTIVITY_UNKNOWN",ValidationStore.storage(root,certificate.id).get("activity"));
        assertTrue(Files.exists(abandoned));
    }
    @Test void retentionReviewPreservesActiveAndRecoveryLinkedReportBytesAndRequiresTheLastReview()throws Exception{
        TableSchema t=table(false);
        var certificate=certificate(t);
        var first=new ValidationRetention.Request(certificate.id,null,90,true);
        try(ValidationStore store=create(t,certificate,new ValidationStore.Limits())){
            assertEquals("VALIDATION_REPORT_BUSY",assertThrows(SyncFailure.class,
                    ()->ValidationRetention.review(root,first,"first_review","operator-a",30)).code);
        }
        assertEquals("VALIDATION_RETENTION_REPORT_INCOMPLETE",assertThrows(SyncFailure.class,
                ()->ValidationRetention.review(root,first,"first_review","operator-a",30)).code);
        var finished=certificate(t);
        Path directory=root.resolve(finished.id);
        var approved=new ValidationRetention.Request(finished.id,null,90,true);
        try(ValidationStore store=create(t,finished,new ValidationStore.Limits())){
            store.source(t.id,row(1,"evidence",null));store.finishSourceTable(t.id);store.completeSource();
            store.finishTarget("target",t.id);store.complete();
        }
        Files.createDirectory(directory.resolve("repair-plans"));
        Path plan=Files.writeString(directory.resolve("repair-plans/existing.json"),"repair plan evidence");
        byte[] database=Files.readAllBytes(directory.resolve("rows.mv.db")),summary=Files.readAllBytes(directory.resolve("summary.json"));
        assertNull(ValidationRetention.observe(root,finished.id));
        assertEquals("VALIDATION_RETENTION_REVIEW_DISABLED",assertThrows(SyncFailure.class,
                ()->ValidationRetention.review(root,approved,"first_review","operator-a",0)).code);
        var saved=ValidationRetention.review(root,approved,"first_review","operator-a",30);
        assertEquals(saved,ValidationRetention.observe(root,finished.id));
        assertEquals(saved,ValidationRetention.review(root,approved,"first_review","operator-a",0));
        assertEquals("VALIDATION_RETENTION_REVIEW_CHANGED",assertThrows(SyncFailure.class,
                ()->ValidationRetention.review(root,approved,"stale_review","operator-b",30)).code);
        var shortReview=new ValidationRetention.Request(finished.id,saved.requestId(),45,true);
        assertEquals("VALIDATION_RETENTION_CANNOT_SHORTEN",assertThrows(SyncFailure.class,
                ()->ValidationRetention.review(root,shortReview,"short_review","operator-b",30)).code);
        var renewal=new ValidationRetention.Request(finished.id,saved.requestId(),120,true);
        var newer=ValidationRetention.review(root,renewal,"second_review","operator-b",30);
        assertEquals("operator-b",newer.actorId());
        assertEquals(saved.requestId(),newer.previousReviewId());
        assertEquals(newer,ValidationRetention.observe(root,finished.id));
        assertArrayEquals(database,Files.readAllBytes(directory.resolve("rows.mv.db")));
        assertArrayEquals(summary,Files.readAllBytes(directory.resolve("summary.json")));
        assertEquals("repair plan evidence",Files.readString(plan));
    }
    @Test void sealedArchiveFencesLostReportsAndRestoresExactBytesWithoutReplacingCollisions()throws Exception{
        Path reportRoot=root.resolve("reports"),cold=Files.createDirectory(root.resolve("cold"));
        ValidationStore.owner(cold,true);
        TableSchema table=table(false);
        var certificate=certificate(table);
        Path live=reportRoot.resolve(certificate.id);
        var scope=new ValidationArchive.Scope("task",UUID.randomUUID().toString(),certificate.id);
        var create=new ValidationArchive.Request(certificate.id,"retention_review",null,true);
        try(ValidationStore store=ValidationStore.create(reportRoot,certificate.id,certificate,List.of(table),
                Map.of("target",Set.of(table.id)),new ValidationStore.Limits())){
            store.source(table.id,row(1,"historical",null));store.finishSourceTable(table.id);store.completeSource();
            store.finishTarget("target",table.id);store.complete();
            assertEquals("VALIDATION_REPORT_BUSY",assertThrows(SyncFailure.class,
                    ()->ValidationArchive.execute(cold,reportRoot,scope,create,ValidationArchive.COPY,"archive_request","admin")).code);
        }
        ValidationRetention.review(reportRoot,new ValidationRetention.Request(certificate.id,null,30,true),
                "retention_review","admin",1);
        byte[] rows=Files.readAllBytes(live.resolve("rows.mv.db")),summary=Files.readAllBytes(live.resolve("summary.json"));
        var archive=ValidationArchive.execute(cold,reportRoot,scope,create,ValidationArchive.COPY,"archive_request","admin");
        assertEquals(archive,ValidationArchive.execute(cold,reportRoot,scope,create,ValidationArchive.COPY,"archive_request","admin"));
        Path saved;
        try(var children=Files.list(cold)){saved=children.findFirst().orElseThrow();}
        assertArrayEquals(rows,Files.readAllBytes(saved.resolve("rows.mv.db")));
        assertArrayEquals(rows,Files.readAllBytes(live.resolve("rows.mv.db")));
        assertArrayEquals(summary,Files.readAllBytes(live.resolve("summary.json")));
        Files.delete(live.resolve("rows.mv.db"));
        assertEquals(true,ValidationStore.storage(reportRoot,certificate.id).get("restoreRequired"));
        assertEquals(certificate.id,Model.JSON.valueToTree(ValidationStore.catalog(reportRoot,"",20)).path("reports").get(0).path("id").textValue());
        assertEquals("VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED",
                assertThrows(SyncFailure.class,()->ValidationStore.open(reportRoot,certificate.id)).code);
        var restore=new ValidationArchive.Request(certificate.id,null,archive.archiveId(),true);
        Files.writeString(saved.resolve("rows.mv.db"),"changed archive");
        assertEquals("VALIDATION_ARCHIVE_BYTES_CHANGED",assertThrows(SyncFailure.class,
                ()->ValidationArchive.execute(cold,reportRoot,scope,restore,ValidationArchive.RESTORE,"restore_request","admin")).code);
        assertFalse(Files.exists(live.resolve("rows.mv.db")));
        Files.write(saved.resolve("rows.mv.db"),rows);
        Files.writeString(live.resolve("rows.mv.db"),"different live evidence");
        assertEquals("VALIDATION_ARCHIVE_HOT_FILE_CHANGED",assertThrows(SyncFailure.class,
                ()->ValidationArchive.execute(cold,reportRoot,scope,restore,ValidationArchive.RESTORE,"restore_request","admin")).code);
        assertEquals("different live evidence",Files.readString(live.resolve("rows.mv.db")));
        Files.write(live.resolve("rows.mv.db"),rows);
        Files.write(live.resolve("archive-restore.json"),Model.JSON.writeValueAsBytes(
                new ValidationArchive.Restoration(1,archive.archiveId(),"restore_request","admin",false)));
        assertEquals("VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED",
                assertThrows(SyncFailure.class,()->ValidationStore.open(reportRoot,certificate.id)).code);
        assertEquals(archive,ValidationArchive.execute(cold,reportRoot,scope,restore,ValidationArchive.RESTORE,"restore_request","admin"));
        assertArrayEquals(rows,Files.readAllBytes(live.resolve("rows.mv.db")));
        try(ValidationStore reopened=ValidationStore.open(reportRoot,certificate.id)){
            assertEquals("VALIDATION_DIFFERENCES",reopened.manifest().code);
        }
        assertEquals(archive,ValidationArchive.execute(cold,reportRoot,scope,restore,ValidationArchive.RESTORE,"restore_request","admin"));
    }
    @Test void reclaimFencesBeforeRemovingOnlyTheArchivedCopyAndReplaysAfterInterruption()throws Exception{
        Path reports=root.resolve("reports"),cold=Files.createDirectory(root.resolve("archive"));
        ValidationStore.owner(cold,true);
        TableSchema table=table(false);
        var certificate=certificate(table);
        Path live=reports.resolve(certificate.id),rows=live.resolve("rows.mv.db"),marker=live.resolve("archive-reclaim.json");
        try(ValidationStore store=ValidationStore.create(reports,certificate.id,certificate,List.of(table),
                Map.of("target",Set.of(table.id)),new ValidationStore.Limits())){
            store.source(table.id,row(1,"source",null));store.finishSourceTable(table.id);store.completeSource();
            store.finishTarget("target",table.id);store.complete();
        }
        var scope=new ValidationArchive.Scope("task",UUID.randomUUID().toString(),certificate.id);
        var hold=ValidationRetention.review(reports,new ValidationRetention.Request(certificate.id,null,1,true),
                "review_request","admin",1);
        var archive=ValidationArchive.execute(cold,reports,scope,new ValidationArchive.Request(certificate.id,hold.requestId(),null,true),
                ValidationArchive.COPY,"archive_request","admin");
        var reclaim=new ValidationArchive.Request(certificate.id,hold.requestId(),archive.archiveId(),true,ValidationArchive.digest(archive));
        byte[] original=Files.readAllBytes(rows),summary=Files.readAllBytes(live.resolve("summary.json"));
        assertEquals("VALIDATION_RETENTION_HOLD_ACTIVE",assertThrows(SyncFailure.class,()->ValidationArchive.execute(cold,reports,
                scope,reclaim,ValidationArchive.RECLAIM,"reclaim_request","admin",()->fail("Premature audit"))).code);
        Files.write(live.resolve("retention.json"),Model.JSON.writeValueAsBytes(new ValidationRetention.Review(1,certificate.id,
                hold.requestId(),hold.actorId(),null,1,"2020-01-01T00:00:00Z","2020-01-02T00:00:00Z")));
        Path repair=Files.createDirectory(live.resolve("repair-plans"));
        assertEquals("VALIDATION_ARCHIVE_RECOVERY_DEPENDENCY",assertThrows(SyncFailure.class,()->ValidationArchive.execute(cold,reports,
                scope,reclaim,ValidationArchive.RECLAIM,"reclaim_request","admin",()->fail("Unsafe audit"))).code);
        assertTrue(Files.exists(rows));assertFalse(Files.exists(marker));
        Files.delete(repair);
        assertThrows(IllegalStateException.class,()->ValidationArchive.execute(cold,reports,scope,reclaim,
                ValidationArchive.RECLAIM,"reclaim_request","admin",()->{throw new IllegalStateException("fixture audit unavailable");}));
        assertTrue(Files.exists(rows));assertFalse(Files.exists(marker));

        // Simulate a vanished process after its durable fence, then after unlink and before its terminal receipt.
        var staged=new ValidationArchive.Reclamation(1,archive.archiveId(),"reclaim_request","admin",hold.requestId(),
                reclaim.expectedArchiveSha256(),"FENCED");
        Files.write(marker,Model.JSON.writeValueAsBytes(staged));
        assertEquals("VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED",
                assertThrows(SyncFailure.class,()->ValidationStore.open(reports,certificate.id)).code);
        Files.delete(rows);
        assertEquals(archive,ValidationArchive.execute(cold,reports,scope,reclaim,ValidationArchive.RECLAIM,
                "reclaim_request","admin",()->assertTrue(ValidationArchive.restoringRequired(live,certificate.id))));
        assertEquals("ARCHIVED",ValidationArchive.reclamation(live,archive).state());
        assertFalse(Files.exists(rows));assertArrayEquals(summary,Files.readAllBytes(live.resolve("summary.json")));
        var restore=new ValidationArchive.Request(certificate.id,null,archive.archiveId(),true);
        ValidationArchive.execute(cold,reports,scope,restore,ValidationArchive.RESTORE,"restore_request","admin");
        assertEquals("RESTORED",ValidationArchive.reclamation(live,archive).state());
        assertFalse(ValidationArchive.restoringRequired(live,certificate.id));
        assertArrayEquals(original,Files.readAllBytes(rows));
        ValidationArchive.execute(cold,reports,scope,reclaim,ValidationArchive.RECLAIM,"reclaim_request","admin",
                ()->fail("Replay must not reclaim restored data"));
        assertArrayEquals(original,Files.readAllBytes(rows));
    }
    @Test void aMixedReportStillProtectsItsMatchingTargetsFromReclaim()throws Exception{
        Path reports=root.resolve("reports"),cold=Files.createDirectory(root.resolve("archive"));
        ValidationStore.owner(cold,true);
        TableSchema table=table(false);
        var certificate=certificate(table);
        certificate.selectedTargets.add("matched");
        var input=row(1,"source",null);
        try(ValidationStore store=ValidationStore.create(reports,certificate.id,certificate,List.of(table),
                Map.of("target",Set.of(table.id),"matched",Set.of(table.id)),new ValidationStore.Limits())){
            store.source(table.id,input);store.finishSourceTable(table.id);store.completeSource();
            store.finishTarget("target",table.id);store.target("matched",table.id,null,input);
            store.finishTarget("matched",table.id);store.complete();
            assertEquals("VALIDATION_DIFFERENCES",store.manifest().code);
        }
        Path live=reports.resolve(certificate.id);
        var hold=ValidationRetention.review(reports,new ValidationRetention.Request(certificate.id,null,1,true),
                "review_request","admin",1);
        Files.write(live.resolve("retention.json"),Model.JSON.writeValueAsBytes(new ValidationRetention.Review(1,certificate.id,
                hold.requestId(),hold.actorId(),null,1,"2020-01-01T00:00:00Z","2020-01-02T00:00:00Z")));
        var scope=new ValidationArchive.Scope("task",UUID.randomUUID().toString(),certificate.id);
        var archive=ValidationArchive.execute(cold,reports,scope,new ValidationArchive.Request(certificate.id,hold.requestId(),null,true),
                ValidationArchive.COPY,"archive_request","admin");
        var reclaim=new ValidationArchive.Request(certificate.id,hold.requestId(),archive.archiveId(),true,ValidationArchive.digest(archive));
        assertEquals("VALIDATION_ARCHIVE_RECOVERY_DEPENDENCY",assertThrows(SyncFailure.class,()->ValidationArchive.execute(cold,reports,
                scope,reclaim,ValidationArchive.RECLAIM,"reclaim_request","admin",()->fail("Unsafe audit"))).code);
        assertTrue(Files.isRegularFile(live.resolve("rows.mv.db")));
        assertFalse(Files.exists(live.resolve("archive-reclaim.json")));
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
