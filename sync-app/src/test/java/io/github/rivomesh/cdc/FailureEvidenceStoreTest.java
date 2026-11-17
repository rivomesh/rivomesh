package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class FailureEvidenceStoreTest {
    @TempDir Path directory;
    private FailureReviewTest journalFixture;
    JdbcJournal journal()throws Exception{if(journalFixture==null){journalFixture=new FailureReviewTest();journalFixture.directory=directory;}return new JdbcJournal(journalFixture.settings());}
    @AfterEach void cleanupExternal()throws Exception{if(journalFixture!=null)journalFixture.cleanupExternalNamespace();}
    FailureEvidence.Scope scope(){FailureEvidence.Scope scope=new FailureEvidence.Scope();scope.taskId="pipeline";scope.ownerNodeId="agent-a";scope.ownerDirectoryId="11111111-1111-1111-1111-111111111111";return scope;}
    Path root(){return directory.resolve("evidence");}
    FailureEvidenceStore store()throws Exception{return new FailureEvidenceStore(root(),scope(),new FailureEvidenceStore.Limits());}
    FailureEvidence sample(){
        TableSchema table=JdbcSinkTest.schema(false);SourceTransaction raw=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"private-business-value"),JdbcSinkTest.insert(table,2,"second-private-value"));
        Journal.FailureReview review=new Journal.FailureReview();review.pipelineId="pipeline";review.generation="generation";review.fingerprint=Codec.sha256("config");review.epoch=1;review.sequence=1;review.failureEpoch=1;
        review.subscription=new Journal.Subscription();review.subscription.id="target";review.subscription.state="PAUSED";review.subscription.errorGeneration=1;review.subscription.errorCode="SQL_23514";
        review.failure=new SubscriberFailure();review.failure.phase=SubscriberFailure.Phase.TARGET_APPLY;review.failure.sequence=1;review.failure.transactionId=raw.id;review.raw=raw;review.rawHash=Codec.sha256(Codec.encode(raw));review.normalized=Codec.decode(Codec.encode(raw),SourceTransaction.class);review.normalizedHash=review.rawHash;
        FailureEvidence evidence=new FailureEvidence();evidence.id=UUID.randomUUID().toString();evidence.requestId="request_12345678";evidence.configurationRevision="revision-1";evidence.createdBy="admin";evidence.createdAt=Instant.now().toString();evidence.scope=scope();evidence.review=review;evidence.schemas.put(table.id+"/1",table);return evidence;
    }
    static void code(String expected,org.junit.jupiter.api.function.Executable action){assertEquals(expected,assertThrows(IllegalStateException.class,action).getMessage());}
    @Test void privateImmutableEvidenceSurvivesReopenAndIdempotentRetries()throws Exception{
        FailureEvidence input=sample();FailureEvidenceStore.Envelope original=store().retain(input);String body=Codec.encode(original.value);
        input.review.raw.changes.clear();FailureEvidenceStore reopened=store();assertEquals(body,Codec.encode(reopened.load(input.id).value));assertEquals(original.hash,reopened.retain(original.value).hash);
        assertEquals(PosixFilePermissions.fromString("rwx------"),Files.getPosixFilePermissions(root()));
        for(String name:Arrays.asList("evidence.key","authority.json","evidence.lock",input.id+".json"))assertEquals(PosixFilePermissions.fromString("rw-------"),Files.getPosixFilePermissions(root().resolve(name)));
        assertEquals(32,Files.size(root().resolve("evidence.key")));
    }
    @Test void modifiedContentOrCallerConflictCannotReplaceOriginal()throws Exception{
        FailureEvidenceStore store=store();FailureEvidence first=sample();FailureEvidenceStore.Envelope saved=store.retain(first);
        FailureEvidence changed=Codec.decode(Codec.encode(first),FailureEvidence.class);changed.createdBy="another-admin";code("FAILURE_EVIDENCE_ID_CONFLICT",()->store.retain(changed));assertEquals(saved.hash,store.load(first.id).hash);
        Path path=root().resolve(first.id+".json");String json=new String(Files.readAllBytes(path),java.nio.charset.StandardCharsets.UTF_8);Files.write(path,json.replace("private-business-value","changed-business-value").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        code("FAILURE_EVIDENCE_CONTENT_CHANGED",()->store.load(first.id));
    }
    @Test void missingOrChangedKeyAndChangedOwnerRefuseInsteadOfRegeneratingTrust()throws Exception{
        FailureEvidence first=sample();store().retain(first);byte[] original=Files.readAllBytes(root().resolve("evidence.key"));
        FailureEvidence.Scope other=scope();other.ownerDirectoryId=UUID.randomUUID().toString();code("FAILURE_EVIDENCE_OWNER_CHANGED",()->new FailureEvidenceStore(root(),other,new FailureEvidenceStore.Limits()));
        Files.delete(root().resolve("evidence.key"));code("FAILURE_EVIDENCE_KEY_MISSING",()->store());assertFalse(Files.exists(root().resolve("evidence.key")));
        Files.write(root().resolve("evidence.key"),new byte[32]);code("FAILURE_EVIDENCE_SIGNATURE_INVALID",()->store());Files.write(root().resolve("evidence.key"),original);assertEquals(first.id,store().load(first.id).value.id);
    }
    @Test void countAndByteQuotasKeepExistingEvidenceAndReturnExplicitCapacity()throws Exception{
        FailureEvidenceStore.Limits limit=new FailureEvidenceStore.Limits();limit.maximumRecords=1;FailureEvidenceStore store=new FailureEvidenceStore(root(),scope(),limit);FailureEvidence first=sample();store.retain(first);FailureEvidence second=sample();
        code("FAILURE_EVIDENCE_CAPACITY",()->store.retain(second));assertEquals(first.id,store.load(first.id).value.id);assertFalse(Files.exists(root().resolve(second.id+".json")));
        limit.maximumRecords=10;long size=0;try(java.util.stream.Stream<Path> files=Files.list(root())){for(Iterator<Path> it=files.iterator();it.hasNext();)size+=Files.size(it.next());}limit.maximumBytes=size+64;
        FailureEvidenceStore full=new FailureEvidenceStore(root(),scope(),limit);code("FAILURE_EVIDENCE_CAPACITY",()->full.retain(second));assertNotNull(full.load(first.id));
    }
    @Test void symlinkedRecordsAndParentDirectoriesCannotEscapePrivateStorage()throws Exception{
        FailureEvidence first=sample();FailureEvidenceStore store=store();store.retain(first);Path record=root().resolve(first.id+".json"),outside=directory.resolve("outside.json");Files.move(record,outside);Files.createSymbolicLink(record,outside);
        code("FAILURE_EVIDENCE_PATH_INVALID",()->store.load(first.id));Path alias=directory.resolve("alias");Files.createSymbolicLink(alias,root());
        code("FAILURE_EVIDENCE_PATH_INVALID",()->new FailureEvidenceStore(alias.resolve("child"),scope(),new FailureEvidenceStore.Limits()));assertFalse(Files.exists(root().resolve("child")));
    }
    @Test void safeSummaryAndBoundedEventPagesPreserveWholeImages()throws Exception{
        FailureEvidence first=sample();FailureEvidenceStore store=store();store.retain(first);String summary=Codec.encode(store.summary(first.id));assertFalse(summary.contains("private-business-value"));assertFalse(summary.contains("\"before\":"));assertFalse(summary.contains("\"after\":"));
        Map<String,Object> page=store.events(first.id,false,0,1,4096);assertEquals(1,page.get("next"));assertEquals(2,page.get("total"));assertTrue(Codec.encode(page).contains("private-business-value"));assertEquals(1,((List<?>)page.get("events")).size());assertNull(store.events(first.id,true,1,1,4096).get("next"));
        assertThrows(IllegalArgumentException.class,()->store.events(first.id,false,-1,1,4096));assertThrows(IllegalArgumentException.class,()->store.events(first.id,false,3,1,4096));
        FailureEvidence large=sample();large.review.raw.changes.get(0).after.get(1).value=new String(new char[20000]).replace('\0','x');large.review.rawHash=Codec.sha256(Codec.encode(large.review.raw));large.review.normalized=null;large.review.normalizedHash=null;store.retain(large);
        code("FAILURE_EVIDENCE_EVENT_TOO_LARGE",()->store.events(large.id,false,0,10,1024));code("FAILURE_EVIDENCE_NOT_NORMALIZED",()->store.events(large.id,true,0,10,4096));
    }
    @Test void historicalColumnMetadataAndImageAvailabilityAccompanyWholeEvents()throws Exception{
        FailureEvidence value=sample();Map<String,String> position=new TreeMap<>(value.review.raw.position.getPayload());position.put("password","never-in-summary");value.review.raw.position=new io.github.rivomesh.api.SourcePosition(1,"MYSQL","source",position);value.review.raw.changes.get(0).after.set(1,Cell.value(null));value.review.raw.changes.get(0).after.set(2,Cell.value("null"));value.review.raw.changes.get(0).after.set(3,Cell.missing());value.review.rawHash=Codec.sha256(Codec.encode(value.review.raw));value.review.normalized=Codec.decode(Codec.encode(value.review.raw),SourceTransaction.class);value.review.normalizedHash=value.review.rawHash;
        FailureEvidenceStore store=store();store.retain(value);Map<String,Object> summary=store.summary(value.id);assertEquals(2,((Map<?,?>)summary.get("beforeImages")).get("absent"));assertEquals(1,((Map<?,?>)summary.get("afterImages")).get("partial"));assertEquals(1,((Map<?,?>)summary.get("afterImages")).get("full"));assertTrue(((Map<?,?>)summary.get("sourcePosition")).containsKey("gtidSet"));assertFalse(Codec.encode(summary).contains("never-in-summary"));
        Map<?,?> item=(Map<?,?>)((List<?>)store.events(value.id,false,0,1,4096).get("events")).get(0);assertEquals("sales.items",item.get("table"));List<?> columns=(List<?>)item.get("columns");assertEquals("email",((Map<?,?>)columns.get(1)).get("name"));assertEquals("JSON",((Map<?,?>)columns.get(2)).get("type"));assertEquals(4,columns.size());
        Change event=(Change)item.get("event");assertTrue(event.after.get(1).present);assertNull(event.after.get(1).value);assertEquals("null",event.after.get(2).value);assertFalse(event.after.get(3).present);
    }
    @Test void concurrentPublishCannotExceedQuotaOrExposePartialRecords()throws Exception{
        FailureEvidenceStore.Limits limits=new FailureEvidenceStore.Limits();limits.maximumRecords=1;FailureEvidenceStore a=new FailureEvidenceStore(root(),scope(),limits),b=new FailureEvidenceStore(root(),scope(),limits);FailureEvidence first=sample(),second=sample();
        CountDownLatch start=new CountDownLatch(1);ExecutorService workers=Executors.newFixedThreadPool(2);List<String> accepted=Collections.synchronizedList(new ArrayList<>());
        try{
            List<Future<?>> futures=new ArrayList<>();for(int i=0;i<2;i++){final FailureEvidenceStore writer=i==0?a:b;final FailureEvidence value=i==0?first:second;futures.add(workers.submit(()->{try{start.await();accepted.add(writer.retain(value).value.id);}catch(IllegalStateException refused){assertTrue(Arrays.asList("FAILURE_EVIDENCE_BUSY","FAILURE_EVIDENCE_CAPACITY").contains(refused.getMessage()));}catch(Exception failure){throw new RuntimeException(failure);}}));}
            start.countDown();for(Future<?> future:futures)future.get(10,TimeUnit.SECONDS);assertEquals(1,accepted.size());assertNotNull(store().load(accepted.get(0)));
            try(java.util.stream.Stream<Path> files=Files.list(root())){assertEquals(1,files.filter(p->p.getFileName().toString().matches("[0-9a-f-]{36}\\.json")).count());}
        }finally{workers.shutdownNow();}
    }
    @Test void captureRemainsReadableAndIdempotentAfterOriginalJournalCollection()throws Exception{
        String id=UUID.randomUUID().toString();FailureEvidenceStore store=store();String hash;
        try(JdbcJournal journal=journal()){
            journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("target"));Journal.Lease lease=journal.claim("owner",30000);TableSchema table=JdbcSinkTest.schema(false);journal.saveSchema(lease,table);SourceTransaction raw=JdbcSinkTest.transaction(1,JdbcSinkTest.insert(table,1,"retained-after-gc"));journal.append(lease,raw);SubscriberFailure phase=new SubscriberFailure();phase.phase=SubscriberFailure.Phase.TARGET_APPLY;phase.sequence=1;phase.transactionId=raw.id;journal.pauseFailure(lease,"target","SQL_23514",phase);
            hash=FailureEvidenceCapture.capture(journal,store,scope(),id,"request_12345678","revision-1","admin","target",1,100000).hash;
            journal.resume("target",1);journal.acknowledge(lease,"target",1);assertEquals(1,journal.collect(lease,100));assertEquals(0,journal.read("target",1).size());
        }
        assertTrue(Codec.encode(store().load(id).value).contains("retained-after-gc"));assertEquals(hash,FailureEvidenceCapture.capture(null,store(),scope(),id,"request_12345678","revision-1","admin","target",1,100000).hash);
        code("FAILURE_EVIDENCE_REQUEST_CONFLICT",()->FailureEvidenceCapture.capture(null,store(),scope(),id,"different_request","revision-1","admin","target",1,100000));
    }
    @Test void missingSchemaAndChangedTransactionCannotCreateValidEvidence()throws Exception{
        FailureEvidence missing=sample();missing.schemas.clear();assertThrows(IllegalArgumentException.class,()->store().retain(missing));assertFalse(Files.exists(root().resolve(missing.id+".json")));
        FailureEvidence changed=sample();changed.review.raw.changes.clear();assertThrows(IllegalArgumentException.class,()->store().retain(changed));
        try(JdbcJournal journal=journal()){
            journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("target"));Journal.Lease lease=journal.claim("owner",30000);journal.append(lease,JdbcSinkTest.transaction(1,JdbcSinkTest.insert(JdbcSinkTest.schema(false),1,"not-retained")));journal.pause(lease,"target","SQL_ERROR");
            String id=UUID.randomUUID().toString();code("HISTORICAL_SCHEMA_MISSING",()->FailureEvidenceCapture.capture(journal,store(),scope(),id,"request_12345678","revision-1","admin","target",1,100000));assertFalse(Files.exists(root().resolve(id+".json")));assertEquals(0,journal.subscriptions().get(0).acknowledged);
        }
    }
    @Test void interruptedOrOversizedFilesNeverBecomeValidRecordsOrDisappearSilently()throws Exception{
        FailureEvidence value=sample();FailureEvidenceStore store=store();store.retain(value);Path record=root().resolve(value.id+".json");byte[] original=Files.readAllBytes(record);
        Path orphan=root().resolve(".evidence-crashed.pending");Files.write(orphan,original);String absent=UUID.randomUUID().toString();code("FAILURE_EVIDENCE_NOT_FOUND",()->store().load(absent));assertTrue(Files.exists(orphan));
        Files.write(record,Arrays.copyOf(original,20));code("FAILURE_EVIDENCE_CORRUPT",()->store.load(value.id));
        byte[] trailing=" {}".getBytes(java.nio.charset.StandardCharsets.UTF_8);byte[] extra=Arrays.copyOf(original,original.length+trailing.length);System.arraycopy(trailing,0,extra,original.length,trailing.length);Files.write(record,extra);code("FAILURE_EVIDENCE_CORRUPT",()->store.load(value.id));Files.write(record,original);
        FailureEvidenceStore.Limits small=new FailureEvidenceStore.Limits();small.maximumRecordBytes=1024;FailureEvidenceStore limited=new FailureEvidenceStore(directory.resolve("small"),scope(),small);FailureEvidence oversized=sample();assertThrows(IllegalArgumentException.class,()->limited.retain(oversized));assertFalse(Files.exists(directory.resolve("small").resolve(oversized.id+".json")));
        Files.delete(root().resolve("authority.json"));code("FAILURE_EVIDENCE_AUTHORITY_MISSING",()->store());
    }
    @Test void historicalSchemaLookupIsScopedAndRefusesCorruptionOrBudgetOverflow()throws Exception{
        TableSchema table=JdbcSinkTest.schema(false);String reference=table.id+"/1";
        try(JdbcJournal journal=journal()){
            journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("target"));Journal.Lease lease=journal.claim("owner",30000);journal.saveSchema(lease,table);
            TableSchema unrelated=Codec.decode(Codec.encode(table),TableSchema.class);unrelated.id="unrelated";unrelated.properties.put("large",new String(new char[100000]).replace('\0','x'));journal.saveSchema(lease,unrelated);
            try(java.sql.Connection c=journal.connect()){JdbcJournal.execute(c,"UPDATE "+journal.table("schema")+" SET payload_hash=? WHERE table_id=?",Codec.sha256("broken"),unrelated.id);}
            assertEquals(Collections.singleton(reference),journal.historicalSchemas(Collections.singleton(reference),100000).keySet());
            code("FAILURE_REVIEW_TOO_LARGE",()->journal.historicalSchemas(Collections.singleton(reference),32));
            try(java.sql.Connection c=journal.connect()){JdbcJournal.execute(c,"UPDATE "+journal.table("schema")+" SET payload_hash=? WHERE table_id=?",Codec.sha256("broken"),table.id);}
            code("SCHEMA_CHECKSUM_MISMATCH",()->journal.historicalSchemas(Collections.singleton(reference),100000));
            try(java.sql.Connection c=journal.connect()){String payload=Codec.encode(unrelated);JdbcJournal.execute(c,"UPDATE "+journal.table("schema")+" SET payload=?,payload_hash=? WHERE table_id=?",payload,Codec.sha256(payload),table.id);}
            code("HISTORICAL_SCHEMA_IDENTITY_CHANGED",()->journal.historicalSchemas(Collections.singleton(reference),200000));
        }
    }
}
