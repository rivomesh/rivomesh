package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.recovery.FailureEvidenceCapture;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class TransactionCompensationTest {
    @TempDir Path directory;
    CommitAcknowledgementTest setup;
    FailureCheckpointInspectionTest fixture;

    @BeforeEach void prepare() throws Exception {
        setup = new CommitAcknowledgementTest();
        setup.directory = directory;
        setup.prepare();
        setup.pauseAndRetain(true);
        fixture = setup.fixture;
    }

    @AfterEach void close() throws Exception {
        if (setup != null) setup.close();
    }

    private Map<String,Object> preview() throws Exception {
        return TransactionCompensation.run(fixture.journal,fixture.target,fixture.retained,null,null,null,null);
    }

    private Map<String,Object> apply(String hash, String reason) throws Exception {
        return TransactionCompensation.run(fixture.journal,fixture.target,fixture.retained,hash,
                "compensate_12345678","admin",reason);
    }

    private void sql(String value) throws Exception {
        JdbcSink.Settings s = fixture.target;
        String table = JdbcSinkTest.q(s,s.databases.get("sales"))+"."+JdbcSinkTest.q(s,"items");
        try (Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password); Statement q=c.createStatement()) {
            String query=value.replace("@items",table);
            for(String column:Arrays.asList("id","email","amount")) query=query.replace("@"+column,JdbcSinkTest.q(s,column));
            q.executeUpdate(query);
        }
    }

    @Test void restoresOnlyReviewedIdentityAndReplaysItsReceiptAfterOrdinarySourceApply() throws Exception {
        sql("INSERT INTO @items (@id,@email,@amount) VALUES (1,'unexpected',1),(2,'outside the transaction',2)");
        String before=Codec.encode(fixture.journal.status());
        Map<String,Object> plan=preview();
        assertEquals("COMPENSATION_PLAN_READY",plan.get("code"));
        assertFalse(Codec.encode(plan).contains("unexpected"));
        assertEquals(2,JdbcSinkTest.count(fixture.target,"items"));
        String reason="Reviewed conflicting inserted row and transaction scope";
        Map<String,Object> receipt=apply((String)plan.get("hash"),reason);
        assertEquals("COMPENSATION_COMPLETED",receipt.get("code"));
        assertEquals(before,Codec.encode(fixture.journal.status()));
        assertEquals(1,JdbcSinkTest.count(fixture.target,"items"));
        fixture.journal.resumePipeline(((Number)fixture.journal.status().get("errorGeneration")).longValue());
        try(JdbcSink sink=JdbcSink.existing(fixture.target)){
            long epoch=sink.checkpoint().epoch;
            sink.prepare(epoch,fixture.table);
            sink.apply(epoch,1,fixture.normalized);
        }
        assertEquals(Codec.encode(receipt),Codec.encode(apply((String)plan.get("hash"),reason)));
        assertEquals(2,JdbcSinkTest.count(fixture.target,"items"));
        assertEquals("COMPENSATION_REQUEST_CONFLICT",assertThrows(IllegalStateException.class,
                ()->apply((String)plan.get("hash"),"A changed reason cannot reuse an applied request")).getMessage());
    }

    @Test void reviewedTargetDriftAndAnAlreadyCommittedOriginalAreRefused() throws Exception {
        sql("INSERT INTO @items (@id,@email,@amount) VALUES (1,'unexpected',1)");
        String originalHash=(String)preview().get("hash");
        sql("UPDATE @items SET @amount=2 WHERE @id=1");
        assertEquals("COMPENSATION_PLAN_CHANGED",assertThrows(IllegalStateException.class,
                ()->apply(originalHash,"Reviewed the previous destination row")).getMessage());
        assertEquals(1,JdbcSinkTest.count(fixture.target,"items"));
        sql("DELETE FROM @items");
        try(JdbcSink sink=JdbcSink.existing(fixture.target)){
            long epoch=sink.checkpoint().epoch;
            sink.prepare(epoch,fixture.table);
            sink.apply(epoch,1,fixture.normalized);
        }
        assertEquals("COMPENSATION_CHECKPOINT_RECONCILIATION_REQUIRED",
                assertThrows(IllegalStateException.class,this::preview).getMessage());
    }

    @Test void failedReceiptInsertRollsBackBusinessWrites() throws Exception {
        sql("INSERT INTO @items (@id,@email,@amount) VALUES (1,'unexpected',1)");
        String hash=(String)preview().get("hash");
        String reason="Reviewed conflicting destination row for restoration";
        assertThrows(IllegalStateException.class,()->apply(Codec.sha256("other"),reason));
        JdbcSink.Settings s=fixture.target;
        String receipt=JdbcSinkTest.q(s,s.metadataSchema)+"."+JdbcSinkTest.q(s,"rm3_sink_compensation");
        try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);Statement q=c.createStatement()){
            q.executeUpdate("ALTER TABLE "+receipt+" ADD CONSTRAINT forbid_receipt CHECK (operation_id <> 'compensate_12345678')");
        }
        assertThrows(SQLException.class,()->apply(hash,reason));
        assertEquals(1,JdbcSinkTest.count(fixture.target,"items"));
        assertEquals(hash,preview().get("hash"));
    }

    @Test void reconstructsRepeatedPrimaryKeyMovesAndKeylessInsertDeleteChains() {
        TableSchema keyed=JdbcSinkTest.schema(false), keyless=JdbcSinkTest.schema(true);
        keyless.name="copies";
        keyless.id=TableSchema.identity(keyless.database,keyless.schema,keyless.name);
        Change initial=JdbcSinkTest.insert(keyed,1,"before");
        Change intermediate=JdbcSinkTest.insert(keyed,1,"intermediate");
        Change moved=JdbcSinkTest.insert(keyed,2,"moved");
        Change one=update(initial,intermediate),two=update(intermediate,moved);
        Change duplicate1=JdbcSinkTest.insert(keyless,3,"duplicate");
        duplicate1.rowId=Codec.sha256("occurrence1");
        Change duplicate2=JdbcSinkTest.insert(keyless,3,"duplicate");
        duplicate2.rowId=Codec.sha256("occurrence2");
        Change removal=update(duplicate1,duplicate1);
        removal.operation=Change.Operation.DELETE;
        removal.after=null;
        SourceTransaction transaction=JdbcSinkTest.transaction(1,one,two,duplicate1,duplicate2,removal);
        Map<String,TableSchema> schemas=new HashMap<>();
        for(TableSchema table:Arrays.asList(keyed,keyless)) schemas.put(table.id+"/"+table.version,table);
        Collection<CompensationRows.Row> result=CompensationRows.reconstruct(transaction,schemas);
        assertEquals(4,result.size());
        assertEquals(1,result.stream().filter(row->row.expected!=null).count());
        assertTrue(result.stream().anyMatch(row->row.expected!=null&&CompensationRows.equal(keyed,row.expected,initial.after)));
        two.before=initial.after;
        assertEquals("COMPENSATION_IMAGE_CHAIN_CHANGED",
                assertThrows(IllegalStateException.class,()->CompensationRows.reconstruct(transaction,schemas)).getMessage());
    }

    private static Change update(Change before, Change after) {
        Change change=new Change();
        change.eventId=after.eventId;
        change.tableId=before.tableId;
        change.schemaVersion=before.schemaVersion;
        change.rowId=before.rowId;
        change.operation=Change.Operation.UPDATE;
        change.before=before.after;
        change.after=after.after;
        return change;
    }
}
