package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.rivomesh.JdbcConnections;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManualCorrectionTest {
    @TempDir Path directory;
    CommitAcknowledgementTest setup;
    FailureCheckpointInspectionTest fixture;

    @BeforeEach void prepare()throws Exception{
        setup=new CommitAcknowledgementTest();
        setup.directory=directory;
        setup.prepare();
        setup.pauseAndRetain(true);
        fixture=setup.fixture;
    }

    @AfterEach void close()throws Exception{if(setup!=null)setup.close();}

    @Test void batchShowsExactBeforeAndAfterAndCommitsOnlyTargetRowsAndReceipt()throws Exception{
        seed();
        CorrectionBatch batch=batch();
        String journal=Codec.encode(fixture.journal.status());
        JsonNode preview=Codec.JSON.valueToTree(run(batch,null));
        assertEquals("CORRECTION_PLAN_READY",preview.path("code").asText());
        assertEquals(3,preview.path("rows").size());
        JsonNode update=edit(preview,"UPDATE");
        assertEquals("existing-person",update.path("before").path("email").asText());
        assertTrue(update.path("after").path("email").isNull());
        assertEquals("99999999999999999999.123456789012345",update.path("after").path("amount").asText());
        assertTrue(edit(preview,"INSERT").path("before").isNull());
        assertTrue(edit(preview,"DELETE").path("after").isNull());
        assertEquals(2,JdbcSinkTest.count(fixture.target,"items"));

        Map<String,Object> receipt=run(batch,preview.path("hash").asText());
        assertEquals("CORRECTION_COMPLETED",receipt.get("code"));
        assertFalse(Codec.encode(receipt).contains("inserted-person"));
        assertFalse(receipt.containsKey("rows"));
        assertEquals(2,JdbcSinkTest.count(fixture.target,"items"));
        assertEquals("1:inserted-person;2:null",rows());
        assertEquals(journal,Codec.encode(fixture.journal.status()));
        assertEquals(Codec.encode(receipt),Codec.encode(run(batch,preview.path("hash").asText())));
        batch.edits.get(1).values.put("email",TextNode.valueOf("different request"));
        assertEquals("COMPENSATION_REQUEST_CONFLICT",assertThrows(IllegalStateException.class,
                ()->run(batch,preview.path("hash").asText())).getMessage());
    }

    @Test void destinationDriftAndFailedReceiptBothPreserveTheBatchBaseline()throws Exception{
        seed();
        CorrectionBatch batch=batch();
        String hash=(String)run(batch,null).get("hash");
        executeSql("UPDATE @items SET @amount=3 WHERE @id=2");
        assertEquals("CORRECTION_PLAN_CHANGED",assertThrows(IllegalStateException.class,()->run(batch,hash)).getMessage());
        assertEquals("2:existing-person;3:extra-person",rows());

        String current=(String)run(batch,null).get("hash");
        executeSql("ALTER TABLE @receipt ADD CONSTRAINT deny_manual_receipt CHECK (operation_id <> 'manual_correction_1234')");
        assertThrows(SQLException.class,()->run(batch,current));
        assertEquals("2:existing-person;3:extra-person",rows());
        assertEquals(current,run(batch,null).get("hash"));
    }

    @Test void rejectsNumericCoercionsPrecisionLossNullKeysAndMissingOrExistingRows()throws Exception{
        CorrectionBatch batch=batch();
        CorrectionBatch.Edit insert=batch.edits.get(0);
        insert.values.put("id",Codec.JSON.getNodeFactory().numberNode(9007199254740993L));
        assertCode("CORRECTION_TYPED_TEXT_REQUIRED",()->run(batch,null));
        insert.values.put("id",NullNode.getInstance());
        assertCode("CORRECTION_NULL_NOT_ALLOWED",()->run(batch,null));
        insert.values.put("id",TextNode.valueOf("1"));
        insert.values.put("amount",TextNode.valueOf("1.1234567890123456"));
        assertCode("CORRECTION_TYPED_VALUE_INVALID",()->run(batch,null));
        insert.values.put("amount",TextNode.valueOf("1.5"));
        insert.values.put("document",TextNode.valueOf("{\"one\":1,\"one\":2}"));
        assertCode("CORRECTION_JSON_INVALID",()->run(batch,null));
        insert.values.put("document",TextNode.valueOf("{}"));
        assertCode("CORRECTION_ROW_MISSING",()->run(batch,null));
        seed();
        insert.values.put("id",TextNode.valueOf("2"));
        assertCode("CORRECTION_DUPLICATE_IDENTITY",()->run(batch,null));
        batch.edits.remove(1);
        assertCode("CORRECTION_ROW_ALREADY_EXISTS",()->run(batch,null));
    }

    @Test void keylessEditUsesExactRowIdAndCannotAllocateAnOccurrence(){
        TableSchema table=JdbcSinkTest.schema(true);
        table.id=TableSchema.identity(table.database,table.schema,table.name);
        CorrectionBatch batch=new CorrectionBatch();
        CorrectionBatch.Edit edit=new CorrectionBatch.Edit();
        edit.tableId=table.id;edit.action=CorrectionBatch.Action.UPDATE;
        edit.values.put("email",TextNode.valueOf("changed"));
        batch.edits.add(edit);
        Map<String,TableSchema> schema=Collections.singletonMap(table.id,table);
        assertCode("CORRECTION_KEYLESS_ROW_ID_REQUIRED",()->batch.bind(schema));
        edit.rowId=Codec.sha256("specific mirrored occurrence");
        assertEquals(edit.rowId,batch.bind(schema).iterator().next().occurrence);
        edit.action=CorrectionBatch.Action.INSERT;
        assertCode("CORRECTION_KEYLESS_ROW_ID_REQUIRED",()->batch.bind(schema));
    }

    private CorrectionBatch batch(){
        CorrectionBatch batch=new CorrectionBatch();
        CorrectionBatch.Edit insert=new CorrectionBatch.Edit();
        insert.tableId=fixture.table.id;insert.action=CorrectionBatch.Action.INSERT;
        insert.values.put("id",TextNode.valueOf("1"));
        insert.values.put("email",TextNode.valueOf("inserted-person"));
        insert.values.put("document",TextNode.valueOf("{\"precise\":18446744073709551615}"));
        insert.values.put("amount",TextNode.valueOf("1.234567890123456"));
        CorrectionBatch.Edit update=new CorrectionBatch.Edit();
        update.tableId=fixture.table.id;update.action=CorrectionBatch.Action.UPDATE;
        update.key.put("id",TextNode.valueOf("2"));
        update.values.put("email",NullNode.getInstance());
        update.values.put("amount",TextNode.valueOf("99999999999999999999.123456789012345"));
        CorrectionBatch.Edit delete=new CorrectionBatch.Edit();
        delete.tableId=fixture.table.id;delete.action=CorrectionBatch.Action.DELETE;
        delete.key.put("id",TextNode.valueOf("3"));
        batch.edits.addAll(Arrays.asList(insert,update,delete));
        return batch;
    }

    private static JsonNode edit(JsonNode preview,String operation){
        for(JsonNode row:preview.path("rows"))if(operation.equals(row.path("action").asText()))return row;
        throw new AssertionError(operation);
    }

    private Map<String,Object> run(CorrectionBatch batch,String hash)throws Exception{
        return TransactionCompensation.run(fixture.journal,fixture.target,fixture.retained,batch,hash,
                "manual_correction_1234","admin","Reviewed complete target correction batch");
    }

    private static void assertCode(String code,org.junit.jupiter.api.function.Executable operation){
        assertEquals(code,assertThrows(RuntimeException.class,operation).getMessage());
    }

    private String query(String template){
        JdbcSink.Settings s=fixture.target;
        String sql=template.replace("@items",JdbcSinkTest.q(s,s.databases.get("sales"))+"."+JdbcSinkTest.q(s,"items"))
                .replace("@receipt",JdbcSinkTest.q(s,s.metadataSchema)+"."+JdbcSinkTest.q(s,"rm3_sink_compensation"));
        for(String column:Arrays.asList("id","email","amount"))sql=sql.replace("@"+column,JdbcSinkTest.q(s,column));
        return sql;
    }

    private Connection connection()throws SQLException{
        JdbcSink.Settings s=fixture.target;
        return JdbcConnections.open(s.driver,s.url,s.username,s.password);
    }

    private void executeSql(String sql)throws Exception{
        try(Connection c=connection();Statement q=c.createStatement()){q.executeUpdate(query(sql));}
    }

    private void seed()throws Exception{
        executeSql("INSERT INTO @items (@id,@email,@amount) VALUES (2,'existing-person',2),(3,'extra-person',3)");
    }

    private String rows()throws Exception{
        List<String> rows=new ArrayList<>();
        try(Connection c=connection();Statement q=c.createStatement();ResultSet r=q.executeQuery(query("SELECT @id,@email FROM @items ORDER BY @id"))){
            while(r.next())rows.add(r.getLong(1)+":"+r.getString(2));
        }
        return String.join(";",rows);
    }
}
