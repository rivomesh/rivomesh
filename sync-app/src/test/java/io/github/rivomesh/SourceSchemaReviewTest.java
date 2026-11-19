package io.github.rivomesh;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.TableSchema;
import org.junit.jupiter.api.Test;

import static io.github.rivomesh.SourceSchemaReview.Scope.CURRENT_MYSQL_CATALOG_VS_JOURNAL;
import static io.github.rivomesh.SourceSchemaReview.Scope.CURRENT_ORACLE_PDB_CATALOG_VS_JOURNAL;
import static io.github.rivomesh.SourceSchemaReview.Scope.CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class SourceSchemaReviewTest {
    @Test void oracleArchiveDdlReviewKeepsTheRecordedDefinitionAndReadOnlyRecoveryActions() throws Exception {
        assertEquals(CURRENT_ORACLE_PDB_CATALOG_VS_JOURNAL, CaptureRecoveryReview.schemaScope("ORACLE","DDL"));
        assertNull(CaptureRecoveryReview.schemaScope("ORACLE","SOURCE_DECODE"));
        assertEquals(List.of("PRESERVE_SOURCE_LOGS","REVIEW_NEW_BASELINE"),
                CaptureRecoveryReview.actions("SOURCE_CHECKPOINT_AVAILABLE","DDL","ORACLE",
                        "ORACLE_REDO_DDL_REQUIRES_PLAN"));
        assertEquals(List.of("PRESERVE_SOURCE_LOGS","REPAIR_CAPTURE_CAUSE","RETRY_ORIGINAL_CHECKPOINT"),
                CaptureRecoveryReview.actions("SOURCE_CHECKPOINT_AVAILABLE","DDL","POSTGRESQL",
                        "POSTGRESQL_RELATION_CHANGED_REQUIRES_PLAN"));

        TableSchema recorded=table("RIVO_ORDERS",2,"ID","PAYLOAD");
        TableSchema live=table("RIVO_ORDERS",1,"ID","PAYLOAD","EXTRA");
        for(TableSchema table:List.of(recorded,live)){
            table.database="RIVO_APP";
            table.id=TableSchema.identity("FREEPDB1",table.database,table.name);
            table.sourceEngine="ORACLE";
            table.primaryKey=List.of("ID");
            table.columns.get(1).type=TableSchema.Type.STRING;
            table.columns.get(1).sourceType="NVARCHAR2";
            table.columns.get(1).charset="AL16UTF16";
        }
        recorded.columns.get(1).length=20;
        live.columns.get(1).length=40;
        JsonNode change=difference(CURRENT_ORACLE_PDB_CATALOG_VS_JOURNAL,recorded,live);
        assertEquals("RIVO_APP",change.path("database").asText());
        assertEquals(2,change.path("recordedVersion").asInt());
        assertEquals("EXTRA",change.path("columns").path("added").get(0).asText());
        assertEquals("PAYLOAD",change.path("columns").path("modified").get(0).asText());
        assertNotEquals(change.path("recordedSha256").asText(),change.path("liveSha256").asText());
        assertEquals(Map.of("scope","CURRENT_ORACLE_PDB_CATALOG_VS_JOURNAL","state","UNAVAILABLE",
                "code","ORACLE_REDO_COLUMN_TYPE_UNSUPPORTED"),
                SourceSchemaReview.unavailable(CURRENT_ORACLE_PDB_CATALOG_VS_JOURNAL,"ORACLE_REDO_COLUMN_TYPE_UNSUPPORTED"));
    }
    @Test void unavailablePostgresqlPublicationKeepsItsOwnScope() {
        assertEquals(Map.of("scope", "CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL", "state", "UNAVAILABLE",
                        "code", "POSTGRESQL_TYPE_UNSUPPORTED"),
                SourceSchemaReview.unavailable(CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL,
                        "POSTGRESQL_TYPE_UNSUPPORTED"));
    }

    @Test
    void comparesOnlyLatestRecordedShapeAndKeepsExpressionsPrivate() throws Exception {
        TableSchema old = table("orders", 1, "id");
        TableSchema baseline = table("orders", 2, "id", "note");
        TableSchema live = table("orders", 1, "id", "note", "extra");
        live.columns.get(1).defaultExpression = "a sensitive default literal";
        live.identityKey.add("id");

        var review = Codec.encode(SourceSchemaReview.compare(CURRENT_MYSQL_CATALOG_VS_JOURNAL, List.of(baseline, old), List.of(live)));
        JsonNode result = Codec.JSON.readTree(review);
        assertEquals("CHANGES_VISIBLE", result.path("state").asText());
        assertEquals(1, result.path("changedTables").asInt());
        JsonNode change = result.path("tables").get(0);
        assertEquals(2, change.path("recordedVersion").asInt());
        assertEquals("extra", change.path("columns").path("added").get(0).asText());
        assertEquals("note", change.path("columns").path("modified").get(0).asText());
        assertTrue(change.path("identityChanged").asBoolean());
        assertNotEquals(change.path("recordedSha256").asText(), change.path("liveSha256").asText());
        assertFalse(review.contains("a sensitive default literal"));
    }

    @Test
    void reportsPostgresTypeModifierAndDistinguishesNewColumnsFromReordering() throws Exception {
        TableSchema baseline = table("orders", 1, "id", "clock");
        baseline.properties.put("pg.modifier.clock", "-1");
        TableSchema live = table("orders", 1, "id", "note", "clock");
        live.properties.put("pg.modifier.clock", "6");
        JsonNode difference = difference(CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL, baseline, live);
        assertEquals("clock", difference.path("columns").path("modified").get(0).asText());
        assertEquals("note", difference.path("columns").path("added").get(0).asText());
        assertFalse(difference.path("columnOrderChanged").asBoolean());

        TableSchema reordered = table("orders", 1, "clock", "id");
        reordered.properties.putAll(baseline.properties);
        difference = difference(CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL, baseline, reordered);
        assertTrue(difference.path("columnOrderChanged").asBoolean());
        assertEquals(0, difference.path("columns").path("modifiedCount").asInt());
    }

    @Test
    void reportsIndexOnlyChangesWithoutExposingTheirExpressions() throws Exception {
        TableSchema baseline = table("orders", 1, "id");
        TableSchema live = table("orders", 1, "id");
        var index = new TableSchema.Index();
        index.name = "orders_index";
        index.expression = "private index expression";
        live.indexes.add(index);
        JsonNode difference = difference(CURRENT_MYSQL_CATALOG_VS_JOURNAL, baseline, live);
        assertTrue(difference.path("indexesChanged").asBoolean());
        assertFalse(difference.path("identityChanged").asBoolean());
        assertFalse(difference.toString().contains("private index expression"));
    }

    private static JsonNode difference(SourceSchemaReview.Scope scope, TableSchema before, TableSchema live)
            throws Exception {
        return Codec.JSON.readTree(Codec.encode(SourceSchemaReview.compare(scope, List.of(before), List.of(live))))
                .path("tables").get(0);
    }

    @Test
    void versionsAloneAreEquivalentAndCurrentOnlyCatalogIsNotAPlan() {
        assertEquals("NO_VISIBLE_CHANGE", SourceSchemaReview.compare(CURRENT_MYSQL_CATALOG_VS_JOURNAL,
                List.of(table("orders", 2, "id")), List.of(table("orders", 1, "id"))).get("state"));
        assertEquals("NO_JOURNAL_SCHEMA", SourceSchemaReview.compare(CURRENT_MYSQL_CATALOG_VS_JOURNAL,
                List.of(), List.of(table("orders", 1, "id"))).get("state"));
    }

    @Test
    void detailsAreBoundedAndMissingTablesStillCount() throws Exception {
        var baseline = IntStream.range(0, 65).mapToObj(i -> table("orders" + i, 1, "id")).toList();
        var json = Codec.JSON.readTree(Codec.encode(SourceSchemaReview.compare(CURRENT_MYSQL_CATALOG_VS_JOURNAL, baseline, List.of())));
        assertEquals(65, json.path("changedTables").asInt());
        assertEquals(64, json.path("tables").size());
        assertTrue(json.path("detailsLimited").asBoolean());
        assertEquals("TABLE_MISSING", json.path("tables").get(0).path("change").asText());
    }

    private static TableSchema table(String name, int version, String... columns) {
        TableSchema table = new TableSchema();
        table.database = "sales";
        table.name = name;
        table.id = TableSchema.identity(table.database, "", name);
        table.version = version;
        table.primaryKey.add("id");
        for (String nameOfColumn : columns) {
            var column = new TableSchema.Column();
            column.name = nameOfColumn;
            column.type = TableSchema.Type.INT64;
            table.columns.add(column);
        }
        return table;
    }
}
