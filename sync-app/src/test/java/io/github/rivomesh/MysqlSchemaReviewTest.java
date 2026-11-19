package io.github.rivomesh;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.TableSchema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class MysqlSchemaReviewTest {
    @Test
    void comparesOnlyLatestRecordedShapeAndKeepsExpressionsPrivate() throws Exception {
        TableSchema old = table("orders", 1, "id");
        TableSchema baseline = table("orders", 2, "id", "note");
        TableSchema live = table("orders", 1, "id", "note", "extra");
        live.columns.get(1).defaultExpression = "a sensitive default literal";
        live.identityKey.add("id");

        var review = Codec.encode(MysqlSchemaReview.compare(List.of(baseline, old), List.of(live)));
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
    void versionsAloneAreEquivalentAndCurrentOnlyCatalogIsNotAPlan() {
        assertEquals("NO_VISIBLE_CHANGE", MysqlSchemaReview.compare(
                List.of(table("orders", 2, "id")), List.of(table("orders", 1, "id"))).get("state"));
        assertEquals("NO_JOURNAL_SCHEMA", MysqlSchemaReview.compare(
                List.of(), List.of(table("orders", 1, "id"))).get("state"));
    }

    @Test
    void detailsAreBoundedAndMissingTablesStillCount() throws Exception {
        var baseline = IntStream.range(0, 65).mapToObj(i -> table("orders" + i, 1, "id")).toList();
        var json = Codec.JSON.readTree(Codec.encode(MysqlSchemaReview.compare(baseline, List.of())));
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
