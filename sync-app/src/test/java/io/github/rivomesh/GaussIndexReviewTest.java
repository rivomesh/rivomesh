package io.github.rivomesh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.IndexPolicy;
import io.github.rivomesh.cdc.JdbcSink;
import io.github.rivomesh.cdc.TableSchema;
import io.github.rivomesh.cdc.TargetProduct;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GaussIndexReviewTest {
    @Test void reportsTheSameDecisionsAsTheSinkForEachGaussMapping() throws Exception {
        var table = table("sales", "contacts");
        table.indexes.add(index("PRIMARY", "BTREE", true, "id", "tenant"));
        table.indexes.get(0).primary = true;
        table.indexes.add(index("phone", "BTREE", false, "phone"));
        table.indexes.add(index("email", "BTREE", true, "email"));
        table.indexes.add(index("business_global", "GSI", true, "email", "tenant", "id"));
        table.indexes.add(index("identity_global", "GSI", true, "tenant", "id"));
        var unsupported = index("functional", "BTREE", false, "email");
        unsupported.expression = "lower('private-expression-value')";
        table.indexes.add(unsupported);
        table.columns.get(table.columns.size()-1).defaultExpression = "'private-default-value'";
        table.columns.get(table.columns.size()-1).generationExpression = "concat('private-generated-value', email)";

        var ordinary = target("ordinary", TargetProduct.GAUSS);
        var preserve = target("preserve", TargetProduct.GAUSS);
        preserve.uniquePolicy = JdbcSink.UniquePolicy.PRESERVE;
        var otherProduct = target("otherProduct", TargetProduct.POSTGRESQL);
        var unmapped = table("private-database", "unmapped");
        unmapped.indexes.add(index("hidden", "BTREE", false, "id"));
        var serialized = Codec.encode(GaussIndexReview.inspect("MYSQL",
                List.of(ordinary, otherProduct, preserve), List.of(table, unmapped)));
        var result = Codec.JSON.readTree(serialized);

        assertEquals("REQUIRES_PLAN", result.path("state").asText());
        assertEquals(2, result.path("tableMappings").asInt());
        var counts = result.path("indexCounts");
        assertEquals(3, counts.path("supported").asInt());
        assertEquals(1, counts.path("downgradedUnique").asInt());
        assertEquals(2, counts.path("replacedGsi").asInt());
        assertEquals(4, counts.path("requiresPlan").asInt());
        assertEquals(2, counts.path("distributionPlanRequired").asInt());
        assertEquals(0, result.path("omittedEntries").asInt());
        assertEquals(10, result.path("entries").size());

        for (var target : List.of(ordinary, preserve)) {
            for (var decision : IndexPolicy.plan(table, target.product, target.uniquePolicy)) {
                var entry = entry(result, target.id, decision.sourceName);
                assertEquals(target.mappings.get(table.database), entry.path("targetSchema").asText());
                assertEquals(table.database, entry.path("sourceDatabase").asText());
                assertEquals(table.name, entry.path("sourceTable").asText());
                assertEquals(decision.action, entry.path("action").asText());
                assertEquals(decision.reason, entry.path("reason").asText());
                assertEquals(Codec.JSON.valueToTree(decision.columns), entry.path("columns"));
                if ("REQUIRES_PLAN".equals(decision.action)) {
                    assertTrue(entry.path("targetIndex").isNull());
                    assertTrue(entry.path("targetUnique").isNull());
                } else {
                    assertEquals(decision.targetName, entry.path("targetIndex").asText());
                    assertEquals(decision.unique, entry.path("targetUnique").asBoolean());
                }
            }
        }
        for (String hidden : List.of("private-expression-value", "private-default-value", "private-generated-value",
                "private-endpoint", "private-user", "private-password", "private-database", "otherProduct")) {
            assertFalse(serialized.contains(hidden));
        }
    }

    @Test void boundsUtf8DetailsAndRetainsBlockersAfterLargePlannableCatalog() throws Exception {
        var tables = new ArrayList<TableSchema>();
        for (int i = 0; i < 100; i++) {
            var table = table("sales", "订单储存".repeat(15) + i);
            var columns = new ArrayList<String>();
            for (int j = 0; j < 16; j++) {
                String name = "商品字段".repeat(15) + j;
                table.columns.add(column(name));
                columns.add(name);
            }
            table.indexes.add(index("索引".repeat(30), "BTREE", false, columns.toArray(String[]::new)));
            tables.add(table);
        }
        var last = table("sales", "late");
        last.indexes.add(index("late_global_identity", "GSI", true, "tenant", "id"));
        tables.add(last);

        var review = GaussIndexReview.inspect("MYSQL", List.of(target("gauss", TargetProduct.GAUSS)), tables);
        var result = Codec.JSON.readTree(Codec.encode(review));
        assertEquals(101, result.path("tableMappings").asInt());
        assertEquals(100, result.path("indexCounts").path("supported").asInt());
        assertEquals(1, result.path("indexCounts").path("distributionPlanRequired").asInt());
        var entries = result.path("entries");
        assertTrue(entries.size() >= 2 && entries.size() < 25);
        assertEquals(101 - entries.size(), result.path("omittedEntries").asInt());
        assertEquals("late_global_identity", entries.get(0).path("sourceIndex").asText());
        assertTrue(Codec.encode(entries).getBytes(StandardCharsets.UTF_8).length <= 16 * 1024);
        ArrayNode planned = entries.deepCopy();
        planned.remove(0);
        assertTrue(Codec.encode(planned).getBytes(StandardCharsets.UTF_8).length <= 8 * 1024);
    }

    @Test void handlesEntryCountLimitAndDoesNotClaimUndiscoveredIndexesAreSupported() throws Exception {
        var target = target("gauss", TargetProduct.GAUSS);
        var table = table("sales", "contacts");
        for (int i = 0; i < 80; i++) table.indexes.add(index("p" + i, "BTREE", false, "phone"));
        var result = Codec.JSON.readTree(Codec.encode(GaussIndexReview.inspect("MYSQL", List.of(target), List.of(table))));
        assertEquals("REVIEWED", result.path("state").asText());
        assertEquals(80, result.path("indexCounts").path("supported").asInt());
        assertEquals(24, result.path("entries").size());
        assertEquals(56, result.path("omittedEntries").asInt());

        for (String source : List.of("POSTGRESQL", "ORACLE")) {
            var unavailable = GaussIndexReview.inspect(source, List.of(target), List.of());
            assertEquals("SOURCE_INDEX_METADATA_UNAVAILABLE", unavailable.get("state"));
            assertEquals(source, unavailable.get("sourceType"));
            assertFalse(unavailable.containsKey("indexCounts"));
        }
        assertTrue(GaussIndexReview.inspect("MYSQL", List.of(target("pg", TargetProduct.POSTGRESQL)), List.of(table)).isEmpty());
    }

    private static JsonNode entry(JsonNode review, String target, String index) {
        for (JsonNode entry : review.path("entries")) {
            if (target.equals(entry.path("targetId").asText()) && index.equals(entry.path("sourceIndex").asText())) {
                return entry;
            }
        }
        throw new AssertionError("Missing decision for " + target + "/" + index);
    }

    private static PipelineConfig.Target target(String id, TargetProduct product) {
        var target = new PipelineConfig.Target();
        target.id = id;
        target.product = product;
        target.url = "jdbc:gaussdb://private-endpoint";
        target.username = "private-user";
        target.password = "private-password";
        target.mappings.put("sales", "sales_" + id);
        return target;
    }

    private static TableSchema table(String database, String name) {
        var table = new TableSchema();
        table.id = TableSchema.identity(database, "", name);
        table.database = database;
        table.name = name;
        table.primaryKey.addAll(List.of("id", "tenant"));
        for (String field : List.of("id", "tenant", "phone", "email")) table.columns.add(column(field));
        return table;
    }

    private static TableSchema.Column column(String name) {
        var column = new TableSchema.Column();
        column.name = name;
        column.type = TableSchema.Type.STRING;
        return column;
    }

    private static TableSchema.Index index(String name, String method, boolean unique, String... columns) {
        var index = new TableSchema.Index();
        index.name = name;
        index.method = method;
        index.unique = unique;
        index.columns.addAll(List.of(columns));
        return index;
    }
}
