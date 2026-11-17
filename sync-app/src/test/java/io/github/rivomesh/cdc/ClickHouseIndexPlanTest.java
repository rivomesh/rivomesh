package io.github.rivomesh.cdc;

import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ClickHouseIndexPlanTest {
    TableSchema table(){TableSchema t=new TableSchema();t.id=TableSchema.identity("sales","","contacts");t.database="sales";t.name="contacts";TableSchema.Column id=new TableSchema.Column();id.name="id";id.type=TableSchema.Type.INT64;TableSchema.Column email=new TableSchema.Column();email.name="email`unsafe";email.type=TableSchema.Type.STRING;t.columns=Arrays.asList(id,email);t.primaryKey=Collections.singletonList("id");return t;}
    TableSchema.Index index(String name,String method,boolean unique,String... columns){TableSchema.Index i=new TableSchema.Index();i.name=name;i.method=method;i.unique=unique;i.columns=Arrays.asList(columns);return i;}
    @Test void uniqueGlobalAndCompositeBecomeDeduplicatedNonUniquePerColumnIndexes(){
        TableSchema t=table();t.indexes=Arrays.asList(index("uq_email","BTREE",true,"email`unsafe"),index("gsi","GSI",true,"email`unsafe","id"),index("sorted","BTREE",false,"id"));
        ClickHouseIndexPlan plan=ClickHouseIndexPlan.plan(t,ClickHouseIndexPlan.Mode.DATA_SKIPPING);plan.requireSupported();assertEquals(2,plan.indexes.size());assertEquals("bloom_filter(0.01)",plan.indexes.get(0).type);assertEquals("minmax",plan.indexes.get(1).type);assertTrue(plan.indexes.get(0).definition().contains("`email\\`unsafe`"));assertFalse(plan.indexes.get(0).definition().contains("UNIQUE"));assertEquals(plan.decisions.get(0).targetNames.get(0),plan.decisions.get(1).targetNames.get(0));
    }
    @Test void unsupportedSqlIsReportedAndNeverRenderedIntoDdl(){
        for(String method:Arrays.asList("FULLTEXT","SPATIAL","HASH")){TableSchema t=table();t.indexes=Collections.singletonList(index("custom",method,false,"id"));ClickHouseIndexPlan p=ClickHouseIndexPlan.plan(t,ClickHouseIndexPlan.Mode.DATA_SKIPPING);assertEquals("REQUIRES_PLAN",p.decisions.get(0).action);assertTrue(p.indexes.isEmpty());assertEquals("CLICKHOUSE_INDEX_CONVERSION_REQUIRES_PLAN",assertThrows(IllegalArgumentException.class,p::requireSupported).getMessage());}
        TableSchema t=table();TableSchema.Index i=index("prefix","BTREE",false,"email`unsafe");i.prefixLengths=Collections.singletonList(10);t.indexes=Collections.singletonList(i);assertThrows(IllegalArgumentException.class,()->ClickHouseIndexPlan.plan(t,ClickHouseIndexPlan.Mode.DATA_SKIPPING).requireSupported());i.prefixLengths=new ArrayList<>();i.expression="x); DROP DATABASE sales";assertThrows(IllegalArgumentException.class,()->ClickHouseIndexPlan.plan(t,ClickHouseIndexPlan.Mode.DATA_SKIPPING).requireSupported());
    }
    @Test void identitiesArePreservedAndExplicitDataOnlyModeHasAnAuditableOmission(){
        TableSchema t=table();TableSchema.Index primary=index("PRIMARY","BTREE",true,"id");primary.primary=true;t.indexes=Arrays.asList(primary,index("business","BTREE",true,"email`unsafe"));ClickHouseIndexPlan plan=ClickHouseIndexPlan.plan(t,ClickHouseIndexPlan.Mode.NONE);plan.requireSupported();assertTrue(plan.indexes.isEmpty());assertEquals("KEEP_ROW_IDENTITY",plan.decisions.get(0).action);assertEquals("OMIT_BY_CONFIGURATION",plan.decisions.get(1).action);assertEquals(Collections.singletonList("id"),t.primaryKey);
    }
    @Test void decimalAndNullableNumbersHaveNumericIndexesWhileTemporalTextUsesBloom(){
        TableSchema t=table();t.columns.get(0).type=TableSchema.Type.DECIMAL;t.columns.get(0).precision=65;t.columns.get(0).scale=30;t.columns.get(1).type=TableSchema.Type.DATETIME;t.indexes=Collections.singletonList(index("both","BTREE",false,"id","email`unsafe"));ClickHouseIndexPlan plan=ClickHouseIndexPlan.plan(t,ClickHouseIndexPlan.Mode.DATA_SKIPPING);assertEquals("minmax",plan.indexes.get(0).type);assertEquals("bloom_filter(0.01)",plan.indexes.get(1).type);
    }
}
