package io.github.rivomesh.cdc;

import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class HiveIndexPlanTest {
    @Test void omissionRequiresApprovalWithoutWeakeningMaterializedIdentity(){
        TableSchema table=new TableSchema();table.database="sales";table.name="items";table.id=TableSchema.identity("sales","","items");TableSchema.Column id=new TableSchema.Column();id.name="id";id.type=TableSchema.Type.INT64;id.nullable=false;table.columns=Collections.singletonList(id);table.primaryKey=Collections.singletonList("id");
        TableSchema.Index primary=new TableSchema.Index();primary.name="PRIMARY";primary.primary=true;primary.unique=true;primary.columns=Collections.singletonList("id");TableSchema.Index gsi=new TableSchema.Index();gsi.name="global_business";gsi.method="GSI";gsi.unique=true;gsi.columns=Collections.singletonList("id");table.indexes=Arrays.asList(primary,gsi);
        HiveIndexPlan pending=HiveIndexPlan.plan(table,false);assertEquals("KEEP_ROW_IDENTITY",pending.decisions.get(0).action);assertEquals(table.primaryKey,pending.identityColumns);assertEquals("GSI",pending.decisions.get(1).sourceMethod);assertThrows(IllegalStateException.class,pending::requireAcknowledged);
        HiveIndexPlan approved=HiveIndexPlan.plan(table,true);approved.requireAcknowledged();assertEquals("OMIT_BY_ACKNOWLEDGEMENT",approved.decisions.get(1).action);assertTrue(approved.decisions.get(1).sourceUnique);assertEquals(table.primaryKey,approved.identityColumns);
    }
}
