package io.github.rivomesh.cdc;

import java.util.*;

/** Hive has no ordinary B-tree equivalent. Omission needs explicit approval; identity does not. */
public final class HiveIndexPlan {
    public static final class Decision {
        public String sourceName,sourceMethod,sourceExpression,action,reason;
        public boolean sourceUnique;
        public List<String> columns=new ArrayList<>();
        public List<Integer> prefixLengths=new ArrayList<>();
    }
    public final String profile="HIVE_EXPLICIT_INDEX_OMISSION_V1";
    public boolean acknowledgementRequired;
    public List<String> identityColumns=new ArrayList<>();
    public List<Decision> decisions=new ArrayList<>();
    public static HiveIndexPlan plan(TableSchema table,boolean acknowledge){
        table.validate();HiveIndexPlan plan=new HiveIndexPlan();plan.identityColumns.addAll(table.primaryKey.isEmpty()?table.identityKey:table.primaryKey);
        for(TableSchema.Index index:table.indexes){
            Decision d=new Decision();d.sourceName=index.name;d.sourceMethod=index.method;d.sourceExpression=index.expression;d.sourceUnique=index.unique;d.columns.addAll(index.columns);d.prefixLengths.addAll(index.prefixLengths);plan.decisions.add(d);
            if(index.primary){d.action="KEEP_ROW_IDENTITY";d.reason="VERSIONED_VIEW_IDENTITY_NOT_DOWNGRADED";}
            else{d.action=acknowledge?"OMIT_BY_ACKNOWLEDGEMENT":"REQUIRES_ACKNOWLEDGEMENT";d.reason="NO_ORDINARY_INDEX_UNIQUENESS_OR_GLOBAL_ROUTING_EQUIVALENT";if(!acknowledge)plan.acknowledgementRequired=true;}
        }return plan;
    }
    public void requireAcknowledged(){if(acknowledgementRequired)throw new IllegalStateException("HIVE_INDEX_PLAN_REQUIRED");}
}
