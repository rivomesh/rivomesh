package io.github.rivomesh.cdc;

import java.util.*;

/** Physical acceleration only. Source row identity remains the materializer's immutable sort key. */
public final class ClickHouseIndexPlan {
    public enum Mode { DATA_SKIPPING, NONE }
    public static final class Decision {
        public String sourceName,action,reason;
        public boolean sourceUnique;
        public List<String> columns=new ArrayList<>(),targetNames=new ArrayList<>();
    }
    public static final class SkippingIndex {
        public String name,column,type;
        public String definition(){return "INDEX "+ClickHouseMaterializer.q(name)+" "+ClickHouseMaterializer.q(column)+" TYPE "+type+" GRANULARITY 1";}
    }
    public String profile;
    public List<Decision> decisions=new ArrayList<>();
    public List<SkippingIndex> indexes=new ArrayList<>();
    public static ClickHouseIndexPlan plan(TableSchema table,Mode mode){
        table.validate();if(mode==null)throw new IllegalArgumentException("CLICKHOUSE_INDEX_MODE_REQUIRED");
        ClickHouseIndexPlan plan=new ClickHouseIndexPlan();plan.profile=mode==Mode.NONE?"NONE":"DATA_SKIPPING_V1";
        Map<String,SkippingIndex> byColumn=new LinkedHashMap<>();
        for(TableSchema.Index source:table.indexes){
            Decision decision=new Decision();decision.sourceName=source.name;decision.sourceUnique=source.unique;decision.columns=new ArrayList<>(source.columns);plan.decisions.add(decision);
            if(source.primary){decision.action="KEEP_ROW_IDENTITY";decision.reason="IMMUTABLE_MATERIALIZATION_KEY_NOT_DOWNGRADED";continue;}
            if(mode==Mode.NONE){decision.action="OMIT_BY_CONFIGURATION";decision.reason="EXPLICIT_DATA_ONLY_PROFILE";continue;}
            String method=source.method==null?"BTREE":source.method.toUpperCase(Locale.ROOT);
            if(source.expression!=null||source.columns.isEmpty()||!Arrays.asList("BTREE","GSI","GLOBAL","GLOBAL_BTREE").contains(method)
                    ||source.prefixLengths.stream().anyMatch(n->n!=null&&n>0)){
                decision.action="REQUIRES_PLAN";decision.reason="EXPRESSION_PREFIX_OR_METHOD_UNSUPPORTED";continue;
            }
            List<TableSchema.Column> columns=new ArrayList<>();boolean supported=true;
            for(String name:source.columns){try{TableSchema.Column column=table.columns.get(table.column(name));columns.add(column);if(column.type==TableSchema.Type.JSON||column.type==TableSchema.Type.GEOMETRY)supported=false;}catch(IllegalArgumentException missing){supported=false;}}
            if(!supported){decision.action="REQUIRES_PLAN";decision.reason="INDEX_COLUMN_TYPE_REQUIRES_PLAN";continue;}
            for(TableSchema.Column column:columns){
                SkippingIndex index=byColumn.get(column.name);
                if(index==null){index=new SkippingIndex();index.column=column.name;index.name="rm_skip_"+Codec.sha256(table.id+"\u0000"+column.name).substring(0,32);
                    switch(column.type){case BOOLEAN:case INT64:case UINT64:case DECIMAL:case FLOAT32:case FLOAT64:index.type="minmax";break;default:index.type="bloom_filter(0.01)";}
                    byColumn.put(column.name,index);
                }
                decision.targetNames.add(index.name);
            }
            decision.action="CREATE_NON_UNIQUE_SKIPPING";
            decision.reason="PER_COLUMN_ACCELERATION_NO_UNIQUENESS_SORT_COLLATION_OR_GLOBAL_ROUTING";
        }
        plan.indexes.addAll(byColumn.values());return plan;
    }
    public void requireSupported(){for(Decision decision:decisions)if("REQUIRES_PLAN".equals(decision.action))throw new IllegalArgumentException("CLICKHOUSE_INDEX_CONVERSION_REQUIRES_PLAN");}
}
