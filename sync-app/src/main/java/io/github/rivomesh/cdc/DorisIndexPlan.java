package io.github.rivomesh.cdc;

import java.util.*;

/** Physical nonunique indexes; business uniqueness and global routing do not transfer. */
public final class DorisIndexPlan {
    public enum Mode { INVERTED,NONE }
    public static final class Decision {
        public String sourceName,action,reason;
        public boolean sourceUnique;
        public List<String> columns=new ArrayList<>(),targetNames=new ArrayList<>();
    }
    public static final class Index {
        public String name,column;
        public String definition(){return "INDEX "+DorisRows.q(name)+" ("+DorisRows.q(column)+") USING INVERTED";}
    }
    public String profile;
    public List<Decision> decisions=new ArrayList<>();
    public List<Index> indexes=new ArrayList<>();
    public static DorisIndexPlan plan(TableSchema table,Mode mode){
        table.validate();if(mode==null)throw new IllegalArgumentException("DORIS_INDEX_MODE_REQUIRED");DorisIndexPlan plan=new DorisIndexPlan();plan.profile=mode.name()+"_V1";Map<String,Index> unique=new LinkedHashMap<>();
        for(TableSchema.Index source:table.indexes){
            Decision decision=new Decision();decision.sourceName=source.name;decision.sourceUnique=source.unique;decision.columns=new ArrayList<>(source.columns);plan.decisions.add(decision);
            if(source.primary){decision.action="KEEP_ROW_IDENTITY";decision.reason="MATERIALIZATION_IDENTITY_NOT_DOWNGRADED";continue;}
            if(mode==Mode.NONE){decision.action="OMIT_BY_CONFIGURATION";decision.reason="EXPLICIT_DATA_ONLY_PROFILE";continue;}
            if(source.expression!=null||source.columns.isEmpty()||!Arrays.asList("BTREE","GSI","GLOBAL","GLOBAL_BTREE").contains(source.method==null?"BTREE":source.method.toUpperCase(Locale.ROOT))||source.prefixLengths.stream().anyMatch(length->length!=null&&length>0)){decision.action="REQUIRES_PLAN";decision.reason="INDEX_METHOD_EXPRESSION_OR_PREFIX_UNSUPPORTED";continue;}
            boolean supported=true;for(String name:source.columns){TableSchema.Column column=table.columns.get(table.column(name));if(Arrays.asList(TableSchema.Type.JSON,TableSchema.Type.BYTES,TableSchema.Type.GEOMETRY,TableSchema.Type.BOOLEAN).contains(column.type)||column.type==TableSchema.Type.DECIMAL&&column.precision>38)supported=false;}
            if(!supported){decision.action="REQUIRES_PLAN";decision.reason="INDEX_COLUMN_TYPE_REQUIRES_PLAN";continue;}
            for(String name:source.columns){Index index=unique.get(name);if(index==null){index=new Index();index.name="rm_inv_"+Codec.sha256(table.id+":"+name).substring(0,32);index.column=name;unique.put(name,index);}decision.targetNames.add(index.name);}
            decision.action="CREATE_NON_UNIQUE_INVERTED";decision.reason="NO_BUSINESS_UNIQUENESS_SORT_COLLATION_OR_GLOBAL_ROUTING";
        }
        plan.indexes.addAll(unique.values());return plan;
    }
    public void requireSupported(){for(Decision decision:decisions)if(decision.action.equals("REQUIRES_PLAN"))throw new IllegalArgumentException("DORIS_INDEX_CONVERSION_REQUIRES_PLAN");}
}
