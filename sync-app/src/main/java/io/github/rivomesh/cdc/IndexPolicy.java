package io.github.rivomesh.cdc;

import java.util.*;

/** Explicit, reviewable secondary-index transformations. Identity keys are never downgraded. */
public final class IndexPolicy {
    private IndexPolicy(){}
    public static final class Decision {
        public String sourceName,targetName,action,reason;
        public boolean unique;
        public List<String> columns;
    }
    public static List<Decision> plan(TableSchema table,TargetProduct product,JdbcSink.UniquePolicy policy){
        List<Decision> result=new ArrayList<>();
        for(TableSchema.Index index:table.indexes){
            if(index.primary)continue;
            Decision d=new Decision();d.sourceName=index.name;d.targetName="rm_"+table.id.substring(0,Math.min(12,table.id.length()))+"_"+Codec.sha256(Codec.encode(index)).substring(0,16);
            d.columns=new ArrayList<>(index.columns);d.unique=index.unique;d.action="CREATE";d.reason="SOURCE_SECONDARY_INDEX";
            String method=index.method==null?"BTREE":index.method.toUpperCase(Locale.ROOT);
            if(index.expression!=null||index.columns.isEmpty()||!table.columns.stream().map(c->c.name).collect(java.util.stream.Collectors.toSet()).containsAll(index.columns)
                    ||index.prefixLengths.stream().anyMatch(n->n!=null&&n>0)||index.descending.stream().anyMatch(Boolean.TRUE::equals)
                    ||!Arrays.asList("BTREE","GSI","GLOBAL","GLOBAL_BTREE").contains(method)){
                d.action="REQUIRES_PLAN";d.reason="EXPRESSION_PREFIX_ORDER_OR_METHOD_UNSUPPORTED";
            }else if(method.contains("GLOBAL")||method.equals("GSI")){
                if(index.columns.equals(table.identityKey)){d.action="REQUIRES_PLAN";d.reason="GLOBAL_IDENTITY_REQUIRES_DISTRIBUTION_PLAN";}
                else{d.unique=false;d.action="REPLACE_GSI";d.reason="ORDINARY_INDEX_LOSES_GLOBAL_ROUTING_AND_GLOBAL_UNIQUENESS";}
            }else if(index.unique&&policy==JdbcSink.UniquePolicy.DOWNGRADE_BUSINESS&&!index.columns.equals(table.identityKey)){
                d.unique=false;d.action="DOWNGRADE_UNIQUE";d.reason="EXCLUSIVE_SYNC_WRITER_RESTORE_CONSTRAINTS_BEFORE_TAKEOVER";
            }
            result.add(d);
        }
        return result;
    }
}
