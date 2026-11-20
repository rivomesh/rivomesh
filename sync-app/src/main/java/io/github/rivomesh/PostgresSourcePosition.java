package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

final class PostgresSourcePosition {
    static final String TYPE="POSTGRESQL";
    private static final String TRANSACTION_PREFIX="pg:";
    private final String sourceId, systemId, timeline, slot;
    private final Map<String,Long> ancestors;

    PostgresSourcePosition(String sourceId,String systemId,String timeline,String slot){
        this(sourceId,systemId,timeline,slot,Collections.emptyMap());
    }

    PostgresSourcePosition(String sourceId,String systemId,String timeline,String slot,Map<String,Long> ancestors){
        this.sourceId=sourceId;
        this.systemId=systemId;
        this.timeline=timeline;
        this.slot=slot;
        this.ancestors=new LinkedHashMap<>(ancestors);
    }

    SourcePosition at(long lsn){
        Map<String,String> payload=new LinkedHashMap<>();
        payload.put("systemId",systemId);
        payload.put("timeline",timeline);
        payload.put("slot",slot);
        payload.put("lsn",format(lsn));
        return new SourcePosition(1,TYPE,sourceId,payload);
    }

    long lsn(SourcePosition position){
        if(position==null||position.getFormatVersion()!=1||!TYPE.equals(position.getSourceType())
                ||!sourceId.equals(position.getSourceId()))throw invalid("POSTGRESQL_POSITION_IDENTITY_MISMATCH");
        Map<String,String> fields=position.getPayload();
        if(!systemId.equals(fields.get("systemId"))||!slot.equals(fields.get("slot")))
            throw invalid("POSTGRESQL_POSITION_LINEAGE_CHANGED");
        long lsn=parse(fields.get("lsn"));
        String priorTimeline=fields.get("timeline");
        if(!timeline.equals(priorTimeline)){
            Long fork=ancestors.get(priorTimeline);
            if(fork==null||Long.compareUnsigned(lsn,fork)>0)
                throw invalid("POSTGRESQL_POSITION_LINEAGE_CHANGED");
        }
        return lsn;
    }

    String validationMessagePrefix(){return "rivomesh.validation."+slot;}

    boolean covers(SourcePosition captured,SourcePosition required){
        return captured!=null&&required!=null&&Long.compareUnsigned(lsn(captured),lsn(required))>=0;
    }

    boolean includes(SourcePosition baseline,String transaction){
        if(transaction==null||!transaction.startsWith(TRANSACTION_PREFIX))throw invalid("POSTGRESQL_TRANSACTION_ID_INVALID");
        return Long.compareUnsigned(lsn(baseline),parse(transaction.substring(TRANSACTION_PREFIX.length())))>=0;
    }

    static String transaction(long lsn){return TRANSACTION_PREFIX+format(lsn);}

    static Map<String,Long> history(String history,String currentTimeline){
        long current=timelineId(currentTimeline);
        if(history==null||history.length()>65536)throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
        Map<String,Long> result=new LinkedHashMap<>();
        long lastTimeline=0,lastFork=0;
        for(String line:history.split("\r?\n")){
            String trimmed=line.trim();
            if(trimmed.isEmpty()||trimmed.startsWith("#"))continue;
            String[] fields=trimmed.split("\\s+",3);
            if(fields.length<2)throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
            long parent=timelineId(fields[0]),fork=parse(fields[1]);
            if(parent<=lastTimeline||parent>=current||Long.compareUnsigned(fork,lastFork)<0)
                throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
            result.put(fields[0],fork);
            lastTimeline=parent;
            lastFork=fork;
        }
        if(result.isEmpty()||!result.containsKey("1"))throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
        return result;
    }

    private static long timelineId(String timeline){
        if(timeline==null||!timeline.matches("[1-9][0-9]{0,9}"))
            throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
        long value=Long.parseLong(timeline);
        if(value>0xffffffffL)throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
        return value;
    }

    static long parse(String lsn){
        if(lsn==null||!lsn.matches("[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}"))
            throw invalid("POSTGRESQL_LSN_INVALID");
        String[] parts=lsn.split("/");
        return Long.parseLong(parts[0],16)<<32|Long.parseLong(parts[1],16);
    }

    static String format(long lsn){
        return Long.toHexString(lsn>>>32)+"/"+Long.toHexString(lsn&0xffffffffL);
    }

    private static IllegalArgumentException invalid(String code){return new IllegalArgumentException(code);}
}
