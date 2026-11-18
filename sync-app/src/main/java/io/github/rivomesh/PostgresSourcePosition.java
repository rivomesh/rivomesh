package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import java.util.LinkedHashMap;
import java.util.Map;

final class PostgresSourcePosition {
    static final String TYPE="POSTGRESQL";
    private static final String TRANSACTION_PREFIX="pg:";
    private final String sourceId, systemId, timeline, slot;

    PostgresSourcePosition(String sourceId,String systemId,String timeline,String slot){
        this.sourceId=sourceId;
        this.systemId=systemId;
        this.timeline=timeline;
        this.slot=slot;
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
        if(!systemId.equals(fields.get("systemId"))||!timeline.equals(fields.get("timeline"))
                ||!slot.equals(fields.get("slot")))throw invalid("POSTGRESQL_POSITION_LINEAGE_CHANGED");
        return parse(fields.get("lsn"));
    }

    boolean covers(SourcePosition captured,SourcePosition required){
        return captured!=null&&required!=null&&Long.compareUnsigned(lsn(captured),lsn(required))>=0;
    }

    boolean includes(SourcePosition baseline,String transaction){
        if(transaction==null||!transaction.startsWith(TRANSACTION_PREFIX))throw invalid("POSTGRESQL_TRANSACTION_ID_INVALID");
        return Long.compareUnsigned(lsn(baseline),parse(transaction.substring(TRANSACTION_PREFIX.length())))>=0;
    }

    static String transaction(long lsn){return TRANSACTION_PREFIX+format(lsn);}

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
