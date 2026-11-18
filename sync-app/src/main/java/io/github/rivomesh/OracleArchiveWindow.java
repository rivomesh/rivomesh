package io.github.rivomesh;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/** Establishes continuous archived redo coverage in a single Oracle redo thread. */
final class OracleArchiveWindow {
    public String startScn,endScnExclusive;

    Map<String,Object> inspect(Connection root,String incarnationScn,String currentScn)throws Exception{
        BigInteger start=scn(startScn);
        BigInteger end=scn(endScnExclusive);
        if(start.compareTo(end)>=0||end.compareTo(scn(currentScn))>0)
            throw new IllegalArgumentException("ORACLE_ARCHIVE_WINDOW_INVALID");
        int thread;
        try(Statement query=root.createStatement()){
            query.setQueryTimeout(20);
            try(ResultSet rows=query.executeQuery("SELECT thread# FROM v$thread WHERE enabled<>'DISABLED'")){
                if(!rows.next())throw new IllegalStateException("ORACLE_SINGLE_REDO_THREAD_REQUIRED");
                thread=rows.getInt(1);
                if(thread<1||rows.next())throw new IllegalStateException("ORACLE_SINGLE_REDO_THREAD_REQUIRED");
            }
        }
        BigInteger covered=start;
        long archives=0;
        try(PreparedStatement query=root.prepareStatement("SELECT first_change#,next_change# "
                +"FROM v$archived_log WHERE thread#=? AND resetlogs_change#=? "
                +"AND archived='YES' AND status='A' AND deleted='NO' AND name IS NOT NULL "
                +"AND first_change#<? AND next_change#>? ORDER BY first_change#,next_change# DESC")){
            query.setQueryTimeout(20);
            query.setFetchSize(256);
            query.setInt(1,thread);
            query.setBigDecimal(2,new BigDecimal(scn(incarnationScn)));
            query.setBigDecimal(3,new BigDecimal(end));
            query.setBigDecimal(4,new BigDecimal(start));
            try(ResultSet rows=query.executeQuery()){
                while(covered.compareTo(end)<0&&rows.next()){
                    BigInteger first=scn(rows.getString(1));
                    BigInteger next=scn(rows.getString(2));
                    if(first.compareTo(covered)>0)break;
                    if(next.compareTo(covered)>0){
                        covered=next;
                        archives++;
                    }
                }
            }
        }
        if(covered.compareTo(end)<0)throw new IllegalStateException("ORACLE_ARCHIVE_LOG_GAP");
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("kind","CATALOG_CONTIGUOUS");
        result.put("thread",thread);
        result.put("startScn",startScn);
        result.put("endScnExclusive",endScnExclusive);
        result.put("segments",archives);
        return result;
    }

    private static BigInteger scn(String value){
        if(value==null||!value.matches("0|[1-9][0-9]{0,19}"))
            throw new IllegalArgumentException("ORACLE_ARCHIVE_SCN_INVALID");
        return new BigInteger(value);
    }
}
