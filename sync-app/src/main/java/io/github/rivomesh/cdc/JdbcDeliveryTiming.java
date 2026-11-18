package io.github.rivomesh.cdc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Optional native source commit times and one direct confirmation sample per SQL destination. */
final class JdbcDeliveryTiming {
    private static final Set<String> MISSING_TABLE=Set.of("42P01","42S02","42102","42104");
    private static final Map<String,Object> UNKNOWN=Map.of("state","UNKNOWN");

    private JdbcDeliveryTiming(){}

    static void initialize(JdbcJournal journal,Connection connection,boolean mysql)throws SQLException{
        journal.ddl(connection,"CREATE TABLE IF NOT EXISTS "+journal.table("source_commit_time")
                +" (pipeline_id VARCHAR(128) NOT NULL,stream_seq BIGINT NOT NULL,source_at VARCHAR(40) NOT NULL,"
                +"PRIMARY KEY(pipeline_id,stream_seq))");
        journal.ddl(connection,"CREATE TABLE IF NOT EXISTS "+journal.table("delivery_timing")
                +" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,stream_seq BIGINT NOT NULL,"
                +"source_at VARCHAR(40) NOT NULL,confirmed_at VARCHAR(40) NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
        if(mysql)try(PreparedStatement query=connection.prepareStatement("SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES"
                +" WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('rm3_source_commit_time','rm3_delivery_timing')")){
            query.setString(1,journal.settings.namespace);
            try(ResultSet rows=query.executeQuery()){
                int count=0;
                while(rows.next()){
                    if(!"InnoDB".equalsIgnoreCase(rows.getString(1))||!"utf8mb4_bin".equalsIgnoreCase(rows.getString(2)))
                        throw new IllegalStateException("DELIVERY_TIMING_STORAGE_INVALID");
                    count++;
                }
                if(count!=2)throw new IllegalStateException("DELIVERY_TIMING_STORAGE_INVALID");
            }
        }
    }

    static void capture(JdbcJournal journal,Connection connection,long sequence,Instant committedAt)throws SQLException{
        JdbcJournal.execute(connection,"INSERT INTO "+journal.table("source_commit_time")
                +" (pipeline_id,stream_seq,source_at) VALUES (?,?,?)",journal.settings.pipelineId,sequence,committedAt.toString());
    }

    static void confirm(JdbcJournal journal,Connection connection,String subscription,long sequence,Instant confirmedAt)throws SQLException{
        String source;
        try(PreparedStatement query=connection.prepareStatement("SELECT source_at FROM "+journal.table("source_commit_time")
                +" WHERE pipeline_id=? AND stream_seq=?")){
            query.setString(1,journal.settings.pipelineId);
            query.setLong(2,sequence);
            try(ResultSet rows=query.executeQuery()){
                if(!rows.next())return;
                source=rows.getString(1);
            }
        }
        String table=journal.table("delivery_timing"),confirmation=confirmedAt.toString();
        int updated=JdbcJournal.execute(connection,"UPDATE "+table+" SET stream_seq=?,source_at=?,confirmed_at=?"
                +" WHERE pipeline_id=? AND subscription_id=?",sequence,source,confirmation,journal.settings.pipelineId,subscription);
        if(updated==0)JdbcJournal.execute(connection,"INSERT INTO "+table
                +" (pipeline_id,subscription_id,stream_seq,source_at,confirmed_at) VALUES (?,?,?,?,?)",
                journal.settings.pipelineId,subscription,sequence,source,confirmation);
    }

    static void collect(JdbcJournal journal,Connection connection,long lastCollected)throws SQLException{
        JdbcJournal.execute(connection,"DELETE FROM "+journal.table("source_commit_time")
                +" WHERE pipeline_id=? AND stream_seq<=?",journal.settings.pipelineId,lastCollected);
    }

    /** The current ACK must match the sample's sequence. A missing or differently acknowledged sample stays unknown. */
    static void enrich(JdbcJournal journal,Connection connection,Map<String,Map<String,Object>> progress)throws SQLException{
        for(Map<String,Object> subscription:progress.values())subscription.put("lastDeliveryTiming",UNKNOWN);
        try(PreparedStatement query=connection.prepareStatement("SELECT d.subscription_id,d.stream_seq,d.source_at,d.confirmed_at FROM "
                +journal.table("delivery_timing")+" d JOIN "+journal.table("subscription")+" s"
                +" ON s.pipeline_id=d.pipeline_id AND s.subscription_id=d.subscription_id AND s.acknowledged=d.stream_seq"
                +" WHERE d.pipeline_id=?")){
            query.setString(1,journal.settings.pipelineId);
            try(ResultSet rows=query.executeQuery()){
                while(rows.next()){
                    Map<String,Object> subscription=progress.get(rows.getString(1));
                    if(subscription!=null)subscription.put("lastDeliveryTiming",sample(rows.getLong(2),rows.getString(3),rows.getString(4)));
                }
            }
        }catch(SQLException missing){
            // This is the final read in the status transaction. An earlier Worker may not have created the optional tables.
            if(missing.getSQLState()==null||!MISSING_TABLE.contains(missing.getSQLState()))throw missing;
        }
    }

    private static Map<String,Object> sample(long sequence,String sourceText,String confirmedText){
        try{
            Instant source=Instant.parse(sourceText),confirmed=Instant.parse(confirmedText);
            Duration elapsed=Duration.between(source,confirmed);
            Map<String,Object> result=new LinkedHashMap<>();
            result.put("state",elapsed.isNegative()?"CLOCK_SKEW":"OBSERVED");
            result.put("sequence",Long.toString(sequence));
            result.put("sourceCommittedAt",source.toString());
            result.put("targetCommitObservedAt",confirmed.toString());
            if(!elapsed.isNegative())result.put("millis",Long.toString(elapsed.toMillis()));
            return result;
        }catch(DateTimeException|ArithmeticException invalid){return UNKNOWN;}
    }
}
