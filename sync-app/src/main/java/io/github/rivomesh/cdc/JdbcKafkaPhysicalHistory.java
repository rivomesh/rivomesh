package io.github.rivomesh.cdc;

import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.cdc.KafkaPhysicalReceipt.Grant;
import io.github.rivomesh.cdc.KafkaPhysicalReceipt.Signed;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import static io.github.rivomesh.cdc.JdbcJournal.execute;

/** The publication checkpoint stages proof. Only a successful subscription ACK makes it historical. */
final class JdbcKafkaPhysicalHistory {
    private static final String PENDING="kafka_physical_pending",HISTORY="kafka_physical_history",ATTESTED="kafka_physical_attested";
    private JdbcKafkaPhysicalHistory(){}

    static void prepare(JdbcJournal journal,Connection c,boolean mayCreate)throws Exception{
        try{
            try{checkPermissions(journal,c);}
            catch(SQLException missing){
                String state=missing.getSQLState();
                if(!mayCreate||!("42P01".equals(state)||"42S02".equals(state)||"42S04".equals(state)))throw missing;
                initialize(journal,c);
                checkPermissions(journal,c);
            }
        }catch(SQLException rejected){
            throw notReady(rejected);
        }
    }

    static SyncFailure notReady(SQLException cause){
        return new SyncFailure("KAFKA_PHYSICAL_JOURNAL_NOT_READY","JOURNAL_STORAGE",false,
                "Kafka ownership evidence requires provisioned and writable journal receipt tables",cause,null,0,null);
    }

    private static void checkPermissions(JdbcJournal journal,Connection c)throws SQLException{
        String pending=journal.table(PENDING);
        try(Statement check=c.createStatement()){
            check.setQueryTimeout(5);
            if(journal.localH2()){
                // H2 EXPLAIN checks the statement shape but skips mutations' privilege checks.
                try(var role=check.executeQuery("SELECT IS_ADMIN FROM INFORMATION_SCHEMA.USERS WHERE USER_NAME=CURRENT_USER")){
                    if(!role.next()||!role.getBoolean(1))throw new SQLException("H2 journal setup requires its local owner","42501");
                }
            }
            // Without ANALYZE the database checks the statement shape and permissions without performing a write.
            check.execute("EXPLAIN SELECT pipeline_id,subscription_id,stream_seq,proof FROM "+pending);
            check.execute("EXPLAIN INSERT INTO "+pending+" (pipeline_id,subscription_id,stream_seq,proof) VALUES ('','',0,'')");
            check.execute("EXPLAIN DELETE FROM "+pending+" WHERE pipeline_id='' AND subscription_id='' AND stream_seq=0");
            journal.requirePhysicalEvidenceTable(c,PENDING);
            for(String name:historyTables(journal)){
                String history=journal.table(name);
                check.execute("EXPLAIN SELECT pipeline_id,subscription_id,generation,configuration_version,stream_seq,proof FROM "+history);
                check.execute("EXPLAIN INSERT INTO "+history
                        +" (pipeline_id,subscription_id,generation,configuration_version,stream_seq,proof) VALUES ('','','','',0,'')");
                journal.requirePhysicalEvidenceTable(c,name);
            }
        }
    }

    private static void initialize(JdbcJournal journal,Connection c)throws SQLException{
        journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+journal.table(PENDING)
                +" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,stream_seq BIGINT NOT NULL,"
                +"proof VARCHAR(6144) NOT NULL,PRIMARY KEY(pipeline_id,subscription_id))");
        for(String name:historyTables(journal))
            journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+journal.table(name)
                    +" (pipeline_id VARCHAR(128) NOT NULL,subscription_id VARCHAR(128) NOT NULL,generation VARCHAR(128) NOT NULL,"
                    +"configuration_version VARCHAR(36) NOT NULL,stream_seq BIGINT NOT NULL,proof VARCHAR(6144) NOT NULL,"
                    +"PRIMARY KEY(pipeline_id,subscription_id,generation,configuration_version))");
    }

    static void stage(JdbcJournal journal,Connection c,String target,String generation,long epoch,long sequence,
                      String transactionHash,Signed signed)throws Exception{
        var receipt=signed.receipt();
        Grant grant=journal.settings.kafkaPhysicalGrant;
        if(grant==null||!grant.verifies(signed)||!Objects.equals(grant.member(),receipt.member())
                ||!journal.settings.pipelineId.equals(receipt.scope().task())||!target.equals(receipt.target())
                ||!generation.equals(receipt.generation())||receipt.epoch()!=epoch||receipt.sequence()!=sequence
                ||!transactionHash.equals(receipt.transactionSha256()))throw mismatch();
        var fixedTopic=journal.settings.kafkaFixedTopics.get(target);
        if(fixedTopic!=null&&!fixedTopic.equals(receipt.observation().topic()))throw mismatch();
        if(grant.member()!=null){
            long publishedAt=journal.now(c);
            if(publishedAt<1)return;
            signed=grant.published(signed,publishedAt);
        }
        String proof=Codec.encode(signed);
        if(proof.length()>KafkaPhysicalReceipt.MAX_BYTES)throw mismatch();
        String pipeline=journal.settings.pipelineId;
        // The feature may have been off when an ordinary ACK retired an earlier pending observation.
        execute(c,"DELETE FROM "+journal.table(PENDING)
                +" WHERE pipeline_id=? AND subscription_id=? AND stream_seq<=(SELECT acknowledged FROM "
                +journal.table("subscription")+" WHERE pipeline_id=? AND subscription_id=?)",pipeline,target,pipeline,target);
        execute(c,"INSERT INTO "+journal.table(PENDING)
                +" (pipeline_id,subscription_id,stream_seq,proof) VALUES (?,?,?,?)",
                journal.settings.pipelineId,target,sequence,proof);
    }

    static void acknowledge(JdbcJournal journal,Connection c,String target,String generation,long epoch,long sequence)throws Exception{
        String pipeline=journal.settings.pipelineId,encoded;
        try(PreparedStatement p=c.prepareStatement("SELECT stream_seq,proof FROM "+journal.table(PENDING)
                +" WHERE pipeline_id=? AND subscription_id=?")){
            p.setString(1,pipeline);p.setString(2,target);
            try(ResultSet r=p.executeQuery()){
                if(!r.next())return;
                long pending=r.getLong(1);
                if(pending<sequence){
                    encoded=null;
                }else{
                    if(pending!=sequence)throw mismatch();
                    encoded=r.getString(2);
                }
            }
        }
        if(encoded==null){
            execute(c,"DELETE FROM "+journal.table(PENDING)
                    +" WHERE pipeline_id=? AND subscription_id=? AND stream_seq<?",pipeline,target,sequence);
            return;
        }
        Signed signed=Codec.decode(encoded,Signed.class);
        var receipt=signed.receipt();
        if(!pipeline.equals(receipt.scope().task())||!target.equals(receipt.target())||!generation.equals(receipt.generation())
                ||receipt.epoch()>epoch||receipt.sequence()!=sequence)throw mismatch();
        try(PreparedStatement p=c.prepareStatement("SELECT generation,stream_seq,transaction_hash FROM "+journal.table("publication")
                +" WHERE pipeline_id=? AND subscription_id=?")){
            p.setString(1,pipeline);p.setString(2,target);
            try(ResultSet r=p.executeQuery()){
                if(!r.next()||!generation.equals(r.getString(1))||r.getLong(2)!=sequence
                        ||!receipt.transactionSha256().equals(r.getString(3)))throw mismatch();
            }
        }
        String version=receipt.scope().configurationVersion();
        Grant current=journal.settings.kafkaPhysicalGrant;
        boolean attested=receipt.member()!=null;
        if(current!=null&&current.verifies(signed)&&(!attested||current.member()!=null)){
            var historical=attested?current.acknowledged(signed,journal.now(c),epoch):signed;
            if(historical!=null){
                var previous=read(journal,c,target,generation,version,attested);
                if(previous==null){
                    execute(c,"INSERT INTO "+journal.table(history(attested))
                            +" (pipeline_id,subscription_id,generation,configuration_version,stream_seq,proof) VALUES (?,?,?,?,?,?)",
                            pipeline,target,generation,version,sequence,Codec.encode(historical));
                }else if(journal.settings.kafkaFixedTopics.containsKey(target)){
                    if(previous.receipt().sequence()>=sequence||!current.verifies(previous)
                            ||!previous.receipt().observation().topic().equals(receipt.observation().topic()))throw mismatch();
                    if(execute(c,"UPDATE "+journal.table(history(attested))
                            +" SET stream_seq=?,proof=? WHERE pipeline_id=? AND subscription_id=? AND generation=?"
                            +" AND configuration_version=? AND stream_seq=?",sequence,Codec.encode(historical),pipeline,
                            target,generation,version,previous.receipt().sequence())!=1)throw mismatch();
                }
            }
        }
        execute(c,"DELETE FROM "+journal.table(PENDING)+" WHERE pipeline_id=? AND subscription_id=? AND stream_seq=?",
                pipeline,target,sequence);
    }

    static Signed verified(JdbcJournal journal,Connection c,String target,String generation,Grant grant)throws Exception{
        boolean attested=grant.member()!=null;
        Signed value=read(journal,c,target,generation,grant.scope().configurationVersion(),attested);
        if(value==null)return null;
        var receipt=value.receipt();
        if(!grant.verifies(value)||!journal.settings.pipelineId.equals(receipt.scope().task())||!target.equals(receipt.target())
                ||!generation.equals(receipt.generation())||attested!=(receipt.member()!=null)
                ||attested&&(receipt.journalTime()==null||receipt.journalTime().acknowledgedUtcMillis()==null))throw mismatch();
        return value;
    }

    static Signed attested(JdbcJournal journal,Connection c,String target,String generation,String version)throws Exception{
        return read(journal,c,target,generation,version,true);
    }

    private static Signed read(JdbcJournal journal,Connection c,String target,String generation,String version,boolean attested)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT stream_seq,proof FROM "+journal.table(history(attested))
                +" WHERE pipeline_id=? AND subscription_id=? AND generation=? AND configuration_version=?")){
            p.setString(1,journal.settings.pipelineId);p.setString(2,target);p.setString(3,generation);p.setString(4,version);
            try(ResultSet r=p.executeQuery()){
                if(!r.next())return null;
                Signed value=Codec.decode(r.getString(2),Signed.class);
                if(value.receipt().sequence()!=r.getLong(1))throw mismatch();
                return value;
            }
        }
    }

    private static List<String> historyTables(JdbcJournal journal){
        return journal.settings.kafkaPhysicalGrant.member()==null?List.of(HISTORY):List.of(HISTORY,ATTESTED);
    }
    private static String history(boolean attested){return attested?ATTESTED:HISTORY;}
    private static IllegalStateException mismatch(){return new IllegalStateException("KAFKA_PHYSICAL_CHECKPOINT_CHANGED");}
}
