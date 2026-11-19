package io.github.rivomesh.cdc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Local origin is installed atomically with the journal job and its initial subscriptions. */
final class JdbcKafkaHistoryOrigin {
    private static final String TABLE="kafka_history_origin",FIRST="kafka_history_first_send";
    private static final Set<String> KINDS=Set.of("SCHEMA","SNAPSHOT_BEGIN","SNAPSHOT_ROW","SNAPSHOT_END","BEGIN","ROW","COMMIT");

    private record WireMessage(String id,String channel,String key,Map<String,String> headers,String bodySha256){}
    private record FirstIntent(KafkaPhysicalReceipt.Scope scope,String target,String generation,long epoch,
                               Set<String> kinds,int count,String wireSha256,String result,
                               @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                               KafkaPhysicalReceipt.Topic physicalTopic){}

    private JdbcKafkaHistoryOrigin(){}

    static void prepare(JdbcJournal journal,Connection c)throws Exception {
        journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+journal.table(TABLE)
                +" (pipeline_id VARCHAR(128) PRIMARY KEY, generation VARCHAR(36) NOT NULL, signature VARCHAR(43) NOT NULL)");
        journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+journal.table(FIRST)
                +" (pipeline_id VARCHAR(128) NOT NULL, subscription_id VARCHAR(128) NOT NULL,"
                +" payload VARCHAR(2048) NOT NULL, signature VARCHAR(43) NOT NULL, PRIMARY KEY(pipeline_id,subscription_id))");
        requireTables(journal,c);
    }

    static void requireTables(JdbcJournal journal,Connection c)throws Exception {
        journal.requirePhysicalEvidenceTable(c,TABLE);
        journal.requirePhysicalEvidenceTable(c,FIRST);
    }

    static void insert(JdbcJournal journal,Connection c,KafkaPhysicalReceipt.Grant grant)throws Exception {
        KafkaHistoryOrigin origin=grant.historyOrigin();
        JdbcJournal.execute(c,"INSERT INTO "+journal.table(TABLE)+" (pipeline_id,generation,signature) VALUES (?,?,?)",
                origin.task(),origin.generation(),origin.signature());
        for(String target:journal.settings.kafkaPublicationTargets){
            JdbcJournal.execute(c,"INSERT INTO "+journal.table(FIRST)+" (pipeline_id,subscription_id,payload,signature) VALUES (?,?,?,?)",
                    origin.task(),target,"",grant.signHistoryIntent(target+"\n"));
        }
    }

    static void verify(JdbcJournal journal,Connection c,KafkaHistoryOrigin origin,String generation)throws Exception {
        if(!origin.generation().equals(generation))throw refused();
        try(PreparedStatement p=c.prepareStatement("SELECT generation,signature FROM "+journal.table(TABLE)+" WHERE pipeline_id=?")){
            p.setString(1,origin.task());
            try(ResultSet r=p.executeQuery()){
                if(!r.next()||!generation.equals(r.getString(1))||!origin.signature().equals(r.getString(2)))throw refused();
            }
        }
    }

    static void beforeSend(JdbcJournal journal,Connection c,String target,String generation,long epoch,
                           List<ConfirmedPublisher.Message> messages)throws Exception {
        KafkaPhysicalReceipt.Grant grant=journal.settings.kafkaPhysicalGrant;
        KafkaHistoryOrigin origin=grant.historyOrigin();
        verify(journal,c,origin,generation);
        String payload;
        try(PreparedStatement p=c.prepareStatement("SELECT payload,signature FROM "+journal.table(FIRST)
                +" WHERE pipeline_id=? AND subscription_id=?")){
            p.setString(1,origin.task());
            p.setString(2,target);
            try(ResultSet r=p.executeQuery()){
                if(!r.next())throw refused();
                payload=r.getString(1);
                if(!grant.verifiesHistoryIntent(target+"\n"+payload,r.getString(2)))throw refused();
            }
        }
        if(!payload.isEmpty())return;
        if(messages.isEmpty()||messages.size()>100)throw refused();
        Set<String> kinds=new TreeSet<>();
        List<WireMessage> wire=new ArrayList<>(messages.size());
        for(ConfirmedPublisher.Message message:messages){
            String kind=message.headers.get("rivomesh_kind");
            if(kind==null||!KINDS.contains(kind))throw refused();
            kinds.add(kind);
            wire.add(new WireMessage(message.id,message.destination,message.key,message.headers,Codec.sha256(message.body)));
        }
        String intent=Codec.encode(new FirstIntent(grant.scope(),target,generation,epoch,kinds,wire.size(),
                Codec.sha256(Codec.encode(wire)),"UNKNOWN",journal.settings.kafkaFixedTopics.get(target)));
        if(JdbcJournal.execute(c,"UPDATE "+journal.table(FIRST)+" SET payload=?,signature=?"
                +" WHERE pipeline_id=? AND subscription_id=? AND payload=?",intent,grant.signHistoryIntent(target+"\n"+intent),
                origin.task(),target,"")!=1)throw refused();
    }

    static IllegalStateException refused(){return new IllegalStateException("KAFKA_HISTORY_ORIGIN_MISSING_OR_CHANGED");}
}
