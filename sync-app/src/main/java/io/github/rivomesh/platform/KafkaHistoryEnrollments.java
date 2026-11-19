package io.github.rivomesh.platform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.cdc.KafkaHistoryOrigin;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

/** Signed enrollment and compare-and-set readiness share the task authority transaction. */
final class KafkaHistoryEnrollments {
    static final String TABLE="kafka_history_origin";
    private static final ObjectMapper YAML=new ObjectMapper(new YAMLFactory())
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    record Enrollment(KafkaHistoryOrigin origin,boolean ready){}

    private final JdbcControlStore db;
    private final String cluster,authority;
    private final ControlCipher cipher;

    KafkaHistoryEnrollments(JdbcControlStore db,String cluster,String authority,ControlCipher cipher){
        this.db=db;this.cluster=cluster;this.authority=authority;this.cipher=cipher;
    }

    static boolean requested(String yaml)throws Exception {
        JsonNode config=YAML.readTree(yaml);
        if(config==null||config.path("formatVersion").asInt()!=3)return false;
        JsonNode option=config.path("kafkaHistoryFromFirstSend");
        if(option.isMissingNode())return false;
        if(!option.isBoolean())throw new TaskService.Conflict("KAFKA_HISTORY_ORIGIN_CONFIG_INVALID");
        return option.booleanValue();
    }

    static void bootstrap(JdbcControlStore db,Connection c,String suffix)throws Exception {
        JdbcControlStore.execute(c,"CREATE TABLE IF NOT EXISTS "+db.table(TABLE)
                +" (cluster_id VARCHAR(128) NOT NULL,task_id VARCHAR(128) NOT NULL,directory_id VARCHAR(36) NOT NULL,"
                +"initial_version VARCHAR(36) NOT NULL,generation VARCHAR(36) NOT NULL,signature VARCHAR(43) NOT NULL,"
                +"phase_signature VARCHAR(43) NOT NULL,PRIMARY KEY(cluster_id,task_id))"+suffix);
    }

    void enroll(Connection c,GlobalTaskStore.Record record)throws Exception {
        db.requireDurableTaskTable(c,TABLE);
        String generation=UUID.randomUUID().toString();
        String signature=cipher.signature(KafkaHistoryOrigin.signatureDomain(authority,record.id,record.directory,record.configurationVersion,generation));
        KafkaHistoryOrigin origin=new KafkaHistoryOrigin(authority,record.id,record.directory,record.configurationVersion,generation,signature);
        db.update(c,"INSERT INTO "+db.table(TABLE)+" (cluster_id,task_id,directory_id,initial_version,generation,signature,phase_signature)"
                +" VALUES (?,?,?,?,?,?,?)",cluster,record.id,record.directory,record.configurationVersion,generation,signature,
                cipher.signature(origin.pendingDomain()));
    }

    Enrollment require(Connection c,GlobalTaskStore.Record record)throws Exception {
        db.requireDurableTaskTable(c,TABLE);
        try(PreparedStatement p=db.prepare(c,"SELECT directory_id,initial_version,generation,signature,phase_signature FROM "
                +db.table(TABLE)+" WHERE cluster_id=? AND task_id=? FOR UPDATE",cluster,record.id);
                ResultSet r=p.executeQuery()){
            if(!r.next())throw new TaskService.Conflict("KAFKA_HISTORY_ORIGIN_CREATE_REQUIRED");
            KafkaHistoryOrigin origin=new KafkaHistoryOrigin(authority,record.id,r.getString(1),r.getString(2),r.getString(3),r.getString(4));
            String phase=r.getString(5);
            boolean ready=cipher.matches(origin.readyDomain(),phase);
            if(!record.directory.equals(origin.directory())||!cipher.matches(origin.signatureDomain(),origin.signature())
                    ||!ready&&!cipher.matches(origin.pendingDomain(),phase))throw invalid();
            return new Enrollment(origin,ready);
        }
    }

    void ready(Connection c,KafkaHistoryOrigin origin)throws Exception {
        if(db.update(c,"UPDATE "+db.table(TABLE)+" SET phase_signature=? WHERE cluster_id=? AND task_id=? AND signature=?"
                +" AND phase_signature=?",cipher.signature(origin.readyDomain()),cluster,origin.task(),origin.signature(),
                cipher.signature(origin.pendingDomain()))!=1)
            throw invalid();
    }

    private static TaskService.Conflict invalid(){return new TaskService.Conflict("KAFKA_HISTORY_ORIGIN_AUTHORITY_CHANGED");}
}
