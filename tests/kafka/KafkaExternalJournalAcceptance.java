package io.github.rivomesh.cdc;

import io.github.rivomesh.SyncFailure;
import io.github.rivomesh.api.SourcePosition;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Runs the exact journal startup and ACK against one disposable SQL server. */
public final class KafkaExternalJournalAcceptance {
    private static final String NAMESPACE="rv_receipt_proof",TASK="physical-external",GENERATION="fixture-generation",TARGET="kafka";
    private static final String FINGERPRINT=Codec.sha256("native fixture fingerprint");
    private static final String PENDING="kafka_physical_pending",HISTORY="kafka_physical_history",ATTESTED="kafka_physical_attested";
    private static final String LIMITED="fixture_receipt_reader";

    public static void main(String[] args)throws Exception{
        boolean mysql="mysql".equals(args[0]);
        String url=args[1],admin=args[2];
        String password=synthetic("RIVOMESH_NATIVE_JOURNAL_SECRET"),restricted=synthetic("RIVOMESH_NATIVE_JOURNAL_READER_SECRET");
        var settings=settings(mysql,url,admin,password);
        try(var journal=new JdbcJournal(settings)){
            journal.initialize(FINGERPRINT,GENERATION,List.of(TARGET));
            assertMissing(journal,PENDING);
            var initial=journal.claim("default-writer",60000);
            journal.append(initial,tx(1));
            journal.acknowledge(initial,TARGET,1);
            journal.release(initial);
            assertAck(journal,1);

            var scope=new KafkaPhysicalReceipt.Scope(UUID.randomUUID().toString(),TASK,UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),Codec.sha256("fixture owner projection"));
            String key=Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
            var member=new KafkaPhysicalReceipt.Member(scope,"fixture-native",UUID.randomUUID().toString(),1,4102444800000L,4102444805000L);
            var grant=new KafkaPhysicalReceipt.Grant(scope,key,new KafkaPhysicalReceipt.MemberWitness(member,key));
            settings.kafkaPhysicalGrant=grant;
            journal.initialize(FINGERPRINT,GENERATION,List.of(TARGET));
            var lease=journal.claim("enabled-writer",60000);
            assertObservedAck(journal,lease,grant);
            journal.release(lease);

            prepareReader(journal,mysql,restricted);
            String readerUrl=mysql?url.replace("/journal?","/"+NAMESPACE+"?"):url;
            var readOnly=settings(mysql,readerUrl,LIMITED,restricted);readOnly.kafkaPhysicalGrant=grant;
            try(var worker=new JdbcJournal(readOnly);var connection=worker.connect()){
                try(var s=connection.createStatement();var result=s.executeQuery("SELECT COUNT(*) FROM "+worker.table(HISTORY))){
                    if(!result.next())throw new AssertionError("Expected real SELECT access to the receipt table");
                }
                deny(()->JdbcKafkaPhysicalHistory.prepare(worker,connection,false),"SQL role without INSERT and DELETE");
                deny(()->worker.initialize(FINGERPRINT,GENERATION,List.of(TARGET)),"startup for that SQL role");
            }
            alterTable(journal,journal.table(ATTESTED)+(mysql?" DROP PRIMARY KEY, ENGINE=MyISAM":" SET UNLOGGED"));
            deny(()->journal.initialize(FINGERPRINT,GENERATION,List.of(TARGET)),mysql?"MyISAM evidence":"UNLOGGED evidence");
            assertAck(journal,2);
            settings.kafkaPhysicalGrant=null;
            journal.initialize(FINGERPRINT,GENERATION,List.of(TARGET));
            var normal=journal.claim("default-again",60000);
            journal.append(normal,tx(3));
            journal.acknowledge(normal,TARGET,3);
            journal.release(normal);
            assertAck(journal,3);
        }
        System.out.println("PASS "+(mysql?"MySQL 8.4":"PostgreSQL 17")
                +" real journal startup, signed UTC receipt ACK, restricted SQL role, unsuitable storage rejection and disabled/default continuation.");
    }

    private static void prepareReader(JdbcJournal journal,boolean mysql,String password)throws Exception{
        String role=mysql?"'"+LIMITED+"'@'%'":LIMITED;
        try(Connection c=journal.connect();var sql=c.createStatement()){
            sql.execute((mysql?"CREATE USER ":"CREATE ROLE ")+role+(mysql?" IDENTIFIED BY '":" LOGIN PASSWORD '")+password+"'");
            if(mysql)sql.execute("GRANT CREATE ON "+NAMESPACE+".* TO "+role);
            else{
                sql.execute("GRANT CREATE ON DATABASE journal TO "+role);
                sql.execute("GRANT USAGE, CREATE ON SCHEMA "+NAMESPACE+" TO "+role);
            }
            for(String table:List.of(PENDING,HISTORY,ATTESTED))sql.execute("GRANT SELECT ON "+journal.table(table)+" TO "+role);
        }
    }

    private static void assertObservedAck(JdbcJournal journal,Journal.Lease lease,KafkaPhysicalReceipt.Grant grant)throws Exception{
        journal.append(lease,tx(2));
        var publication=new JdbcPublicationStore(journal,TARGET,lease);
        publication.activate(lease.epoch);
        // Sequence one was already ACKed before this fixture enabled the native observation.
        publication.committed(lease.epoch,1,"fixture-1",Codec.sha256("fixture transaction 1"));
        String hash=Codec.sha256("fixture transaction 2");
        var topic=new KafkaPhysicalReceipt.Topic(Codec.sha256("test cluster"),"cKtKfC5KQGq-qM1RDCb_3w");
        var observation=new KafkaPhysicalReceipt.Observation(topic,List.of(new KafkaPhysicalReceipt.Wire("orders",15,Codec.sha256("test broker COMMIT"))));
        publication.committed(lease.epoch,2,"fixture-2",hash,grant.sign(TARGET,GENERATION,lease.epoch,2,hash,observation));
        if(publication.kafkaPhysicalReceipt(GENERATION,grant)!=null)throw new AssertionError("Receipt became historical before ACK");
        journal.acknowledge(lease,TARGET,2);
        var historical=publication.kafkaPhysicalReceipt(GENERATION,grant);
        if(historical==null||!grant.verifies(historical)||!grant.member().equals(historical.receipt().member()))throw new AssertionError("Missing exact signed receipt");
        var time=historical.receipt().journalTime();
        if(time.publishedUtcMillis()>time.acknowledgedUtcMillis()||time.acknowledgementEpoch()!=lease.epoch)
            throw new AssertionError("Incorrect journal-local chronology");
        assertAck(journal,2);
    }

    private static void alterTable(JdbcJournal journal,String relation)throws Exception{
        try(var c=journal.connect();var sql=c.createStatement()){sql.execute("ALTER TABLE "+relation);}
    }

    private static void assertAck(JdbcJournal journal,long expected)throws Exception{
        if(journal.subscriptions().getFirst().acknowledged!=expected)throw new AssertionError("Business ACK did not match "+expected);
    }

    private static void assertMissing(JdbcJournal journal,String relation)throws Exception{
        try(var c=journal.connect();var sql=c.createStatement()){
            try(var ignored=sql.executeQuery("SELECT 1 FROM "+journal.table(relation))){
                throw new AssertionError("Default startup created an optional evidence table");
            }catch(SQLException failure){
                String state=failure.getSQLState();
                if(!Set.of("42P01","42S02").contains(state))throw failure;
            }
        }
    }

    private static SourceTransaction tx(int sequence){
        var transaction=new SourceTransaction();transaction.id="fixture-"+sequence;transaction.streamGeneration=GENERATION;
        String uuid="25b043c4-e42d-41ad-960f-53a7f0958a73";
        transaction.position=new SourcePosition(1,"MYSQL","fixture-source",
                Map.of("gtidSet",uuid+(sequence==1?":1":":1-"+sequence),"serverUuid",uuid,
                        "file","binlog.000001","offset",Integer.toString(103+sequence)));
        return transaction;
    }

    private static JdbcJournal.Settings settings(boolean mysql,String url,String username,String password){
        var settings=new JdbcJournal.Settings();settings.driver=mysql?"com.mysql.cj.jdbc.Driver":"org.postgresql.Driver";
        settings.url=url;settings.username=username;settings.password=password;settings.pipelineId=TASK;
        settings.namespace=NAMESPACE;settings.kafkaPublicationTargets=Set.of(TARGET);return settings;
    }

    private static String synthetic(String name){
        String value=System.getenv(name);
        if(value==null||!value.matches("[a-f0-9]{48}"))throw new IllegalArgumentException("Fixture requires synthetic credentials");
        return value;
    }

    @FunctionalInterface private interface Attempt {void run()throws Exception;}
    private static void deny(Attempt action,String scenario)throws Exception{
        try {
            action.run();
            throw new AssertionError("Accepted "+scenario);
        }catch(SyncFailure rejected){
            if(!"KAFKA_PHYSICAL_JOURNAL_NOT_READY".equals(rejected.code))
                throw new AssertionError("Wrong startup rejection for "+scenario,rejected);
        }
    }
}
