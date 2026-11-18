package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class JdbcDeliveryTimingTest {
    @TempDir Path directory;
    private static final String FINGERPRINT=Codec.sha256("delivery timing");
    private static final Instant COMMIT=Instant.parse("2024-01-02T03:04:05.123456Z");

    @Test void timingIsPerTargetAndSurvivesCollectionWithoutTreatingOtherAcksAsMeasurements()throws Exception{
        JdbcJournal.Settings settings=settings();
        try(JdbcJournal journal=new JdbcJournal(settings)){
            initialize(journal);
            Journal.Lease lease=journal.claim("owner",30000);
            assertEquals("UNKNOWN",timing(journal,"fast").get("state"));
            SourceTransaction timed=transaction(1,COMMIT);
            assertEquals(Codec.encode(transaction(1,null)),Codec.encode(timed));
            assertEquals(1,journal.append(lease,timed));
            assertEquals(1,journal.append(lease,transaction(1,COMMIT.plusSeconds(1))));
            journal.acknowledgeApplied(lease,"fast",1);
            Map<?,?> observed=timing(journal,"fast");
            assertEquals("OBSERVED",observed.get("state"));
            assertEquals("1",observed.get("sequence"));
            assertEquals(COMMIT.toString(),observed.get("sourceCommittedAt"));
            assertEquals(Long.toString(Duration.between(COMMIT,Instant.parse((String)observed.get("targetCommitObservedAt"))).toMillis()),
                    observed.get("millis"));
            assertEquals("UNKNOWN",timing(journal,"slow").get("state"));
            assertEquals(0,journal.collect(lease,100));
            assertEquals(1,pendingTimings(journal));
            journal.acknowledge(lease,"slow",1);
            assertEquals(1,journal.collect(lease,100));
            assertEquals(0,pendingTimings(journal));
            assertEquals(observed,timing(journal,"fast"));
            assertEquals("UNKNOWN",timing(journal,"slow").get("state"));
            journal.pause(lease,"fast","SQL_CONSTRAINT");
            assertEquals(observed,timing(journal,"fast"));
            journal.release(lease);
        }
        try(JdbcJournal reopened=new JdbcJournal(settings)){
            assertEquals("OBSERVED",timing(reopened,"fast").get("state"));
            assertEquals("UNKNOWN",timing(reopened,"slow").get("state"));
            Journal.Lease lease=reopened.claim("restarted",30000);
            reopened.resume("fast",1);
            reopened.append(lease,transaction(2,null));
            reopened.acknowledgeApplied(lease,"fast",2);
            assertEquals(Map.of("state","UNKNOWN"),timing(reopened,"fast"));
            reopened.append(lease,transaction(3,Instant.now().plusSeconds(3600)));
            reopened.acknowledgeApplied(lease,"fast",3);
            Map<?,?> invalid=timing(reopened,"fast");
            assertEquals("CLOCK_SKEW",invalid.get("state"));
            assertFalse(invalid.containsKey("millis"));
        }
    }

    @Test void statusCanReadAnExistingJournalBeforeTheOptionalTablesHaveBeenCreated()throws Exception{
        JdbcJournal.Settings original=settings();
        original.deliveryTiming=false;
        try(JdbcJournal journal=new JdbcJournal(original)){initialize(journal);}
        try(JdbcJournal journal=new JdbcJournal(settings())){
            assertEquals("UNKNOWN",timing(journal,"fast").get("state"));
        }
    }

    private JdbcJournal.Settings settings(){
        JdbcJournal.Settings settings=new JdbcJournal.Settings();
        settings.driver="org.h2.Driver";
        settings.url="jdbc:h2:file:"+directory.resolve("delivery-timing")+";WRITE_DELAY=0;LOCK_TIMEOUT=3000";
        settings.pipelineId="delivery-timing";
        settings.deliveryTiming=true;
        return settings;
    }

    private static void initialize(JdbcJournal journal)throws Exception{
        journal.initialize(FINGERPRINT,"generation",List.of("fast","slow"));
    }

    private static Map<?,?> timing(JdbcJournal journal,String subscription)throws Exception{
        Map<?,?> subscriptions=(Map<?,?>)journal.deliveryProgress().get("subscriptions");
        return (Map<?,?>)((Map<?,?>)subscriptions.get(subscription)).get("lastDeliveryTiming");
    }

    private static long pendingTimings(JdbcJournal journal)throws Exception{
        try(Connection connection=journal.connect();Statement query=connection.createStatement();
            ResultSet rows=query.executeQuery("SELECT COUNT(*) FROM "+journal.table("source_commit_time"))){
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    private static SourceTransaction transaction(int sequence,Instant committedAt){
        SourceTransaction tx=new SourceTransaction();
        tx.id="postgresql:"+sequence;
        tx.streamGeneration="generation";
        tx.position=new SourcePosition(1,"POSTGRESQL","source",Map.of("lsn","0/"+sequence));
        tx.sourceCommittedAt=committedAt;
        Change change=new Change();
        change.tableId="table";
        change.eventId=tx.id+":0";
        change.operation=Change.Operation.INSERT;
        change.after=List.of(Cell.value("value"));
        tx.changes.add(change);
        return tx;
    }
}
