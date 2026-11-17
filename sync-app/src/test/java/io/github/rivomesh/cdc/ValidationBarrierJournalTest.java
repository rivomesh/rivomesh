package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ValidationBarrierJournalTest {
    @TempDir Path directory;
    JdbcJournal.Settings settings(){JdbcJournal.Settings s=new JdbcJournal.Settings();s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0";s.pipelineId="validation";return s;}
    SourcePosition position(int end){return new SourcePosition(1,"MYSQL","source",Collections.singletonMap("gtidSet","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-"+end));}
    TableSchema schema(){TableSchema t=new TableSchema();t.id="table";t.database="business";t.name="items";TableSchema.Column c=new TableSchema.Column();c.name="id";c.type=TableSchema.Type.INT64;c.nullable=false;t.columns.add(c);t.primaryKey.add(c.name);return t;}
    Journal.Lease initialize(JdbcJournal journal)throws Exception{journal.initialize(Codec.sha256("config"),"generation",Arrays.asList("selected","offline"));Journal.Lease l=journal.claim("worker",30000);journal.baseline(l,position(1));TableSchema t=schema();journal.saveSchema(l,t);Journal.SnapshotState sp=journal.beginSnapshot(l,"source",t,position(1));journal.finishSnapshot(l,sp);journal.acknowledgeSnapshot(l,"selected",journal.snapshot("source",t.id));return l;}
    SourceTransaction transaction(int end){SourceTransaction t=new SourceTransaction();t.id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:"+end;t.streamGeneration="generation";t.position=position(end);Change b=new Change();b.operation=Change.Operation.BARRIER;b.eventId=t.id+":0";t.changes.add(b);return t;}
    @Test void selectedTargetsCanQuiesceWhileAnUnselectedTargetIsOffline()throws Exception{
        JdbcJournal.Settings s=settings();try(JdbcJournal j=new JdbcJournal(s)){
            Journal.Lease lease=initialize(j);j.pause(lease,"offline","SQL_23514");
            Journal.PipelineState paused=j.pauseDrained("generation",lease.epoch,0,position(1),Collections.singleton("selected"),1);
            assertEquals("PAUSED",paused.state);assertEquals("VALIDATION_BARRIER",paused.errorCode);assertEquals(lease.epoch+1,paused.epoch);
            assertThrows(JdbcJournal.Fenced.class,()->j.heartbeat(lease,30000));assertEquals(position(1),j.captured());
            assertEquals("PAUSED",j.subscriptions().stream().filter(x->x.id.equals("offline")).findFirst().get().state);
        }
        try(JdbcJournal j=new JdbcJournal(s)){assertEquals("PAUSED",j.status().get("run_state"));assertEquals("VALIDATION_BARRIER",j.status().get("errorCode"));assertThrows(JdbcJournal.PipelinePaused.class,()->j.claim("restart",30000));}
    }
    @Test void sourceMovementAndWrongGenerationCannotPauseOrRewriteProgress()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){Journal.Lease l=initialize(j);
            for(String generation:Arrays.asList("other","generation")){
                IllegalStateException failure=assertThrows(IllegalStateException.class,()->j.pauseDrained(generation,l.epoch,0,position(2),Collections.singleton("selected"),1));assertEquals("VALIDATION_BARRIER_MOVED",failure.getMessage());
            }
            assertEquals("RUNNING",j.status().get("run_state"));assertEquals(l.epoch,j.status().get("epoch"));assertEquals(position(1),j.captured());j.heartbeat(l,30000);
        }
    }
    @Test void normalizationAndTargetAckMustBothReachTheCut()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){Journal.Lease l=initialize(j);j.append(l,transaction(2));
            assertEquals("VALIDATION_BARRIER_MOVED",assertThrows(IllegalStateException.class,()->j.pauseDrained("generation",l.epoch,1,position(2),Collections.singleton("selected"),1)).getMessage());
            Journal.Record record=j.read("selected",1).get(0);j.normalize(l,record,Collections.emptyMap(),Collections.emptyMap(),(p,id)->false);
            assertEquals("VALIDATION_TARGET_NOT_DRAINED",assertThrows(IllegalStateException.class,()->j.pauseDrained("generation",l.epoch,1,position(2),Collections.singleton("selected"),1)).getMessage());
            j.acknowledge(l,"selected",1);Journal.PipelineState state=j.pauseDrained("generation",l.epoch,1,position(2),Collections.singleton("selected"),1);assertEquals("PAUSED",state.state);assertEquals(1L,j.status().get("lastSequence"));assertEquals(position(2),j.captured());
        }
    }
    @Test void rowSnapshotsWithoutTheSelectedTargetsSnapshotAckAreNotReady()throws Exception{
        try(JdbcJournal j=new JdbcJournal(settings())){Journal.Lease l=initialize(j);
            assertEquals("VALIDATION_TARGET_SNAPSHOT_INCOMPLETE",assertThrows(IllegalStateException.class,()->j.pauseDrained("generation",l.epoch,0,position(1),Collections.singleton("offline"),1)).getMessage());assertEquals("RUNNING",j.status().get("run_state"));
            assertEquals("VALIDATION_SNAPSHOT_INCOMPLETE",assertThrows(IllegalStateException.class,()->j.pauseDrained("generation",l.epoch,0,position(1),Collections.singleton("selected"),2)).getMessage());assertEquals("RUNNING",j.status().get("run_state"));
        }
    }
}
