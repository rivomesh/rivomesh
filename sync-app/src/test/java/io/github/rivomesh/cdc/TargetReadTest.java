package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class TargetReadTest {
    @TempDir Path directory;
    JdbcSink.Settings settings(){JdbcSink.Settings s=new JdbcSink.Settings();s.id="target";s.pipelineId="pipeline";s.generation="generation";s.fingerprint=Codec.sha256("config");s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("target")+";WRITE_DELAY=0;LOCK_TIMEOUT=5000";s.product=TargetProduct.H2;s.databases.put("sales","sales_replica");return s;}
    static void insert(JdbcSink sink,TableSchema table,long epoch,long sequence)throws Exception{sink.apply(epoch,sequence,JdbcSinkTest.transaction((int)sequence,JdbcSinkTest.insert(table,sequence,"same")));}
    @Test void typedRowsMatchCanonicalSourceAndCheckpointChangesAreRejected()throws Exception{
        JdbcSink.Settings settings=settings();TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);JdbcSinkTest.emptySnapshot(sink,1,table);insert(sink,table,1,1);SinkAdapter.Checkpoint checkpoint=sink.checkpoint();
            try(TargetRead read=sink.validationRead(table,checkpoint,5000,4096)){
                List<TargetRead.Row> rows=read.next(1);assertEquals(1,rows.size());assertEquals(RowValues.canonical(table,JdbcSinkTest.row(1,"same")),RowValues.canonical(table,rows.get(0).values));assertTrue(read.next(1).isEmpty());read.verify();
            }
            insert(sink,table,1,2);assertEquals("VALIDATION_TARGET_CHECKPOINT_MISMATCH",assertThrows(IllegalStateException.class,()->sink.validationRead(table,checkpoint,5000,4096)).getMessage());
            SinkAdapter.Checkpoint stale=sink.checkpoint();stale.epoch=2;assertEquals("SINK_FENCED",assertThrows(IllegalStateException.class,()->sink.validationRead(table,stale,5000,4096)).getMessage());
        }
    }
    @Test void readerHoldsTheSamePhysicalFenceUntilClosed()throws Exception{
        JdbcSink.Settings settings=settings();TableSchema table=JdbcSinkTest.schema(false);ExecutorService executor=Executors.newSingleThreadExecutor();
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);JdbcSinkTest.emptySnapshot(sink,1,table);insert(sink,table,1,1);
            Future<?> writer;try(TargetRead read=sink.validationRead(table,sink.checkpoint(),5000,4096)){
                CountDownLatch started=new CountDownLatch(1);writer=executor.submit(()->{started.countDown();insert(sink,table,1,2);return null;});assertTrue(started.await(1,TimeUnit.SECONDS));
                assertThrows(TimeoutException.class,()->writer.get(200,TimeUnit.MILLISECONDS));assertEquals(1,read.next(10).size());assertTrue(read.next(1).isEmpty());
            }
            writer.get(5,TimeUnit.SECONDS);assertEquals(2,sink.checkpoint().sequence);
        }finally{executor.shutdownNow();}
    }
    @Test void keylessReadsPreserveOccurrenceIdsAndPartialCloseReleasesFence()throws Exception{
        JdbcSink.Settings settings=settings();TableSchema table=JdbcSinkTest.schema(true);Journal.SnapshotState snapshot=JdbcSinkTest.snapshot(table);Journal.SnapshotPart part=new Journal.SnapshotPart();part.rows=Arrays.asList(JdbcSinkTest.row(1,"duplicate"),JdbcSinkTest.row(1,"duplicate"));snapshot.nextPart=1;snapshot.rows=2;
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);sink.prepare(1,table);sink.beginSnapshot(1,table,snapshot);sink.snapshotPart(1,table,snapshot.snapshotId,part);sink.finishSnapshot(1,table,snapshot);
            Set<String> ids=new HashSet<>();try(TargetRead read=sink.validationRead(table,sink.checkpoint(),5000,4096)){for(TargetRead.Row row:read.next(10))ids.add(row.rowId);assertTrue(read.next(1).isEmpty());}
            assertEquals(new HashSet<>(Arrays.asList(Codec.sha256(snapshot.snapshotId+":0:0"),Codec.sha256(snapshot.snapshotId+":0:1"))),ids);
            try(TargetRead read=sink.validationRead(table,sink.checkpoint(),5000,4096)){assertEquals(1,read.next(1).size());}
            assertEquals(2,sink.activate(2).epoch);
        }
    }
    @Test void deadlineAndOversizedLobDoNotProduceSuccessfulRows()throws Exception{
        JdbcSink.Settings settings=settings();TableSchema table=JdbcSinkTest.schema(false);
        try(JdbcSink sink=new JdbcSink(settings)){
            sink.activate(1);JdbcSinkTest.emptySnapshot(sink,1,table);insert(sink,table,1,1);
            try(TargetRead read=sink.validationRead(table,sink.checkpoint(),1000,4096)){Thread.sleep(1200);assertEquals("VALIDATION_TARGET_READ_TIMED_OUT",assertThrows(IllegalStateException.class,()->read.next(1)).getMessage());}
            try(Connection c=JdbcConnections.open(settings.driver,settings.url,"","");PreparedStatement p=c.prepareStatement("UPDATE \"sales_replica\".\"items\" SET \"document\"=?")){p.setString(1,"{\"value\":\""+String.join("",Collections.nCopies(5000,"x"))+"\"}");p.executeUpdate();}
            try(TargetRead read=sink.validationRead(table,sink.checkpoint(),5000,1024)){assertEquals("VALIDATION_ROW_TOO_LARGE",assertThrows(IllegalStateException.class,()->read.next(1)).getMessage());}
            assertEquals(1,sink.checkpoint().sequence);
        }
    }
}
