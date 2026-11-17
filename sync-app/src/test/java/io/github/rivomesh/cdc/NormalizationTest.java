package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NormalizationTest {
    @TempDir Path directory;
    JdbcJournal.Settings settings(){JdbcJournal.Settings s=new JdbcJournal.Settings();s.driver="org.h2.Driver";s.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0";s.pipelineId="pipeline";return s;}
    static Change change(TableSchema t,String id,Change.Operation operation,List<Cell> before,List<Cell> after){Change ch=new Change();ch.tableId=t.id;ch.eventId=id;ch.operation=operation;ch.before=before;ch.after=after;return ch;}
    static void contract(JdbcJournal journal)throws Exception{
        new JournalTest().initialize(journal);Journal.Lease lease=journal.claim("owner",30000);TableSchema t=JdbcSinkTest.schema(true);
        Journal.SnapshotState snapshot=journal.beginSnapshot(lease,"source",t,new JournalTest().transaction(1).position);
        List<Cell> duplicate=JdbcSinkTest.row(1,"duplicate");journal.appendSnapshot(lease,snapshot,0,Collections.singletonList(duplicate));journal.appendSnapshot(lease,snapshot,1,Collections.singletonList(duplicate));journal.finishSnapshot(lease,snapshot);
        assertFalse(journal.seedIdentities(lease,t,snapshot));assertTrue(journal.seedIdentities(lease,t,snapshot));long original=((Number)journal.status().get("identityBytes")).longValue();assertTrue(journal.seedIdentities(lease,t,snapshot));assertEquals(original,((Number)journal.status().get("identityBytes")).longValue());
        Map<String,TableSchema> schemas=Collections.singletonMap(t.id,t);Map<String,SourcePosition> baselines=Collections.singletonMap(t.id,snapshot.baseline);
        NormalizedJournal.BaselineFilter filter=(position,gtid)->gtid.endsWith(":1");
        journal.append(lease,JdbcSinkTest.transaction(1,change(t,"already-in-snapshot",Change.Operation.DELETE,duplicate,null)));
        SourceTransaction before=journal.normalize(lease,journal.read("fast",1).get(0),schemas,baselines,filter);assertTrue(before.changes.isEmpty());assertEquals(original,((Number)journal.status().get("identityBytes")).longValue());journal.acknowledge(lease,"fast",1);
        // JSON key order and numeric scale differ from the snapshot but have the same row value.
        List<Cell> equivalent=new ArrayList<>(duplicate);equivalent.set(2,Cell.value("{\"null\":null,\"integer\":18446744073709551615.0}"));
        SourceTransaction raw=JdbcSinkTest.transaction(2,change(t,"delete-one",Change.Operation.DELETE,equivalent,null),change(t,"update-other",Change.Operation.UPDATE,equivalent,JdbcSinkTest.row(2,"changed")));
        journal.append(lease,raw);Journal.Record record=journal.read("fast",1).get(0);SourceTransaction normalized=journal.normalize(lease,record,schemas,baselines,filter);
        assertNotEquals(normalized.changes.get(0).rowId,normalized.changes.get(1).rowId);assertEquals(64,normalized.changes.get(1).rowId.length());
        assertEquals(Codec.encode(normalized),Codec.encode(journal.normalize(lease,record,schemas,baselines,filter)));journal.acknowledge(lease,"fast",2);
        long identityBytes=((Number)journal.status().get("identityBytes")).longValue();assertTrue(identityBytes<original);
        List<Journal.Record> slow=journal.read("slow",2);for(Journal.Record r:slow)journal.normalize(lease,r,schemas,baselines,filter);assertEquals(identityBytes,((Number)journal.status().get("identityBytes")).longValue(),"A slow target must replay the stored normalization, not mutate the identity map again");
        SourceTransaction broken=JdbcSinkTest.transaction(3,change(t,"insert-before-failure",Change.Operation.INSERT,null,JdbcSinkTest.row(3,"temporary")),change(t,"missing-occurrence",Change.Operation.DELETE,duplicate,null));journal.append(lease,broken);Journal.Record fault=journal.read("fast",1).get(0);
        assertThrows(IllegalStateException.class,()->journal.normalize(lease,fault,schemas,baselines,filter));assertEquals(2,((Number)journal.status().get("normalizedSequence")).longValue());assertEquals(identityBytes,((Number)journal.status().get("identityBytes")).longValue());
        try(Connection c=journal.connect();PreparedStatement p=c.prepareStatement("SELECT COUNT(*) FROM "+journal.table("identity")+" WHERE pipeline_id=?")){p.setString(1,journal.settings.pipelineId);try(ResultSet rs=p.executeQuery()){rs.next();assertEquals(1,rs.getLong(1));}}
        journal.acknowledgeSnapshot(lease,"fast",snapshot);assertEquals(0,journal.collectSnapshots(lease,100));journal.acknowledgeSnapshot(lease,"slow",snapshot);assertEquals(2,journal.collectSnapshots(lease,100));assertEquals(0,((Number)journal.status().get("snapshotBytes")).longValue());
        Journal.SnapshotState released=journal.snapshot("source",t.id);assertFalse(released.retained);assertTrue(journal.seedIdentities(lease,t,released));assertThrows(IllegalStateException.class,()->journal.snapshotParts("source",t.id,snapshot.snapshotId,0,1));
        journal.release(lease);Journal.Lease next=journal.claim("next",30000);assertEquals(Codec.encode(normalized),Codec.encode(journal.normalize(next,record,schemas,baselines,filter)));
    }
    @Test void duplicateOccurrencesBaselineFilteringAtomicFailureAndSlowReplay()throws Exception{try(JdbcJournal journal=new JdbcJournal(settings())){contract(journal);}}
    @Test void normalizationCannotJumpOverACommitOrInventRawContent()throws Exception{
        try(JdbcJournal journal=new JdbcJournal(settings())){
            new JournalTest().initialize(journal);Journal.Lease lease=journal.claim("owner",30000);SourceTransaction first=JdbcSinkTest.transaction(1),second=JdbcSinkTest.transaction(2);journal.append(lease,first);journal.append(lease,second);
            Journal.Record r=journal.read("fast",2).get(1);assertThrows(IllegalStateException.class,()->journal.normalize(lease,r,Collections.emptyMap(),Collections.emptyMap(),(p,id)->false));
            r.sequence=1;assertThrows(IllegalStateException.class,()->journal.normalize(lease,r,Collections.emptyMap(),Collections.emptyMap(),(p,id)->false));assertEquals(0,((Number)journal.status().get("normalizedSequence")).longValue());
        }
    }
    @Test void floatRenderingDifferencesDoNotLoseKeylessOccurrences(){
        TableSchema table=new JournalTest().schema();table.columns.get(0).type=TableSchema.Type.FLOAT32;
        assertEquals(JdbcNormalization.canonical(table,Collections.singletonList(Cell.value("1.0000001"))),JdbcNormalization.canonical(table,Collections.singletonList(Cell.value("1.0000001192092896"))));
        table.columns.get(0).type=TableSchema.Type.FLOAT64;
        assertEquals(JdbcNormalization.canonical(table,Collections.singletonList(Cell.value("4.9406564584124654E-324"))),JdbcNormalization.canonical(table,Collections.singletonList(Cell.value("4.9E-324"))));
        assertEquals(JdbcNormalization.canonical(table,Collections.singletonList(Cell.value("-0.0"))),JdbcNormalization.canonical(table,Collections.singletonList(Cell.value("0.0"))));
    }
    @Test void aFullJournalCanNormalizeItsReservedCommitAndReclaimExactlyOnce()throws Exception{
        for(boolean keyless:Arrays.asList(false,true)){
            TableSchema table=JdbcSinkTest.schema(keyless);Map<String,TableSchema> schemas=Collections.singletonMap(table.id,table);SourceTransaction tx=JdbcSinkTest.transaction(2,JdbcSinkTest.insert(table,1,"reserved"));
            long reserve=NormalizationBudget.reserve(tx,schemas),raw=Codec.encode(tx).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;JdbcJournal.Settings s=settings();s.url="jdbc:h2:file:"+directory.resolve("reserved-"+keyless)+";WRITE_DELAY=0";s.maxPendingBytes=raw+reserve;
            try(JdbcJournal j=new JdbcJournal(s)){
                new JournalTest().initialize(j);Journal.Lease lease=j.claim("owner",30000);io.github.rivomesh.api.SourcePosition baseline=JdbcSinkTest.transaction(1).position;j.baseline(lease,baseline);Journal.SnapshotState snapshot=j.beginSnapshot(lease,"source",table,baseline);j.finishSnapshot(lease,snapshot);assertTrue(j.seedIdentities(lease,table,snapshot));
                j.append(lease,tx,reserve);assertEquals(s.maxPendingBytes,((Number)j.status().get("pendingBytes")).longValue());assertThrows(JdbcJournal.Capacity.class,()->j.append(lease,JdbcSinkTest.transaction(3),reserve));
                Journal.Record record=j.read("fast",1).get(0);SourceTransaction normalized=j.normalize(lease,record,schemas,Collections.singletonMap(table.id,baseline),(position,id)->id.endsWith(":1"));assertEquals(1,normalized.changes.size());long charged=((Number)j.status().get("pendingBytes")).longValue()+((Number)j.status().get("identityBytes")).longValue();assertTrue(charged<=s.maxPendingBytes);assertEquals(Codec.encode(normalized),Codec.encode(j.normalize(lease,record,schemas,Collections.singletonMap(table.id,baseline),(position,id)->false)));assertEquals(charged,((Number)j.status().get("pendingBytes")).longValue()+((Number)j.status().get("identityBytes")).longValue());
                j.acknowledge(lease,"fast",1);j.acknowledge(lease,"slow",1);assertEquals(1,j.collect(lease,100));assertEquals(0,((Number)j.status().get("pendingBytes")).longValue());assertEquals(tx.position,j.captured());
            }
        }
    }
}
