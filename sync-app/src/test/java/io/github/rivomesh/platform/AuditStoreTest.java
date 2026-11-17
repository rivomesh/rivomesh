package io.github.rivomesh.platform;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class AuditStoreTest {
    @TempDir Path directory;
    static AuditStore.Event event(){AuditStore.Event value=new AuditStore.Event();value.kind="TEST";value.status="OK";return value;}
    @Test void committedEventsSurviveRestartAndRetriesDoNotDuplicate()throws Exception{
        AuditStore.Event first=event();
        try(AuditStore store=new AuditStore(directory,1000)){store.append(first);store.append(first);assertEquals(1L,store.page(0,0,10).get("lastSequence"));first.status="CHANGED";assertThrows(AuditStore.Unavailable.class,()->store.append(first));}
        try(AuditStore store=new AuditStore(directory,1000)){Map<String,Object> page=store.page(0,0,10);assertEquals(1,((List<?>)page.get("events")).size());assertEquals(1L,page.get("lastSequence"));}
    }
    @Test void retentionHasAnExplicitAnchorAndExpiredCursorsNeverSkipSilently()throws Exception{
        try(AuditStore store=new AuditStore(directory,3)){
            for(int i=0;i<5;i++)store.append(event());Map<String,Object> page=store.page(0,0,2);assertEquals(2L,page.get("anchorSequence"));assertEquals(4L,page.get("next"));assertEquals(5L,page.get("through"));
            List<?> rows=(List<?>)page.get("events");assertEquals(page.get("anchorHash"),((Map<?,?>)rows.get(0)).get("previousHash"));
            assertThrows(TaskService.Conflict.class,()->store.page(1,5,2));assertEquals(1,((List<?>)store.page(4,5,2).get("events")).size());
            store.append(event());Map<String,Object> oldWindow=store.page(4,5,2);assertNull(oldWindow.get("next"));assertEquals(5L,oldWindow.get("through"));
        }
        try(AuditStore reopened=new AuditStore(directory,3)){assertEquals(3L,reopened.page(0,0,3).get("anchorSequence"));}
    }
    @Test void changedContentBlocksReopen()throws Exception{
        try(AuditStore store=new AuditStore(directory,1000)){store.append(event());}
        try(Connection connection=open();Statement sql=connection.createStatement()){sql.executeUpdate("UPDATE rm_audit_event SET payload=REPLACE(payload,'TEST','OTHER')");}
        assertThrows(AuditStore.Unavailable.class,()->new AuditStore(directory,1000));
    }
    @Test void missingCommittedTailBlocksReopen()throws Exception{
        try(AuditStore store=new AuditStore(directory,1000)){store.append(event());store.append(event());}
        try(Connection connection=open();Statement sql=connection.createStatement()){sql.executeUpdate("DELETE FROM rm_audit_event WHERE event_seq=2");}
        assertThrows(AuditStore.Unavailable.class,()->new AuditStore(directory,1000));
    }
    @Test void concurrentAppendsRemainContiguousAndPagesHaveFixedUpperBounds()throws Exception{
        try(AuditStore store=new AuditStore(directory,1000)){
            ExecutorService writers=Executors.newFixedThreadPool(4);try{List<Future<?>> jobs=new ArrayList<>();for(int i=0;i<40;i++)jobs.add(writers.submit(()->{store.append(event());return null;}));for(Future<?> job:jobs)job.get();}finally{writers.shutdownNow();}
            Map<String,Object> page=store.page(0,0,7);long through=((Number)page.get("through")).longValue();store.append(event());int count=((List<?>)page.get("events")).size();while(page.get("next")!=null){page=store.page(((Number)page.get("next")).longValue(),through,7);count+=((List<?>)page.get("events")).size();}assertEquals(40,count);
        }
    }
    Connection open()throws Exception{return DriverManager.getConnection("jdbc:h2:file:"+directory.resolve("audit/events").toAbsolutePath()+";DB_CLOSE_ON_EXIT=FALSE;WRITE_DELAY=0","sa","");}
}
