package io.github.rivomesh;

import io.github.rivomesh.platform.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.integration",matches="true")
class PlatformIntegrationTest {
    @TempDir Path folder;
    @Test void independentWorkersPauseResumeAndControllerRestartKeepDurableProgress()throws Exception{
        Config one=ClusterHarness.config(),two=ClusterHarness.config();two.source.serverId=one.source.serverId+1;
        PlatformConfig platform=new PlatformConfig();platform.authToken="integration-test-token-long-enough";platform.pollMillis=100;
        platform.tasksDirectory=folder.resolve("tasks").toString();platform.dataDirectory=folder.resolve("data").toString();
        try(ClusterHarness a=new ClusterHarness(one);ClusterHarness b=new ClusterHarness(two)){
            assertTrue(new TaskBackend.Database().status(one).isEmpty(),"Uninitialized metadata must be discoverable without DDL");
            try(TaskService service=new TaskService(platform)){
                service.create(Model.JSON.writeValueAsString(one),"create_first");service.create(Model.JSON.writeValueAsString(two),"create_second");
                action(service,one.jobId,"START","start_first",-1);action(service,two.jobId,"START","start_second",-1);
                try { a.await(()->"STREAMING".equals(a.store.phase())&&a.count("1=1")==40,90); }
                catch(AssertionError error) {
                    StringBuilder detail=new StringBuilder(Model.JSON.writeValueAsString(service.list()));
                    try(java.util.stream.Stream<Path> logs=Files.list(folder.resolve("data/logs"))){
                        logs.filter(Files::isRegularFile).forEach(path->{try{detail.append('\n').append(path.getFileName()).append('\n').append(new String(Files.readAllBytes(path),java.nio.charset.StandardCharsets.UTF_8));}catch(Exception ignored){}});
                    }
                    throw new AssertionError(detail.toString(),error);
                }
                b.await(()->"STREAMING".equals(b.store.phase())&&b.count("1=1")==40,90);
                action(service,one.jobId,"PAUSE","pause_first",-1);
                a.await(()->"PAUSED".equals(a.store.runState()),30);
                assertThrows(StateStore.JobPausedException.class,()->new StateStore(one).claim());
                ClusterHarness.sql("mysql","INSERT INTO "+a.sourceTable()+" VALUES(41,200,'after-pause')");
                ClusterHarness.sql("mysql","INSERT INTO "+b.sourceTable()+" VALUES(41,200,'independent')");
                b.await(()->b.count("id=41")==1,30);assertEquals(0,a.count("id=41"));
                long generation=((Number)a.store.status().get("error_count")).longValue();
                a.await(()->service.view(one.jobId).get("operations").toString().contains("SHARED_PAUSED"),20);
                action(service,one.jobId,"RESUME","resume_first",generation);
                a.await(()->a.count("id=41")==1,30);
                assertEquals(0,((Number)a.store.status().get("pending_transactions")).intValue());
            }
            try(TaskService restarted=new TaskService(platform)){
                ClusterHarness.sql("mysql","INSERT INTO "+a.sourceTable()+" VALUES(42,300,'restart')");
                a.await(()->a.count("id=42")==1&&"RUNNING".equals(a.store.runState()),45);
                assertEquals("RUNNING",restarted.view(two.jobId).get("desired"));
                assertEquals(40,a.store.tables().get(0).copiedRows,"Controller restart must not repeat snapshot");
            }
        }
    }
    private void action(TaskService service,String id,String action,String request,long generation)throws Exception{
        service.command(id,action,(String)service.view(id).get("revision"),request,generation);
    }
    @Test void administrativePauseFencesOldOwnersAndOldResumeCannotClearNewFailures()throws Exception{
        Config config=ClusterHarness.config();StateStore store=new StateStore(config);
        try{
            store.initialize();store.create("test",new Model.Position("bin.000001",4),Collections.emptyList());store.claim();
            new StateStore(config).pause();
            assertThrows(StateStore.LeaseLostException.class,store::heartbeat);
            assertThrows(StateStore.JobPausedException.class,()->new StateStore(config).claim());
            long generation=((Number)store.status().get("error_count")).longValue();
            new StateStore(config).resume(generation);new StateStore(config).resume(generation);
            StateStore next=new StateStore(config);next.claim();next.recordFailure(new IllegalStateException("new failure"),"APPLY");
            assertThrows(IllegalStateException.class,()->new StateStore(config).resume(generation));
            assertEquals("PAUSED",store.runState());
            assertEquals(generation+1,((Number)store.status().get("error_count")).longValue());
        }finally{
            try(java.sql.Connection c=store.connect()){
                Sql.execute(c,"DROP SCHEMA IF EXISTS "+Sql.pg(config.target.schema)+" CASCADE");
                Sql.execute(c,"DROP SCHEMA IF EXISTS "+Sql.pg(config.target.metadataSchema)+" CASCADE");
            }
        }
    }
    @Test void consoleBoundsTableMetadataWithoutLosingTheTotalCount()throws Exception{
        Config config=ClusterHarness.config();StateStore store=new StateStore(config);
        try{
            store.initialize();List<Model.TableProgress> tables=new ArrayList<>();
            for(int i=0;i<105;i++){Model.TableProgress p=new Model.TableProgress();p.table=new Model.Table();p.table.name=String.format("table_%03d",i);tables.add(p);}
            store.create("test",new Model.Position("bin.000001",4),tables);
            Map<String,Object> status=new TaskBackend.Database().status(config);
            assertEquals(105,((Number)status.get("table_count")).intValue());assertEquals(100,((List<?>)status.get("tables")).size());
            assertEquals(105,((List<?>)store.status().get("tables")).size(),"CLI full status remains available");
        }finally{
            try(java.sql.Connection c=store.connect()){
                Sql.execute(c,"DROP SCHEMA IF EXISTS "+Sql.pg(config.target.schema)+" CASCADE");
                Sql.execute(c,"DROP SCHEMA IF EXISTS "+Sql.pg(config.target.metadataSchema)+" CASCADE");
            }
        }
    }
}
