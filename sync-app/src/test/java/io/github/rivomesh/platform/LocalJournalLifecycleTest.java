package io.github.rivomesh.platform;

import io.github.rivomesh.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class LocalJournalLifecycleTest {
    @TempDir Path directory;
    static final class Backend implements TaskBackend {
        final AtomicInteger observations=new AtomicInteger();
        public Map<String,Object> status(Config c){observations.incrementAndGet();return Collections.singletonMap("run_state","RUNNING");}
        public Map<String,Object> status(TaskDefinition task){return status(task.pipeline.legacy());}
        public void pause(Config c){} public void resume(Config c,long generation){}
        public Map<String,Object> pause(TaskDefinition task){return Collections.singletonMap("pipelineState","PAUSED");}
        public Map<String,Object> check(Config c){return Collections.emptyMap();}
    }
    PlatformConfig config(){PlatformConfig c=new PlatformConfig();c.port=0;c.authToken=PlatformTest.TOKEN;c.pollMillis=100;c.tasksDirectory=directory.resolve("tasks").toString();c.dataDirectory=directory.resolve("data").toString();return c;}
    void create(TaskService tasks)throws Exception{ObjectNode document=Model.JSON.valueToTree(PlatformTest.pipeline("job"));((ObjectNode)document.get("journal")).remove("path");tasks.create(Model.JSON.writeValueAsString(document),"create_local_journal");}
    void await(PlatformTest.Condition condition)throws Exception{
        long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
        while(System.nanoTime()<deadline){if(condition.get())return;Thread.sleep(50);}fail("LOCAL_JOURNAL_LIFECYCLE_TIMEOUT");
    }
    Properties binding(Path file)throws Exception{Properties properties=new Properties();try(InputStream stream=Files.newInputStream(file)){properties.load(stream);}return properties;}
    void remoteProbe(Properties binding)throws Exception{
        assertNotNull(binding.getProperty("server"));assertNotNull(binding.getProperty("id"));Class.forName("org.h2.Driver");
        try(Connection c=DriverManager.getConnection("jdbc:h2:tcp://"+binding.getProperty("server")+"/"+binding.getProperty("id"),"","");Statement statement=c.createStatement();ResultSet r=statement.executeQuery("SELECT 1")){assertTrue(r.next());assertEquals(1,r.getInt(1));}
    }
    @Test void pausedAndStoppedTasksRetainTheSameUsableTcpEndpointUntilControllerClose()throws Exception{
        PlatformConfig c=config();Backend backend=new Backend();PlatformTest.Workers workers=new PlatformTest.Workers();Path lockFile=directory.resolve("data/journals/job/rivomesh.lock.db");
        try(TaskService tasks=new TaskService(c,backend,workers)){
            create(tasks);tasks.command("job","START",PlatformTest.revision(tasks,"job"),"start_local_journal",-1);await(()->workers.starts.get()==1);Properties original=binding(lockFile);
            tasks.command("job","PAUSE",PlatformTest.revision(tasks,"job"),"pause_local_journal",-1);await(()->"PAUSED".equals(tasks.view("job").get("workerState")));final int pausedReads=backend.observations.get();await(()->backend.observations.get()>=pausedReads+3);
            assertEquals(original.getProperty("server"),binding(lockFile).getProperty("server"));remoteProbe(original);
            tasks.command("job","STOP",PlatformTest.revision(tasks,"job"),"stop_local_journal",-1);await(()->"STOPPED".equals(tasks.view("job").get("workerState")));final int before=backend.observations.get();await(()->backend.observations.get()>=before+3);remoteProbe(original);
        }
        assertFalse(Files.exists(lockFile),"Controller close must release the H2 endpoint");
        Backend reopened=new Backend();try(TaskService tasks=new TaskService(c,reopened,new PlatformTest.Workers())){await(()->reopened.observations.get()>2);remoteProbe(binding(lockFile));assertEquals("STOPPED",tasks.view("job").get("desired"));}
        assertFalse(Files.exists(lockFile));
    }
    @Test void statusOfANeverStartedTaskDoesNotCreateALocalDatabase()throws Exception{
        Backend backend=new Backend();try(TaskService tasks=new TaskService(config(),backend,new PlatformTest.Workers())){create(tasks);await(()->backend.observations.get()>2);assertFalse(Files.exists(directory.resolve("data/journals/job/rivomesh.mv.db")));}
    }
}
