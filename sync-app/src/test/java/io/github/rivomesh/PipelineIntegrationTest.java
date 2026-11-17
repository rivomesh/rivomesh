package io.github.rivomesh;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.cdc.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.v3.integration",matches="true")
class PipelineIntegrationTest {
    @TempDir Path directory;
    Process worker;Path log;
    PipelineConfig config;String first,second;
    Connection target(PipelineConfig.Target t)throws Exception{return JdbcConnections.open(PipelineConfig.driver(t.product),t.url,t.username,t.password);}
    void targetSql(PipelineConfig.Target t,String sql)throws Exception{try(Connection c=target(t);Statement st=c.createStatement()){st.execute(sql);}}
    String quote(PipelineConfig.Target t,String name){return t.product==TargetProduct.MYSQL?Sql.mysql(name):Sql.pg(name);}
    long count(PipelineConfig.Target t,String database,String table,String condition)throws Exception{try(Connection c=target(t);Statement st=c.createStatement();ResultSet rs=st.executeQuery("SELECT COUNT(*) FROM "+quote(t,t.mappings.get(database))+"."+quote(t,table)+" WHERE "+condition)){rs.next();return rs.getLong(1);}}
    void start()throws Exception{
        config.nodeId="node-"+UUID.randomUUID().toString().substring(0,8);Path file=directory.resolve("pipeline.yml");new ObjectMapper(new YAMLFactory()).writeValue(file.toFile(),config);log=directory.resolve(config.nodeId+".log");
        worker=new ProcessBuilder(System.getProperty("java.home")+"/bin/java","-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),Main.class.getName(),"run",file.toAbsolutePath().toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }
    void kill()throws Exception{if(worker!=null&&worker.isAlive()){worker.destroyForcibly();assertTrue(worker.waitFor(10,TimeUnit.SECONDS));}}
    interface Condition{boolean get()throws Exception;}
    void await(Condition condition)throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);Exception last=null;
        while(System.nanoTime()<until){if(worker!=null&&!worker.isAlive())fail("Worker exited\n"+new String(Files.readAllBytes(log),java.nio.charset.StandardCharsets.UTF_8));try{if(condition.get())return;}catch(SQLException e){last=e;}Thread.sleep(50);}
        fail("Timeout: "+last+"\n"+new String(Files.readAllBytes(log),java.nio.charset.StandardCharsets.UTF_8));
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void twoDatabasesFanOutToThreeInstancesWithIsolatedSqlFaultAndCrashRecovery(boolean localJournal)throws Exception{
        Config legacy=ClusterHarness.config();first=legacy.source.database;second=first+"_other";config=new PipelineConfig();config.pipelineId=first;config.source=legacy.source;config.databases=Arrays.asList(first,second);config.chunkSize=1;config.leaseMillis=4000;config.heartbeatMillis=300;config.pollIntervalMillis=50;config.retryDelayMillis=200;
        config.journal.mode="EXTERNAL";config.journal.product=TargetProduct.POSTGRESQL;config.journal.url=System.getProperty("target.url");config.journal.username="sync_test";config.journal.password=ClusterHarness.PASSWORD;config.journal.namespace=first+"_journal";
        config.journal.maxBytes=12000;
        if(localJournal){config.journal.mode="LOCAL";config.journal.path=directory.resolve("journal").toString();}
        for(boolean gauss:Arrays.asList(false,true)){
            PipelineConfig.Target t=new PipelineConfig.Target();t.id=gauss?"gauss":"postgres";t.product=gauss?TargetProduct.GAUSS:TargetProduct.POSTGRESQL;t.url=gauss?"jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10":System.getProperty("target.url");t.username="sync_test";t.password=ClusterHarness.PASSWORD;t.metadataSchema=first+"_meta";t.mappings.put(first,first+"_sales");t.mappings.put(second,first+"_inventory");config.targets.add(t);
        }
        PipelineConfig.Target pg=config.targets.get(0),gauss=config.targets.get(1);String items=Sql.mysql(first)+".items",duplicates=Sql.mysql(second)+".duplicates";
        PipelineConfig.Target mysql=new PipelineConfig.Target();mysql.id="mysql";mysql.product=TargetProduct.MYSQL;mysql.url="jdbc:mysql://mysql-target:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000";mysql.username="root";mysql.password=ClusterHarness.PASSWORD;mysql.metadataSchema=first+"_meta";mysql.mappings.putAll(pg.mappings);config.targets.add(mysql);
        ClusterHarness.sql("mysql","CREATE DATABASE "+Sql.mysql(first));ClusterHarness.sql("mysql","CREATE DATABASE "+Sql.mysql(second));
        try{
            ClusterHarness.sql("mysql","CREATE TABLE "+items+" (id INT PRIMARY KEY,amount INT,email VARCHAR(100),document JSON,twice INT GENERATED ALWAYS AS (amount*2) STORED,label VARCHAR(100) GENERATED ALWAYS AS (CONCAT('n=',amount)) VIRTUAL,UNIQUE(email)) ENGINE=InnoDB");
            ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,amount,email,document) VALUES (1,10,'one','{\"large\":18446744073709551615}')");
            ClusterHarness.sql("mysql","CREATE TABLE "+duplicates+" (label VARCHAR(100),document JSON) ENGINE=InnoDB");ClusterHarness.sql("mysql","INSERT INTO "+duplicates+" VALUES ('duplicate','null'),('duplicate','null')");
            start();await(()->count(pg,first,"items","amount=10 AND twice=20 AND label='n=10'")==1&&count(gauss,first,"items","amount=10")==1&&count(pg,second,"duplicates","TRUE")==2&&count(gauss,second,"duplicates","TRUE")==2);
            try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){await(()->((Number)journal.status().get("snapshotBytes")).longValue()==0);}
            // A target-local checkpoint lock stalls one destination while the other instances keep applying.
            try(Connection stalled=target(gauss)){
                stalled.setAutoCommit(false);try(PreparedStatement lock=stalled.prepareStatement("SELECT epoch FROM "+Sql.pg(gauss.metadataSchema)+".rm3_sink_job WHERE pipeline_id=? AND subscription_id='gauss' FOR UPDATE")){lock.setString(1,config.pipelineId);try(ResultSet rs=lock.executeQuery()){assertTrue(rs.next());}}
                ClusterHarness.sql("mysql","UPDATE "+items+" SET amount=11 WHERE id=1");
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);boolean delivered=false;
                while(System.nanoTime()<deadline){if(count(pg,first,"items","amount=11")==1&&count(mysql,first,"items","amount=11")==1){delivered=true;break;}Thread.sleep(20);}
                assertTrue(delivered,"A stalled destination must not hold the other delivery threads");assertEquals(1,count(gauss,first,"items","amount=10"));stalled.rollback();
            }
            await(()->count(gauss,first,"items","amount=11")==1);
            targetSql(gauss,"ALTER TABLE "+Sql.pg(gauss.mappings.get(first))+".items ADD CONSTRAINT positive_amount CHECK(amount>=0)");
            try(Connection c=ClusterHarness.mysql("mysql");Statement st=c.createStatement()){
                c.setAutoCommit(false);st.executeUpdate("UPDATE "+items+" SET amount=-1 WHERE id=1");st.executeUpdate("INSERT INTO "+duplicates+" VALUES ('new','{\"n\":1}')");c.commit();
            }
            ClusterHarness.sql("mysql","UPDATE "+duplicates+" SET label='changed' WHERE label='duplicate' LIMIT 1");
            await(()->count(pg,first,"items","amount=-1 AND twice=-2 AND label='n=-1'")==1&&count(pg,second,"duplicates","label='changed'")==1);
            try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
                await(()->journal.subscriptions().stream().anyMatch(s->s.id.equals("gauss")&&s.state.equals("PAUSED")));
                assertEquals(1,count(gauss,first,"items","amount=11"));assertEquals(2,count(gauss,second,"duplicates","TRUE"));assertTrue(((Number)journal.status().get("pendingBytes")).longValue()>0);
                kill();ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,amount,email,document) VALUES (2,15,'two','null')");start();await(()->count(pg,first,"items","id=2")==1);
                assertEquals("PAUSED",journal.subscriptions().stream().filter(s->s.id.equals("gauss")).findFirst().get().state);
                targetSql(gauss,"ALTER TABLE "+Sql.pg(gauss.mappings.get(first))+".items DROP CONSTRAINT positive_amount");
                long generation=journal.subscriptions().stream().filter(s->s.id.equals("gauss")).findFirst().get().errorGeneration;journal.resume("gauss",generation);
                await(()->count(gauss,first,"items","id=2")==1&&count(gauss,first,"items","amount=-1 AND twice=-2 AND label='n=-1'")==1&&count(gauss,second,"duplicates","label='changed'")==1&&count(gauss,second,"duplicates","label='new'")==1);
                ClusterHarness.sql("mysql","DELETE FROM "+duplicates+" WHERE label='duplicate' LIMIT 1");await(()->count(pg,second,"duplicates","TRUE")==2&&count(gauss,second,"duplicates","TRUE")==2);
                await(()->count(mysql,first,"items","amount=-1 AND twice=-2 AND label='n=-1'")==1&&count(mysql,first,"items","id=2")==1&&count(mysql,second,"duplicates","TRUE")==2&&count(mysql,second,"duplicates","label='changed'")==1);
                ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,amount,email,document) VALUES (3,0,'',NULL)");await(()->count(mysql,first,"items","id=3 AND email='' AND email IS NOT NULL AND document IS NULL")==1&&count(pg,first,"items","id=3 AND email='' AND email IS NOT NULL AND document IS NULL")==1&&count(gauss,first,"items","id=3 AND email='' AND email IS NOT NULL AND document IS NULL")==1);
                await(()->((Number)journal.status().get("pendingBytes")).longValue()==0);
                // Fill the shared journal behind one paused target. Reserved workspace must
                // still allow normalization, other deliveries, heartbeats, and eventual GC.
                targetSql(gauss,"ALTER TABLE "+Sql.pg(gauss.mappings.get(first))+".items ADD CONSTRAINT block_bulk CHECK(id<100)");
                for(int id=100;id<120;id++)ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,amount,email,document) VALUES ("+id+",5,NULL,'{}')");
                await(()->journal.subscriptions().stream().anyMatch(s->s.id.equals("gauss")&&s.state.equals("PAUSED")));
                await(()->new String(Files.readAllBytes(log),java.nio.charset.StandardCharsets.UTF_8).contains("JOURNAL_BACKPRESSURE"));
                long held=((Number)journal.status().get("lastSequence")).longValue(),heldEpoch=((Number)journal.status().get("epoch")).longValue();
                await(()->((Number)journal.status().get("normalizedSequence")).longValue()==held);
                Thread.sleep(config.source.binlogIdleTimeoutMillis+800);assertTrue(worker.isAlive());assertEquals("RUNNING",journal.status().get("run_state"));assertEquals(held,((Number)journal.status().get("lastSequence")).longValue());assertEquals(heldEpoch,((Number)journal.status().get("epoch")).longValue());assertThrows(JdbcJournal.Busy.class,()->journal.claim("standby-while-backpressured",4000));assertTrue(count(pg,first,"items","id>=100")<20);
                PipelineControls.pause(config);assertEquals("PAUSED",journal.status().get("run_state"));targetSql(gauss,"ALTER TABLE "+Sql.pg(gauss.mappings.get(first))+".items DROP CONSTRAINT block_bulk");
                journal.resume("gauss",journal.subscriptions().stream().filter(s->s.id.equals("gauss")).findFirst().get().errorGeneration);journal.resumePipeline(((Number)journal.status().get("errorGeneration")).longValue());
                await(()->count(pg,first,"items","id>=100")==20&&count(mysql,first,"items","id>=100")==20&&count(gauss,first,"items","id>=100")==20);await(()->((Number)journal.status().get("pendingBytes")).longValue()==0);
                io.github.rivomesh.api.SourcePosition beforeLarge=journal.captured();String large=String.join("",Collections.nCopies(20000,"x"));
                try(Connection source=ClusterHarness.mysql("mysql");PreparedStatement insert=source.prepareStatement("INSERT INTO "+items+" (id,amount,email,document) VALUES (5,35,NULL,?)")){insert.setString(1,"{\"blob\":\""+large+"\"}");insert.executeUpdate();}
                long largeDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);while(!"ERROR".equals(journal.status().get("run_state"))&&System.nanoTime()<largeDeadline)Thread.sleep(50);
                assertEquals("ERROR",journal.status().get("run_state"));assertEquals("TRANSACTION_EXCEEDS_JOURNAL_QUOTA",journal.status().get("errorCode"));assertEquals(beforeLarge,journal.captured());assertEquals(0,count(pg,first,"items","id=5"));assertTrue(worker.waitFor(10,TimeUnit.SECONDS));
                config.journal.maxBytes=256000;journal.resumePipeline(((Number)journal.status().get("errorGeneration")).longValue());start();await(()->count(pg,first,"items","id=5 AND twice=70 AND label='n=35'")==1&&count(mysql,first,"items","id=5")==1&&count(gauss,first,"items","id=5")==1);
                for(PipelineConfig.Target destination:config.targets)try(Connection c=target(destination);Statement st=c.createStatement();ResultSet rs=st.executeQuery("SELECT document FROM "+quote(destination,destination.mappings.get(first))+".items WHERE id=5")){assertTrue(rs.next());assertEquals(large,Codec.JSON.readTree(rs.getString(1)).get("blob").asText());}
                await(()->((Number)journal.status().get("pendingBytes")).longValue()==0);
                Map<String,Object> paused=PipelineControls.pause(config);assertEquals("PAUSED",journal.status().get("run_state"));
                @SuppressWarnings("unchecked") Map<String,String> fences=(Map<String,String>)paused.get("targetFences");assertTrue(fences.values().stream().allMatch("FENCED"::equals));
                ClusterHarness.sql("mysql","INSERT INTO "+items+" (id,amount,email,document) VALUES (4,5,'after-pause','null')");Thread.sleep(800);assertEquals(0,count(pg,first,"items","id=4"));assertEquals(0,count(gauss,first,"items","id=4"));assertEquals(0,count(mysql,first,"items","id=4"));
                long errorGeneration=((Number)journal.status().get("errorGeneration")).longValue();assertThrows(IllegalStateException.class,()->journal.resumePipeline(errorGeneration-1));journal.resumePipeline(errorGeneration);
                await(()->count(pg,first,"items","id=4")==1&&count(gauss,first,"items","id=4")==1&&count(mysql,first,"items","id=4")==1);
                io.github.rivomesh.api.SourcePosition lastGood=journal.captured();ClusterHarness.sql("mysql","ALTER TABLE "+items+" ADD COLUMN needs_ddl_plan INT");
                long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);while(!"ERROR".equals(journal.status().get("run_state"))&&System.nanoTime()<until)Thread.sleep(50);
                assertEquals("ERROR",journal.status().get("run_state"));assertEquals("DDL_PLAN_REQUIRED",journal.status().get("errorCode"));assertEquals(lastGood,journal.captured());assertThrows(JdbcJournal.PipelinePaused.class,()->journal.claim("standby-cannot-bypass",30000));assertTrue(worker.waitFor(10,TimeUnit.SECONDS));
            }
        }finally{
            kill();for(PipelineConfig.Target target:config.targets){List<String> schemas=new ArrayList<>(target.mappings.values());schemas.add(target.metadataSchema);for(String schema:schemas)targetSql(target,(target.product==TargetProduct.MYSQL?"DROP DATABASE IF EXISTS ":"DROP SCHEMA IF EXISTS ")+quote(target,schema)+(target.product==TargetProduct.MYSQL?"":" CASCADE"));}
            targetSql(pg,"DROP SCHEMA IF EXISTS "+Sql.pg(config.journal.namespace)+" CASCADE");ClusterHarness.sql("mysql","DROP DATABASE "+Sql.mysql(first));ClusterHarness.sql("mysql","DROP DATABASE "+Sql.mysql(second));
        }
    }
}
