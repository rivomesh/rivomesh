package io.github.rivomesh.cdc;

import io.github.rivomesh.*;
import redis.clients.jedis.*;
import redis.clients.jedis.util.KeyValue;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.messaging.integration",matches="true")
class RedisMessagePublisherIntegrationTest {
    @TempDir Path directory;
    PipelineConfig.Target target(String host){PipelineConfig.Target t=new PipelineConfig.Target();t.id="fast";t.product=TargetProduct.REDIS_STREAMS;t.url="redis://"+host+":6379/0";t.messaging=new PipelineConfig.Messaging();t.messaging.stream="fault_"+UUID.randomUUID().toString().replace("-","");t.messaging.requestTimeoutMillis=1000;t.mappings.put("sales","replica");return t;}
    @Test void connectionReplacementCannotConfirmOldWritesAndDeletedStreamsAreNotRecreated()throws Exception{
        PipelineConfig.Target target=target("redis");JdbcJournal.Settings settings=new JdbcJournal.Settings();settings.pipelineId="pipeline";settings.driver="org.h2.Driver";settings.url="jdbc:h2:file:"+directory.resolve("journal")+";WRITE_DELAY=0";AtomicBoolean reconnect=new AtomicBoolean();
        try(Jedis admin=new Jedis("redis",6379);JdbcJournal journal=new JdbcJournal(settings)){
            journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("fast"));Journal.Lease lease=journal.claim("owner",30000);TableSchema table=JdbcSinkTest.schema(false);
            try(MessageSink sink=new MessageSink("fast",target.mappings,new JdbcPublicationStore(journal,"fast",lease),()->{
                Jedis client=new Jedis("redis",6379){@Override public KeyValue<Long,Long> waitAOF(long local,long replicas,long timeout){if(reconnect.getAndSet(false))disconnect();return super.waitAOF(local,replicas,timeout);}};
                return new RedisMessagePublisher(target,"isolated-test-binding",client);
            },65536)){
                sink.activate(lease.epoch);new MessageSinkTest().empty(journal,lease,sink,table);SourceTransaction tx=JdbcSinkTest.transaction(2,JdbcSinkTest.insert(table,1,"confirmed"));reconnect.set(true);
                SyncFailure changed=assertThrows(SyncFailure.class,()->sink.apply(lease.epoch,1,tx));assertEquals("REDIS_CONNECTION_CHANGED_DURING_CONFIRM",changed.code);assertTrue(changed.retryable);assertEquals(0,sink.checkpoint().sequence);
                sink.apply(lease.epoch,1,tx);assertEquals(1,sink.checkpoint().sequence);List<MessageEvent> begins=new ArrayList<>();for(redis.clients.jedis.resps.StreamEntry entry:admin.xrange(target.messaging.stream,"-","+")){MessageEvent event=Codec.decode(entry.getFields().get("body"),MessageEvent.class);if("BEGIN".equals(event.kind))begins.add(event);}assertEquals(2,begins.size());assertEquals(begins.get(0).id,begins.get(1).id);
                admin.del(target.messaging.stream);SourceTransaction next=JdbcSinkTest.transaction(3,JdbcSinkTest.insert(table,2,"after-loss"));assertEquals("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED",assertThrows(SyncFailure.class,()->sink.apply(lease.epoch,2,next)).code);assertFalse(admin.exists(target.messaging.stream));assertEquals(1,sink.checkpoint().sequence);assertThrows(SyncFailure.class,()->sink.apply(lease.epoch,2,next));assertFalse(admin.exists(target.messaging.stream));
            }finally{admin.del(target.messaging.stream);admin.hdel(RedisMessagePublisher.OWNERS,target.messaging.stream);}
        }
    }
    @Test void readOnlyChecksRejectUnsafePersistenceAndEvictionWithoutCreatingKeys()throws Exception{
        PipelineConfig.Target target=target("redis-unsafe");try(Jedis admin=new Jedis("redis-unsafe",6379)){
            long before=admin.dbSize();assertEquals("REDIS_AOF_REQUIRED",assertThrows(SyncFailure.class,()->MessagePublishers.check(target)).code);assertEquals(before,admin.dbSize());
            try{admin.configSet("appendonly","yes");assertEquals("REDIS_NOEVICTION_REQUIRED",assertThrows(SyncFailure.class,()->MessagePublishers.check(target)).code);assertEquals(before,admin.dbSize());}
            finally{admin.configSet("appendonly","no");}
        }
    }
    @Test void missingReplicaFsyncCannotAdvanceReceiptAndReplayRetainsEventIdentity()throws Exception{
        PipelineConfig.Target target=target("redis");target.messaging.replicas=2;
        JdbcJournal.Settings settings=new JdbcJournal.Settings();settings.pipelineId="replica-confirm";settings.driver="org.h2.Driver";settings.url="jdbc:h2:file:"+directory.resolve("replica-journal")+";WRITE_DELAY=0";
        try(Jedis admin=new Jedis("redis",6379);JdbcJournal journal=new JdbcJournal(settings)){
            journal.initialize(Codec.sha256("config"),"generation",Collections.singletonList("fast"));Journal.Lease lease=journal.claim("owner",30000);TableSchema table=JdbcSinkTest.schema(false);
            try(MessageSink sink=new MessageSink("fast",target.mappings,new JdbcPublicationStore(journal,"fast",lease),()->new RedisMessagePublisher(target,"replica-test-binding"),65536)){
                sink.activate(lease.epoch);SyncFailure failure=assertThrows(SyncFailure.class,()->sink.prepare(lease.epoch,table));assertEquals("REDIS_DURABILITY_NOT_CONFIRMED",failure.code);assertTrue(failure.retryable);assertEquals(0,sink.checkpoint().sequence);
                // Test-only change: production durability settings are fingerprinted configuration.
                target.messaging.replicas=1;new MessageSinkTest().empty(journal,lease,sink,table);
                List<MessageEvent> schemas=new ArrayList<>();for(redis.clients.jedis.resps.StreamEntry entry:admin.xrange(target.messaging.stream,"-","+")){MessageEvent event=Codec.decode(entry.getFields().get("body"),MessageEvent.class);if("SCHEMA".equals(event.kind))schemas.add(event);}assertEquals(2,schemas.size());assertEquals(schemas.get(0).id,schemas.get(1).id);
            }finally{admin.del(target.messaging.stream);admin.hdel(RedisMessagePublisher.OWNERS,target.messaging.stream);}
        }
    }
    @Test void initialHandshakeEofIsRetryableEvenWithoutAnIOExceptionCause()throws Exception{
        // A real socket accepts the Jedis SETINFO handshake, then disappears before replying.
        try(ServerSocket server=new ServerSocket(0,1,InetAddress.getLoopbackAddress())){
            server.setSoTimeout(5000);ExecutorService thread=Executors.newSingleThreadExecutor();
            try{
                Future<?> closed=thread.submit(()->{try(Socket socket=server.accept()){socket.setSoTimeout(5000);assertTrue(socket.getInputStream().read()>=0);}catch(Exception e){throw new RuntimeException(e);}});
                PipelineConfig.Target target=target("127.0.0.1");target.url="redis://127.0.0.1:"+server.getLocalPort()+"/0";
                SyncFailure failure=assertThrows(SyncFailure.class,()->MessagePublishers.check(target));assertEquals("REDIS_CONNECTION_LOST",failure.code);assertTrue(failure.retryable);assertTrue(Sql.retryable(failure));closed.get(5,TimeUnit.SECONDS);
            }finally{thread.shutdownNow();}
        }
    }
    @Test void aReopenedRawSocketCannotWriteIntoTheDefaultDatabase()throws Exception{
        PipelineConfig.Target target=target("redis");target.url="redis://redis:6379/2";String stream=target.messaging.stream;
        ConfirmedPublisher.Message message=new ConfirmedPublisher.Message();message.id="one";message.destination="replica.items";message.body="{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try(Jedis admin=new Jedis("redis",6379)){
            assertFalse(admin.exists(stream));admin.select(2);
            try{
                Jedis socket=RedisMessagePublisher.connect(target);
                try(RedisMessagePublisher publisher=new RedisMessagePublisher(target,"nondefault-db",socket)){
                    publisher.prepare(Collections.singleton(message.destination));socket.disconnect();SyncFailure changed=assertThrows(SyncFailure.class,()->publisher.publish(Collections.singletonList(message)));assertEquals("REDIS_CONNECTION_LOST",changed.code);assertTrue(changed.retryable);assertEquals(0,admin.xlen(stream));
                }
                try(RedisMessagePublisher reopened=new RedisMessagePublisher(target,"nondefault-db")){reopened.require(Collections.singleton(message.destination));reopened.publish(Collections.singletonList(message));}assertEquals(1,admin.xlen(stream));admin.select(0);assertFalse(admin.exists(stream));
            }finally{admin.select(2);admin.del(stream);admin.hdel(RedisMessagePublisher.OWNERS,stream);}
        }
    }
}
