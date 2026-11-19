package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.KafkaPhysicalReceipt;
import io.github.rivomesh.cdc.TargetProduct;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class KafkaPhysicalWorkerGrantTest {
    @TempDir Path directory;

    @Test void managedLaunchCarriesExactSharedActiveVersionAndTheRealProjection()throws Exception{
        GlobalControlHttpTest fixture=new GlobalControlHttpTest();fixture.directory=directory;
        PlatformConfig controller=fixture.config("a"),owner=fixture.config("b");
        owner.kafkaPhysicalOwnershipEvidence=true;
        CapturingWorker captured=new CapturingWorker();
        try(TaskService first=new TaskService(controller,new PlatformTest.Backend(),new PlatformTest.Workers());
            TaskService second=new TaskService(owner,new ManagedFailureReviewTest.Backend(),captured);
            ConsoleServer a=new ConsoleServer(controller,first);ConsoleServer b=new ConsoleServer(owner,second)){
            a.start();b.start();String api=fixture.base(a);
            PipelineConfig pipeline=PlatformTest.pipeline("job");
            var target=pipeline.targets.getFirst();target.id="kafka";target.product=TargetProduct.KAFKA;
            target.url="kafka://fixture-broker:9092";target.messaging=new PipelineConfig.Messaging();target.messaging.topic="write-evidence";
            ObjectNode configuration=Model.JSON.valueToTree(pipeline);
            ((ObjectNode)configuration.path("journal")).remove("path");
            ObjectNode create=Model.JSON.createObjectNode().put("ownerNodeId","b");create.set("config",configuration);
            var created=PlatformTest.http(api,"/api/tasks",PlatformTest.TOKEN,create.toString(),"create_grant_job");
            assertEquals(202,created.status,created.text);
            PlatformTest.await(()->fixture.completed(api,"job","create_grant_job"));
            String active=task(fixture,api).path("activeConfigurationVersion").asText();
            fixture.action(api,"job","START","start_grant_first",PlatformTest.TOKEN);
            var firstGrant=captured.grants.poll(8,TimeUnit.SECONDS);
            assertNotNull(firstGrant);assertEquals(active,firstGrant.scope().configurationVersion());
            assertEquals(first.globalTaskIdentity(),firstGrant.scope().authorityId());
            assertEquals(second.ownerDirectoryIdentity(),firstGrant.scope().ownerDirectory());

            PlatformTest.await(()->fixture.completed(api,"job","start_grant_first"));
            fixture.action(api,"job","STOP","stop_grant_first",PlatformTest.TOKEN);
            PlatformTest.await(()->fixture.completed(api,"job","stop_grant_first")
                    &&"STOPPED".equals(task(fixture,api).path("workerState").asText()));
            String route="/api/tasks/job/configuration";
            JsonNode saved=fixture.get(api,route,PlatformTest.TOKEN);
            ObjectNode config=(ObjectNode)saved.path("config").deepCopy();
            ((ObjectNode)config.path("source")).put("password","source-fixture-rotation");
            ObjectNode edit=Model.JSON.createObjectNode().put("expectedRevision",saved.path("revision").asText());
            edit.set("config",config);
            var edited=PlatformConfigurationTest.request(api,route,"PUT",PlatformTest.TOKEN,edit.toString(),"edit_grant_job");
            assertEquals(202,edited.status,edited.text);
            PlatformTest.await(()->fixture.completed(api,"job","edit_grant_job"));
            String replacement=task(fixture,api).path("activeConfigurationVersion").asText();
            assertNotEquals(active,replacement);
            fixture.action(api,"job","START","start_grant_second",PlatformTest.TOKEN);
            var secondGrant=captured.grants.poll(8,TimeUnit.SECONDS);
            assertNotNull(secondGrant);assertEquals(replacement,secondGrant.scope().configurationVersion());
            assertNotEquals(firstGrant.scope().workerProjectionSha256(),secondGrant.scope().workerProjectionSha256());
            var store=new GlobalTaskStore(controller.cluster,first.sharedAccess(),controller.cluster.taskKey,
                    first.globalTaskIdentity(),controller.cluster.maximumTasks);
            var cipher=store.commandCipher();var witness=secondGrant.member();var member=witness.member();
            assertEquals("b",member.node());
            assertEquals(secondGrant.scope(),member.scope());
            assertEquals(firstGrant.member().member().incarnation(),member.incarnation());
            assertEquals(firstGrant.member().member().epoch(),member.epoch());
            assertTrue(member.expiresUtcMillis()-member.observedUtcMillis()<=owner.cluster.leaseMillis);
            assertTrue(store.service((c,accounts)->store.database().now(c))>=member.observedUtcMillis());
            assertTrue(cipher.matches(member.signatureDomain(),witness.signature()));
            var forged=new KafkaPhysicalReceipt.Member(member.scope(),member.node(),member.incarnation(),member.epoch()+1,
                    member.observedUtcMillis(),member.expiresUtcMillis());
            assertFalse(cipher.matches(forged.signatureDomain(),witness.signature()));
            assertFalse(cipher.matches(secondGrant.scope().keyDomain(),witness.signature()));
            var expected=new KafkaPhysicalReceipt.Grant(secondGrant.scope(),cipher.signature(secondGrant.scope().keyDomain()),witness);
            assertEquals(expected.encode(),secondGrant.encode());
        }
    }

    @Test void leaseWitnessUsesTheSameControlClockAndRejectsExpiredAndReplacedMembers()throws Exception{
        var config=ControlStoreTest.cluster("witness_"+directory.getFileName(),"b");
        try(var store=new JdbcControlStore(config)){
            var initial=store.register(ControlStoreTest.node("b"),List.of());
            try(var c=store.connect()){
                c.setAutoCommit(false);
                var window=store.requireLiveOwnLease(c,initial);
                assertTrue(window.observedUtcMillis()<=store.now(c));
                assertTrue(window.observedUtcMillis()<window.expiresUtcMillis());
                assertTrue(window.expiresUtcMillis()-window.observedUtcMillis()<=config.leaseMillis);
                c.commit();
            }
            ControlStoreTest.expire(config,"b");
            try(var c=store.connect()){
                c.setAutoCommit(false);
                assertThrows(ControlStore.Expired.class,()->store.requireLiveOwnLease(c,initial));
                c.rollback();
            }
            var replacement=store.register(ControlStoreTest.node("b"),List.of());
            assertNotEquals(initial.incarnation,replacement.incarnation);
            assertEquals(initial.epoch+1,replacement.epoch);
            try(var c=store.connect()){
                c.setAutoCommit(false);
                assertThrows(ControlStore.Fenced.class,()->store.requireLiveOwnLease(c,initial));
                assertNotNull(store.requireLiveOwnLease(c,replacement));
                c.commit();
            }
        }
    }

    @Test void ordinaryWorkerSecretGrantCannotExposeTheManagedEvidenceKey()throws Exception{
        String key=KafkaPhysicalReceipt.GRANT_ENV;
        assertFalse(TaskWorker.Jvm.environment(Map.of(key,"fixture-key"),"jobId: plain\n",Set.of()).containsKey(key));
        assertThrows(IllegalArgumentException.class,()->TaskWorker.Jvm.environment(Map.of(key,"fixture-key"),
                "jobId: plain\nsource:\n  password: ${"+key+"}\n",Set.of()));
        assertThrows(IllegalArgumentException.class,()->WorkerSecretReferences.validate(Map.of("plain",List.of(key)),Set.of()));
    }

    private static JsonNode task(GlobalControlHttpTest fixture,String api)throws Exception{
        return fixture.get(api,"/api/tasks/job",PlatformTest.TOKEN);
    }

    private static final class CapturingWorker implements TaskWorker.Factory {
        final LinkedBlockingQueue<KafkaPhysicalReceipt.Grant> grants=new LinkedBlockingQueue<>();
        @Override public TaskWorker start(Path config,Path log,int heap){throw new AssertionError("Expected current ownership binding");}
        @Override public TaskWorker start(Path config,Path log,int heap,KafkaPhysicalReceipt.Grant grant)throws Exception{
            assertNotNull(grant);
            var worker=KafkaPhysicalReceipt.Grant.open(grant.encode(),"job",Files.readAllBytes(config));
            grants.add(worker);
            return new TaskWorker(){
                private volatile boolean active=true;
                @Override public boolean alive(){return active;}
                @Override public int exitCode(){return 0;}
                @Override public void stop(){active=false;}
            };
        }
    }
}
