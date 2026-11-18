package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.recovery.FailureEvidence;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ManagedCompensationTest {
    @TempDir Path directory;

    @Test void adminPreviewAndApprovalFollowTheOwnerCommandAndBindTheOriginalScope() throws Exception {
        GlobalControlHttpTest helper=new GlobalControlHttpTest();
        helper.directory=directory;
        PlatformConfig a=helper.config("a"), b=helper.config("b");
        for(PlatformConfig config:Arrays.asList(a,b)){
            config.cluster.recoveryReview=true;
            config.cluster.recoveryCommit=true;
            config.cluster.recoverySkip=true;
        }
        ManagedGapVerificationTest.Backend owner=new ManagedGapVerificationTest.Backend(){
            @Override public Map<String,Object> compensate(TaskDefinition task,Path directory,FailureEvidence.Scope scope,
                    String action,CompensationRequest request,String operation,String actor,String subscription,long generation,java.util.function.BooleanSupplier cancelled){
                assertEquals("pg",subscription);
                assertEquals(1,generation);
                assertEquals("b",scope.ownerNodeId);
                Map<String,Object> result=new LinkedHashMap<>();
                result.put("hash",Codec.sha256("reviewed-compensation"));
                result.put("code","COMPENSATION_PLAN".equals(action)?"COMPENSATION_PLAN_READY":"COMPENSATION_COMPLETED");
                if("COMPENSATION_EXECUTE".equals(action))assertEquals(result.get("hash"),request.approvedHash);
                return result;
            }
        };
        try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());
            TaskService second=new TaskService(b,owner,new PlatformTest.Workers());
            ConsoleServer one=new ConsoleServer(a,first);ConsoleServer two=new ConsoleServer(b,second)){
            one.start();two.start();
            String api=helper.base(one),route="/api/tasks/job";
            helper.createPipeline(api);
            helper.action(api,"job","PAUSE","compensation_pause_1234",PlatformTest.TOKEN);
            PlatformTest.await(()->helper.completed(api,"job","compensation_pause_1234"));
            ObjectNode plan=Model.JSON.createObjectNode();
            plan.put("action","COMPENSATION_PLAN");
            plan.put("expectedRevision",helper.get(api,route,PlatformTest.TOKEN).path("revision").asText());
            plan.put("subscriptionId","pg");plan.put("errorCount",1);
            ObjectNode scope=plan.putObject("compensation");
            scope.put("evidenceId","11111111-1111-1111-1111-111111111111");
            scope.put("evidenceHash",Codec.sha256("retained"));
            assertEquals(202,PlatformTest.http(api,route+"/actions",PlatformTest.TOKEN,plan.toString(),"compensation_plan_1234").status);
            PlatformTest.await(()->helper.completed(api,"job","compensation_plan_1234"));
            PlatformTest.await(()->helper.operation(api,"compensation_plan_1234").path("result").hasNonNull("hash"));
            JsonNode reviewed=helper.operation(api,"compensation_plan_1234").path("result");
            assertEquals("COMPENSATION_PLAN_READY",reviewed.path("code").asText());
            ObjectNode execute=plan.deepCopy();execute.put("action","COMPENSATION_EXECUTE");
            execute.put("expectedRevision",helper.get(api,route,PlatformTest.TOKEN).path("revision").asText());
            assertEquals(400,PlatformTest.http(api,route+"/actions",PlatformTest.TOKEN,execute.toString(),"compensation_unapproved_1234").status);
            ObjectNode approval=(ObjectNode)execute.get("compensation");
            approval.put("approvedHash",reviewed.path("hash").asText());approval.put("reason","Approved the reviewed target restoration scope");
            assertEquals(202,PlatformTest.http(helper.base(two),route+"/actions",PlatformTest.TOKEN,execute.toString(),"compensation_apply_1234").status);
            PlatformTest.await(()->helper.completed(api,"job","compensation_apply_1234"));
            assertEquals("PAUSED",helper.get(api,route,PlatformTest.TOKEN).path("desired").asText());
        }
    }
}
