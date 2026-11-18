package io.github.rivomesh.platform;

import io.github.rivomesh.Config;
import io.github.rivomesh.recovery.FailureEvidence;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;

/** Docker acceptance fixture that can lose the Agent after native COMMIT and before owner persistence. */
public final class RecoveryAgentFixture {
    private RecoveryAgentFixture() {}

    public static void main(String[] args)throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        PlatformConfig config=PlatformConfig.load(Paths.get(args[0]));
        Path halt=Paths.get(args[1]);
        try(TaskService tasks=new TaskService(config,new Backend(halt),
                (path,log,heap)->new TaskWorker.Jvm(path,log,heap,config.protectedEnvironment));
            ConsoleServer server=new ConsoleServer(config,tasks)){
            server.start();
            new CountDownLatch(1).await();
        }
    }

    private static final class Backend implements TaskBackend {
        private final Database database=new Database();
        private final Path halt;

        Backend(Path halt){this.halt=halt;}

        @Override public Map<String,Object> status(Config config)throws Exception{return database.status(config);}
        @Override public void pause(Config config)throws Exception{database.pause(config);}
        @Override public void resume(Config config,long generation)throws Exception{database.resume(config,generation);}
        @Override public Map<String,Object> check(Config config)throws Exception{return database.check(config);}

        private Map<String,Object> afterCommit(Map<String,Object> result,String operation)throws Exception{
            if(Files.isRegularFile(halt)&&Files.readAllLines(halt).contains(operation)&&Files.deleteIfExists(halt))
                Runtime.getRuntime().halt(137);
            return result;
        }

        @Override public Map<String,Object> compensate(TaskDefinition task,Path directory,FailureEvidence.Scope scope,
                String action,CompensationRequest request,String operation,String actor,String subscription,long generation,java.util.function.BooleanSupplier cancelled)throws Exception{
            Map<String,Object> result=TaskBackend.super.compensate(task,directory,scope,action,request,operation,actor,subscription,generation,cancelled);
            return "COMPENSATION_EXECUTE".equals(action)?afterCommit(result,operation):result;
        }

        @Override public Map<String,Object> correction(TaskDefinition task,Path directory,FailureEvidence.Scope scope,
                CorrectionRequest request,boolean schema,String operation,String actor,String subscription,long generation,java.util.function.BooleanSupplier cancelled)throws Exception{
            Map<String,Object> result=TaskBackend.super.correction(task,directory,scope,request,schema,operation,actor,subscription,generation,cancelled);
            return operation==null?result:afterCommit(result,operation);
        }
    }
}
