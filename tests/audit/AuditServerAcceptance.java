package io.github.rivomesh.acceptance;

import io.github.rivomesh.Config;
import io.github.rivomesh.platform.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/** Actual release HTTP/audit storage; a stub database backend isolates management from database availability. */
public final class AuditServerAcceptance {
    public static void main(String[] arguments)throws Exception{
        PlatformConfig config=new PlatformConfig();config.bind="0.0.0.0";config.port=8080;config.authToken="${RIVOMESH_TOKEN}";config.dataDirectory=System.getenv("RIVOMESH_AUDIT_DIR")+"/data";config.tasksDirectory=System.getenv("RIVOMESH_AUDIT_DIR")+"/tasks";config.pollMillis=100;config.auditMaxEvents=1000;
        for(PlatformConfig.Role role:Arrays.asList(PlatformConfig.Role.VIEWER,PlatformConfig.Role.OPERATOR)){PlatformConfig.User user=new PlatformConfig.User();user.id=role.name().toLowerCase(Locale.ROOT);user.role=role;user.token="${RIVOMESH_"+role+"_TOKEN}";user.taskIds=role==PlatformConfig.Role.OPERATOR?Arrays.asList("native_audit"):Collections.emptyList();config.users.add(user);}
        java.nio.file.Path settings=java.nio.file.Paths.get(System.getenv("RIVOMESH_AUDIT_DIR"),"platform.yml");java.nio.file.Files.write(settings,io.github.rivomesh.Model.JSON.writeValueAsBytes(config));java.nio.file.Files.setPosixFilePermissions(settings,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));config=PlatformConfig.load(settings);
        TaskBackend backend=new TaskBackend(){public Map<String,Object> status(Config ignored){return Collections.emptyMap();}public void pause(Config ignored){}public void resume(Config ignored,long generation){}public Map<String,Object> check(Config ignored){return Collections.singletonMap("fixture",true);}};
        TaskService service=new TaskService(config,backend,(path,log,heap)->{throw new IllegalStateException("FIXTURE_WORKERS_DISABLED");});ConsoleServer server=new ConsoleServer(config,service);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{server.close();try{service.close();}catch(Exception failure){throw new RuntimeException(failure);}}));
        server.start();System.out.println("AUDIT_SERVER_READY");new CountDownLatch(1).await();
    }
}
