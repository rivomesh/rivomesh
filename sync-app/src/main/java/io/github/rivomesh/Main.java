package io.github.rivomesh;

import java.nio.file.Paths;
import java.util.TimeZone;

public final class Main {
    private Main() {}
    public static void main(String[] args) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        if(args.length==2&&args[0].equals("probe-oracle")){
            System.out.println(Model.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(OracleSourceProbe.inspect(Paths.get(args[1]))));
            return;
        }
        if(args.length==3&&args[0].equals("recover-access")){io.github.rivomesh.platform.TaskService.recoverAccess(io.github.rivomesh.platform.PlatformConfig.load(Paths.get(args[1])),args[2]);System.out.println("ACCESS_RECOVERED_OFFLINE_NO_WORKERS_STARTED");return;}
        if(args.length>=2&&!args[0].equals("serve")&&!args[0].equals("agent")&&PipelineConfig.isV3(Paths.get(args[1]))){PipelineMain.execute(args);return;}
        if (args.length == 2 && (args[0].equals("serve")||args[0].equals("agent"))) {
            io.github.rivomesh.platform.PlatformConfig platform = io.github.rivomesh.platform.PlatformConfig.load(Paths.get(args[1]));
            if(args[0].equals("agent")&&(platform.cluster==null||platform.cluster.role!=io.github.rivomesh.platform.ClusterConfig.Role.AGENT))throw new IllegalArgumentException("AGENT_ROLE_REQUIRED");
            try (io.github.rivomesh.platform.TaskService tasks = new io.github.rivomesh.platform.TaskService(platform);
                 io.github.rivomesh.platform.ConsoleServer server = new io.github.rivomesh.platform.ConsoleServer(platform,tasks)) {
                java.util.concurrent.CountDownLatch shutdown = new java.util.concurrent.CountDownLatch(1);
                Thread hook = new Thread(() -> {
                    server.close();
                    try { tasks.close(); }
                    catch (Exception e) { System.err.println("平台停止未完成: " + e.getClass().getSimpleName()); }
                    finally { shutdown.countDown(); }
                },"platform-shutdown");
                Runtime.getRuntime().addShutdownHook(hook);server.start();
                System.out.println("Rivomesh "+(platform.cluster!=null&&platform.cluster.role==io.github.rivomesh.platform.ClusterConfig.Role.AGENT?"Agent probes":"console")+": http://"+platform.bind+":"+server.port());
                try { shutdown.await(); }
                finally { try { Runtime.getRuntime().removeShutdownHook(hook); } catch (IllegalStateException ignored) { /* shutdown */ } }
            }
            return;
        }
        if (args.length != 2 || !(args[0].equals("run") || args[0].equals("status") || args[0].equals("check") || args[0].equals("resume"))) {
            System.err.println("用法: java -jar rivomesh-app.jar <run|status|check|resume> config/local.yml | <serve|agent> config/platform.yml");
            System.exit(2); return;
        }
        Config config = Config.load(Paths.get(args[1]));
        if (args[0].equals("status")) {
            System.out.println(Model.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(new StateStore(config).status()));
        } else if (args[0].equals("resume")) {
            new StateStore(config).resume(); System.out.println("任务已允许恢复，将重试原失败事务；没有跳过数据或修改 GTID。");
        } else if (args[0].equals("check")) {
            MysqlSource source = new SourceSelector(config).select(null, "");
            String identity = source.validateAndIdentity();
            int count = source.discover().size();
            try (java.sql.Connection c = new StateStore(config).connect()) {
                System.out.println("连接检查通过；源实例 " + identity + "，发现 " + count + " 张表；目标 " + c.getMetaData().getDatabaseProductName());
            }
        } else {
            try (SyncEngine engine = new SyncEngine(config)) {
                Thread hook = new Thread(engine::close, "sync-shutdown");
                Runtime.getRuntime().addShutdownHook(hook);
                try { engine.run(); }
                finally { try { Runtime.getRuntime().removeShutdownHook(hook); } catch (IllegalStateException ignored) { /* JVM shutdown */ } }
            }
        }
    }
}
