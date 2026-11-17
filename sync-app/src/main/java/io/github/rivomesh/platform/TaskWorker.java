package io.github.rivomesh.platform;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.*;
import java.util.regex.*;
import java.nio.charset.StandardCharsets;

/** Separate JVMs bound heap and isolate source failures between independent task configurations. */
public interface TaskWorker {
    boolean alive();
    int exitCode();
    void stop() throws Exception;
    default void requestStop() {}
    interface Factory { TaskWorker start(Path config, Path log, int heapMiB) throws Exception; }

    final class Jvm implements TaskWorker {
        private final Process process;
        private final Thread output;
        private volatile long stopRequested;
        public Jvm(Path config, Path log, int heapMiB) throws Exception {this(config,log,heapMiB,Collections.emptySet());}
        Jvm(Path config,Path log,int heapMiB,Set<String> protectedNames)throws Exception{this(config,log,heapMiB,protectedNames,System.getenv(),"io.github.rivomesh.Main");}
        Jvm(Path config,Path log,int heapMiB,Set<String> protectedNames,Map<String,String> parent,String main)throws Exception{
            String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            ProcessBuilder builder=new ProcessBuilder(Paths.get(System.getProperty("java.home"),"bin","java").toString(),"-Xmx"+heapMiB+"m","-cp",cp,main,"run",config.toAbsolutePath().toString()).redirectErrorStream(true);
            Map<String,String> environment=environment(parent,new String(PrivateFiles.read(config,1024*1024),StandardCharsets.UTF_8),protectedNames);builder.environment().clear();builder.environment().putAll(environment);process=builder.start();
            output = new Thread(() -> {
                try (InputStream in = process.getInputStream()) {
                    byte[] buffer = new byte[8192]; int n;
                    while ((n = in.read(buffer)) >= 0) {
                        if (Files.exists(log) && Files.size(log) + n > 8L * 1024 * 1024)
                            Files.move(log, log.resolveSibling(log.getFileName()+".1"), StandardCopyOption.REPLACE_EXISTING);
                        try (OutputStream out = Files.newOutputStream(log, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) { PrivateFiles.ownerOnly(log); out.write(buffer,0,n); }
                    }
                } catch (IOException ignored) {
                    // Losing log storage cannot leave an unobserved worker consuming unbounded output.
                    process.destroy();
                }
            }, "task-log-" + config.getFileName());
            output.setDaemon(true); output.start();
        }
        static Map<String,String> environment(Map<String,String> parent,String yaml,Set<String> protectedNames){
            Map<String,String> result=new LinkedHashMap<>();for(String key:Arrays.asList("PATH","HOME","LANG","LC_ALL","LC_CTYPE","TZ","TMPDIR","TEMP","TMP","SystemRoot","WINDIR"))if(parent.containsKey(key)&&!protectedNames.contains(key))result.put(key,parent.get(key));
            Matcher references=Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}").matcher(yaml);
            while(references.find()){
                String key=references.group(1);if(protectedNames.contains(key)||key.equals("RIVOMESH_TOKEN")||key.equals("RIVOMESH_PREVIEW_TOKEN")||key.startsWith("RIVOMESH_CONTROL_"))throw new IllegalArgumentException("WORKER_ENVIRONMENT_DENIED");
                if(parent.containsKey(key))result.put(key,parent.get(key));
            }return result;
        }
        public boolean alive() { return process.isAlive(); }
        public int exitCode() { return process.exitValue(); }
        public synchronized void requestStop() {
            if(stopRequested==0)stopRequested=System.nanoTime();
            process.destroy();
        }
        public void stop() throws Exception {
            if (process.isAlive()) {
                requestStop();
                long remaining=Math.max(0,TimeUnit.SECONDS.toNanos(15)-(System.nanoTime()-stopRequested));
                if (!process.waitFor(remaining, TimeUnit.NANOSECONDS)) { process.destroyForcibly(); process.waitFor(5,TimeUnit.SECONDS); }
            }
            output.join(1000);
        }
    }
}
