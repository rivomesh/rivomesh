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
    default void fence() { requestStop(); }
    default Diagnostic diagnostic(){return null;}

    final class Diagnostic {
        final io.github.rivomesh.cdc.PipelineFailure.Phase phase;
        final String code;
        private Diagnostic(io.github.rivomesh.cdc.PipelineFailure.Phase phase,String code){
            this.phase=phase;
            this.code=code;
        }
    }
    interface Factory {
        TaskWorker start(Path config, Path log, int heapMiB) throws Exception;
        default TaskWorker start(Path config,Path log,int heapMiB,io.github.rivomesh.cdc.KafkaPhysicalReceipt.Grant grant)throws Exception{
            if(grant!=null)throw new IllegalStateException("KAFKA_PHYSICAL_WORKER_REQUIRED");
            return start(config,log,heapMiB);
        }
    }

    final class Jvm implements TaskWorker {
        private final Process process;
        private final Thread output;
        private volatile long stopRequested;
        private volatile Diagnostic diagnostic;
        private static final int DIAGNOSTIC_LINE_LIMIT=320;
        private static final String DIAGNOSTIC_PREFIX="PIPELINE_DIAGNOSTIC ";
        private static final Pattern DIAGNOSTIC=Pattern.compile("([A-Z_]{1,32}) ([A-Z][A-Z0-9_]{1,127})");
        public Jvm(Path config, Path log, int heapMiB) throws Exception {this(config,log,heapMiB,Collections.emptySet());}
        Jvm(Path config,Path log,int heapMiB,Set<String> protectedNames)throws Exception{this(config,log,heapMiB,protectedNames,null);}
        Jvm(Path config,Path log,int heapMiB,Set<String> protectedNames,Map<String,List<String>> grants)throws Exception{
            this(config,log,heapMiB,protectedNames,grants,System.getenv(),"io.github.rivomesh.Main");
        }
        Jvm(Path config,Path log,int heapMiB,Set<String> protectedNames,Map<String,String> parent,String main)throws Exception{
            this(config,log,heapMiB,protectedNames,null,parent,main);
        }
        Jvm(Path config,Path log,int heapMiB,Set<String> protectedNames,Map<String,List<String>> grants,
            Map<String,String> parent,String main)throws Exception{
            this(config,log,heapMiB,protectedNames,grants,parent,main,null);
        }
        static Factory factory(PlatformConfig deployment){
            return new Factory(){
                @Override public TaskWorker start(Path config,Path log,int heapMiB)throws Exception{return start(config,log,heapMiB,null);}
                @Override public TaskWorker start(Path config,Path log,int heapMiB,io.github.rivomesh.cdc.KafkaPhysicalReceipt.Grant grant)throws Exception{
                    return new Jvm(config,log,heapMiB,deployment.protectedEnvironment,deployment.workerSecretReferences,
                            System.getenv(),"io.github.rivomesh.Main",grant);
                }
            };
        }
        private Jvm(Path config,Path log,int heapMiB,Set<String> protectedNames,Map<String,List<String>> grants,
                    Map<String,String> parent,String main,io.github.rivomesh.cdc.KafkaPhysicalReceipt.Grant grant)throws Exception{
            String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            ProcessBuilder builder=new ProcessBuilder(Paths.get(System.getProperty("java.home"),"bin","java").toString(),"-Xmx"+heapMiB+"m","-cp",cp,main,"run",config.toAbsolutePath().toString()).redirectErrorStream(true);
            byte[] projection=PrivateFiles.read(config,1024*1024);
            String definition=new String(projection,StandardCharsets.UTF_8);
            Map<String,String> environment=environment(parent,definition,protectedNames,grants);
            if(grant!=null){
                if(!io.github.rivomesh.cdc.Codec.sha256(projection).equals(grant.scope().workerProjectionSha256()))
                    throw new IllegalStateException("KAFKA_PHYSICAL_WORKER_PROJECTION_CHANGED");
                environment.put(io.github.rivomesh.cdc.KafkaPhysicalReceipt.GRANT_ENV,grant.encode());
            }
            builder.environment().clear();builder.environment().putAll(environment);process=builder.start();
            output = new Thread(() -> {
                try (InputStream in = process.getInputStream()) {
                    byte[] buffer = new byte[8192]; int n;
                    StringBuilder line=new StringBuilder();
                    while ((n = in.read(buffer)) >= 0) {
                        for(int i=0;i<n;i++){
                            if(buffer[i]=='\n'){
                                observeDiagnostic(line,log);
                                line.setLength(0);
                            }else if(line.length()<DIAGNOSTIC_LINE_LIMIT)line.append((char)buffer[i]);
                        }
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
        @Override public Diagnostic diagnostic(){return diagnostic;}

        private void observeDiagnostic(CharSequence line,Path log){
            if(line.length()==0||line.length()>=DIAGNOSTIC_LINE_LIMIT)return;
            String message=line.toString().trim();
            if("JOURNAL_BACKPRESSURE".equals(message))record(log,WorkerDiagnostics.Event.BACKPRESSURE,null);
            else if("JOURNAL_BACKPRESSURE_CLEARED".equals(message))record(log,WorkerDiagnostics.Event.BACKPRESSURE_CLEARED,null);
            else if(message.startsWith("PIPELINE_RETRY "))record(log,WorkerDiagnostics.Event.RETRY,null);
            else if(message.startsWith("TARGET_")){
                try{WorkerDiagnostics.observeTarget(log,message);}
                catch(IOException unavailable){/* Target diagnostics are best effort, like capture diagnostics. */}
            }else if(message.startsWith(DIAGNOSTIC_PREFIX)){
                String value=message.substring(DIAGNOSTIC_PREFIX.length());
                if("CLEAR".equals(value)){
                    diagnostic=null;
                    record(log,WorkerDiagnostics.Event.CAPTURE_READY,null);
                }else{
                    Matcher match=DIAGNOSTIC.matcher(value);
                    if(match.matches())try{
                        diagnostic=new Diagnostic(io.github.rivomesh.cdc.PipelineFailure.Phase.valueOf(match.group(1)),match.group(2));
                        record(log,WorkerDiagnostics.Event.CAPTURE_FAILURE,diagnostic.phase);
                    }catch(IllegalArgumentException invalid){/* only known phases */ }
                }
            }
        }

        private void record(Path log,WorkerDiagnostics.Event event,io.github.rivomesh.cdc.PipelineFailure.Phase phase){
            try{WorkerDiagnostics.append(log,event,phase);}
            catch(IOException unavailable){/* Preserve the existing private raw log and let the worker continue. */}
        }
        static Map<String,String> environment(Map<String,String> parent,String yaml,Set<String> protectedNames)
                throws IOException{return environment(parent,yaml,protectedNames,null);}
        static Map<String,String> environment(Map<String,String> parent,String yaml,Set<String> protectedNames,
                                              Map<String,List<String>> grants)throws IOException{
            Map<String,String> result=new LinkedHashMap<>();for(String key:Arrays.asList("PATH","HOME","LANG","LC_ALL","LC_CTYPE","TZ","TMPDIR","TEMP","TMP","SystemRoot","WINDIR"))if(parent.containsKey(key)&&!protectedNames.contains(key))result.put(key,parent.get(key));
            for(String key:WorkerSecretReferences.permitted(yaml,grants)){
                if(WorkerSecretReferences.protectedName(key,protectedNames))throw new IllegalArgumentException("WORKER_ENVIRONMENT_DENIED");
                if(parent.containsKey(key))result.put(key,parent.get(key));
                else if(grants!=null)throw new IllegalArgumentException("WORKER_SECRET_REFERENCE_UNSET");
            }return result;
        }
        public boolean alive() { return process.isAlive(); }
        public int exitCode() { return process.exitValue(); }
        public synchronized void requestStop() {
            if(stopRequested==0)stopRequested=System.nanoTime();
            process.destroy();
        }
        @Override public void fence() { process.destroyForcibly(); }
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
