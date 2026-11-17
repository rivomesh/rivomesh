package io.github.rivomesh.platform;

import java.net.URI;
import java.util.Arrays;

/** Optional shared control namespace; omission keeps the local controller behavior. */
public final class ClusterConfig {
    public String id, nodeId, url, username, password;
    public boolean sharedAccess=false;
    public boolean sharedTasks=false;
    /** Protocol5 opt-in; old execution members must stop before enabling new review requests. */
    public boolean recoveryReview=false;
    /** Protocol6 opt-in for evidence-bound native commit acknowledgment. */
    public boolean recoveryCommit=false;
    /** Protocol7 opt-in for controlled transaction skipping and durable gaps. */
    public boolean recoverySkip=false;
    public enum Role {COMBINED,CONTROLLER,AGENT}
    public boolean separateExecution=false;
    public Role role=Role.COMBINED;
    public String taskKey;
    public int maximumTasks=10000,maximumCommands=1000;
    public String driver="org.postgresql.Driver", namespace="rivomesh_control", advertiseUrl;
    public long heartbeatMillis=2000, leaseMillis=15000;
    public int maximumNodes=1000;
    void validate() {
        if(recoveryReview&&!sharedTasks)throw new IllegalArgumentException("RECOVERY_REVIEW_POLICY_INVALID");
        if(recoveryCommit&&!recoveryReview)throw new IllegalArgumentException("RECOVERY_COMMIT_POLICY_INVALID");
        if(recoverySkip&&!recoveryCommit)throw new IllegalArgumentException("RECOVERY_SKIP_POLICY_INVALID");
        if(role==null||separateExecution&&!sharedTasks||role!=Role.COMBINED&&!separateExecution)throw new IllegalArgumentException("EXECUTION_ROLE_POLICY_INVALID");
        if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||nodeId==null||!nodeId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}"))throw new IllegalArgumentException("CLUSTER_ID_INVALID");
        if(namespace==null||!namespace.matches("[a-z][a-z0-9_]{0,62}")||url==null||username==null||password==null||username.contains("${")||password.contains("${"))throw new IllegalArgumentException("CONTROL_STORE_CONFIG_INVALID");
        if(!Arrays.asList("org.postgresql.Driver","com.mysql.cj.jdbc.Driver","org.h2.Driver").contains(driver)
                ||!(driver.equals("org.postgresql.Driver")&&url.startsWith("jdbc:postgresql:")||driver.equals("com.mysql.cj.jdbc.Driver")&&url.startsWith("jdbc:mysql:")||driver.equals("org.h2.Driver")&&url.startsWith("jdbc:h2:mem:")))throw new IllegalArgumentException("CONTROL_STORE_DRIVER_INVALID");
        if(heartbeatMillis<100||heartbeatMillis>30000||leaseMillis<heartbeatMillis*3||leaseMillis<1000||leaseMillis>300000||maximumNodes<1||maximumNodes>10000)throw new IllegalArgumentException("CLUSTER_LIMITS_INVALID");
        if(sharedTasks){
            if(!sharedAccess||maximumTasks<1||maximumTasks>200000||maximumCommands<1||maximumCommands>100000)throw new IllegalArgumentException("GLOBAL_TASK_POLICY_INVALID");
            try{byte[] decoded=java.util.Base64.getDecoder().decode(taskKey);if(decoded.length!=32||!java.util.Base64.getEncoder().encodeToString(decoded).equals(taskKey))throw new IllegalArgumentException();}catch(RuntimeException invalid){throw new IllegalArgumentException("CONTROL_KEY_INVALID");}
        }else if(taskKey!=null)throw new IllegalArgumentException("GLOBAL_TASK_POLICY_INVALID");
        if(advertiseUrl!=null){URI uri;try{uri=URI.create(advertiseUrl);}catch(Exception e){throw new IllegalArgumentException("NODE_URL_INVALID");}
            if(!Arrays.asList("http","https").contains(uri.getScheme())||uri.getHost()==null||uri.getRawUserInfo()!=null||uri.getRawQuery()!=null||uri.getRawFragment()!=null||!"".equals(uri.getRawPath()))throw new IllegalArgumentException("NODE_URL_INVALID");}
    }
    boolean executes(){return role!=Role.CONTROLLER;}
    String roleName(){return role==Role.COMBINED?"CONTROLLER_WORKER":role.name();}
}
