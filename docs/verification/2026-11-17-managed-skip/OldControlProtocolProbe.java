package io.github.rivomesh.platform;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.jar.*;

/** Reflection-only actual older-binary control-protocol guard,including pre-rename artifacts. */
public final class OldControlProtocolProbe {
    public static void main(String[] args)throws Exception{
        if(args.length!=2)throw new IllegalArgumentException("PROBE_ARGUMENTS_INVALID");File artifact=new File(System.getenv("RIVOMESH_LEGACY_ARTIFACT")).getCanonicalFile();String prefix=null;
        try(JarFile jar=new JarFile(artifact)){Enumeration<JarEntry> entries=jar.entries();while(entries.hasMoreElements()){String n=entries.nextElement().getName();if(n.endsWith("/platform/ClusterConfig.class")){if(prefix!=null)throw new AssertionError("LEGACY_CONFIG_AMBIGUOUS");prefix=n.substring(0,n.length()-"ClusterConfig.class".length()).replace('/','.');}}}
        if(prefix==null)throw new AssertionError("LEGACY_CONFIG_MISSING");Class<?> configType=Class.forName(prefix+"ClusterConfig"),storeType=Class.forName(prefix+"JdbcControlStore");if(!new File(storeType.getProtectionDomain().getCodeSource().getLocation().toURI()).getCanonicalFile().equals(artifact))throw new AssertionError("LEGACY_STORE_NOT_FROM_ARTIFACT");
        Object config=configType.getConstructor().newInstance();Map<String,Object> fields=new LinkedHashMap<>();fields.put("namespace",args[0]);fields.put("id",args[1]);fields.put("nodeId","legacy-probe");fields.put("taskKey",Base64.getEncoder().encodeToString(new byte[32]));for(String flag:Arrays.asList("sharedAccess","sharedTasks","separateExecution","recoveryReview","recoveryCommit"))fields.put(flag,true);
        String[] names={"driver","url","username","password"},environment={"DRIVER","URL","USER","PASSWORD"};for(int i=0;i<names.length;i++)fields.put(names[i],System.getenv("RIVOMESH_FAILURE_DB_"+environment[i]));for(Map.Entry<String,Object> field:fields.entrySet())configType.getField(field.getKey()).set(config,field.getValue());
        Constructor<?> constructor=storeType.getDeclaredConstructor(configType);constructor.setAccessible(true);
        try{AutoCloseable accepted=(AutoCloseable)constructor.newInstance(config);accepted.close();throw new AssertionError("LEGACY_PROTOCOL6_ACCEPTED_PROTOCOL7");}
        catch(InvocationTargetException failure){if(!"CONTROL_STORE_PROTOCOL_MISMATCH".equals(failure.getCause().getMessage()))throw failure;}
        System.out.println("LEGACY_PROTOCOL6_REFUSED_PROTOCOL7");
    }
}
