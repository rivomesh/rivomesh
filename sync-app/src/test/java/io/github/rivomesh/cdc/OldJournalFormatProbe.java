package io.github.rivomesh.cdc;

import java.util.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.jar.*;

/** Uses only reflection so an older artifact may have a different application namespace. */
public final class OldJournalFormatProbe {
    public static void main(String[] args)throws Exception{
        if(args.length!=4)throw new IllegalArgumentException("PROBE_ARGUMENTS_INVALID");
        File artifact=new File(System.getenv("RIVOMESH_LEGACY_ARTIFACT")).getCanonicalFile();String journalClass=null;
        try(JarFile jar=new JarFile(artifact)){Enumeration<JarEntry> entries=jar.entries();while(entries.hasMoreElements()){String name=entries.nextElement().getName();if(name.endsWith("/cdc/JdbcJournal.class")){if(journalClass!=null)throw new AssertionError("LEGACY_JOURNAL_AMBIGUOUS");journalClass=name.substring(0,name.length()-6).replace('/','.');}}}
        if(journalClass==null)throw new AssertionError("LEGACY_JOURNAL_MISSING");Class<?> type=Class.forName(journalClass),settingsType=Class.forName(journalClass+"$Settings");
        if(!new File(type.getProtectionDomain().getCodeSource().getLocation().toURI()).getCanonicalFile().equals(artifact))throw new AssertionError("LEGACY_JOURNAL_NOT_FROM_ARTIFACT");
        Object settings=settingsType.getConstructor().newInstance();String[] fields={"driver","url","username","password"},variables={"DRIVER","URL","USER","PASSWORD"};for(int i=0;i<fields.length;i++)settingsType.getField(fields[i]).set(settings,System.getenv("RIVOMESH_FAILURE_DB_"+variables[i]));settingsType.getField("namespace").set(settings,args[0]);settingsType.getField("pipelineId").set(settings,args[1]);
        try(AutoCloseable journal=(AutoCloseable)type.getConstructor(settingsType).newInstance(settings)){
            refused(type.getMethod("initialize",String.class,String.class,List.class),journal,args[2],args[3],Collections.singletonList("target"));
            refused(type.getMethod("verifyIdentity",String.class,List.class),journal,args[2],Collections.singletonList("target"));
            System.out.println("LEGACY_FORMAT4_REFUSED_BEFORE_CLAIM");
        }
    }
    private static void refused(Method method,Object journal,Object...arguments)throws Exception{
        try{method.invoke(journal,arguments);throw new AssertionError("LEGACY_"+method.getName()+"_ACCEPTED_FORMAT4");}
        catch(InvocationTargetException failure){Throwable cause=failure.getCause();if(!(cause instanceof IllegalStateException)||!"JOURNAL_IDENTITY_MISMATCH".equals(cause.getMessage()))throw failure;}
    }
}
