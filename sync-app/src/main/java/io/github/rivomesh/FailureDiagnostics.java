package io.github.rivomesh;

import java.util.*;

/** Stack locations only: exception messages may contain credentials, SQL or row values. */
public final class FailureDiagnostics {
    private FailureDiagnostics(){}
    public static void targetPaused(String target,String code,Throwable failure){
        targetFailure("TARGET_PAUSED",target,code,failure);
    }
    public static void targetRetrying(String target,String code,Throwable failure){targetFailure("TARGET_RETRYING",target,code,failure);}
    public static void targetRetryCleared(String target){System.err.println("TARGET_RETRY_CLEARED "+target);}
    private static void targetFailure(String state,String target,String code,Throwable failure){
        System.err.println(state+" "+target+" "+code);
        Set<Throwable> seen=Collections.newSetFromMap(new IdentityHashMap<Throwable,Boolean>());
        for(Throwable cause=failure;cause!=null&&seen.size()<8&&seen.add(cause);cause=cause.getCause()){
            System.err.println("  type="+cause.getClass().getName());
            if(cause instanceof java.sql.SQLException){java.sql.SQLException sql=(java.sql.SQLException)cause;String sqlState=sql.getSQLState();System.err.println("  sqlstate="+(sqlState!=null&&sqlState.matches("[A-Z0-9]{5}")?sqlState:"UNKNOWN")+" vendor="+sql.getErrorCode());}
            StackTraceElement[] stack=cause.getStackTrace();
            for(int i=0;i<Math.min(8,stack.length);i++)System.err.println("    at "+stack[i]);
        }
    }
}
