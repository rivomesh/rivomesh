package io.github.rivomesh.cdc;

import java.util.UUID;

/** An enrolled journal generation for a newly registered managed Kafka task. */
public record KafkaHistoryOrigin(String authority,String task,String directory,String initialConfiguration,String generation,
                                 String signature) {
    public KafkaHistoryOrigin {
        if(!uuid(authority)||!uuid(directory)||!uuid(initialConfiguration)||!uuid(generation)
                ||task==null||!task.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                ||signature==null||!signature.matches("[A-Za-z0-9_-]{43}"))
            throw new IllegalArgumentException("KAFKA_HISTORY_ORIGIN_INVALID");
    }

    public static String signatureDomain(String authority,String task,String directory,String configuration,String generation) {
        return "kafka-history-origin-v1\n"+authority+"\n"+task+"\n"+directory+"\n"+configuration+"\n"+generation;
    }

    public String signatureDomain(){return signatureDomain(authority,task,directory,initialConfiguration,generation);}
    public String intentKeyDomain(){return "kafka-history-origin-intent-key-v1\n"+signature;}
    public String pendingDomain(){return "kafka-history-origin-pending-v1\n"+signature;}
    public String readyDomain(){return "kafka-history-origin-ready-v1\n"+signature;}

    boolean belongsTo(KafkaPhysicalReceipt.Scope scope) {
        return authority.equals(scope.authorityId())&&task.equals(scope.task())&&directory.equals(scope.ownerDirectory());
    }

    private static boolean uuid(String value) {
        try{return value!=null&&UUID.fromString(value).toString().equals(value);}
        catch(IllegalArgumentException invalid){return false;}
    }
}
