package io.github.rivomesh.platform;

/** Imports a suspended private configuration. It issues no configuration, archive or Worker admission. */
final class ManagedRecipientImport {
    record Grant(String authority,HandoffPreparations.Plan committed,String successorYaml){
        @Override public String toString(){return "Grant[preparation="+committed.id()+"]";}
    }
    record FrozenConfig(String preparation,HandoffPreparations.Binding candidate,String desired,
                        String configurationSha256,String stagingSha256,String successorYaml){
        @Override public String toString(){return "FrozenConfig[preparation="+preparation+", configurationSha256="+configurationSha256+"]";}
    }
    record Receipt(String preparation,String configurationSha256,String stagingSha256){}
    @FunctionalInterface interface FrozenReader { FrozenConfig read(HandoffPreparations.Plan committed)throws Exception; }
    @FunctionalInterface interface AuthorityReader { Grant read()throws Exception; }
    private ManagedRecipientImport(){}

    static Receipt install(GlobalTaskStore authority,AgentDirectory agents,TaskService local,ControlStore.Lease lease,
                           String directory,String task,FrozenReader reader)throws Exception{
        return install(local,()->current(authority,agents,lease,directory,task),reader);
    }
    static Receipt install(TaskService local,AuthorityReader authority,FrozenReader reader)throws Exception{
        Grant original=authority.read();
        FrozenConfig frozen=reader.read(original.committed());
        if(!original.equals(authority.read()))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_AUTHORITY_CHANGED");
        // The database scopes are closed before taking the local controller lock or appending local audit.
        return local.installStoppedRecipient(original,frozen);
    }
    private static Grant current(GlobalTaskStore authority,AgentDirectory agents,ControlStore.Lease lease,String directory,
                                 String task)throws Exception{
        return authority.service((c,accounts)->{
            agents.require(c,lease,directory);
            Grant verified=authority.verifiedStoppedRecipient(c,task,lease,directory);
            agents.require(c,lease,directory);
            return verified;
        });
    }
    static Receipt validate(Grant grant,FrozenConfig frozen){
        HandoffPreparations.Plan plan=grant.committed();
        HandoffPreparations.WriterCommit commit=plan.writerCommit();
        if(frozen==null||commit==null||!"COMMITTED".equals(plan.phase())||!plan.id().equals(frozen.preparation())
                ||!plan.evidence().candidate().equals(frozen.candidate())||!"STOPPED".equals(frozen.desired())
                ||!commit.stagingSha256().equals(frozen.stagingSha256())||!commit.configurationSha256().equals(frozen.configurationSha256())
                ||!grant.successorYaml().equals(frozen.successorYaml())||!AuditStore.hash(frozen.successorYaml()).equals(frozen.configurationSha256()))
            throw new TaskService.Conflict("HANDOFF_RECIPIENT_FROZEN_CONFIG_CHANGED");
        return new Receipt(plan.id(),commit.configurationSha256(),commit.stagingSha256());
    }
}
