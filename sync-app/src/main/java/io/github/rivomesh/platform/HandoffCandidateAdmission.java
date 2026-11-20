package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** The candidate validates and seals its STOPPED successor outside the live task directory. */
final class HandoffCandidateAdmission {
    private static final int MAX_YAML=1024*1024,MAX_ENVELOPE=3*1024*1024;
    private static final String STOPPED="STOPPED";
    private static final Set<String> COMMITTED_PHASES=Set.of("COMMITTED","PROJECTED","EVIDENCE_ADOPTED","FENCE_PENDING","TARGETS_FENCED","START_PENDING","WORKER_ADMITTED","WORKER_PROJECTED");

    @FunctionalInterface interface Validator { void validate(String successorYaml)throws Exception; }
    @FunctionalInterface interface AdmittedEvidence<T>{ T accept(ReadyConfig ready,HandoffStaging.Verified staged)throws Exception; }
    record Ready(String preparation,HandoffPreparations.Binding binding,String desired,
                 String configurationSha256,String stagingSha256,
                 @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                 RawCaptureHandoffStaging.Proof rawCapture,
                 @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT)
                 int failureRecords){
        Ready(String preparation,HandoffPreparations.Binding binding,String desired,String configurationSha256,String stagingSha256){
            this(preparation,binding,desired,configurationSha256,stagingSha256,null,0);
        }
        Ready(String preparation,HandoffPreparations.Binding binding,String desired,String configurationSha256,String stagingSha256,
                 RawCaptureHandoffStaging.Proof rawCapture){
            this(preparation,binding,desired,configurationSha256,stagingSha256,rawCapture,0);
        }
    }
    record ReadyConfig(Ready receipt,String successorYaml){
        @Override public String toString(){return "ReadyConfig[receipt="+receipt+"]";}
    }
    private record Seal(int version,HandoffStaging.Binding stage,HandoffPreparations.Binding original,
                        HandoffPreparations.WriterActivation activation,Ready receipt,String successorYaml){}

    private HandoffCandidateAdmission(){}

    static Ready prepare(Path state,HandoffStaging.Binding staged,HandoffPreparations.Plan plan,String originalYaml,
                         ControlCipher cipher,Validator validator)throws Exception{
        return prepare(state,staged,plan,originalYaml,cipher,validator,null);
    }

    static Ready prepare(Path state,HandoffStaging.Binding staged,HandoffPreparations.Plan plan,String originalYaml,
                         ControlCipher cipher,Validator validator,RawCaptureHandoffStaging.Binding rawBinding)throws Exception{
        requireScope(plan,staged,"PREPARED");
        RawCaptureHandoffStaging.Proof raw=rawBinding==null?null:RawCaptureHandoffStaging.requireProof(state,rawBinding,cipher);
        if(raw!=null&&(!raw.matches(plan)||!rawBinding.scope().equals(staged.scope())))
            throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
        String successor=successorConfiguration(plan,originalYaml,validator);
        return HandoffStaging.withVerifiedFiles(state,staged,cipher,
                verified->sealReady(state,plan,cipher,raw,successor,verified));
    }

    static Ready prepareFrozen(Path state,SharedHandoffEvidence.Scope scope,HandoffPreparations.Plan plan,String originalYaml,
                               ControlCipher cipher,Validator validator)throws Exception{
        if(!"ORIGINAL_FROZEN".equals(plan.phase())||plan.writerFreeze()==null||plan.writerFreeze().offlineCommit()!=null
                ||plan.activation()==null||!plan.activation().checkedInControlTransaction())
            throw refused("HANDOFF_OFFLINE_COMMIT_FREEZE_REQUIRED");
        String successor=successorConfiguration(plan,originalYaml,validator);
        return HandoffStaging.withFrozenInventory(state,scope,plan,cipher,
                verified->sealReady(state,plan,cipher,null,successor,verified));
    }

    private static String successorConfiguration(HandoffPreparations.Plan plan,String originalYaml,Validator validator)throws Exception{
        if(originalYaml==null||originalYaml.getBytes(StandardCharsets.UTF_8).length>MAX_YAML)
            throw refused("HANDOFF_CANDIDATE_CONFIGURATION_LIMIT");
        ManagedWriterDeclaration original=ManagedWriterDeclaration.read(originalYaml);
        if(original==null||!Objects.equals(plan.activation().originalAssignmentSha256(),Codec.sha256(original.assignment())))
            throw refused("HANDOFF_CANDIDATE_CONFIGURATION_CHANGED");
        String successor=ManagedWriterDeclaration.successorYaml(originalYaml,plan.evidence().original().node(),
                plan.evidence().candidate().node(),plan.activation().nextAssignment());
        if(successor.getBytes(StandardCharsets.UTF_8).length>MAX_YAML)
            throw refused("HANDOFF_CANDIDATE_CONFIGURATION_LIMIT");
        validator.validate(successor);
        return successor;
    }

    private static Ready sealReady(Path state,HandoffPreparations.Plan plan,ControlCipher cipher,RawCaptureHandoffStaging.Proof raw,
                                   String successor,HandoffStaging.Verified verified)throws Exception{
        HandoffStaging.Binding retained=verified.manifest().binding();
        Ready receipt=new Ready(plan.id(),retained.candidate(),STOPPED,Codec.sha256(successor),verified.sealSha256(),raw,retained.archive().failureRecords());
        Seal intended=new Seal(1,retained,plan.evidence().original(),plan.activation(),receipt,successor);
        Path file=sealPath(state,plan,true);
        if(Files.notExists(file,LinkOption.NOFOLLOW_LINKS)){
            String envelope;
            try{envelope=cipher.seal(purpose(plan),Model.JSON.writeValueAsString(intended));}
            catch(IllegalArgumentException tooLarge){throw refused("HANDOFF_CANDIDATE_CONFIGURATION_LIMIT");}
            PrivateFiles.write(file,envelope.getBytes(StandardCharsets.US_ASCII));
        }
        Seal current=read(file,plan,cipher);
        if(!intended.equals(current))throw refused("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        return current.receipt();
    }

    static ReadyConfig verifyCommitted(Path state,HandoffPreparations.Plan plan,ControlCipher cipher)throws Exception{
        return withCommittedEvidence(state,plan,cipher,(ready,staged)->ready);
    }
    static <T> T withCommittedEvidence(Path state,HandoffPreparations.Plan plan,ControlCipher cipher,
                                       AdmittedEvidence<T> read)throws Exception{
        if(!COMMITTED_PHASES.contains(plan.phase())||plan.writerCommit()==null)
            throw refused("HANDOFF_CANDIDATE_COMMIT_REQUIRED");
        Ready expected=new Ready(plan.id(),plan.evidence().candidate(),STOPPED,
                plan.writerCommit().configurationSha256(),plan.writerCommit().stagingSha256(),plan.writerCommit().rawCapture(),plan.writerCommit().failureRecords());
        return withEvidence(state,plan,cipher,expected,read);
    }

    static ReadyConfig verifyPrepared(Path state,HandoffPreparations.Plan plan,ControlCipher cipher,Ready expected)throws Exception{
        if(expected.rawCapture()!=null||expected.failureRecords()>0)
            throw refused("HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED");
        if(!"PREPARED".equals(plan.phase())||plan.executionRequest()==null)
            throw refused("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        return withEvidence(state,plan,cipher,expected,(config,staged)->config);
    }

    private static <T> T withEvidence(Path state,HandoffPreparations.Plan plan,ControlCipher cipher,Ready expected,
                                      AdmittedEvidence<T> read)throws Exception{
        Path file=sealPath(state,plan,false);
        Seal observed=read(file,plan,cipher);
        requireScope(plan,observed.stage(),plan.phase());
        if(!plan.id().equals(expected.preparation())||!plan.evidence().candidate().equals(expected.binding())
                ||!STOPPED.equals(expected.desired()))throw refused("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        return HandoffStaging.withVerifiedFiles(state,observed.stage(),cipher,staged->{
            Seal current=read(file,plan,cipher);
            if(expected.rawCapture()!=null&&!expected.rawCapture().matches(plan))
                throw refused("HANDOFF_RAW_STAGING_SCOPE_CHANGED");
            if(expected.failureRecords()!=staged.manifest().binding().archive().failureRecords()
                    ||!current.equals(observed)||!current.original().equals(plan.evidence().original())
                    ||!current.activation().equals(plan.activation())||!expected.equals(current.receipt())
                    ||!staged.sealSha256().equals(expected.stagingSha256())
                    ||!Codec.sha256(current.successorYaml()).equals(expected.configurationSha256()))
                throw refused("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
            ManagedWriterDeclaration declaration=ManagedWriterDeclaration.read(current.successorYaml());
            if(declaration==null||!declaration.node().equals(expected.binding().node())
                    ||!declaration.assignment().equals(plan.activation().nextAssignment()))
                throw refused("HANDOFF_CANDIDATE_CONFIGURATION_CHANGED");
            return read.accept(new ReadyConfig(current.receipt(),current.successorYaml()),staged);
        });
    }

    private static void requireScope(HandoffPreparations.Plan plan,HandoffStaging.Binding staged,String phase){
        HandoffPreparations.Evidence evidence=plan.evidence();
        if(!phase.equals(plan.phase())||plan.activation()==null||!plan.activation().canPlace()
                ||evidence==null||staged==null||!plan.id().equals(staged.preparation())
                ||!plan.task().equals(staged.scope().task())||!evidence.candidate().equals(staged.candidate())
                ||!evidence.assessment().equals(staged.scope().assessment())||!evidence.revision().equals(staged.scope().revision())
                ||!evidence.configuration().equals(staged.scope().configuration())
                ||!evidence.original().directory().equals(staged.scope().originalDirectory())
                ||!evidence.proof().equals(staged.evidence())||!evidence.custody().equals(staged.archive().sha256()))
            throw refused("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
    }

    private static Path sealPath(Path state,HandoffPreparations.Plan plan,boolean create)throws Exception{
        Path ownerState=state.toRealPath();
        Path root=ownerState.resolve("handoff-admission");
        if(create&&Files.notExists(root,LinkOption.NOFOLLOW_LINKS)){
            HandoffStaging.createDirectory(root);
            HandoffStaging.force(ownerState);
        }
        HandoffStaging.secureDirectory(root);
        return root.resolve(Codec.sha256(Codec.encode(List.of(plan.task(),plan.id())))+".seal");
    }

    private static Seal read(Path file,HandoffPreparations.Plan plan,ControlCipher cipher)throws Exception{
        String envelope=new String(HandoffStaging.readFile(file,MAX_ENVELOPE),StandardCharsets.US_ASCII);
        Seal seal=Model.JSON.readValue(cipher.open(purpose(plan),envelope),Seal.class);
        if(seal==null||seal.version()!=1||seal.stage()==null||seal.original()==null||seal.activation()==null
                ||seal.receipt()==null||seal.successorYaml()==null)
            throw refused("HANDOFF_CANDIDATE_ADMISSION_CHANGED");
        return seal;
    }

    private static String purpose(HandoffPreparations.Plan plan){return "handoff-admission/"+plan.task()+"/"+plan.id();}
    private static TaskService.Conflict refused(String code){return new TaskService.Conflict(code);}
}
