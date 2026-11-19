package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.validation.ValidationStore;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Publishes an immutable local read namespace for the exact projected recipient. */
final class HandoffEvidenceAdoption {
    private static final String ROOT="handoff-adopted",SEAL=ValidationStore.HANDOFF_ADOPTION_SEAL,
            RECEIPTS="closed-receipts.json",ROWS="rows.mv.db",SUMMARY="summary.json",LOCK="validation.lock";
    private static final int MAX_SEAL=64*1024,MAX_RECEIPTS=128*1024;
    record Receipt(String preparation,String task,HandoffPreparations.Binding candidate,String committedRevision,
                   String configurationSha256,String stagingSha256,String adoptionSha256,int reports,int closedReceipts){}
    private record Seal(int version,String preparation,String task,String originalNode,HandoffPreparations.Binding candidate,String committedRevision,
                        String configurationSha256,String stagingSha256,int reports,int closedReceipts,
                        SortedMap<String,HandoffStaging.FileSeal> files){
        Receipt receipt()throws Exception{
            return new Receipt(preparation,task,candidate,committedRevision,configurationSha256,stagingSha256,
                    Codec.sha256(Model.JSON.writeValueAsBytes(this)),reports,closedReceipts);
        }
    }
    private record Publication(Path directory,Seal seal){}

    private HandoffEvidenceAdoption(){}

    static Receipt install(Path state,HandoffPreparations.Plan plan,ControlCipher cipher)throws Exception{
        if(!HandoffPreparations.evidenceEligible(plan))throw refused("HANDOFF_RECIPIENT_PROJECTION_REQUIRED");
        return HandoffCandidateAdmission.withCommittedEvidence(state,plan,cipher,(ready,verified)->{
            SharedHandoffEvidence.Archive archive=verified.manifest().binding().archive();
            if(archive.rawCaptures()!=0||archive.failureRecords()!=0)throw refused("HANDOFF_EVIDENCE_PROFILE_UNSUPPORTED");
            SortedMap<String,HandoffStaging.FileSeal> adopted=new TreeMap<>();
            for(var artifact:verified.manifest().files().entrySet())adopted.put(adoptedName(artifact.getKey()),artifact.getValue());
            HandoffPreparations.WriterCommit commit=plan.writerCommit();
            Seal intended=new Seal(1,plan.id(),plan.task(),plan.evidence().original().node(),ready.receipt().binding(),commit.revision(),
                    ready.receipt().configurationSha256(),verified.sealSha256(),archive.reports(),archive.closedReceipts(),adopted);
            Receipt receipt=intended.receipt();
            if(HandoffPreparations.evidenceInstalled(plan)&&!receipt.equals(plan.evidenceAdoption()))
                throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
            Path owner=state.toRealPath(),root=owner.resolve(ROOT);
            if(Files.notExists(root,LinkOption.NOFOLLOW_LINKS)){HandoffStaging.createDirectory(root);HandoffStaging.force(owner);}
            HandoffStaging.secureDirectory(root);
            String key=key(plan.task(),plan.id());
            Path complete=root.resolve(key),pending=root.resolve(key+".pending");
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
            if(!Files.notExists(complete,LinkOption.NOFOLLOW_LINKS)){
                if(!Files.isDirectory(complete,LinkOption.NOFOLLOW_LINKS)
                        ||!Files.isRegularFile(complete.resolve(SEAL),LinkOption.NOFOLLOW_LINKS))
                    throw refused("HANDOFF_ADOPTED_PATH_OCCUPIED");
                verify(complete,intended,cipher,deadline);
                return receipt;
            }
            if(Files.notExists(pending,LinkOption.NOFOLLOW_LINKS))HandoffStaging.createDirectory(pending);
            HandoffStaging.secureDirectory(pending);
            installIntent(pending,intended,cipher,deadline);
            for(var artifact:verified.manifest().files().entrySet()){
                Path target=pending.resolve(adoptedName(artifact.getKey()));
                if(!target.getParent().equals(pending))prepareReport(target.getParent());
                copy(verified.directory().resolve(artifact.getKey()),target,artifact.getValue(),deadline);
            }
            verify(pending,intended,cipher,deadline);
            for(String id:reportIds(intended))HandoffStaging.force(pending.resolve(id));
            HandoffStaging.force(pending);
            if(!Files.notExists(complete,LinkOption.NOFOLLOW_LINKS))throw refused("HANDOFF_ADOPTED_PATH_OCCUPIED");
            Files.move(pending,complete,StandardCopyOption.ATOMIC_MOVE);
            HandoffStaging.force(root);
            return receipt;
        });
    }

    static Path reports(Path state,String task,String node,String directory,String revision,ManagedRecipientImport.Receipt config,
                        ControlCipher cipher,String id,java.util.function.Predicate<String> nodeGranted,Receipt authorized)throws Exception{
        Publication adopted=publication(state,task,node,directory,revision,config,cipher,authorized);
        if(adopted==null)return null;
        if(!nodeGranted.test(adopted.seal().originalNode()))throw new NoSuchElementException("VALIDATION_REPORT_NOT_CREATED");
        checkRoot(adopted.directory(),adopted.seal());
        if(id!=null){
            if(!reportIds(adopted.seal()).contains(id))throw new NoSuchElementException("VALIDATION_REPORT_NOT_CREATED");
            Path report=adopted.directory().resolve(id);
            HandoffStaging.secureDirectory(report);
            HandoffStaging.exactNames(report,Set.of(ROWS,SUMMARY,LOCK));
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
            for(String kind:List.of(ROWS,SUMMARY)){
                HandoffStaging.FileSeal proof=adopted.seal().files().get(id+"/"+kind);
                HandoffStaging.checkFile(report.resolve(kind),proof,proof.bytes(),deadline);
            }
        }
        return adopted.directory();
    }

    static SharedHandoffEvidence.ClosedReceipt receipt(Path state,String task,String node,String directory,String revision,
                        ManagedRecipientImport.Receipt config,ControlCipher cipher,String id,java.util.function.Predicate<String> nodeGranted,Receipt authorized)throws Exception{
        Publication adopted=publication(state,task,node,directory,revision,config,cipher,authorized);
        if(adopted==null||!nodeGranted.test(adopted.seal().originalNode()))return null;
        byte[] bytes=HandoffStaging.readFile(adopted.directory().resolve(RECEIPTS),MAX_RECEIPTS);
        var observed=new HandoffStaging.FileSeal(bytes.length,Codec.sha256(bytes));
        if(!observed.equals(adopted.seal().files().get(RECEIPTS)))throw refused("HANDOFF_ADOPTED_BYTES_CHANGED");
        var receipts=Model.JSON.readValue(bytes,SharedHandoffEvidence.ClosedReceipt[].class);
        if(receipts.length!=adopted.seal().closedReceipts())throw refused("HANDOFF_ADOPTED_BYTES_CHANGED");
        return Arrays.stream(receipts).filter(receipt->receipt.request().equals(id)).findFirst().orElse(null);
    }

    private static Publication publication(Path state,String task,String node,String directory,String revision,
                        ManagedRecipientImport.Receipt config,ControlCipher cipher,Receipt authorized)throws Exception{
        if(authorized==null)return null;
        Path root=state.toRealPath().resolve(ROOT);
        if(Files.notExists(root,LinkOption.NOFOLLOW_LINKS))return null;
        HandoffStaging.secureDirectory(root);
        Path complete=root.resolve(key(task,config.preparation()));
        if(Files.notExists(complete,LinkOption.NOFOLLOW_LINKS))return null;
        HandoffStaging.secureDirectory(complete);
        Seal seal=read(complete,task,config.preparation(),cipher);
        if(!task.equals(seal.task())||!config.preparation().equals(seal.preparation())||!revision.equals(seal.committedRevision())
                ||!node.equals(seal.candidate().node())||!directory.equals(seal.candidate().directory())
                ||!config.configurationSha256().equals(seal.configurationSha256())||!config.stagingSha256().equals(seal.stagingSha256())
                ||!authorized.equals(seal.receipt()))
            throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
        return new Publication(complete,seal);
    }

    private static void installIntent(Path pending,Seal intended,ControlCipher cipher,long deadline)throws Exception{
        Path seal=pending.resolve(SEAL);
        if(Files.notExists(seal,LinkOption.NOFOLLOW_LINKS)){
            Path part=pending.resolve(SEAL+".part");
            HandoffStaging.exactNames(pending,Files.notExists(part,LinkOption.NOFOLLOW_LINKS)?Set.of():Set.of(part.getFileName().toString()));
            discardPart(part);
            byte[] envelope=cipher.seal(purpose(intended.task(),intended.preparation()),Model.JSON.writeValueAsString(intended))
                    .getBytes(StandardCharsets.US_ASCII);
            HandoffStaging.writeFile(part,new ByteArrayInputStream(envelope),MAX_SEAL,deadline);
            Files.move(part,seal,StandardCopyOption.ATOMIC_MOVE);
            HandoffStaging.force(pending);
        }
        if(!intended.equals(read(pending,intended.task(),intended.preparation(),cipher)))
            throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
    }
    private static void prepareReport(Path report)throws Exception{
        if(Files.notExists(report,LinkOption.NOFOLLOW_LINKS))HandoffStaging.createDirectory(report);
        HandoffStaging.secureDirectory(report);
        Path lock=report.resolve(LOCK);
        if(Files.notExists(lock,LinkOption.NOFOLLOW_LINKS))
            Files.createFile(lock,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        HandoffStaging.secureFile(lock);
    }
    private static void copy(Path source,Path target,HandoffStaging.FileSeal proof,long deadline)throws Exception{
        Path part=target.resolveSibling(target.getFileName()+".part");
        discardPart(part);
        if(Files.notExists(target,LinkOption.NOFOLLOW_LINKS)){
            HandoffStaging.secureFile(source);
            try(InputStream payload=Files.newInputStream(source,LinkOption.NOFOLLOW_LINKS)){
                if(!proof.equals(HandoffStaging.writeFile(part,payload,proof.bytes(),deadline)))
                    throw refused("HANDOFF_ADOPTED_BYTES_CHANGED");
            }
            Files.move(part,target,StandardCopyOption.ATOMIC_MOVE);
        }
        HandoffStaging.checkFile(target,proof,proof.bytes(),deadline);
    }
    private static void discardPart(Path part)throws Exception{
        if(!Files.notExists(part,LinkOption.NOFOLLOW_LINKS)){HandoffStaging.secureFile(part);Files.delete(part);}
    }
    private static void verify(Path directory,Seal intended,ControlCipher cipher,long deadline)throws Exception{
        HandoffStaging.secureDirectory(directory);
        if(!intended.equals(read(directory,intended.task(),intended.preparation(),cipher)))
            throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
        checkRoot(directory,intended);
        for(String id:reportIds(intended)){
            Path report=directory.resolve(id);HandoffStaging.secureDirectory(report);
            HandoffStaging.exactNames(report,Set.of(ROWS,SUMMARY,LOCK));HandoffStaging.secureFile(report.resolve(LOCK));
        }
        for(var file:intended.files().entrySet())HandoffStaging.checkFile(directory.resolve(file.getKey()),file.getValue(),file.getValue().bytes(),deadline);
    }
    private static void checkRoot(Path root,Seal seal)throws Exception{
        Set<String> names=new HashSet<>(reportIds(seal));names.add(SEAL);names.add(RECEIPTS);
        HandoffStaging.exactNames(root,names);
    }
    private static Set<String> reportIds(Seal seal){
        Set<String> ids=new TreeSet<>();
        for(String name:seal.files().keySet()){
            if(RECEIPTS.equals(name))continue;
            if(!name.matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}/(?:rows\\.mv\\.db|summary\\.json)"))
                throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
            if(name.endsWith("/"+ROWS))ids.add(name.substring(0,36));
        }
        if(ids.size()!=seal.reports()||ids.stream().anyMatch(id->!seal.files().containsKey(id+"/"+SUMMARY)))
            throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
        return ids;
    }
    private static String adoptedName(String staged){
        if(RECEIPTS.equals(staged))return staged;
        String[] parts=staged.split("/");
        if(parts.length!=3||!"reports".equals(parts[0])||!parts[1].matches("[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"))
            throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
        return parts[1]+"/"+switch(parts[2]){
            case ValidationStore.STAGED_ROWS_FILE->ROWS;
            case "summary.staged.json"->SUMMARY;
            default->throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
        };
    }
    private static Seal read(Path directory,String task,String preparation,ControlCipher cipher)throws Exception{
        byte[] envelope=HandoffStaging.readFile(directory.resolve(SEAL),MAX_SEAL);
        Seal seal=Model.JSON.readValue(cipher.open(purpose(task,preparation),new String(envelope,StandardCharsets.US_ASCII)),Seal.class);
        if(seal==null||seal.version()!=1||seal.originalNode()==null||seal.candidate()==null||seal.files()==null||seal.reports()<0||seal.reports()>32
                ||seal.closedReceipts()<1||seal.closedReceipts()>SharedHandoffEvidence.MAX_RECEIPTS
                ||seal.files().size()!=1+2*seal.reports()||!seal.files().containsKey(RECEIPTS)
                ||seal.files().values().stream().anyMatch(file->file==null||file.bytes()<1||file.sha256()==null||!file.sha256().matches("[a-f0-9]{64}")))throw refused("HANDOFF_ADOPTED_SCOPE_CHANGED");
        return seal;
    }
    private static String key(String task,String preparation){return Codec.sha256(Codec.encode(List.of(task,preparation)));}
    private static String purpose(String task,String preparation){return "handoff-adopted/"+key(task,preparation);}
    private static TaskService.Conflict refused(String code){return new TaskService.Conflict(code);}
}
