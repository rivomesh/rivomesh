package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.cdc.Codec;
import java.util.Map;

/** Complete, bounded custody for a task whose only private recovery artifacts are closed receipts. */
final class HandoffRecoveryInventory {
    static final String PROFILE="CLOSED_RECEIPTS_ONLY";
    private static final Map<String,String> ABSENT=Map.of(
            "validationReports","ABSENT","failureEvidence","ABSENT","sourceEvidence","ABSENT");

    record ReceiptOnly(int version,String scopeSha256,String archiveSha256,
                       int closedReceipts,String receiptPayloadSha256,Map<String,String> originInventory){
        ReceiptOnly{
            if(version!=1||!hash(scopeSha256)||!hash(archiveSha256)||!hash(receiptPayloadSha256)
                    ||closedReceipts<1||closedReceipts>SharedHandoffEvidence.MAX_RECEIPTS
                    ||!ABSENT.equals(originInventory))throw invalid();
            originInventory=Map.copyOf(originInventory);
        }

        void requireScope(SharedHandoffEvidence.Scope expected){
            if(!separateDirectories(expected)||!scopeSha256.equals(scopeDigest(expected)))throw invalid();
        }

        void requireArchive(SharedHandoffEvidence.Scope expected,SharedHandoffEvidence.Archive archive){
            requireScope(expected);
            if(!receiptsOnly(archive)||!archiveSha256.equals(archive.sha256())
                    ||closedReceipts!=archive.closedReceipts())throw invalid();
        }

        void requireReceipts(SharedHandoffEvidence.ReceiptPlan current)throws Exception{
            if(current==null||current.refusal()!=null||closedReceipts!=current.receipts().size()
                    ||!receiptPayloadSha256.equals(receiptDigest(current)))throw invalid();
        }

        void requirePayload(byte[] bytes){
            if(!receiptPayloadSha256.equals(Codec.sha256(bytes)))throw invalid();
        }
    }

    private HandoffRecoveryInventory(){}

    static ReceiptOnly observe(SharedHandoffEvidence.Scope scope,Map<String,String> inventory,boolean priorAdoption,
                               SharedHandoffEvidence.ReceiptPlan local,SharedHandoffEvidence.Archive archive)throws Exception{
        if(priorAdoption||!separateDirectories(scope)||!ABSENT.equals(inventory)||local==null||local.refusal()!=null
                ||!receiptsOnly(archive)||local.receipts().size()!=archive.closedReceipts())return null;
        return new ReceiptOnly(1,scopeDigest(scope),archive.sha256(),archive.closedReceipts(),receiptDigest(local),inventory);
    }

    static ReceiptOnly validate(Object observed,SharedHandoffEvidence.Scope scope,SharedHandoffEvidence.Archive archive,
                                SharedHandoffEvidence.ReceiptPlan current)throws Exception{
        ReceiptOnly inventory;
        try{inventory=Model.JSON.convertValue(observed,ReceiptOnly.class);}
        catch(IllegalArgumentException malformed){throw invalid();}
        if(inventory==null)throw invalid();
        inventory.requireArchive(scope,archive);
        inventory.requireReceipts(current);
        return inventory;
    }

    static boolean receiptsOnly(SharedHandoffEvidence.Archive archive){
        return archive!=null&&archive.closedReceipts()>0&&archive.reports()==0
                &&archive.failureRecords()==0&&archive.rawCaptures()==0;
    }

    private static boolean separateDirectories(SharedHandoffEvidence.Scope scope){
        return scope!=null&&scope.originalDirectory()!=null&&scope.candidateDirectory()!=null
                &&!scope.originalDirectory().equals(scope.candidateDirectory());
    }

    private static String scopeDigest(SharedHandoffEvidence.Scope scope){return Codec.sha256(Codec.encode(scope));}
    private static boolean hash(String value){return value!=null&&value.matches("[a-f0-9]{64}");}
    private static String receiptDigest(SharedHandoffEvidence.ReceiptPlan plan)throws Exception{
        return Codec.sha256(Model.JSON.writeValueAsBytes(plan.receipts()));
    }
    private static TaskService.Conflict invalid(){return new TaskService.Conflict("HANDOFF_OFFLINE_INVENTORY_UNVERIFIED");}
}
