package io.github.rivomesh;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.recovery.FailureEvidence;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;

/** Private authentication of the exact retained MySQL archive file. */
public final class CaptureArchiveReceipt {
    private static final String KEY_FILE=".capture-key",RECEIPT_PREFIX=".capture-receipt-";
    private static final ObjectMapper READER=Codec.JSON.copy()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    static final int KEY_BYTES=32,MAX_BYTES=4096;

    public record Owner(String task,String node,String directory) {
        public Owner {
            if(!id(task)||!id(node))throw new IllegalArgumentException("CAPTURE_EVIDENCE_OWNER_INVALID");
            FailureEvidence.uuid(directory);
        }
        private static boolean id(String value){
            return value!=null&&value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
        }
    }

    record Capture(Owner owner,String fingerprint,String generation,String checkpointHash,
                   long errorGeneration,String boundaryHash) {
        Capture {
            if(owner==null||!FailureEvidence.hash(fingerprint)||generation==null||generation.isEmpty()
                    ||generation.length()>128||!FailureEvidence.hash(checkpointHash)||errorGeneration<0
                    ||!FailureEvidence.hash(boundaryHash))throw new IllegalArgumentException("CAPTURE_EVIDENCE_BINDING_INVALID");
        }
        String file(){
            return locator(fingerprint,generation,checkpointHash,errorGeneration,boundaryHash)+".json";
        }
    }
    private record Binding(Capture capture,String file,long bytes,String sha256) {}
    private record Signed(int version,Binding binding,String signature) {}

    private CaptureArchiveReceipt() {}

    static String locator(String fingerprint,String generation,String checkpoint,long errorGeneration,String boundary){
        return Codec.sha256(fingerprint+"\n"+generation+"\n"+checkpoint+"\n"+errorGeneration+"\n"+boundary);
    }

    static Path path(Path directory,String file){
        if(file==null||!file.matches("[0-9a-f]{64}\\.json"))
            throw new IllegalArgumentException("CAPTURE_EVIDENCE_LOCATOR_INVALID");
        return directory.resolve(RECEIPT_PREFIX+file);
    }

    /** Called under the publisher's archive lock with fresh source bytes. */
    static void retain(Path root,Capture capture,String file,byte[] bytes)throws IOException{
        if(verifyExpected(root,capture,file,bytes))return;
        Binding binding=binding(capture,file,bytes);
        byte[] key=key(root,true);
        try{
            byte[] envelope=Codec.JSON.writeValueAsBytes(new Signed(1,binding,sign(binding,key)));
            if(envelope.length>MAX_BYTES)throw invalid();
            MysqlContentArchive.publish(path(root,file),envelope);
        }finally{Arrays.fill(key,(byte)0);}
    }

    static boolean verifyExpected(Path root,Capture expected,String file,byte[] bytes)throws IOException{
        Capture observed=verifyIfPresent(root,expected.owner(),file,bytes);
        if(observed!=null&&!expected.equals(observed))throw invalid();
        return observed!=null;
    }

    /** A retained proof can be read using the original owner even after an operation rolls out of history. */
    static Capture verifyIfPresent(Path root,Owner owner,String file,byte[] bytes)throws IOException{
        Path path=path(root,file);
        if(Files.notExists(path,LinkOption.NOFOLLOW_LINKS))return null;
        Signed signed;
        try{signed=READER.readValue(MysqlContentArchive.read(path,MAX_BYTES),Signed.class);}
        catch(IOException malformed){throw invalid();}
        if(signed==null||signed.version()!=1||signed.binding()==null||signed.binding().capture()==null
                ||!FailureEvidence.hash(signed.signature()))throw invalid();
        Capture captured=signed.binding().capture();
        if(!owner.equals(captured.owner())||!binding(captured,file,bytes).equals(signed.binding()))throw invalid();
        byte[] key=key(root,false);
        try{
            if(!MessageDigest.isEqual(sign(signed.binding(),key).getBytes(StandardCharsets.US_ASCII),
                    signed.signature().getBytes(StandardCharsets.US_ASCII)))throw invalid();
            return captured;
        }finally{Arrays.fill(key,(byte)0);}
    }

    private static Binding binding(Capture capture,String file,byte[] bytes)throws IOException{
        if(!capture.file().equals(file)||bytes.length==0)throw invalid();
        return new Binding(capture,file,bytes.length,Codec.sha256(bytes));
    }

    private static byte[] key(Path root,boolean create)throws IOException{
        Path path=root.resolve(KEY_FILE);
        if(Files.notExists(path,LinkOption.NOFOLLOW_LINKS)){
            if(!create)throw new IOException("MYSQL_BINLOG_EVIDENCE_KEY_MISSING");
            try(DirectoryStream<Path> receipts=Files.newDirectoryStream(root,RECEIPT_PREFIX+"*.json")){
                if(receipts.iterator().hasNext())throw new IOException("MYSQL_BINLOG_EVIDENCE_KEY_MISSING");
            }
            byte[] fresh=new byte[KEY_BYTES];
            try{
                new SecureRandom().nextBytes(fresh);
                MysqlContentArchive.publish(path,fresh);
            }finally{Arrays.fill(fresh,(byte)0);}
        }
        byte[] found=MysqlContentArchive.read(path,KEY_BYTES);
        if(found.length!=KEY_BYTES)throw new IOException("MYSQL_BINLOG_EVIDENCE_KEY_INVALID");
        return found;
    }

    private static String sign(Binding binding,byte[] key)throws IOException{
        try{
            Mac mac=Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key,"HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(("rivomesh.mysql-capture-receipt.v1\n"+Codec.encode(binding))
                    .getBytes(StandardCharsets.UTF_8)));
        }catch(GeneralSecurityException failure){throw new IOException("MYSQL_BINLOG_EVIDENCE_AUTHENTICATION_UNAVAILABLE");}
    }

    private static IOException invalid(){return new IOException("MYSQL_BINLOG_EVIDENCE_ATTESTATION_INVALID");}
}
