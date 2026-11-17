package io.github.rivomesh.platform;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/** Versioned authenticated control payloads. A database copy cannot expose task credentials. */
final class ControlCipher {
    private static final int MAX_PLAIN=2*1024*1024;
    private final byte[] encryption,authentication;
    private final String realm;
    private final SecureRandom random=new SecureRandom();

    ControlCipher(String encoded,String cluster,String identity){
        byte[] master;
        try{master=Base64.getDecoder().decode(encoded);}catch(RuntimeException invalid){throw new IllegalArgumentException("CONTROL_KEY_INVALID");}
        if(master.length!=32||!Base64.getEncoder().encodeToString(master).equals(encoded))throw new IllegalArgumentException("CONTROL_KEY_INVALID");
        realm="rivomesh-control-v1\n"+cluster+"\n"+identity;
        try{encryption=derive(master,"encryption");authentication=derive(master,"authentication");}finally{Arrays.fill(master,(byte)0);}
    }
    private byte[] derive(byte[] master,String purpose){
        try{Mac mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(master,"HmacSHA256"));return mac.doFinal((realm+"\n"+purpose).getBytes(StandardCharsets.UTF_8));}
        catch(GeneralSecurityException impossible){throw new IllegalStateException("CONTROL_CRYPTO_UNAVAILABLE");}
    }
    String signature(String purpose){
        try{Mac mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(authentication,"HmacSHA256"));return Base64.getUrlEncoder().withoutPadding().encodeToString(mac.doFinal(purpose.getBytes(StandardCharsets.UTF_8)));}
        catch(GeneralSecurityException impossible){throw new IllegalStateException("CONTROL_CRYPTO_UNAVAILABLE");}
    }
    boolean matches(String purpose,String signature){return signature!=null&&MessageDigest.isEqual(signature(purpose).getBytes(StandardCharsets.US_ASCII),signature.getBytes(StandardCharsets.UTF_8));}
    String seal(String scope,String plaintext){
        byte[] input=plaintext.getBytes(StandardCharsets.UTF_8);if(input.length>MAX_PLAIN)throw new IllegalArgumentException("CONTROL_PAYLOAD_LIMIT");
        byte[] nonce=new byte[12];random.nextBytes(nonce);
        try{Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.ENCRYPT_MODE,new SecretKeySpec(encryption,"AES"),new GCMParameterSpec(128,nonce));cipher.updateAAD((realm+"\n"+scope).getBytes(StandardCharsets.UTF_8));byte[] ciphertext=cipher.doFinal(input),envelope=new byte[nonce.length+ciphertext.length];System.arraycopy(nonce,0,envelope,0,nonce.length);System.arraycopy(ciphertext,0,envelope,nonce.length,ciphertext.length);return "v1."+Base64.getEncoder().encodeToString(envelope);}
        catch(GeneralSecurityException invalid){throw new IllegalStateException("CONTROL_CRYPTO_UNAVAILABLE");}finally{Arrays.fill(input,(byte)0);}
    }
    String open(String scope,String envelope){
        if(envelope==null||!envelope.startsWith("v1.")||envelope.length()>(MAX_PLAIN+28)*4/3+8)throw new TaskService.Conflict("CONTROL_PAYLOAD_INVALID");
        try{byte[] bytes=Base64.getDecoder().decode(envelope.substring(3));if(bytes.length<28||bytes.length>MAX_PLAIN+28)throw new GeneralSecurityException();Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.DECRYPT_MODE,new SecretKeySpec(encryption,"AES"),new GCMParameterSpec(128,Arrays.copyOf(bytes,12)));cipher.updateAAD((realm+"\n"+scope).getBytes(StandardCharsets.UTF_8));byte[] plain=cipher.doFinal(bytes,12,bytes.length-12);try{return new String(plain,StandardCharsets.UTF_8);}finally{Arrays.fill(plain,(byte)0);}}
        catch(GeneralSecurityException|IllegalArgumentException invalid){throw new TaskService.Conflict("CONTROL_PAYLOAD_INVALID");}
    }
}
