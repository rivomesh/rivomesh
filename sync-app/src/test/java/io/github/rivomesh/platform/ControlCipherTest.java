package io.github.rivomesh.platform;

import java.util.Base64;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ControlCipherTest {
    private static String key(int seed){byte[] value=new byte[32];java.util.Arrays.fill(value,(byte)seed);return Base64.getEncoder().encodeToString(value);}
    @Test void separateEndpointsCanReadButDatabaseAndOtherRealmsCannot(){
        ControlCipher a=new ControlCipher(key(1),"cluster","store"),b=new ControlCipher(key(1),"cluster","store");
        String sealed=a.seal("config/task/version","password: private-value\n数据库: 源库");
        assertFalse(sealed.contains("private-value"));assertEquals("password: private-value\n数据库: 源库",b.open("config/task/version",sealed));
        assertThrows(TaskService.Conflict.class,()->new ControlCipher(key(2),"cluster","store").open("config/task/version",sealed));
        assertThrows(TaskService.Conflict.class,()->new ControlCipher(key(1),"cluster-2","store").open("config/task/version",sealed));
        assertThrows(TaskService.Conflict.class,()->new ControlCipher(key(1),"cluster","store-2").open("config/task/version",sealed));
    }
    @Test void configurationCommandAndVersionSubstitutionIsRejected(){
        ControlCipher a=new ControlCipher(key(3),"cluster","store");String sealed=a.seal("config/task-a/version-1","sensitive");
        for(String scope:new String[]{"command/task-a/version-1","config/task-b/version-1","config/task-a/version-2"})assertThrows(TaskService.Conflict.class,()->a.open(scope,sealed));
        byte[] bytes=Base64.getDecoder().decode(sealed.substring(3));bytes[bytes.length-1]^=1;String altered="v1."+Base64.getEncoder().encodeToString(bytes);
        assertThrows(TaskService.Conflict.class,()->a.open("config/task-a/version-1",altered));
    }
    @Test void sameContentHasDifferentNoncesAndStableScopedSignatures(){
        ControlCipher a=new ControlCipher(key(4),"cluster","store"),b=new ControlCipher(key(4),"cluster","store");
        assertNotEquals(a.seal("scope","same"),a.seal("scope","same"));assertTrue(b.matches("secret/task/path/revision",a.signature("secret/task/path/revision")));
        assertFalse(b.matches("secret/task/path/revision-2",a.signature("secret/task/path/revision")));assertFalse(b.matches("scope",null));
    }
    @Test void invalidKeysAndEnvelopesNeverRevealPayloads(){
        for(String invalid:new String[]{"","not-base64",Base64.getEncoder().encodeToString(new byte[16]),key(1).replace("=","")})assertThrows(IllegalArgumentException.class,()->new ControlCipher(invalid,"cluster","store"));
        ControlCipher a=new ControlCipher(key(5),"cluster","store");
        for(String invalid:new String[]{"v0.private-value","v1.!private-value","v1.","private-value"}){
            TaskService.Conflict failure=assertThrows(TaskService.Conflict.class,()->a.open("scope",invalid));assertEquals("CONTROL_PAYLOAD_INVALID",failure.getMessage());assertNull(failure.getCause());
        }
    }
    @Test void limitsCountUtf8BytesAndRejectOversizedDatabaseValuesBeforeDecoding(){
        ControlCipher a=new ControlCipher(key(6),"cluster","store");String large=new String(new char[800000]).replace('\0','库');
        assertThrows(IllegalArgumentException.class,()->a.seal("scope",large));
        String invalid="v1."+new String(new char[3000000]).replace('\0','a');assertThrows(TaskService.Conflict.class,()->a.open("scope",invalid));
    }
}
