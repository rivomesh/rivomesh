package io.github.rivomesh.cdc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DmProfileTest {
    @Test void missingObjectDiagnosisUsesTheRealVendorRatherThanAllDataExceptions(){
        assertEquals("TARGET_TABLE_MISSING_REBUILD_REQUIRED",io.github.rivomesh.Sql.targetErrorCode(new dm.jdbc.driver.DMException("missing","22000",-2106,null)));
        assertEquals("TARGET_TABLE_MISSING_REBUILD_REQUIRED", io.github.rivomesh.Sql.targetErrorCode(
                new dm.jdbc.driver.DMException("subclass", "22000", -2106, null) {}));
        assertEquals("SQL_22000",io.github.rivomesh.Sql.targetErrorCode(new java.sql.SQLException("other vendor","22000",-2106)));
        assertEquals("SQL_22000",io.github.rivomesh.Sql.targetErrorCode(new dm.jdbc.driver.DMException("precision","22000",-6121,null)));
    }
    @Test void nativeInstanceAndDurableCommitAreRequired(){
        DmProfile.validate(1,1,0,1,"IMMEDIATE,WAIT");DmProfile.validate(1,1,0,1,"BATCH, WAIT");
        assertThrows(IllegalArgumentException.class,()->DmProfile.validate(0,1,0,1,"IMMEDIATE,WAIT"));
        assertThrows(IllegalArgumentException.class,()->DmProfile.validate(1,0,0,1,"IMMEDIATE,WAIT"));
        assertThrows(IllegalArgumentException.class,()->DmProfile.validate(1,1,2,1,"IMMEDIATE,WAIT"));
        assertThrows(IllegalArgumentException.class,()->DmProfile.validate(1,1,0,0,"IMMEDIATE,WAIT"));
        for(String option:new String[]{"IMMEDIATE,NOWAIT","IMMEDIATE,NOTIFY",null})assertThrows(IllegalArgumentException.class,()->DmProfile.validate(1,1,0,1,option));
    }
}
