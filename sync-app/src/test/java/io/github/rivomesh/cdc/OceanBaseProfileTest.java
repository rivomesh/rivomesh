package io.github.rivomesh.cdc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OceanBaseProfileTest {
    @Test void vendorModeAndUserTenantAreAllRequired(){
        OceanBaseProfile.validate("5.7.25-OceanBase_CE-v4.3.5.6","MYSQL",1002);
        assertThrows(IllegalArgumentException.class,()->OceanBaseProfile.validate("8.4.13","MYSQL",1002));
        assertThrows(IllegalArgumentException.class,()->OceanBaseProfile.validate("OceanBase 4.3.5","ORACLE",1002));
        assertThrows(IllegalArgumentException.class,()->OceanBaseProfile.validate("OceanBase 4.3.5","MYSQL",1));
        assertThrows(IllegalArgumentException.class,()->OceanBaseProfile.validate("OceanBase 4.3.5",null,1002));
    }
}
