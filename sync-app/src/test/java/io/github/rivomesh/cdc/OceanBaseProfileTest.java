package io.github.rivomesh.cdc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static io.github.rivomesh.cdc.Gbase8cProfileTest.proxy;
import static org.junit.jupiter.api.Assertions.*;

class OceanBaseProfileTest {
    @Test void vendorModeAndUserTenantAreAllRequired(){
        OceanBaseProfile.validate("5.7.25-OceanBase_CE-v4.3.5.6","MYSQL",1002);
        assertThrows(IllegalArgumentException.class,()->OceanBaseProfile.validate("8.4.13","MYSQL",1002));
        assertThrows(IllegalArgumentException.class,()->OceanBaseProfile.validate("OceanBase 4.3.5","ORACLE",1002));
        assertThrows(IllegalArgumentException.class,()->OceanBaseProfile.validate("OceanBase 4.3.5","MYSQL",1));
        assertThrows(IllegalArgumentException.class,()->OceanBaseProfile.validate("OceanBase 4.3.5",null,1002));
    }

    @Test void sessionProbeKeepsTheOriginalConnectionFailureAndRestoresTimeout() {
        var failure=new SQLNonTransientConnectionException("Target went offline", "08S01");
        var statement=proxy(Statement.class,(instance,method,args)->switch(method.getName()){
            case "executeQuery" -> throw failure;
            case "close" -> null;
            default -> throw new AssertionError(method.getName());
        });
        var timeouts=new ArrayList<Integer>();
        var connection=proxy(Connection.class,(instance,method,args)->switch(method.getName()){
            case "getNetworkTimeout" -> 60_000;
            case "setNetworkTimeout" -> {timeouts.add((Integer)args[1]);yield null;}
            case "createStatement" -> statement;
            case "isClosed" -> false;
            default -> throw new AssertionError(method.getName());
        });

        assertSame(failure,assertThrows(SQLNonTransientConnectionException.class,()->OceanBaseProfile.check(connection)));
        assertEquals(List.of(10_000,60_000),timeouts);
    }
}
