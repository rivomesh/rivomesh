package io.github.rivomesh.cdc;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class Gbase8cProfileTest {
    @Test void vendorDriverCoexistsWithOtherPostgresProtocolDrivers()throws Exception{
        java.sql.Driver driver=(java.sql.Driver)Class.forName("com.gbase8c.Driver").getDeclaredConstructor().newInstance();
        assertTrue(driver.acceptsURL("jdbc:gbase8c://host:5432/postgres"));
        assertFalse(driver.acceptsURL("jdbc:kingbase8://host/test"));
        assertEquals("org.postgresql.Driver",Class.forName("org.postgresql.Driver").getName());
        assertEquals("io.github.rivomesh.gaussdb.jdbc.Driver",Class.forName("io.github.rivomesh.gaussdb.jdbc.Driver").getName());
    }
    @Test void productAndModeAreIndependentRequirements(){
        Gbase8cProfile.validate("GBase8c","(GBase8cV6 S1.3.0B19) on aarch64","pg");
        assertThrows(IllegalArgumentException.class,()->Gbase8cProfile.validate("PostgreSQL","PostgreSQL 17","pg"));
        assertThrows(IllegalArgumentException.class,()->Gbase8cProfile.validate("GBase8c","PostgreSQL 17","pg"));
        for(String mode:Arrays.asList("oracle","mysql","sqlserver",null))assertThrows(IllegalArgumentException.class,()->Gbase8cProfile.validate("GBase8c","GBase8c V9",mode));
    }
    @Test void invalidModeRefusesBeforeChangingSessionOrRunningDdl()throws Exception{
        Session session=new Session();session.mode="oracle";
        assertEquals("GBASE8C_PG_MODE_REQUIRED",assertThrows(IllegalArgumentException.class,()->Gbase8cProfile.check(session.connection())).getMessage());
        assertEquals(Arrays.asList("SELECT version()","SHOW sql_compatibility"),session.queries);
    }
    @Test void wrongVendorRefusesBeforeAnyVendorSpecificQuery()throws Exception{
        Session session=new Session();session.version="PostgreSQL 17.12";
        assertEquals("GBASE8C_PRODUCT_REQUIRED",assertThrows(IllegalArgumentException.class,()->Gbase8cProfile.check(session.connection())).getMessage());
        assertEquals(Collections.singletonList("SELECT version()"),session.queries);
    }
    @Test void sessionGuaranteesAreCheckedAgainAfterReconnect()throws Exception{
        Session first=new Session();assertEquals("pg",Gbase8cProfile.check(first.connection()).get("mode"));
        assertTrue(first.queries.contains("SET synchronous_commit = on"));assertTrue(first.queries.contains("SET TIME ZONE 'UTC'"));
        Session readonly=new Session();readonly.readonly="on";
        SQLTransientConnectionException error=assertThrows(SQLTransientConnectionException.class,()->Gbase8cProfile.check(readonly.connection()));assertEquals("08006",error.getSQLState());
        Session legacy=new Session();legacy.emptyIsNull=true;assertEquals("GBASE8C_EMPTY_STRING_SEMANTICS_UNSUPPORTED",assertThrows(IllegalArgumentException.class,()->Gbase8cProfile.check(legacy.connection())).getMessage());
        Session encoding=new Session();encoding.encoding="GBK";assertEquals("GBASE8C_UTF8_REQUIRED",assertThrows(IllegalArgumentException.class,()->Gbase8cProfile.check(encoding.connection())).getMessage());
        assertTrue(first.queries.stream().noneMatch(q->q.startsWith("CREATE")||q.startsWith("ALTER")));
    }
    @SuppressWarnings("unchecked") static <T>T proxy(Class<T> type,InvocationHandler body){return(T)Proxy.newProxyInstance(type.getClassLoader(),new Class<?>[]{type},body);}
    static final class Session {
        String version="GBase8c V9",mode="pg",encoding="UTF8",readonly="off";boolean emptyIsNull;List<String>queries=new ArrayList<>();
        Connection connection(){return proxy(Connection.class,(p,m,a)->{
            if(m.getName().equals("getMetaData"))return proxy(DatabaseMetaData.class,(x,n,b)->n.getName().equals("getDatabaseProductName")?"GBase8c":null);
            if(m.getName().equals("createStatement"))return proxy(Statement.class,(x,n,b)->{
                if(n.getName().equals("executeQuery")){String q=(String)b[0];queries.add(q);Object value=q.equals("SELECT version()")?version:q.equals("SHOW sql_compatibility")?mode:q.equals("SHOW server_encoding")?encoding:q.equals("SHOW transaction_read_only")?readonly:emptyIsNull;boolean[]next={false};
                    return proxy(ResultSet.class,(y,o,c)->{switch(o.getName()){case"next":boolean result=!next[0];next[0]=true;return result;case"getString":return value;case"getBoolean":return value;default:return null;}});}
                if(n.getName().equals("execute")){queries.add((String)b[0]);return false;}return null;
            });return null;
        });}
    }
}
