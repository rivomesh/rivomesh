package io.github.rivomesh.cdc;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KingbaseProfileTest {
    @Test void productAndModeAreIndependentRequirements(){
        KingbaseProfile.validate("KingbaseES","KingbaseES V009R001C001B0025 on aarch64","pg");
        assertThrows(IllegalArgumentException.class,()->KingbaseProfile.validate("PostgreSQL","PostgreSQL 17","pg"));
        assertThrows(IllegalArgumentException.class,()->KingbaseProfile.validate("KingbaseES","PostgreSQL 17","pg"));
        for(String mode:Arrays.asList("oracle","mysql","sqlserver",null))assertThrows(IllegalArgumentException.class,()->KingbaseProfile.validate("KingbaseES","KingbaseES V9",mode));
    }
    @Test void invalidModeRefusesBeforeChangingSessionOrRunningDdl()throws Exception{
        Session session=new Session();session.mode="oracle";
        assertEquals("KINGBASE_PG_MODE_REQUIRED",assertThrows(IllegalArgumentException.class,()->KingbaseProfile.check(session.connection())).getMessage());
        assertEquals(Arrays.asList("SELECT version()","SHOW database_mode"),session.queries);
    }
    @Test void wrongVendorRefusesBeforeAnyVendorSpecificQuery()throws Exception{
        Session session=new Session();session.version="PostgreSQL 17.12";
        assertEquals("KINGBASE_PRODUCT_REQUIRED",assertThrows(IllegalArgumentException.class,()->KingbaseProfile.check(session.connection())).getMessage());
        assertEquals(Collections.singletonList("SELECT version()"),session.queries);
    }
    @Test void sessionGuaranteesAreCheckedAgainAfterReconnect()throws Exception{
        Session first=new Session();assertEquals("pg",KingbaseProfile.check(first.connection()).get("mode"));
        assertTrue(first.queries.contains("SET synchronous_commit = on"));assertTrue(first.queries.contains("SET TIME ZONE 'UTC'"));
        Session readonly=new Session();readonly.readonly="on";
        SQLTransientConnectionException error=assertThrows(SQLTransientConnectionException.class,()->KingbaseProfile.check(readonly.connection()));assertEquals("08006",error.getSQLState());
        Session legacy=new Session();legacy.emptyIsNull=true;assertEquals("KINGBASE_EMPTY_STRING_SEMANTICS_UNSUPPORTED",assertThrows(IllegalArgumentException.class,()->KingbaseProfile.check(legacy.connection())).getMessage());
        Session encoding=new Session();encoding.encoding="GBK";assertEquals("KINGBASE_UTF8_REQUIRED",assertThrows(IllegalArgumentException.class,()->KingbaseProfile.check(encoding.connection())).getMessage());
        assertTrue(first.queries.stream().noneMatch(q->q.startsWith("CREATE")||q.startsWith("ALTER")));
    }
    @SuppressWarnings("unchecked") static <T>T proxy(Class<T> type,InvocationHandler body){return(T)Proxy.newProxyInstance(type.getClassLoader(),new Class<?>[]{type},body);}
    static final class Session {
        String version="KingbaseES V9",mode="pg",encoding="UTF8",readonly="off";boolean emptyIsNull;List<String>queries=new ArrayList<>();
        Connection connection(){return proxy(Connection.class,(p,m,a)->{
            if(m.getName().equals("getMetaData"))return proxy(DatabaseMetaData.class,(x,n,b)->n.getName().equals("getDatabaseProductName")?"KingbaseES":null);
            if(m.getName().equals("createStatement"))return proxy(Statement.class,(x,n,b)->{
                if(n.getName().equals("executeQuery")){String q=(String)b[0];queries.add(q);Object value=q.equals("SELECT version()")?version:q.equals("SHOW database_mode")?mode:q.equals("SHOW server_encoding")?encoding:q.equals("SHOW transaction_read_only")?readonly:emptyIsNull;boolean[]next={false};
                    return proxy(ResultSet.class,(y,o,c)->{switch(o.getName()){case"next":boolean result=!next[0];next[0]=true;return result;case"getString":return value;case"getBoolean":return value;default:return null;}});}
                if(n.getName().equals("execute")){queries.add((String)b[0]);return false;}return null;
            });return null;
        });}
    }
}
