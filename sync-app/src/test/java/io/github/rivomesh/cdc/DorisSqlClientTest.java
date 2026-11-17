package io.github.rivomesh.cdc;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DorisSqlClientTest {
    @Test void observedBackendStartupErrorsRetryWithSafeStructuredDiagnostics(){
        for(String suffix:new String[]{"", "[1794717059371: not alive]"}){SQLException error=new SQLException("errCode = 2, detailMessage = No backend available as scan node, please check the status of your backends."+suffix,"HY000",1105);assertTrue(DorisSqlClient.classified(error).retryable);assertEquals("DORIS_SQL_CONNECTION_RETRY",DorisSqlClient.classified(error).code);assertFalse(DorisSqlClient.classified(error).getMessage().contains("179471"));}
        assertTrue(DorisSqlClient.classified(new SQLException("connection interrupted","08S01",0)).retryable);
    }
    @Test void genericSyntaxAndAuthorizationDoNotBecomeStartupRetries(){
        for(SQLException error:new SQLException[]{new SQLException("errCode = 2, detailMessage = Syntax error near 'No backend available as scan node'","HY000",1105),new SQLException("permission denied","42000",1142),new SQLException("access denied","28000",1045),new SQLException("errCode = 2, detailMessage = No backend available as scan node","HY000",1142)})assertFalse(DorisSqlClient.classified(error).retryable);
    }
}
