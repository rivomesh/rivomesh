package io.github.rivomesh;

import java.io.*;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FailureDiagnosticsTest {
    @Test void sqlDiagnosticsExposeOnlyStructuredCodesNotMessages()throws Exception{
        PrintStream original=System.err;ByteArrayOutputStream output=new ByteArrayOutputStream();try{System.setErr(new PrintStream(output,true,"UTF-8"));FailureDiagnostics.targetPaused("target","SAFE_CODE",new SQLException("password=private-row-and-password","42000",1105));FailureDiagnostics.targetRetrying("target","SAFE_CODE",new SQLException("second-private-payload","unsafe-private-state",1));}finally{System.setErr(original);}String text=output.toString("UTF-8");assertTrue(text.contains("sqlstate=42000 vendor=1105"));assertTrue(text.contains("sqlstate=UNKNOWN vendor=1"));assertFalse(text.contains("private"));assertFalse(text.contains("password="));
    }
}
