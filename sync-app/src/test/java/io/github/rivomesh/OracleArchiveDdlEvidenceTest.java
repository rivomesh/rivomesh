package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.OracleDdlEvidence;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OracleArchiveDdlEvidenceTest {
    private static final String GUID = "1234567890ABCDEF1234567890ABCDEF";

    private final SourcePosition checkpoint = new SourcePosition(1, "ORACLE", "fixture",
            Map.of("scn","100","archiveStartScn","80","pdbGuid",GUID,"pdbUid","42"));
    private final OracleArchiveWindow.Coverage range =
            new OracleArchiveWindow.Coverage(BigInteger.valueOf(80),BigInteger.valueOf(120));

    @Test void keepsOnlyBoundedFingerprintsOfTheObservedRow() {
        String ddl = "ALTER TABLE RIVO_APP.RIVO_ORDERS ADD EXTRA NUMBER(12)";
        OracleDdlEvidence row = OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"42","0","0","0",
                "110"," 0x000005.00000010.0010 ","0","USER DDL (PlSql=0 RecDep=0)",ddl);
        assertNotNull(row);
        assertEquals("0x000005.00000010.0010",row.recordSetId());
        assertEquals("110",row.eventScn());
        assertEquals("120",row.archiveEndScnExclusive());
        assertEquals(OracleDdlEvidence.Kind.USER_DDL,row.kind());
        assertEquals(Codec.sha256(ddl),row.decodedSqlSha256());
        assertEquals(row,OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"42","0","0","0",
                "110"," 0x000005.00000010.0010 ","0","USER DDL",ddl));
        assertFalse(Codec.encode(row).contains("ALTER TABLE"));
        assertFalse(Codec.encode(row).contains(GUID));
    }

    @Test void refusesIncompleteWrongPdbAndOutOfRangeEvidence() {
        assertNull(OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"43","0","0","0",
                "110","0x000005.00000010.0010","0","USER DDL","ADD"));
        assertNull(OracleArchiveRead.ddlEvidence(checkpoint,range,null,null,"0","0","0",
                "110","0x000005.00000010.0010","0","USER DDL","ADD"));
        assertNull(OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"42","1","0","0",
                "110","0x000005.00000010.0010","0","USER DDL","ADD"));
        assertNull(OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"42","0","2","0",
                "110","0x000005.00000010.0010","0","USER DDL","ADD"));
        assertNull(OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"42","0","0","0",
                "120","0x000005.00000010.0010","0","USER DDL","ADD"));
        assertNull(OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"42","0","0","0",
                "110","0x000005.00000010.0010",null,"USER DDL","ADD"));
        assertNull(OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"42","0","0","0",
                "110","0x000005.00000010.0010","0",null,"ADD"));
        assertNull(OracleArchiveRead.ddlEvidence(checkpoint,range,GUID,"42","0","0","0",
                "110","0x000005.00000010.0010","0","USER DDL (PlSql=1 RecDep=0)","ADD"));
    }
}
