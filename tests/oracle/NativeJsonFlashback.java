import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;

import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonValue;

/** JDBC side of the disposable Oracle native JSON diagnostic. */
final class NativeJsonFlashback {
    private static final OracleJsonFactory JSON = new OracleJsonFactory();
    private static final Base64.Encoder BASE64 = Base64.getEncoder();

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Provide a disposable PDB URL and SCNs");
        }
        try (Connection connection = DriverManager.getConnection(args[0], "C##RIVO_PROBE",
                System.getenv("RIVOMESH_ORACLE_PROBE_PASSWORD"))) {
            for (int i = 1; i < args.length; i++) {
                BigDecimal scn = new BigDecimal(args[i]);
                read(connection, scn, "RIVO_NATIVE_JR", "ID");
                read(connection, scn, "RIVO_NATIVE_JK", "TAG");
            }
        }
    }

    private static void read(Connection connection, BigDecimal scn, String table, String key) throws Exception {
        String sql = "SELECT ROWIDTOCHAR(ROWID)," + key + ",PAYLOAD,"
                + "JSON_SERIALIZE(PAYLOAD RETURNING BLOB ORDERED ERROR ON ERROR) FROM RIVO_APP." + table
                + " AS OF SCN ? ORDER BY " + key + ",ROWID";
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setQueryTimeout(30);
            query.setBigDecimal(1, scn);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    OracleJsonValue typed = rows.getObject(3, OracleJsonValue.class);
                    boolean sqlNull = rows.wasNull();
                    if (sqlNull != (typed == null)) {
                        throw new AssertionError("JDBC SQL NULL indicator differs from the typed value");
                    }
                    byte[] serialized;
                    try (InputStream input = rows.getBinaryStream(4)) {
                        serialized = input == null ? null : input.readNBytes(32769);
                    }
                    if (sqlNull != (serialized == null) || serialized != null && serialized.length > 32768) {
                        throw new AssertionError("Unexpected SQL NULL or oversize diagnostic JSON");
                    }
                    byte[] raw = null;
                    String rawType;
                    try {
                        raw = rows.getBytes(3);
                        rawType = raw == null ? "SQL_NULL"
                                : JSON.createJsonBinaryValue(ByteBuffer.wrap(raw)).getOracleJsonType().name();
                    } catch (SQLException error) {
                        rawType = "JDBC_ERROR_" + error.getErrorCode();
                    } catch (RuntimeException error) {
                        rawType = "OSON_DECODE_" + error.getClass().getSimpleName();
                    }
                    System.out.println(String.join("\t", scn.toPlainString(), table, rows.getString(1),
                            rows.getString(2), typed == null ? "SQL_NULL" : typed.getOracleJsonType().name(),
                            typed == null ? "-" : encode(typed.toString().getBytes(StandardCharsets.UTF_8)),
                            encode(serialized), rawType, encode(raw)));
                }
            }
        }
    }

    private static String encode(byte[] bytes) {
        return bytes == null ? "-" : BASE64.encodeToString(bytes);
    }
}
