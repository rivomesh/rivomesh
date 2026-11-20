import io.github.rivomesh.JdbcConnections;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HexFormat;
import java.util.StringJoiner;

public class DmSourceTargetProbe {
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault("RIVOMESH_DM_PROBE_URL", "jdbc:dm://dm:5236");
        try (Connection connection = JdbcConnections.open("dm.jdbc.driver.DmDriver", url,
                "SYSDBA", System.getenv("RM_PG_DM_PASSWORD"));
             Statement query = connection.createStatement()) {
            if ("--version".equals(args[0])) {
                System.out.println(connection.getMetaData().getDatabaseProductVersion());
                return;
            }
            query.setQueryTimeout(15);
            try (ResultSet rows = query.executeQuery(args[0])) {
                int columns = rows.getMetaData().getColumnCount();
                while (rows.next()) {
                    StringJoiner row = new StringJoiner("|");
                    for (int column = 1; column <= columns; column++) {
                        int type = rows.getMetaData().getColumnType(column);
                        if (type == Types.BLOB || type == Types.BINARY
                                || type == Types.VARBINARY || type == Types.LONGVARBINARY) {
                            byte[] value = rows.getBytes(column);
                            row.add(value == null ? "<NULL>" : HexFormat.of().formatHex(value));
                        } else {
                            String value = rows.getString(column);
                            row.add(value == null ? "<NULL>" : value);
                        }
                    }
                    System.out.println(row);
                }
            }
        }
    }
}
