package io.github.rivomesh;

import io.github.rivomesh.cdc.TableSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

final class OracleCharacters {
    private static final String DATABASE_CHARSET = "AL32UTF8";
    private static final String NATIONAL_CHARSET = "AL16UTF16";

    private OracleCharacters() {}

    static boolean national(TableSchema.Column column) {
        return "NCHAR".equals(column.sourceType) || "NVARCHAR2".equals(column.sourceType);
    }

    private static boolean requiresProfile(TableSchema.Column column) {
        return "CHAR".equals(column.sourceType) || national(column);
    }

    static void requireProfile(Connection pdb, TableSchema table, int querySeconds) throws SQLException {
        if (table.columns.stream().noneMatch(OracleCharacters::requiresProfile)) {
            return;
        }
        String databaseCharset = null;
        String nationalCharset = null;
        try (PreparedStatement query = pdb.prepareStatement("SELECT parameter,value FROM nls_database_parameters "
                + "WHERE parameter IN ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET')")) {
            query.setQueryTimeout(querySeconds);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    if ("NLS_CHARACTERSET".equals(rows.getString(1))) {
                        databaseCharset = rows.getString(2);
                    } else {
                        nationalCharset = rows.getString(2);
                    }
                }
            }
        }
        requireCharsets(databaseCharset, nationalCharset, table.columns.stream().anyMatch(OracleCharacters::national));
        for (TableSchema.Column column : table.columns) {
            if (requiresProfile(column)) {
                column.charset = national(column) ? NATIONAL_CHARSET : DATABASE_CHARSET;
            }
        }
    }

    static void requireCharsets(String databaseCharset, String nationalCharset, boolean nationalColumns) {
        if (!DATABASE_CHARSET.equals(databaseCharset) || nationalColumns && !NATIONAL_CHARSET.equals(nationalCharset)) {
            throw new IllegalStateException("ORACLE_CHARACTER_CHARSET_UNSUPPORTED");
        }
    }

    static String unistr(String literal) {
        StringBuilder value = new StringBuilder(literal.length());
        for (int offset = 0; offset < literal.length(); ) {
            char c = literal.charAt(offset++);
            if (c != '\\') {
                value.append(c);
                continue;
            }
            if (offset < literal.length() && literal.charAt(offset) == '\\') {
                value.append('\\');
                offset++;
                continue;
            }
            if (literal.length() - offset < 4) {
                throw invalidLiteral();
            }
            int unit = 0;
            for (int i = 0; i < 4; i++) {
                char hex = literal.charAt(offset++);
                int digit = hex <= 0x7f ? Character.digit(hex, 16) : -1;
                if (digit < 0) {
                    throw invalidLiteral();
                }
                unit = unit * 16 + digit;
            }
            value.append((char) unit);
        }
        if (value.codePoints().anyMatch(cp -> cp == 0 || cp >= Character.MIN_SURROGATE && cp <= Character.MAX_SURROGATE)) {
            throw invalidLiteral();
        }
        return value.toString();
    }

    private static IllegalStateException invalidLiteral() {
        return new IllegalStateException("ORACLE_REDO_UNISTR_UNSUPPORTED");
    }
}
