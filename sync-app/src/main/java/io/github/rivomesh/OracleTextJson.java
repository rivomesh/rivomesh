package io.github.rivomesh;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Identifies and reads bounded, strictly constrained Oracle VARCHAR2 JSON. */
final class OracleTextJson {
    private static final String STRICT_UNIQUE = "STRICT\\s+WITH\\s+UNIQUE\\s+KEYS";
    private static final Pattern CONSTRAINT = Pattern.compile(
            "(?i)\\s*(?:\"((?:[^\"]|\"\")++)\"|([A-Z][A-Z0-9_$#]*))"
                    + "\\s+IS\\s+JSON(?:\\s*\\(\\s*" + STRICT_UNIQUE + "\\s*\\)|\\s+" + STRICT_UNIQUE + ")\\s*");
    private static final JsonFactory JSON = JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .build();

    private OracleTextJson() {}

    static Set<String> columns(Connection pdb, String owner, String table, int querySeconds) throws SQLException {
        Set<String> columns = new HashSet<>();
        // The dictionary can truncate long conditions. Only inspect short, direct checks.
        try (PreparedStatement query = pdb.prepareStatement("SELECT search_condition_vc FROM all_constraints "
                + "WHERE owner=? AND table_name=? AND constraint_type='C' AND status='ENABLED' "
                + "AND validated='VALIDATED' AND deferrable='NOT DEFERRABLE' "
                + "AND LENGTHB(search_condition_vc)<=512")) {
            query.setQueryTimeout(querySeconds);
            query.setString(1, owner);
            query.setString(2, table);
            try (ResultSet constraints = query.executeQuery()) {
                while (constraints.next()) {
                    String column = constraintColumn(constraints.getString(1));
                    if (column != null) columns.add(column);
                }
            }
        }
        return columns;
    }

    static String constraintColumn(String expression) {
        if (expression == null) return null;
        Matcher constraint = CONSTRAINT.matcher(expression);
        if (!constraint.matches()) return null;
        if (constraint.group(1) != null) return constraint.group(1).replace("\"\"", "\"");
        return constraint.group(2).toUpperCase(Locale.ROOT);
    }

    static String value(String input) {
        if (input == null) return null;
        try (JsonParser parser = JSON.createParser(input)) {
            if (parser.nextToken() == null) throw invalid();
            parser.skipChildren();
            if (parser.nextToken() != null) throw invalid();
            return input;
        } catch (IOException rejected) {
            throw invalid();
        }
    }

    private static IllegalStateException invalid() {
        return new IllegalStateException("ORACLE_TEXT_JSON_VALUE_INVALID");
    }
}
