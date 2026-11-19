package io.github.rivomesh;

import io.github.rivomesh.cdc.TableSchema;

import java.util.regex.Pattern;

/** A PostgreSQL clock value includes the distinct end-of-day value 24:00:00. */
public final class PostgresClock {
    static final int OID = 1083;
    private static final String SOURCE_TYPE = Integer.toString(OID);
    private static final Pattern CLOCK = Pattern.compile(
            "(?:(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\\.[0-9]{1,6})?|24:00:00(?:\\.0{1,6})?)");

    private PostgresClock() {}

    public static boolean isColumn(TableSchema.Column column) {
        return column.type == TableSchema.Type.TIME && SOURCE_TYPE.equals(column.sourceType);
    }

    public static String canonical(String text) {
        if (!CLOCK.matcher(text).matches()) {
            throw new IllegalArgumentException("POSTGRESQL_TIME_OUT_OF_RANGE");
        }
        return MysqlDuration.canonical(text);
    }
}
