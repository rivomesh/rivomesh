package io.github.rivomesh;

import io.github.rivomesh.cdc.TableSchema;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.regex.Pattern;

/** Canonical microsecond timestamp values shared by snapshot, pgoutput, and JDBC delivery. */
public final class PostgresTimestamp {
    static final int CIVIL_OID = 1114;
    static final int INSTANT_OID = 1184;
    private static final String OUT_OF_RANGE = "POSTGRESQL_TIMESTAMP_OUT_OF_RANGE";
    private static final LocalDateTime FIRST = LocalDateTime.of(1000, 1, 1, 0, 0);
    private static final LocalDateTime LAST = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 499_999_000);
    private static final Pattern TEXT = Pattern.compile(
            "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?");
    private static final DateTimeFormatter FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);

    private PostgresTimestamp() {}

    public static boolean isColumn(TableSchema.Column column) {
        return (column.type == TableSchema.Type.DATETIME && "1114".equals(column.sourceType))
                || (column.type == TableSchema.Type.TIMESTAMP && "1184".equals(column.sourceType));
    }

    static String canonicalSource(String text, boolean utcInstant) {
        if (utcInstant) {
            if (text.endsWith("+00")) {
                text = text.substring(0, text.length() - 3);
            } else if (text.endsWith("+00:00")) {
                text = text.substring(0, text.length() - 6);
            } else {
                throw new IllegalStateException(OUT_OF_RANGE);
            }
        }
        try {
            return format(localTime(text));
        } catch (IllegalArgumentException | java.time.DateTimeException invalid) {
            throw new IllegalStateException(OUT_OF_RANGE, invalid);
        }
    }

    public static LocalDateTime localTime(String text) {
        if (!TEXT.matcher(text).matches()) {
            throw new IllegalArgumentException(OUT_OF_RANGE);
        }
        return inRange(LocalDateTime.parse(text, FORMAT));
    }

    public static String format(LocalDateTime value) {
        return FORMAT.format(inRange(value));
    }

    private static LocalDateTime inRange(LocalDateTime value) {
        if (value.isBefore(FIRST) || value.isAfter(LAST) || value.getNano() % 1000 != 0) {
            throw new IllegalArgumentException(OUT_OF_RANGE);
        }
        return value;
    }
}
