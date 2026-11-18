package io.github.rivomesh;

import io.github.rivomesh.cdc.TableSchema;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;

/** Oracle DATE stores a wall-clock time through seconds. */
public final class OracleDate {
    private static final DateTimeFormatter NUMERIC=formatter("uuuu-MM-dd HH:mm:ss");
    private static final DateTimeFormatter MONTH_NAME=formatter("dd-MMM-uuuu HH:mm:ss");
    private static final LocalDate GREGORIAN_START=LocalDate.of(1582,10,15);

    private OracleDate(){}

    public static boolean isColumn(TableSchema.Column column){
        return column.type==TableSchema.Type.DATETIME&&"DATE".equals(column.sourceType);
    }

    public static LocalDateTime localTime(String value){
        return LocalDateTime.parse(value.replace(' ','T'));
    }

    static void prepareSession(Connection connection)throws Exception{
        try(Statement session=connection.createStatement()){
            session.setQueryTimeout(30);
            session.execute("ALTER SESSION SET NLS_CALENDAR='GREGORIAN'");
            session.execute("ALTER SESSION SET NLS_DATE_LANGUAGE='AMERICAN'");
            session.execute("ALTER SESSION SET NLS_DATE_FORMAT='SYYYY-MM-DD HH24:MI:SS'");
        }
    }

    static String snapshotExpression(String column){
        return "TO_CHAR("+column+", 'SYYYY-MM-DD HH24:MI:SS')";
    }

    static String snapshotValue(String value){
        return value==null?null:redoValue(value,"SYYYY-MM-DD HH24:MI:SS");
    }

    static String redoValue(String value,String mask){
        DateTimeFormatter formatter;
        switch(mask.toUpperCase(Locale.ROOT)){
            case "SYYYY-MM-DD HH24:MI:SS":
            case "YYYY-MM-DD HH24:MI:SS":formatter=NUMERIC;break;
            case "DD-MON-YYYY HH24:MI:SS":formatter=MONTH_NAME;break;
            default:throw new IllegalStateException("ORACLE_DATE_FORMAT_UNSUPPORTED");
        }
        try{
            LocalDateTime date=LocalDateTime.parse(value.trim(),formatter);
            if(date.getYear()>9999||date.toLocalDate().isBefore(GREGORIAN_START))
                throw new IllegalStateException("ORACLE_DATE_VALUE_OUT_OF_RANGE");
            return date.format(NUMERIC);
        }catch(DateTimeParseException invalid){
            throw new IllegalStateException("ORACLE_DATE_FORMAT_UNSUPPORTED");
        }
    }

    private static DateTimeFormatter formatter(String pattern){
        return new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern(pattern)
                .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);
    }
}
