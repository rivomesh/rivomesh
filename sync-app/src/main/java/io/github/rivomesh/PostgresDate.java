package io.github.rivomesh;

import io.github.rivomesh.cdc.TableSchema;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

/** The shared civil date range for PostgreSQL and MySQL destinations. */
public final class PostgresDate {
    static final int OID=1082;
    private static final String SOURCE_TYPE=Integer.toString(OID);

    private PostgresDate(){}

    public static boolean isColumn(TableSchema.Column column){
        return column.type==TableSchema.Type.DATE&&SOURCE_TYPE.equals(column.sourceType);
    }

    static String canonicalSource(String value){
        if(value.length()==10){
            try{
                LocalDate date=LocalDate.parse(value);
                if(date.getYear()>=1000)return date.toString();
            }catch(DateTimeParseException invalid){
                throw new IllegalStateException("POSTGRESQL_DATE_OUT_OF_RANGE");
            }
        }
        throw new IllegalStateException("POSTGRESQL_DATE_OUT_OF_RANGE");
    }
}
