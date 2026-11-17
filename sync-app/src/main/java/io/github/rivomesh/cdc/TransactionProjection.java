package io.github.rivomesh.cdc;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/** The exact subscriber transaction hashed by both automatic apply and recovery. */
public final class TransactionProjection {
    private TransactionProjection(){}
    public static SourceTransaction project(SourceTransaction normalized,Function<Change,TableSchema> schemas,Predicate<TableSchema> includes){
        SourceTransaction tx=Codec.decode(Codec.encode(normalized),SourceTransaction.class);
        List<Change> changes=new ArrayList<>();
        for(Change change:tx.changes){
            if(change.operation==Change.Operation.BARRIER){changes.add(change);continue;}
            TableSchema table=schemas.apply(change);
            if(table==null)throw new IllegalStateException("HISTORICAL_SCHEMA_MISSING");
            if(includes.test(table))changes.add(change);
        }
        tx.changes=changes;return tx;
    }
}
