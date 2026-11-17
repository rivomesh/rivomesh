package io.github.rivomesh.cdc;

import java.nio.charset.StandardCharsets;
import java.util.*;

/** Conservative logical workspace reserved before acknowledging a captured raw commit. */
public final class NormalizationBudget {
    private NormalizationBudget(){}
    public static long reserve(SourceTransaction transaction,Map<String,TableSchema> schemas){
        long bytes=Codec.encode(transaction).getBytes(StandardCharsets.UTF_8).length,identities=0;
        for(Change change:transaction.changes){
            if(change.operation==Change.Operation.BARRIER)continue;TableSchema table=schemas.get(change.tableId);
            if(table==null||table.version!=change.schemaVersion)throw new IllegalStateException("HISTORICAL_SCHEMA_MISSING");
            if(table.keyless()){
                // null (4 bytes) becomes a quoted 64-character SHA-256 ID (66 bytes).
                if(change.rowId==null)bytes=Math.addExact(bytes,62);else if(!change.rowId.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("ROW_IDENTITY_INVALID");
                // Updates may free an old identity, but reserving the full replacement is safe.
                if(change.after!=null)identities=Math.addExact(identities,JdbcNormalization.canonical(table,change.after).getBytes(StandardCharsets.UTF_8).length);
            }
        }
        return Math.addExact(bytes,identities);
    }
}
