package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.util.*;

/** One durable normalization order shared by every subscriber. */
public interface NormalizedJournal extends Journal {
    interface BaselineFilter { boolean includes(SourcePosition baseline,String transactionId); }
    /** Charge normalization/identity workspace with raw capture; consume it atomically during normalize. */
    default long append(Lease lease,SourceTransaction transaction,long normalizationReserve)throws Exception{
        if(normalizationReserve!=0)throw new UnsupportedOperationException("NORMALIZATION_RESERVATION_UNSUPPORTED");return append(lease,transaction);
    }
    /** Process at most one snapshot part; true means all occurrence identities are durably seeded. */
    boolean seedIdentities(Lease lease,TableSchema table,SnapshotState snapshot)throws Exception;
    SourceTransaction normalize(Lease lease,Record record,Map<String,TableSchema> schemas,Map<String,SourcePosition> baselines,BaselineFilter filter)throws Exception;
}
