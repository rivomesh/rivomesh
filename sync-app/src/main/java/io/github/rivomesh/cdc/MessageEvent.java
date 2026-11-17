package io.github.rivomesh.cdc;

import io.github.rivomesh.api.SourcePosition;
import java.util.*;

/** Versioned lossless wire payload. Transport epochs belong in headers, outside the immutable event hash. */
public final class MessageEvent {
    public int version=1,schemaVersion,ordinal,totalChanges,part,parts;
    public long sequence,rows;
    public String pipelineId,subscriptionId,generation,id,kind,namespace,tableId,transactionId,transactionKind,snapshotId,beforeKey,afterKey,contentHash;
    public SourcePosition position;
    public TableSchema schema;
    public Change change;
    public Map<String,Integer> expectedChanges=new TreeMap<>();
    public Map<String,String> attributes=new TreeMap<>();
    public Map<Integer,Change> controls=new TreeMap<>();
}
