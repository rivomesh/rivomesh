package io.github.rivomesh.cdc;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.rivomesh.api.SourcePosition;
import java.time.Instant;
import java.util.*;

/** Complete source commit boundary, with IDs stable across transport retries and primary promotion. */
public final class SourceTransaction {
    public enum Kind { DATA,SNAPSHOT,DDL,BARRIER }
    public String id,streamGeneration;
    public SourcePosition position;
    public Kind kind=Kind.DATA;
    public List<Change> changes=new ArrayList<>();
    public Map<String,String> attributes=new TreeMap<>();
    /** Source-native timing metadata. Never participates in source, projection or receipt hashes. */
    @JsonIgnore public Instant sourceCommittedAt;
    public void validate(){
        if(id==null||id.isEmpty()||id.length()>512||streamGeneration==null||streamGeneration.isEmpty()||position==null||kind==null||changes==null)
            throw new IllegalArgumentException("TRANSACTION_INVALID");
        Set<String> seen=new HashSet<>();
        for(Change change:changes){change.validate();if(!seen.add(change.eventId))throw new IllegalArgumentException("DUPLICATE_EVENT_ID");}
    }
}
