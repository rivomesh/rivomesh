package io.github.rivomesh.api;

import java.util.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Source-specific, versioned position. Intentionally has no lexical/temporal ordering operation. */
public final class SourcePosition {
    private final int formatVersion;
    private final String sourceType, sourceId;
    private final Map<String,String> payload;
    @JsonCreator
    public SourcePosition(@JsonProperty("formatVersion") int version, @JsonProperty("sourceType") String type,
                          @JsonProperty("sourceId") String id, @JsonProperty("payload") Map<String,String> payload) {
        if (version < 1 || type == null || type.isEmpty() || id == null || id.isEmpty() || payload == null)
            throw new IllegalArgumentException("INVALID_SOURCE_POSITION");
        this.formatVersion=version; this.sourceType=type; this.sourceId=id;
        this.payload=Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }
    public int getFormatVersion() { return formatVersion; }
    public String getSourceType() { return sourceType; }
    public String getSourceId() { return sourceId; }
    public Map<String,String> getPayload() { return payload; }
    @Override public boolean equals(Object value) {
        if(!(value instanceof SourcePosition))return false;SourcePosition p=(SourcePosition)value;
        return formatVersion==p.formatVersion&&sourceType.equals(p.sourceType)&&sourceId.equals(p.sourceId)&&payload.equals(p.payload);
    }
    @Override public int hashCode(){return Objects.hash(formatVersion,sourceType,sourceId,payload);}
}
