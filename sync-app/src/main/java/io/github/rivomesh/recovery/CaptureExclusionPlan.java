package io.github.rivomesh.recovery;

import io.github.rivomesh.cdc.Codec;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** One verified MySQL commit that would be excluded before the next journal sequence. */
public final class CaptureExclusionPlan {
    public String pipelineId, generation, checkpointHash, transactionId, boundaryHash, archiveSha256;
    public long epoch, errorGeneration, afterSequence;
    public Map<String,List<String>> affectedSubscriptions = new TreeMap<>();

    public String hash() {
        return Codec.sha256(Codec.encode(this));
    }

    public Map<String,Object> summary() {
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("planHash", hash());
        result.put("transactionId", transactionId);
        result.put("afterSequence", Long.toString(afterSequence));
        result.put("checkpointHash", checkpointHash);
        result.put("boundaryHash", boundaryHash);
        result.put("archiveSha256", archiveSha256);
        result.put("errorGeneration", errorGeneration);
        result.put("affectedSubscriptions", affectedSubscriptions);
        return result;
    }
}
