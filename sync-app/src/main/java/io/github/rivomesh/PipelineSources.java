package io.github.rivomesh;

import io.github.rivomesh.cdc.SourceAdapter;
import java.util.TreeSet;

/** Entry point shared by execution, preflight and validation for a configured source protocol. */
final class PipelineSources {
    private PipelineSources() {}

    static void requireSupported(PipelineConfig config) {
        if(!"MYSQL".equals(config.effectiveSourceType()))
            throw new IllegalArgumentException("SOURCE_ADAPTER_UNAVAILABLE");
    }

    static SourceAdapter open(PipelineConfig config,String generation)throws Exception {
        requireSupported(config);
        return new MysqlCdcSource(config.legacy(),new TreeSet<>(config.databases),generation);
    }
}
