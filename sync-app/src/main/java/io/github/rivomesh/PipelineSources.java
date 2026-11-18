package io.github.rivomesh;

import io.github.rivomesh.cdc.SourceAdapter;
import java.util.TreeSet;

/** Entry point shared by execution, preflight and validation for a configured source protocol. */
final class PipelineSources {
    private PipelineSources() {}

    static void validate(PipelineConfig config) {
        String type=config.effectiveSourceType();
        if(config.source.postgresql!=null&&!"POSTGRESQL".equals(type))
            throw new IllegalArgumentException("POSTGRESQL_OPTIONS_REQUIRE_POSTGRESQL_SOURCE");
        if(config.source.oracle!=null&&!"ORACLE".equals(type))
            throw new IllegalArgumentException("ORACLE_OPTIONS_REQUIRE_ORACLE_SOURCE");
        switch(type){
            case "MYSQL":config.legacy().validate();break;
            case "POSTGRESQL":PostgresqlCdcSource.validate(config);break;
            case "ORACLE":OracleCdcSource.validate(config);break;
            default:throw new IllegalArgumentException("SOURCE_ADAPTER_UNAVAILABLE");
        }
    }

    static SourceAdapter open(PipelineConfig config,String generation)throws Exception {
        switch(config.effectiveSourceType()){
            case "MYSQL":return new MysqlCdcSource(config.legacy(),new TreeSet<>(config.databases),generation);
            case "POSTGRESQL":return new PostgresqlCdcSource(config,generation);
            case "ORACLE":return new OracleCdcSource(config,generation);
            default:throw new IllegalArgumentException("SOURCE_ADAPTER_UNAVAILABLE");
        }
    }
}
