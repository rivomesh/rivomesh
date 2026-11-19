package io.github.rivomesh;

import io.github.rivomesh.cdc.*;

/** Selects the destination's actual checkpoint and consistency protocol. */
public final class PipelineSinks {
    private PipelineSinks(){}
    public static SinkAdapter open(PipelineConfig config,PipelineConfig.Target target,String generation,JdbcJournal journal,Journal.Lease lease)throws Exception{
        if(target.product==TargetProduct.HIVE)return new HiveSink(config.hiveSettings(target,generation),new JdbcPublicationStore(journal,target.id,lease),target.hive.maxMessageBytes);
        if(target.product==TargetProduct.DORIS)return new DorisSink(config.dorisSettings(target,generation),new JdbcPublicationStore(journal,target.id,lease),target.doris.maxMessageBytes);
        if(target.product==TargetProduct.CLICKHOUSE)return new ClickHouseSink(config.clickHouseSettings(target,generation),new JdbcPublicationStore(journal,target.id,lease),target.clickhouse==null?4*1024*1024:target.clickhouse.maxMessageBytes);
        if(target.product.messaging())return new MessageSink(target.id,target.mappings,new JdbcPublicationStore(journal,target.id,lease),()->MessagePublishers.open(target,MessagePublishers.binding(config.pipelineId,target.id,generation)),target.messaging.maxMessageBytes,
                target.product==TargetProduct.KAFKA?config.kafkaPhysicalGrant:null);
        return new JdbcSink(config.targetSettings(target,generation));
    }
}
