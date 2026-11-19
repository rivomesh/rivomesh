package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;

import java.util.Map;

/** The existing ClickHouse-specific CLI entry delegates to the shared materialization receipt protocol. */
public final class ClickHouseRecoveryReview {
    private ClickHouseRecoveryReview(){}

    public static Map<String,Object> inspect(PipelineConfig config,String targetId,long expectedErrorGeneration)throws Exception{
        PipelineConfig.Target target=config.targets.stream().filter(item->item.id.equals(targetId)).findFirst()
                .orElseThrow(()->new IllegalArgumentException("CLICKHOUSE_RECOVERY_TARGET_UNKNOWN"));
        if(target.product!=TargetProduct.CLICKHOUSE)throw new IllegalArgumentException("CLICKHOUSE_RECOVERY_TARGET_UNSUPPORTED");
        return DestinationReceiptReview.inspect(config,targetId,expectedErrorGeneration);
    }
}
