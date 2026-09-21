package com.jnks.iot.server.service.queue.processing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.queue.SubmitStrategy;


@Component
@Slf4j
public class JnksIotRuleEngineSubmitStrategyFactory {

    public JnksIotRuleEngineSubmitStrategy newInstance(String name, SubmitStrategy submitStrategy) {
        switch (submitStrategy.getType()) {
            case BURST:
                return new BurstJnksIotRuleEngineSubmitStrategy(name);
            case BATCH:
                return new BatchJnksIotRuleEngineSubmitStrategy(name, submitStrategy.getBatchSize());
            case SEQUENTIAL_BY_ORIGINATOR:
                return new SequentialByOriginatorIdJnksIotRuleEngineSubmitStrategy(name);
            case SEQUENTIAL_BY_TENANT:
                return new SequentialByTenantIdJnksIotRuleEngineSubmitStrategy(name);
            case SEQUENTIAL:
                return new SequentialJnksIotRuleEngineSubmitStrategy(name);
            default:
                throw new RuntimeException("JnksIotRuleEngineProcessingStrategy with type " + submitStrategy.getType() + " is not supported!");
        }
    }

}
