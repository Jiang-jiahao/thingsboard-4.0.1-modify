package com.jnks.iot.server.service.queue.ruleengine;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.queue.provider.JnksIotQueueProducerProvider;
import com.jnks.iot.server.queue.provider.JnksIotRuleEngineQueueFactory;
import com.jnks.iot.server.service.queue.processing.JnksIotRuleEngineProcessingStrategyFactory;
import com.jnks.iot.server.service.queue.processing.JnksIotRuleEngineSubmitStrategyFactory;
import com.jnks.iot.server.service.stats.RuleEngineStatisticsService;

/**
 * 任务执行引擎消费者上下文对象
 */
@Component
@Slf4j
@Data
public class JnksIotRuleEngineConsumerContext {

    @Value("${queue.rule-engine.poll-interval}")
    private long pollDuration;
    @Value("${queue.rule-engine.pack-processing-timeout}")
    private long packProcessingTimeout;
    @Value("${queue.rule-engine.stats.enabled:true}")
    private boolean statsEnabled;
    @Value("${queue.rule-engine.prometheus-stats.enabled:false}")
    private boolean prometheusStatsEnabled;
    @Value("${queue.rule-engine.topic-deletion-delay:15}")
    private int topicDeletionDelayInSec;
    @Value("${queue.rule-engine.management-thread-pool-size:12}")
    private int mgmtThreadPoolSize;

    private final ActorSystemContext actorContext;
    private final StatsFactory statsFactory;
    private final JnksIotRuleEngineSubmitStrategyFactory submitStrategyFactory;
    private final JnksIotRuleEngineProcessingStrategyFactory processingStrategyFactory;
    private final JnksIotRuleEngineQueueFactory queueFactory;
    private final RuleEngineStatisticsService statisticsService;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final PartitionService partitionService;
    private final JnksIotQueueProducerProvider producerProvider;
    private final JnksIotQueueAdmin queueAdmin;

}
