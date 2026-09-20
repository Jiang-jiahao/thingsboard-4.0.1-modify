package com.jnks.iot.server.service.queue.ruleengine;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.queue.TbQueueAdmin;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.TbServiceInfoProvider;
import com.jnks.iot.server.queue.provider.TbQueueProducerProvider;
import com.jnks.iot.server.queue.provider.TbRuleEngineQueueFactory;
import com.jnks.iot.server.service.queue.processing.TbRuleEngineProcessingStrategyFactory;
import com.jnks.iot.server.service.queue.processing.TbRuleEngineSubmitStrategyFactory;
import com.jnks.iot.server.service.stats.RuleEngineStatisticsService;

/**
 * 任务执行引擎消费者上下文对象
 */
@Component
@Slf4j
@Data
public class TbRuleEngineConsumerContext {

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
    private final TbRuleEngineSubmitStrategyFactory submitStrategyFactory;
    private final TbRuleEngineProcessingStrategyFactory processingStrategyFactory;
    private final TbRuleEngineQueueFactory queueFactory;
    private final RuleEngineStatisticsService statisticsService;
    private final TbServiceInfoProvider serviceInfoProvider;
    private final PartitionService partitionService;
    private final TbQueueProducerProvider producerProvider;
    private final TbQueueAdmin queueAdmin;

}
