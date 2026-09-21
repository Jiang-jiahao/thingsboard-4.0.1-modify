package com.jnks.iot.server.service.stats;

import com.jnks.iot.server.service.queue.JnksIotRuleEngineConsumerStats;

/**
 * 规则引擎统计服务
 */
public interface RuleEngineStatisticsService {

    void reportQueueStats(long ts, JnksIotRuleEngineConsumerStats stats);
}
