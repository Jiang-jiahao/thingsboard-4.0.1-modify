package org.thingsboard.server.service.stats;

import org.thingsboard.server.service.queue.TbRuleEngineConsumerStats;

/**
 * 规则引擎统计服务
 */
public interface RuleEngineStatisticsService {

    void reportQueueStats(long ts, TbRuleEngineConsumerStats stats);
}
