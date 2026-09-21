package com.jnks.iot.server.service.stats;

import com.google.common.util.concurrent.FutureCallback;
import jakarta.annotation.Nullable;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.server.common.data.id.QueueStatsId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.JsonDataEntry;
import com.jnks.iot.server.common.data.kv.LongDataEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.queue.QueueStats;
import com.jnks.iot.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import com.jnks.iot.server.dao.queue.QueueStatsService;
import com.jnks.iot.server.dao.usagerecord.ApiLimitService;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.service.queue.JnksIotRuleEngineConsumerStats;
import com.jnks.iot.server.service.telemetry.TelemetrySubscriptionService;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 负责收集、存储和报告规则引擎的性能指标和异常信息
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DefaultRuleEngineStatisticsService implements RuleEngineStatisticsService {

    public static final String RULE_ENGINE_EXCEPTION = "ruleEngineException";
    public static final FutureCallback<Void> CALLBACK = new FutureCallback<Void>() {
        @Override
        public void onSuccess(@Nullable Void result) {

        }

        @Override
        public void onFailure(Throwable t) {
            log.warn("Failed to persist statistics", t);
        }
    };

    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final TelemetrySubscriptionService tsService;
    private final QueueStatsService queueStatsService;
    private final ApiLimitService apiLimitService;
    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<TenantQueueKey, QueueStatsId> tenantQueueStats = new ConcurrentHashMap<>();

    @Value("${queue.rule-engine.stats.max-error-message-length:4096}")
    private int maxErrorMessageLength;

    @Override
    public void reportQueueStats(long ts, JnksIotRuleEngineConsumerStats ruleEngineStats) {
        String queueName = ruleEngineStats.getQueueName();
        ruleEngineStats.getTenantStats().forEach((id, stats) -> {
            try {
                TenantId tenantId = TenantId.fromUUID(id);
                QueueStatsId queueStatsId = getQueueStatsId(tenantId, queueName);
                if (stats.getTotalMsgCounter().get() > 0) {
                    List<TsKvEntry> tsList = stats.getCounters().entrySet().stream()
                            .map(kv -> new BasicTsKvEntry(ts, new LongDataEntry(kv.getKey(), (long) kv.getValue().get())))
                            .collect(Collectors.toList());
                    if (!tsList.isEmpty()) {
                        long ttl = apiLimitService.getLimit(tenantId, DefaultTenantProfileConfiguration::getQueueStatsTtlDays);
                        ttl = TimeUnit.DAYS.toSeconds(ttl);
                        tsService.saveTimeseriesInternal(TimeseriesSaveRequest.builder()
                                .tenantId(tenantId)
                                .entityId(queueStatsId)
                                .entries(tsList)
                                .ttl(ttl)
                                .callback(CALLBACK)
                                .build());
                    }
                }
            } catch (Exception e) {
                if (!"Asset is referencing to non-existent tenant!".equalsIgnoreCase(e.getMessage())) {
                    log.debug("[{}] Failed to store the statistics", id, e);
                }
            }
        });
        ruleEngineStats.getTenantExceptions().forEach((tenantId, e) -> {
            try {
                TsKvEntry tsKv = new BasicTsKvEntry(e.getTs(), new JsonDataEntry(RULE_ENGINE_EXCEPTION, e.toJsonString(maxErrorMessageLength)));
                long ttl = apiLimitService.getLimit(tenantId, DefaultTenantProfileConfiguration::getRuleEngineExceptionsTtlDays);
                ttl = TimeUnit.DAYS.toSeconds(ttl);
                tsService.saveTimeseriesInternal(TimeseriesSaveRequest.builder()
                        .tenantId(tenantId)
                        .entityId(getQueueStatsId(tenantId, queueName))
                        .entry(tsKv)
                        .ttl(ttl)
                        .callback(CALLBACK)
                        .build());
            } catch (Exception e2) {
                if (!"Asset is referencing to non-existent tenant!".equalsIgnoreCase(e2.getMessage())) {
                    log.debug("[{}] Failed to store the statistics", tenantId, e2);
                }
            }
        });
    }

    private QueueStatsId getQueueStatsId(TenantId tenantId, String queueName) {
        TenantQueueKey key = new TenantQueueKey(tenantId, queueName);
        QueueStatsId queueStatsId = tenantQueueStats.get(key);
        if (queueStatsId == null) {
            lock.lock();
            try {
                queueStatsId = tenantQueueStats.get(key);
                if (queueStatsId == null) {
                    QueueStats queueStats = queueStatsService.findByTenantIdAndNameAndServiceId(tenantId, queueName , serviceInfoProvider.getServiceId());
                    if (queueStats == null) {
                        queueStats = new QueueStats();
                        queueStats.setTenantId(tenantId);
                        queueStats.setQueueName(queueName);
                        queueStats.setServiceId(serviceInfoProvider.getServiceId());
                        queueStats = queueStatsService.save(tenantId, queueStats);
                    }
                    queueStatsId = queueStats.getId();
                    tenantQueueStats.put(key, queueStatsId);
                }
            } finally {
                lock.unlock();
            }
        }
        return queueStatsId;
    }

    @Data
    private static class TenantQueueKey {
        private final TenantId tenantId;
        private final String queueName;
    }
}
