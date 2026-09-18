package org.thingsboard.server.service.ttl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.thingsboard.server.dao.event.EventService;
import org.thingsboard.server.queue.discovery.PartitionService;

import java.util.concurrent.TimeUnit;

/**
 * 事件 TTL 清理服务：按配置删除过期的常规事件与调试事件。
 * <p>
 * <b>触发方式：</b>定时 TTL（{@code sql.ttl.events.execution_interval_ms}）。
 * <p>
 * <b>清理对象：</b>event 表中超过 {@code events_ttl} / {@code debug_events_ttl} 的记录。
 */
@Slf4j
@Service
public class EventsCleanUpService extends AbstractCleanUpService {

    public static final String RANDOM_DELAY_INTERVAL_MS_EXPRESSION =
            "#{T(org.apache.commons.lang3.RandomUtils).nextLong(0, ${sql.ttl.events.execution_interval_ms})}";

    @Value("${sql.ttl.events.events_ttl}")
    private long ttlInSec;

    @Value("${sql.ttl.events.debug_events_ttl}")
    private long debugTtlInSec;

    @Value("${sql.ttl.events.enabled}")
    private boolean ttlTaskExecutionEnabled;

    private final EventService eventService;

    public EventsCleanUpService(PartitionService partitionService, EventService eventService) {
        super(partitionService);
        this.eventService = eventService;
    }

    /** 按 TTL 清理常规事件与调试事件。 */
    @Scheduled(initialDelayString = RANDOM_DELAY_INTERVAL_MS_EXPRESSION, fixedDelayString = "${sql.ttl.events.execution_interval_ms}")
    public void cleanUp() {
        if (ttlTaskExecutionEnabled) {
            long ts = System.currentTimeMillis();
            long regularEventExpTs = ttlInSec > 0 ? ts - TimeUnit.SECONDS.toMillis(ttlInSec) : 0;
            long debugEventExpTs = debugTtlInSec > 0 ? ts - TimeUnit.SECONDS.toMillis(debugTtlInSec) : 0;
            eventService.cleanupEvents(regularEventExpTs, debugEventExpTs, isSystemTenantPartitionMine());
        }
    }

}
