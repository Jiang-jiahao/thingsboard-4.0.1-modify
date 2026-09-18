package org.thingsboard.server.service.ttl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.thingsboard.server.dao.timeseries.TimeseriesService;
import org.thingsboard.server.queue.discovery.PartitionService;
/**
 * 时序 TTL 清理服务：按系统配置清理过期时序数据。
 * <p>
 * <b>触发方式：</b>定时 TTL（{@code sql.ttl.ts.execution_interval_ms}）。
 * <p>
 * <b>清理对象：</b>超过 {@code sql.ttl.ts.ts_key_value_ttl} 的时序；仅系统分区节点执行。
 */
@Slf4j
@Service
public class TimeseriesCleanUpService extends AbstractCleanUpService {

    @Value("${sql.ttl.ts.ts_key_value_ttl}")
    protected long systemTtl;

    @Value("${sql.ttl.ts.enabled}")
    private boolean ttlTaskExecutionEnabled;

    private final TimeseriesService timeseriesService;

    public TimeseriesCleanUpService(PartitionService partitionService, TimeseriesService timeseriesService) {
        super(partitionService);
        this.timeseriesService = timeseriesService;
    }

    /** 按系统 TTL 清理过期时序。 */
    @Scheduled(initialDelayString = "${sql.ttl.ts.execution_interval_ms}", fixedDelayString = "${sql.ttl.ts.execution_interval_ms}")
    public void cleanUp() {
        if (ttlTaskExecutionEnabled && isSystemTenantPartitionMine()) {
            timeseriesService.cleanup(systemTtl);
        }
    }

}