package org.thingsboard.server.service.ttl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.thingsboard.server.dao.audit.AuditLogDao;
import org.thingsboard.server.dao.sqlts.insert.sql.SqlPartitioningRepository;
import org.thingsboard.server.queue.discovery.PartitionService;

import java.util.concurrent.TimeUnit;

import static org.thingsboard.server.dao.model.ModelConstants.AUDIT_LOG_TABLE_NAME;

/**
 * 审计日志 TTL 清理服务：删除过期审计日志分区。
 * <p>
 * <b>触发方式：</b>定时 TTL（{@code sql.ttl.audit_logs.checking_interval_ms}）。
 * <p>
 * <b>清理对象：</b>audit_log 过期数据；非本系统分区则只清本地分区缓存。
 */
@Service
@ConditionalOnExpression("${sql.ttl.audit_logs.enabled:true} && ${sql.ttl.audit_logs.ttl:0} > 0")
@Slf4j
public class AuditLogsCleanUpService extends AbstractCleanUpService {

    private final AuditLogDao auditLogDao;
    private final SqlPartitioningRepository partitioningRepository;

    @Value("${sql.ttl.audit_logs.ttl:0}")
    private long ttlInSec;
    @Value("${sql.audit_logs.partition_size:168}")
    private int partitionSizeInHours;

    public AuditLogsCleanUpService(PartitionService partitionService, AuditLogDao auditLogDao, SqlPartitioningRepository partitioningRepository) {
        super(partitionService);
        this.auditLogDao = auditLogDao;
        this.partitioningRepository = partitioningRepository;
    }

    /** 按 TTL 清理过期审计日志。 */
    @Scheduled(initialDelayString = "#{T(org.apache.commons.lang3.RandomUtils).nextLong(0, ${sql.ttl.audit_logs.checking_interval_ms})}",
            fixedDelayString = "${sql.ttl.audit_logs.checking_interval_ms}")
    public void cleanUp() {
        long auditLogsExpTime = getCurrentTimeMillis() - TimeUnit.SECONDS.toMillis(ttlInSec);
        log.debug("cleanup {}", auditLogsExpTime);
        if (isSystemTenantPartitionMine()) {
            auditLogDao.cleanUpAuditLogs(auditLogsExpTime);
        } else {
            partitioningRepository.cleanupPartitionsCache(AUDIT_LOG_TABLE_NAME, auditLogsExpTime, TimeUnit.HOURS.toMillis(partitionSizeInHours));
        }
    }

    /** 返回当前毫秒时间戳（便于测试替换）。 */
    public long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

}
