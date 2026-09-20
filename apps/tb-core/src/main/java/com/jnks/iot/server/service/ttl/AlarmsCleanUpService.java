package com.jnks.iot.server.service.ttl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageDataIterable;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.dao.alarm.AlarmDao;
import com.jnks.iot.server.dao.alarm.AlarmService;
import com.jnks.iot.server.dao.tenant.TbTenantProfileCache;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.service.action.EntityActionService;

import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 告警 TTL 清理服务：按租户配置删除过期告警并推送 ALARM_DELETE 到规则引擎。
 * <p>
 * <b>职责：</b>遍历租户，读取租户配置 {@code alarmsTtlDays}，批量删除结束时间早于过期点的告警，
 * 并清理已无实例的告警类型。
 * <p>
 * <b>触发方式：</b>定时 TTL（{@code sql.ttl.alarms.checking_interval}）。
 * <p>
 * <b>清理对象：</b>过期告警及其类型；仅处理本节点负责的租户分区。
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AlarmsCleanUpService {

    @Value("${sql.ttl.alarms.removal_batch_size}")
    private Integer removalBatchSize;

    private final TenantService tenantService;
    private final AlarmDao alarmDao;
    private final AlarmService alarmService;
    private final EntityActionService entityActionService;
    private final PartitionService partitionService;
    private final TbTenantProfileCache tenantProfileCache;

    /** 按租户 TTL 批量删除过期告警。 */
    @Scheduled(initialDelayString = "#{T(org.apache.commons.lang3.RandomUtils).nextLong(0, ${sql.ttl.alarms.checking_interval})}", fixedDelayString = "${sql.ttl.alarms.checking_interval}")
    public void cleanUp() {
        PageDataIterable<TenantId> tenants = new PageDataIterable<>(tenantService::findTenantsIds, 10_000);
        for (TenantId tenantId : tenants) {
            try {
                cleanUp(tenantId);
            } catch (Exception e) {
                getLogger().warn("Failed to clean up alarms by ttl for tenant {}", tenantId, e);
            }
        }
    }

    private void cleanUp(TenantId tenantId) {
        if (!partitionService.resolve(ServiceType.TB_CORE, tenantId, tenantId).isMyPartition()) {
            return;
        }

        Optional<DefaultTenantProfileConfiguration> tenantProfileConfiguration = tenantProfileCache.get(tenantId).getProfileConfiguration();
        if (tenantProfileConfiguration.isEmpty() || tenantProfileConfiguration.get().getAlarmsTtlDays() == 0) {
            return;
        }

        long ttl = TimeUnit.DAYS.toMillis(tenantProfileConfiguration.get().getAlarmsTtlDays());
        long expirationTime = System.currentTimeMillis() - ttl;

        PageLink removalBatchRequest = new PageLink(removalBatchSize, 0);
        long totalRemoved = 0;
        Set<String> typesToRemove = new HashSet<>();
        while (true) {
            PageData<AlarmId> toRemove = alarmDao.findAlarmsIdsByEndTsBeforeAndTenantId(expirationTime, tenantId, removalBatchRequest);
            for (AlarmId alarmId : toRemove.getData()) {
                Alarm alarm = alarmService.delAlarm(tenantId, alarmId, false).getAlarm();
                if (alarm != null) {
                    entityActionService.pushEntityActionToRuleEngine(alarm.getOriginator(), alarm, tenantId, null, ActionType.ALARM_DELETE, null);
                    totalRemoved++;
                    typesToRemove.add(alarm.getType());
                }
            }
            if (!toRemove.hasNext()) {
                break;
            }
        }

        alarmService.delAlarmTypes(tenantId, typesToRemove);

        if (totalRemoved > 0) {
            getLogger().info("Removed {} outdated alarm(s) for tenant {} older than {}", totalRemoved, tenantId, new Date(expirationTime));
        }
    }

    // wrapper for tests to spy on static logger
    Logger getLogger() {
        return log;
    }

}
