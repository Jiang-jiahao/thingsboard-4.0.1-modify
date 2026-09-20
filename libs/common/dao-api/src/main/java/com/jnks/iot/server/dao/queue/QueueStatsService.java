package com.jnks.iot.server.dao.queue;

import com.jnks.iot.server.common.data.id.QueueStatsId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.queue.QueueStats;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;

public interface QueueStatsService extends EntityDaoService {

    QueueStats save(TenantId tenantId, QueueStats queueStats);

    QueueStats findQueueStatsById(TenantId tenantId, QueueStatsId queueStatsId);

    List<QueueStats> findQueueStatsByIds(TenantId tenantId, List<QueueStatsId> queueStatsId);

    QueueStats findByTenantIdAndNameAndServiceId(TenantId tenantId, String queueName, String serviceId);

    PageData<QueueStats> findByTenantId(TenantId tenantId, PageLink pageLink);

}
