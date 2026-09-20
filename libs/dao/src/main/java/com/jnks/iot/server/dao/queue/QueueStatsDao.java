package com.jnks.iot.server.dao.queue;

import com.jnks.iot.server.common.data.id.QueueStatsId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.queue.QueueStats;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.TenantEntityDao;

import java.util.List;

public interface QueueStatsDao extends Dao<QueueStats>, TenantEntityDao<QueueStats> {

    QueueStats findByTenantIdQueueNameAndServiceId(TenantId tenantId, String queueName, String serviceId);

    void deleteByTenantId(TenantId tenantId);

    List<QueueStats> findByIds(TenantId tenantId, List<QueueStatsId> queueStatsIds);

}