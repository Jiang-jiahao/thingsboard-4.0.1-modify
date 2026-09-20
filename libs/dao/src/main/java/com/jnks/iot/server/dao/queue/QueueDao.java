package com.jnks.iot.server.dao.queue;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.dao.Dao;

import java.util.List;

public interface QueueDao extends Dao<Queue> {
    Queue findQueueByTenantIdAndTopic(TenantId tenantId, String topic);

    Queue findQueueByTenantIdAndName(TenantId tenantId, String name);

    List<Queue> findAllMainQueues();

    List<Queue> findAllQueues();

    List<Queue> findAllByTenantId(TenantId tenantId);

    PageData<Queue> findQueuesByTenantId(TenantId tenantId, PageLink pageLink);
}