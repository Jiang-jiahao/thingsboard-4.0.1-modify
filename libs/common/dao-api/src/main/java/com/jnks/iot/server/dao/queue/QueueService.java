package com.jnks.iot.server.dao.queue;

import com.jnks.iot.server.common.data.id.QueueId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;

public interface QueueService extends EntityDaoService {

    Queue saveQueue(Queue queue);

    void deleteQueue(TenantId tenantId, QueueId queueId);

    List<Queue> findQueuesByTenantId(TenantId tenantId);

    PageData<Queue> findQueuesByTenantId(TenantId tenantId, PageLink pageLink);

    List<Queue> findAllQueues();

    Queue findQueueById(TenantId tenantId, QueueId queueId);

    Queue findQueueByTenantIdAndName(TenantId tenantId, String name);

    Queue findQueueByTenantIdAndNameInternal(TenantId tenantId, String queueName);

    void deleteQueuesByTenantId(TenantId tenantId);
}