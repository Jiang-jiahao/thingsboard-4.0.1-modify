package org.thingsboard.server.service.entitiy.queue;

import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.id.QueueId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.queue.Queue;

import java.util.List;

/**
 * 规则引擎队列业务层契约：保存/删除队列，以及按租户配置同步队列。
 * <p>
 * 由 QueueController 与租户配置变更路径调用。
 */
public interface TbQueueService {

    /** 保存队列并通知集群。 */
    Queue saveQueue(Queue queue);

    /** 按 ID 删除队列。 */
    void deleteQueue(TenantId tenantId, QueueId queueId);

    /** 按名称删除队列。 */
    void deleteQueueByQueueName(TenantId tenantId, String queueName);

    /** 租户配置变更后，为相关租户增删/更新队列。 */
    void updateQueuesByTenants(List<TenantId> tenantIds, TenantProfile newTenantProfile, TenantProfile oldTenantProfile);
}
