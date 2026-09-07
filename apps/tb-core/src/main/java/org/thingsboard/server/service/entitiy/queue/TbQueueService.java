/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
