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
package org.thingsboard.server.service.apiusage;

import org.springframework.context.ApplicationListener;
import org.thingsboard.rule.engine.api.RuleEngineApiUsageStateService;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.TenantProfileId;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.common.stats.TbApiUsageStateClient;
import org.thingsboard.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;

/**
 * api使用统计服务接口
 */
public interface TbApiUsageStateService extends TbApiUsageStateClient, RuleEngineApiUsageStateService, ApplicationListener<PartitionChangeEvent> {

    /**
     * core服务接收并处理来自其他微服务（如规则引擎、传输层）上报的API使用数据
     * @param msg api使用数据
     * @param callback 回调函数
     */
    void process(TbProtoQueueMsg<ToUsageStatsServiceMsg> msg, TbCallback callback);

    /**
     * 当租户的配置（如API限额）变更时，触发core服务和执行引擎服务实例所有相关租户的API使用状态重新计算与更新。
     * @param tenantProfileId 租户配置id
     */
    void onTenantProfileUpdate(TenantProfileId tenantProfileId);

    /**
     * 当单个租户信息（如关联的配置）变更时，触发core服务和执行引擎服务实例所有相关租户的API使用状态重新计算与更新。
     * @param tenantId 租户id
     */
    void onTenantUpdate(TenantId tenantId);

    /**
     * 清理被删除租户的API使用状态和统计数据，触发core服务和执行引擎服务实例释放资源。
     * @param tenantId 租户id
     */
    void onTenantDelete(TenantId tenantId);

    /**
     * core服务和执行引擎服务实例，清理被删除客户的API使用状态和统计数据。
     * @param customerId 客户id
     */
    void onCustomerDelete(CustomerId customerId);

    /**
     * 当API使用状态实体在数据库中被更新后，用于同步更新core服务和执行引擎服务实例内部缓存，保证集群内状态一致。
     * @param tenantId 租户id
     */
    void onApiUsageStateUpdate(TenantId tenantId);
}
