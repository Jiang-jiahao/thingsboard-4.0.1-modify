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
package org.thingsboard.server.service.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.exception.TenantNotFoundException;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.queue.discovery.TenantRoutingInfo;
import org.thingsboard.server.queue.discovery.TenantRoutingInfoService;

/**
 * 租户级队列路由信息提供者（仅 monolith / tb-core 生效）。
 * <p>
 * 根据租户档案判断规则引擎是否隔离，供分区服务决定消息投递到共享还是租户专属队列。
 *
 * @see TenantRoutingInfoService
 */
@Slf4j
@Service
@ConditionalOnExpression("'${service.type:null}'=='monolith' || '${service.type:null}'=='tb-core'")
public class DefaultTenantRoutingInfoService implements TenantRoutingInfoService {

    private final TbTenantProfileCache tenantProfileCache;

    public DefaultTenantRoutingInfoService(TbTenantProfileCache tenantProfileCache) {
        this.tenantProfileCache = tenantProfileCache;
    }

    /**
     * 按租户档案返回路由信息；档案不存在时抛 {@link TenantNotFoundException}。
     */
    @Override
    public TenantRoutingInfo getRoutingInfo(TenantId tenantId) {
        TenantProfile tenantProfile = tenantProfileCache.get(tenantId);
        if (tenantProfile != null) {
            return new TenantRoutingInfo(tenantId, tenantProfile.getId(), tenantProfile.isIsolatedTbRuleEngine());
        } else {
            throw new TenantNotFoundException(tenantId);
        }
    }
}
