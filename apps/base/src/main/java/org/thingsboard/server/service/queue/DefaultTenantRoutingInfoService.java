package org.thingsboard.server.service.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.exception.TenantNotFoundException;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.queue.discovery.TenantRoutingInfo;
import org.thingsboard.server.queue.discovery.TenantRoutingInfoService;

/**
 * 租户级队列路由信息提供者。
 * <p>
 * 根据租户档案判断规则引擎是否隔离，供分区服务决定消息投递到共享还是租户专属队列。
 *
 * @see TenantRoutingInfoService
 */
@Slf4j
@Service
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
