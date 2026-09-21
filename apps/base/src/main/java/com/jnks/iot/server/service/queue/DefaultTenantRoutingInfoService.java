package com.jnks.iot.server.service.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.exception.TenantNotFoundException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.tenant.JnksIotTenantProfileCache;
import com.jnks.iot.server.queue.discovery.TenantRoutingInfo;
import com.jnks.iot.server.queue.discovery.TenantRoutingInfoService;

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

    private final JnksIotTenantProfileCache tenantProfileCache;

    public DefaultTenantRoutingInfoService(JnksIotTenantProfileCache tenantProfileCache) {
        this.tenantProfileCache = tenantProfileCache;
    }

    /**
     * 按租户档案返回路由信息；档案不存在时抛 {@link TenantNotFoundException}。
     */
    @Override
    public TenantRoutingInfo getRoutingInfo(TenantId tenantId) {
        TenantProfile tenantProfile = tenantProfileCache.get(tenantId);
        if (tenantProfile != null) {
            return new TenantRoutingInfo(tenantId, tenantProfile.getId(), tenantProfile.isIsolatedJnksIotRuleEngine());
        } else {
            throw new TenantNotFoundException(tenantId);
        }
    }
}
