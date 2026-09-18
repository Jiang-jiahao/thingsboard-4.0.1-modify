package org.thingsboard.transport.base;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.transport.TransportTenantProfileCache;
import org.thingsboard.server.queue.discovery.TenantRoutingInfo;
import org.thingsboard.server.queue.discovery.TenantRoutingInfoService;

@Slf4j
@Service
public class TransportTenantRoutingInfoService implements TenantRoutingInfoService {

    private final TransportTenantProfileCache tenantProfileCache;

    public TransportTenantRoutingInfoService(TransportTenantProfileCache tenantProfileCache) {
        this.tenantProfileCache = tenantProfileCache;
    }

    @Override
    public TenantRoutingInfo getRoutingInfo(TenantId tenantId) {
        TenantProfile profile = tenantProfileCache.get(tenantId);
        return new TenantRoutingInfo(tenantId, profile.getId(), profile.isIsolatedTbRuleEngine());
    }

}
