package com.jnks.iot.transport.base;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.transport.TransportTenantProfileCache;
import com.jnks.iot.server.queue.discovery.TenantRoutingInfo;
import com.jnks.iot.server.queue.discovery.TenantRoutingInfoService;

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
        return new TenantRoutingInfo(tenantId, profile.getId(), profile.isIsolatedJnksIotRuleEngine());
    }

}
