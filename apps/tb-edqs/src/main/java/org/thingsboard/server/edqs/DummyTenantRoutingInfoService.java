package org.thingsboard.server.edqs;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.queue.discovery.TenantRoutingInfo;
import org.thingsboard.server.queue.discovery.TenantRoutingInfoService;

@Service
public class DummyTenantRoutingInfoService implements TenantRoutingInfoService {
    @Override
    public TenantRoutingInfo getRoutingInfo(TenantId tenantId) {
        return null;
    }

}
