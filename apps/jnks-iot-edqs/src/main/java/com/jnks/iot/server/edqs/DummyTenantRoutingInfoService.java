package com.jnks.iot.server.edqs;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.queue.discovery.TenantRoutingInfo;
import com.jnks.iot.server.queue.discovery.TenantRoutingInfoService;

@Service
public class DummyTenantRoutingInfoService implements TenantRoutingInfoService {
    @Override
    public TenantRoutingInfo getRoutingInfo(TenantId tenantId) {
        return null;
    }

}
