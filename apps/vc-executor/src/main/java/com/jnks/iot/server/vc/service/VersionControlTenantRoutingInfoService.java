package com.jnks.iot.server.vc.service;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.queue.discovery.TenantRoutingInfo;
import com.jnks.iot.server.queue.discovery.TenantRoutingInfoService;

@Service
public class VersionControlTenantRoutingInfoService implements TenantRoutingInfoService {
    @Override
    public TenantRoutingInfo getRoutingInfo(TenantId tenantId) {
        //This dummy implementation is ok since Version Control service does not produce any rule engine messages.
        return new TenantRoutingInfo(tenantId, null, false);
    }
}
