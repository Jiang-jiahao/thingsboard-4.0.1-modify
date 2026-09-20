package com.jnks.iot.server.queue.discovery;

import com.jnks.iot.server.common.data.id.TenantId;

public interface TenantRoutingInfoService {

    TenantRoutingInfo getRoutingInfo(TenantId tenantId);

}
