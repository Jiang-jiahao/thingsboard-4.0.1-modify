package com.jnks.iot.server.queue.discovery;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;

@Data
public class TenantRoutingInfo {
    private final TenantId tenantId;
    private final TenantProfileId profileId;
    private final boolean isolated;
}
