package com.jnks.iot.server.cache.resourceInfo;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TbResourceId;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
public class ResourceInfoEvictEvent {
    private final TenantId tenantId;
    private final TbResourceId resourceId;
}
