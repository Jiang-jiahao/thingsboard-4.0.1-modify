package com.jnks.iot.server.dao.tenant;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
public class TenantEvictEvent {
    private final TenantId tenantId;
    private final boolean invalidateExists;
}
