package com.jnks.iot.server.common.transport.limits;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
public class EntityLimitKey {

    private final TenantId tenantId;
    private final String deviceName;

}
