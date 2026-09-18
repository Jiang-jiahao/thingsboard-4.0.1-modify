package org.thingsboard.server.common.transport.limits;

import lombok.Data;
import org.thingsboard.server.common.data.id.TenantId;

@Data
public class EntityLimitKey {

    private final TenantId tenantId;
    private final String deviceName;

}
