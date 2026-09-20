package com.jnks.iot.server.dao.tenant;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantProfileId;

@Data
public class TenantProfileEvictEvent {
    private final TenantProfileId tenantProfileId;
    private final boolean defaultProfile;
}
