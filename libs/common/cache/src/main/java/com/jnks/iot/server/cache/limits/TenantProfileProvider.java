package com.jnks.iot.server.cache.limits;

import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantId;

public interface TenantProfileProvider {

    TenantProfile get(TenantId tenantId);

}
