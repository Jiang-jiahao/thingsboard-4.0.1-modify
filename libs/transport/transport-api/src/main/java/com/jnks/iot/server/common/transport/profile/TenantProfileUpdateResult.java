package com.jnks.iot.server.common.transport.profile;

import lombok.Data;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.Set;

@Data
public class TenantProfileUpdateResult {

    private final TenantProfile profile;
    private final Set<TenantId> affectedTenants;

}
