package org.thingsboard.server.queue.discovery;

import lombok.Data;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.TenantProfileId;

@Data
public class TenantRoutingInfo {
    private final TenantId tenantId;
    private final TenantProfileId profileId;
    private final boolean isolated;
}
