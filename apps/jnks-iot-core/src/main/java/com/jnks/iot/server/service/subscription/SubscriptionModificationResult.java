package com.jnks.iot.server.service.subscription;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;

/**
 * The modification result of entity subscription
 */
@Builder
@Data
public class SubscriptionModificationResult {

    private TenantId tenantId;
    private EntityId entityId;
    private JnksIotSubscription<?> subscription;
    private JnksIotSubscription<?> missedUpdatesCandidate;
    private JnksIotEntitySubEvent event;

    public boolean hasEvent() {
        return event != null;
    }
}
