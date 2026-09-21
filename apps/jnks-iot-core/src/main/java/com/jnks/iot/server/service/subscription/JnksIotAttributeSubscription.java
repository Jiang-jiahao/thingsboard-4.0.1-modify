package com.jnks.iot.server.service.subscription;

import lombok.Builder;
import lombok.Getter;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.service.ws.telemetry.sub.TelemetrySubscriptionUpdate;

import java.util.Map;
import java.util.function.BiConsumer;

public class JnksIotAttributeSubscription extends JnksIotSubscription<TelemetrySubscriptionUpdate> {

    @Getter private final long queryTs;
    @Getter private final boolean allKeys;
    @Getter private final Map<String, Long> keyStates;
    @Getter private final JnksIotAttributeSubscriptionScope scope;

    @Builder
    public JnksIotAttributeSubscription(String serviceId, String sessionId, int subscriptionId, TenantId tenantId, EntityId entityId,
                                   BiConsumer<JnksIotSubscription<TelemetrySubscriptionUpdate>, TelemetrySubscriptionUpdate> updateProcessor,
                                   long queryTs, boolean allKeys, Map<String, Long> keyStates, JnksIotAttributeSubscriptionScope scope) {
        super(serviceId, sessionId, subscriptionId, tenantId, entityId, JnksIotSubscriptionType.ATTRIBUTES, updateProcessor);
        this.queryTs = queryTs;
        this.allKeys = allKeys;
        this.keyStates = keyStates;
        this.scope = scope;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
