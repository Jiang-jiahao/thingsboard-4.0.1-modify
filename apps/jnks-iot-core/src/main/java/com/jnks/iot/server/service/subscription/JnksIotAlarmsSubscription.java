package com.jnks.iot.server.service.subscription;

import lombok.Builder;
import lombok.Getter;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.service.ws.telemetry.sub.AlarmSubscriptionUpdate;

import java.util.function.BiConsumer;

public class JnksIotAlarmsSubscription extends JnksIotSubscription<AlarmSubscriptionUpdate> {

    @Getter
    private final long ts;

    @Builder
    public JnksIotAlarmsSubscription(String serviceId, String sessionId, int subscriptionId, TenantId tenantId, EntityId entityId,
                                BiConsumer<JnksIotSubscription<AlarmSubscriptionUpdate>, AlarmSubscriptionUpdate> updateProcessor, long ts) {
        super(serviceId, sessionId, subscriptionId, tenantId, entityId, JnksIotSubscriptionType.ALARMS, updateProcessor);
        this.ts = ts;
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
