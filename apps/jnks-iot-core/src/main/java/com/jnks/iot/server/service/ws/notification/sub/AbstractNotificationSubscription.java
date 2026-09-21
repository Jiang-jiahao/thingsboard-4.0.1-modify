package com.jnks.iot.server.service.ws.notification.sub;


import lombok.Getter;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.service.subscription.JnksIotSubscription;
import com.jnks.iot.server.service.subscription.JnksIotSubscriptionType;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@Getter
public abstract class AbstractNotificationSubscription<T> extends JnksIotSubscription<T> {

    protected final AtomicInteger sequence = new AtomicInteger();
    protected final AtomicInteger totalUnreadCounter = new AtomicInteger();

    public AbstractNotificationSubscription(String serviceId, String sessionId, int subscriptionId, TenantId tenantId, EntityId entityId, JnksIotSubscriptionType type, BiConsumer<JnksIotSubscription<T>, T> updateProcessor) {
        super(serviceId, sessionId, subscriptionId, tenantId, entityId, type, updateProcessor);
    }

}
