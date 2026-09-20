package com.jnks.iot.server.service.ws.notification.sub;


import lombok.Getter;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.service.subscription.TbSubscription;
import com.jnks.iot.server.service.subscription.TbSubscriptionType;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@Getter
public abstract class AbstractNotificationSubscription<T> extends TbSubscription<T> {

    protected final AtomicInteger sequence = new AtomicInteger();
    protected final AtomicInteger totalUnreadCounter = new AtomicInteger();

    public AbstractNotificationSubscription(String serviceId, String sessionId, int subscriptionId, TenantId tenantId, EntityId entityId, TbSubscriptionType type, BiConsumer<TbSubscription<T>, T> updateProcessor) {
        super(serviceId, sessionId, subscriptionId, tenantId, entityId, type, updateProcessor);
    }

}
