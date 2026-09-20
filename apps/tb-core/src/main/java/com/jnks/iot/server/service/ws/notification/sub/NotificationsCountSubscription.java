package com.jnks.iot.server.service.ws.notification.sub;

import lombok.Builder;
import lombok.Getter;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.service.subscription.TbSubscription;
import com.jnks.iot.server.service.subscription.TbSubscriptionType;
import com.jnks.iot.server.service.ws.notification.cmd.UnreadNotificationsCountUpdate;

import java.util.function.BiConsumer;

@Getter
public class NotificationsCountSubscription extends AbstractNotificationSubscription<NotificationsSubscriptionUpdate> {

    @Builder
    public NotificationsCountSubscription(String serviceId, String sessionId, int subscriptionId, TenantId tenantId, EntityId entityId,
                                          BiConsumer<TbSubscription<NotificationsSubscriptionUpdate>, NotificationsSubscriptionUpdate> updateProcessor) {
        super(serviceId, sessionId, subscriptionId, tenantId, entityId, TbSubscriptionType.NOTIFICATIONS_COUNT, updateProcessor);
    }

    public UnreadNotificationsCountUpdate createUpdate() {
        return UnreadNotificationsCountUpdate.builder()
                .cmdId(getSubscriptionId())
                .totalUnreadCount(totalUnreadCounter.get())
                .sequenceNumber(sequence.incrementAndGet())
                .build();
    }

}
