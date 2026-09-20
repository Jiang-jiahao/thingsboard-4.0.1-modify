package com.jnks.iot.server.service.notification.channels;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;
import com.jnks.iot.server.common.data.notification.targets.NotificationRecipient;
import com.jnks.iot.server.common.data.notification.template.DeliveryMethodNotificationTemplate;
import com.jnks.iot.server.service.notification.NotificationProcessingContext;

public interface NotificationChannel<R extends NotificationRecipient, T extends DeliveryMethodNotificationTemplate> {

    void sendNotification(R recipient, T processedTemplate, NotificationProcessingContext ctx) throws Exception;

    void check(TenantId tenantId) throws Exception;

    NotificationDeliveryMethod getDeliveryMethod();

}
