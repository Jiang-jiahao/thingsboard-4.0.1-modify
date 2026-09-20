package com.jnks.iot.rule.engine.api;

import com.google.common.util.concurrent.FutureCallback;
import com.jnks.iot.server.common.data.id.NotificationId;
import com.jnks.iot.server.common.data.id.NotificationRequestId;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;
import com.jnks.iot.server.common.data.notification.NotificationRequest;
import com.jnks.iot.server.common.data.notification.NotificationRequestStats;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.common.data.notification.info.GeneralNotificationInfo;
import com.jnks.iot.server.common.data.notification.info.NotificationInfo;
import com.jnks.iot.server.common.data.notification.targets.platform.UsersFilter;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplate;

import java.util.List;

public interface NotificationCenter {

    NotificationRequest processNotificationRequest(TenantId tenantId, NotificationRequest notificationRequest, FutureCallback<NotificationRequestStats> callback);

    void sendGeneralWebNotification(TenantId tenantId, UsersFilter recipients, NotificationTemplate template, GeneralNotificationInfo info); // for future use

    void sendSystemNotification(TenantId tenantId, NotificationTargetId targetId, NotificationType type, NotificationInfo info); // for future use and compatibility with PE

    void deleteNotificationRequest(TenantId tenantId, NotificationRequestId notificationRequestId);

    void markNotificationAsRead(TenantId tenantId, UserId recipientId, NotificationId notificationId);

    void markAllNotificationsAsRead(TenantId tenantId, NotificationDeliveryMethod deliveryMethod, UserId recipientId);

    void deleteNotification(TenantId tenantId, UserId recipientId, NotificationId notificationId);

    List<NotificationDeliveryMethod> getAvailableDeliveryMethods(TenantId tenantId);

}
