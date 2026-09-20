package com.jnks.iot.server.dao.notification;

import com.jnks.iot.server.common.data.id.NotificationId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.notification.Notification;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;

import java.util.Set;

public interface NotificationService {

    Notification saveNotification(TenantId tenantId, Notification notification);

    Notification findNotificationById(TenantId tenantId, NotificationId notificationId);

    boolean markNotificationAsRead(TenantId tenantId, UserId recipientId, NotificationId notificationId);

    int markAllNotificationsAsRead(TenantId tenantId, NotificationDeliveryMethod deliveryMethod, UserId recipientId);

    PageData<Notification> findNotificationsByRecipientIdAndReadStatus(TenantId tenantId, NotificationDeliveryMethod deliveryMethod, UserId recipientId, boolean unreadOnly, PageLink pageLink);

    PageData<Notification> findLatestUnreadNotificationsByRecipientIdAndNotificationTypes(TenantId tenantId, NotificationDeliveryMethod deliveryMethod, UserId recipientId, Set<NotificationType> types, int limit);

    int countUnreadNotificationsByRecipientId(TenantId tenantId, NotificationDeliveryMethod deliveryMethod, UserId recipientId);

    boolean deleteNotification(TenantId tenantId, UserId recipientId, NotificationId notificationId);

}
