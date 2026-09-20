package com.jnks.iot.server.service.ws.notification.sub;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.notification.Notification;
import com.jnks.iot.server.common.data.notification.NotificationStatus;
import com.jnks.iot.server.common.data.notification.NotificationType;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotificationUpdate {

    private UUID notificationId;
    private NotificationType notificationType;

    private boolean created;
    private Notification notification;

    private boolean updated;
    private NotificationStatus newStatus;
    private boolean allNotifications;

    private boolean deleted;

}
