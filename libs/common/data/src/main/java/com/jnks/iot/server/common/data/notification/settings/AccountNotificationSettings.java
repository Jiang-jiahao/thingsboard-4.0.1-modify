package com.jnks.iot.server.common.data.notification.settings;

import lombok.Data;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;

import java.util.Set;

@Data
public class AccountNotificationSettings {

    private UserId userId;
    private Set<NotificationDeliveryMethod> allowedNotifications;

}
