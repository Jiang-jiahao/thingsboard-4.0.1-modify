package org.thingsboard.server.common.data.notification.settings;

import jakarta.validation.constraints.NotEmpty;
import lombok.Data;
import org.thingsboard.server.common.data.notification.NotificationDeliveryMethod;

@Data
public class SlackNotificationDeliveryMethodConfig implements NotificationDeliveryMethodConfig {

    @NotEmpty
    private String botToken;

    @Override
    public NotificationDeliveryMethod getMethod() {
        return NotificationDeliveryMethod.SLACK;
    }

}
