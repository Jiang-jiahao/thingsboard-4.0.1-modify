package com.jnks.iot.server.common.data.notification.settings;

import jakarta.validation.constraints.NotEmpty;
import lombok.Data;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;

@Data
public class MobileAppNotificationDeliveryMethodConfig implements NotificationDeliveryMethodConfig {

    private String firebaseServiceAccountCredentialsFileName;
    @NotEmpty
    private String firebaseServiceAccountCredentials;

    @Override
    public NotificationDeliveryMethod getMethod() {
        return NotificationDeliveryMethod.MOBILE_APP;
    }

}
