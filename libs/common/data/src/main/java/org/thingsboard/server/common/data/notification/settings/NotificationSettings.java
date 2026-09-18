package org.thingsboard.server.common.data.notification.settings;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.thingsboard.server.common.data.notification.NotificationDeliveryMethod;

import java.io.Serializable;
import java.util.Map;

@Data
public class NotificationSettings implements Serializable {

    @NotNull
    @Valid
    private Map<NotificationDeliveryMethod, NotificationDeliveryMethodConfig> deliveryMethodsConfigs;

}
