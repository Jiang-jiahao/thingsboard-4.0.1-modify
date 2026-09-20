package com.jnks.iot.server.common.data.notification;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.BaseData;
import com.jnks.iot.server.common.data.id.NotificationId;
import com.jnks.iot.server.common.data.id.NotificationRequestId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.notification.info.NotificationInfo;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = true)
public class Notification extends BaseData<NotificationId> {

    private NotificationRequestId requestId;
    private UserId recipientId;
    private NotificationType type;
    private NotificationDeliveryMethod deliveryMethod;
    private String subject;
    private String text;
    private JsonNode additionalConfig;
    private NotificationInfo info;
    private NotificationStatus status;

}
