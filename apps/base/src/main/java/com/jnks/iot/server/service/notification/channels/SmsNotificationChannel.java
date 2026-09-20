package com.jnks.iot.server.service.notification.channels;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import com.jnks.iot.rule.engine.api.SmsService;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;
import com.jnks.iot.server.common.data.notification.template.SmsDeliveryMethodNotificationTemplate;
import com.jnks.iot.server.service.notification.NotificationProcessingContext;

@Component
@RequiredArgsConstructor
public class SmsNotificationChannel implements NotificationChannel<User, SmsDeliveryMethodNotificationTemplate> {

    private final SmsService smsService;

    @Override
    public void sendNotification(User recipient, SmsDeliveryMethodNotificationTemplate processedTemplate, NotificationProcessingContext ctx) throws Exception {
        String phone = recipient.getPhone();
        if (StringUtils.isBlank(phone)) {
            throw new RuntimeException("User does not have phone number");
        }

        smsService.sendSms(ctx.getTenantId(), null, new String[]{phone}, processedTemplate.getBody());
    }

    @Override
    public void check(TenantId tenantId) throws Exception {
        if (!smsService.isConfigured(tenantId)) {
            throw new RuntimeException("SMS provider is not configured");
        }
    }

    @Override
    public NotificationDeliveryMethod getDeliveryMethod() {
        return NotificationDeliveryMethod.SMS;
    }

}
