package com.jnks.iot.server.service.notification.channels;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import com.jnks.iot.rule.engine.api.MailService;
import com.jnks.iot.rule.engine.api.TbEmail;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;
import com.jnks.iot.server.common.data.notification.template.EmailDeliveryMethodNotificationTemplate;
import com.jnks.iot.server.service.notification.NotificationProcessingContext;

@Component
@RequiredArgsConstructor
public class EmailNotificationChannel implements NotificationChannel<User, EmailDeliveryMethodNotificationTemplate> {

    private final MailService mailService;

    @Override
    public void sendNotification(User recipient, EmailDeliveryMethodNotificationTemplate processedTemplate, NotificationProcessingContext ctx) throws Exception {
        mailService.send(ctx.getTenantId(), null, TbEmail.builder()
                .to(recipient.getEmail())
                .subject(processedTemplate.getSubject())
                .body(processedTemplate.getBody())
                .html(true)
                .build());
    }

    @Override
    public void check(TenantId tenantId) throws Exception {
        if (!mailService.isConfigured(tenantId)) {
            throw new RuntimeException("Mail server is not configured");
        }
    }

    @Override
    public NotificationDeliveryMethod getDeliveryMethod() {
        return NotificationDeliveryMethod.EMAIL;
    }

}
