package com.jnks.iot.server.service.notification.channels;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import com.jnks.iot.rule.engine.api.notification.SlackService;

import java.util.Optional;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;
import com.jnks.iot.server.common.data.notification.settings.NotificationSettings;
import com.jnks.iot.server.common.data.notification.settings.SlackNotificationDeliveryMethodConfig;
import com.jnks.iot.server.common.data.notification.targets.slack.SlackConversation;
import com.jnks.iot.server.common.data.notification.template.SlackDeliveryMethodNotificationTemplate;
import com.jnks.iot.server.dao.notification.NotificationSettingsService;
import com.jnks.iot.server.service.notification.NotificationProcessingContext;

@Component
@RequiredArgsConstructor
public class SlackNotificationChannel implements NotificationChannel<SlackConversation, SlackDeliveryMethodNotificationTemplate> {

    private final Optional<SlackService> slackService;
    private final NotificationSettingsService notificationSettingsService;

    @Override
    public void sendNotification(SlackConversation conversation, SlackDeliveryMethodNotificationTemplate processedTemplate, NotificationProcessingContext ctx) throws Exception {
        SlackNotificationDeliveryMethodConfig config = ctx.getDeliveryMethodConfig(NotificationDeliveryMethod.SLACK);
        slackService.orElseThrow(() -> new IllegalStateException("SlackService is not available"))
                .sendMessage(ctx.getTenantId(), config.getBotToken(), conversation.getId(), processedTemplate.getBody());
    }

    @Override
    public void check(TenantId tenantId) throws Exception {
        NotificationSettings notificationSettings = notificationSettingsService.findNotificationSettings(tenantId);
        if (!notificationSettings.getDeliveryMethodsConfigs().containsKey(NotificationDeliveryMethod.SLACK)) {
            throw new RuntimeException("Slack API token is not configured");
        }
    }

    @Override
    public NotificationDeliveryMethod getDeliveryMethod() {
        return NotificationDeliveryMethod.SLACK;
    }

}
