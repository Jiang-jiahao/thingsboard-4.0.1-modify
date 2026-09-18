package org.thingsboard.server.common.data.notification.rule.trigger.config;

import lombok.Data;

@Data
public class NewPlatformVersionNotificationRuleTriggerConfig implements NotificationRuleTriggerConfig {

    @Override
    public NotificationRuleTriggerType getTriggerType() {
        return NotificationRuleTriggerType.NEW_PLATFORM_VERSION;
    }

}
