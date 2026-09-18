package org.thingsboard.server.common.data.notification.rule.trigger.config;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TaskProcessingFailureNotificationRuleTriggerConfig implements NotificationRuleTriggerConfig {

    @Override
    public NotificationRuleTriggerType getTriggerType() {
        return NotificationRuleTriggerType.TASK_PROCESSING_FAILURE;
    }

}
