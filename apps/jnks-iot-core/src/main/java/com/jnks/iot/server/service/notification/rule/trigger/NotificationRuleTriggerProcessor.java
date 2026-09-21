package com.jnks.iot.server.service.notification.rule.trigger;

import com.jnks.iot.server.common.data.notification.info.RuleOriginatedNotificationInfo;
import com.jnks.iot.server.common.data.notification.rule.trigger.NotificationRuleTrigger;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

public interface NotificationRuleTriggerProcessor<T extends NotificationRuleTrigger, C extends NotificationRuleTriggerConfig> {

    boolean matchesFilter(T trigger, C triggerConfig);

    default boolean matchesClearRule(T trigger, C triggerConfig) {
        return false;
    }

    RuleOriginatedNotificationInfo constructNotificationInfo(T trigger);

    NotificationRuleTriggerType getTriggerType();

}
