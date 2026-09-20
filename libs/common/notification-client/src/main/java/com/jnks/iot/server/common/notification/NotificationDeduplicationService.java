package com.jnks.iot.server.common.notification;

import com.jnks.iot.server.common.data.notification.rule.NotificationRule;
import com.jnks.iot.server.common.data.notification.rule.trigger.NotificationRuleTrigger;

public interface NotificationDeduplicationService {

    boolean alreadyProcessed(NotificationRuleTrigger trigger);

    boolean alreadyProcessed(NotificationRuleTrigger trigger, NotificationRule rule);

}
