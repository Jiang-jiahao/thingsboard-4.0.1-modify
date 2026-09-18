package org.thingsboard.server.common.notification;

import org.thingsboard.server.common.data.notification.rule.NotificationRule;
import org.thingsboard.server.common.data.notification.rule.trigger.NotificationRuleTrigger;

public interface NotificationDeduplicationService {

    boolean alreadyProcessed(NotificationRuleTrigger trigger);

    boolean alreadyProcessed(NotificationRuleTrigger trigger, NotificationRule rule);

}
