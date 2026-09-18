package org.thingsboard.server.common.msg.notification;

import org.thingsboard.server.common.data.notification.rule.trigger.NotificationRuleTrigger;

public interface NotificationRuleProcessor {

    void process(NotificationRuleTrigger trigger);

}
