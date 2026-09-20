package com.jnks.iot.server.common.msg.notification;

import com.jnks.iot.server.common.data.notification.rule.trigger.NotificationRuleTrigger;

public interface NotificationRuleProcessor {

    void process(NotificationRuleTrigger trigger);

}
