package com.jnks.iot.server.service.notification.rule.cache;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.NotificationRule;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

import java.util.List;

public interface NotificationRulesCache {

    List<NotificationRule> getEnabled(TenantId tenantId, NotificationRuleTriggerType triggerType);

}
