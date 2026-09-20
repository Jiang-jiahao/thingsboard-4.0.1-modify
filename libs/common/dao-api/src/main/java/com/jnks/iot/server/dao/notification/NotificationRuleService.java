package com.jnks.iot.server.dao.notification;

import com.jnks.iot.server.common.data.id.NotificationRuleId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.NotificationRule;
import com.jnks.iot.server.common.data.notification.rule.NotificationRuleInfo;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;

import java.util.List;

public interface NotificationRuleService {

    NotificationRule saveNotificationRule(TenantId tenantId, NotificationRule notificationRule);

    NotificationRule findNotificationRuleById(TenantId tenantId, NotificationRuleId id);

    NotificationRuleInfo findNotificationRuleInfoById(TenantId tenantId, NotificationRuleId id);

    PageData<NotificationRuleInfo> findNotificationRulesInfosByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<NotificationRule> findNotificationRulesByTenantId(TenantId tenantId, PageLink pageLink);

    List<NotificationRule> findEnabledNotificationRulesByTenantIdAndTriggerType(TenantId tenantId, NotificationRuleTriggerType triggerType);

    void deleteNotificationRuleById(TenantId tenantId, NotificationRuleId id);

    void deleteNotificationRulesByTenantId(TenantId tenantId);

}
