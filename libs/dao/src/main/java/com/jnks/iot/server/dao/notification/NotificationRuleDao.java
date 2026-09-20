package com.jnks.iot.server.dao.notification;

import com.jnks.iot.server.common.data.id.NotificationRuleId;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.NotificationRule;
import com.jnks.iot.server.common.data.notification.rule.NotificationRuleInfo;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ExportableEntityDao;

import java.util.List;

public interface NotificationRuleDao extends Dao<NotificationRule>, ExportableEntityDao<NotificationRuleId, NotificationRule> {

    PageData<NotificationRule> findByTenantIdAndPageLink(TenantId tenantId, PageLink pageLink);

    PageData<NotificationRuleInfo> findInfosByTenantIdAndPageLink(TenantId tenantId, PageLink pageLink);

    boolean existsByTenantIdAndTargetId(TenantId tenantId, NotificationTargetId targetId);

    List<NotificationRule> findByTenantIdAndTriggerTypeAndEnabled(TenantId tenantId, NotificationRuleTriggerType triggerType, boolean enabled);

    NotificationRuleInfo findInfoById(TenantId tenantId, NotificationRuleId id);

    void removeByTenantId(TenantId tenantId);

}
