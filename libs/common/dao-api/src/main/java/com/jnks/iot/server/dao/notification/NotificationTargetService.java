package com.jnks.iot.server.dao.notification;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.common.data.notification.info.RuleOriginatedNotificationInfo;
import com.jnks.iot.server.common.data.notification.targets.NotificationTarget;
import com.jnks.iot.server.common.data.notification.targets.platform.PlatformUsersNotificationTargetConfig;
import com.jnks.iot.server.common.data.notification.targets.platform.UsersFilterType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;

import java.util.List;

public interface NotificationTargetService {

    NotificationTarget saveNotificationTarget(TenantId tenantId, NotificationTarget notificationTarget);

    NotificationTarget findNotificationTargetById(TenantId tenantId, NotificationTargetId id);

    PageData<NotificationTarget> findNotificationTargetsByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<NotificationTarget> findNotificationTargetsByTenantIdAndSupportedNotificationType(TenantId tenantId, NotificationType notificationType, PageLink pageLink);

    List<NotificationTarget> findNotificationTargetsByTenantIdAndIds(TenantId tenantId, List<NotificationTargetId> ids);

    List<NotificationTarget> findNotificationTargetsByTenantIdAndUsersFilterType(TenantId tenantId, UsersFilterType filterType);

    PageData<User> findRecipientsForNotificationTarget(TenantId tenantId, CustomerId customerId, NotificationTargetId targetId, PageLink pageLink);

    PageData<User> findRecipientsForNotificationTargetConfig(TenantId tenantId, PlatformUsersNotificationTargetConfig targetConfig, PageLink pageLink);

    PageData<User> findRecipientsForRuleNotificationTargetConfig(TenantId tenantId, PlatformUsersNotificationTargetConfig targetConfig, RuleOriginatedNotificationInfo info, PageLink pageLink);

    void deleteNotificationTargetById(TenantId tenantId, NotificationTargetId id);

    void deleteNotificationTargetsByTenantId(TenantId tenantId);

    long countNotificationTargetsByTenantId(TenantId tenantId);

}
