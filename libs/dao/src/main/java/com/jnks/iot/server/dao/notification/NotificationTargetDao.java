package com.jnks.iot.server.dao.notification;

import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.common.data.notification.targets.NotificationTarget;
import com.jnks.iot.server.common.data.notification.targets.platform.UsersFilterType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ExportableEntityDao;
import com.jnks.iot.server.dao.TenantEntityDao;

import java.util.List;

public interface NotificationTargetDao extends Dao<NotificationTarget>, TenantEntityDao<NotificationTarget>, ExportableEntityDao<NotificationTargetId, NotificationTarget> {

    PageData<NotificationTarget> findByTenantIdAndPageLink(TenantId tenantId, PageLink pageLink);

    PageData<NotificationTarget> findByTenantIdAndSupportedNotificationTypeAndPageLink(TenantId tenantId, NotificationType notificationType, PageLink pageLink);

    List<NotificationTarget> findByTenantIdAndIds(TenantId tenantId, List<NotificationTargetId> ids);

    List<NotificationTarget> findByTenantIdAndUsersFilterType(TenantId tenantId, UsersFilterType filterType);

    void removeByTenantId(TenantId tenantId);

}
