package com.jnks.iot.server.dao.notification;

import com.jnks.iot.server.common.data.id.NotificationTemplateId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplate;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ExportableEntityDao;

import java.util.Collection;
import java.util.List;

public interface NotificationTemplateDao extends Dao<NotificationTemplate>, ExportableEntityDao<NotificationTemplateId, NotificationTemplate> {

    PageData<NotificationTemplate> findByTenantIdAndNotificationTypesAndPageLink(TenantId tenantId, List<NotificationType> notificationTypes, PageLink pageLink);

    int countByTenantIdAndNotificationTypes(TenantId tenantId, Collection<NotificationType> notificationTypes);

    void removeByTenantId(TenantId tenantId);

}
