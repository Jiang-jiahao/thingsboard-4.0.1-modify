package com.jnks.iot.server.dao.notification;

import com.fasterxml.jackson.databind.JsonNode;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.common.data.notification.settings.NotificationSettings;
import com.jnks.iot.server.common.data.notification.settings.UserNotificationSettings;

import java.util.Map;

public interface NotificationSettingsService {

    void saveNotificationSettings(TenantId tenantId, NotificationSettings settings);

    NotificationSettings findNotificationSettings(TenantId tenantId);

    void deleteNotificationSettings(TenantId tenantId);

    UserNotificationSettings saveUserNotificationSettings(TenantId tenantId, UserId userId, UserNotificationSettings settings);

    UserNotificationSettings getUserNotificationSettings(TenantId tenantId, UserId userId, boolean format);

    void createDefaultNotificationConfigs(TenantId tenantId);

    void updateDefaultNotificationConfigs(TenantId tenantId);

    void moveMailTemplatesToNotificationCenter(TenantId tenantId, JsonNode mailTemplates, Map<String, NotificationType> mailTemplatesNames);

}
