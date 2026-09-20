package com.jnks.iot.server.dao.user;

import com.fasterxml.jackson.databind.JsonNode;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.settings.UserSettings;
import com.jnks.iot.server.common.data.settings.UserSettingsType;

import java.util.List;

public interface UserSettingsService {

    void updateUserSettings(TenantId tenantId, UserId userId, UserSettingsType type, JsonNode settings);

    UserSettings saveUserSettings(TenantId tenantId, UserSettings userSettings);

    UserSettings findUserSettings(TenantId tenantId, UserId userId, UserSettingsType type);

    void deleteUserSettings(TenantId tenantId, UserId userId, UserSettingsType type, List<String> jsonPaths);

}
