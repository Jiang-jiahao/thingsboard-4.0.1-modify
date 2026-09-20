package com.jnks.iot.server.dao.settings;

import com.jnks.iot.server.common.data.AdminSettings;
import com.jnks.iot.server.common.data.id.AdminSettingsId;
import com.jnks.iot.server.common.data.id.TenantId;

public interface AdminSettingsService {

    AdminSettings findAdminSettingsById(TenantId tenantId, AdminSettingsId adminSettingsId);

    AdminSettings findAdminSettingsByKey(TenantId tenantId, String key);

    AdminSettings findAdminSettingsByTenantIdAndKey(TenantId tenantId, String key);

    AdminSettings saveAdminSettings(TenantId tenantId, AdminSettings adminSettings);

    boolean deleteAdminSettingsByTenantIdAndKey(TenantId tenantId, String key);

    void deleteAdminSettingsByTenantId(TenantId tenantId);

}
