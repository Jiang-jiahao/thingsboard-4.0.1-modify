package com.jnks.iot.server.dao.settings;

import com.jnks.iot.server.common.data.AdminSettings;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.Dao;

import java.util.UUID;

public interface AdminSettingsDao extends Dao<AdminSettings> {

    /**
     * Save or update admin settings object
     *
     * @param adminSettings the admin settings object
     * @return saved admin settings object
     */
    AdminSettings save(TenantId tenantId, AdminSettings adminSettings);
    
    /**
     * Find admin settings by key.
     *
     * @param key the key
     * @return the admin settings object
     */
    AdminSettings findByTenantIdAndKey(UUID tenantId, String key);

    boolean removeByTenantIdAndKey(UUID tenantId, String key);

    void removeByTenantId(UUID tenantId);

}
