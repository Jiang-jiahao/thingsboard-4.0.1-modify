package com.jnks.iot.server.service.sync.vc;

import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.cache.JnksIotTransactionalCache;
import com.jnks.iot.server.common.data.AdminSettings;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.settings.AdminSettingsService;

import java.io.Serializable;

/**
 * 版本控制相关租户设置的抽象存储：以 {@link AdminSettings} JSON 持久化，外加事务缓存。
 *
 * @param <T> 设置对象类型
 */
public abstract class JnksIotAbstractVersionControlSettingsService<T extends Serializable> {

    private final String settingsKey;
    private final AdminSettingsService adminSettingsService;
    private final JnksIotTransactionalCache<TenantId, T> cache;
    private final Class<T> clazz;

    public JnksIotAbstractVersionControlSettingsService(AdminSettingsService adminSettingsService, JnksIotTransactionalCache<TenantId, T> cache, Class<T> clazz, String settingsKey) {
        this.adminSettingsService = adminSettingsService;
        this.cache = cache;
        this.clazz = clazz;
        this.settingsKey = settingsKey;
    }

    /**
     * 按租户读取设置（先缓存，未命中则从 AdminSettings 反序列化）。
     */
    public T get(TenantId tenantId) {
        return cache.getAndPutInTransaction(tenantId, () -> {
            AdminSettings adminSettings = adminSettingsService.findAdminSettingsByTenantIdAndKey(tenantId, settingsKey);
            if (adminSettings != null) {
                try {
                    return JacksonUtil.convertValue(adminSettings.getJsonValue(), clazz);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to load " + settingsKey + " settings!", e);
                }
            }
            return null;
        }, true);
    }

    /**
     * 将设置序列化写入 AdminSettings 并失效缓存。
     */
    public T save(TenantId tenantId, T settings) {
        AdminSettings adminSettings = adminSettingsService.findAdminSettingsByTenantIdAndKey(tenantId, settingsKey);
        if (adminSettings == null) {
            adminSettings = new AdminSettings();
            adminSettings.setKey(settingsKey);
            adminSettings.setTenantId(tenantId);
        }
        adminSettings.setJsonValue(JacksonUtil.valueToTree(settings));
        AdminSettings savedAdminSettings = adminSettingsService.saveAdminSettings(tenantId, adminSettings);
        T savedSettings;
        try {
            savedSettings = JacksonUtil.convertValue(savedAdminSettings.getJsonValue(), clazz);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load auto commit settings!", e);
        }
        //API calls to adminSettingsService are not in transaction, so we can simply evict the cache.
        cache.evict(tenantId);
        return savedSettings;
    }

    /**
     * 按租户删除设置并失效缓存。
     */
    public boolean delete(TenantId tenantId) {
        boolean result = adminSettingsService.deleteAdminSettingsByTenantIdAndKey(tenantId, settingsKey);
        cache.evict(tenantId);
        return result;
    }

}
