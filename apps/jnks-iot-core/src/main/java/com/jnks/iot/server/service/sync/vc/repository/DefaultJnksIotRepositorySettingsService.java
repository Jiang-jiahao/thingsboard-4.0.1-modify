package com.jnks.iot.server.service.sync.vc.repository;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.JnksIotTransactionalCache;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.vc.RepositoryAuthMethod;
import com.jnks.iot.server.common.data.sync.vc.RepositorySettings;
import com.jnks.iot.server.dao.settings.AdminSettingsService;
import com.jnks.iot.server.service.sync.vc.JnksIotAbstractVersionControlSettingsService;

/**
 * {@link JnksIotRepositorySettingsService} 实现，设置键为 {@code entitiesVersionControl}。
 * <p>
 * {@link #restore} 在更新时若未提交密码/私钥，则从已存储设置中回填，避免覆盖密钥。
 */
@Service
public class DefaultJnksIotRepositorySettingsService extends JnksIotAbstractVersionControlSettingsService<RepositorySettings> implements JnksIotRepositorySettingsService {

    public static final String SETTINGS_KEY = "entitiesVersionControl";

    public DefaultJnksIotRepositorySettingsService(AdminSettingsService adminSettingsService, JnksIotTransactionalCache<TenantId, RepositorySettings> cache) {
        super(adminSettingsService, cache, RepositorySettings.class, SETTINGS_KEY);
    }

    /**
     * 更新仓库设置时回填未提交的密码或私钥，避免把已存密钥清空。
     */
    @Override
    public RepositorySettings restore(TenantId tenantId, RepositorySettings settings) {
        RepositorySettings storedSettings = get(tenantId);
        if (storedSettings != null) {
            RepositoryAuthMethod authMethod = settings.getAuthMethod();
            if (RepositoryAuthMethod.USERNAME_PASSWORD.equals(authMethod) && settings.getPassword() == null) {
                settings.setPassword(storedSettings.getPassword());
            } else if (RepositoryAuthMethod.PRIVATE_KEY.equals(authMethod) && settings.getPrivateKey() == null) {
                settings.setPrivateKey(storedSettings.getPrivateKey());
                if (settings.getPrivateKeyPassword() == null) {
                    settings.setPrivateKeyPassword(storedSettings.getPrivateKeyPassword());
                }
            }
        }
        return settings;
    }

    @Override
    public RepositorySettings get(TenantId tenantId) {
        RepositorySettings settings = super.get(tenantId);
        if (settings != null) {
            settings = new RepositorySettings(settings);
        }
        return settings;
    }

}
