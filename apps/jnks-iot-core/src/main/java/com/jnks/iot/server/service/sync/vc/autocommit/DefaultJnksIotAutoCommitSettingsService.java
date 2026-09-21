package com.jnks.iot.server.service.sync.vc.autocommit;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.JnksIotTransactionalCache;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.vc.AutoCommitSettings;
import com.jnks.iot.server.dao.settings.AdminSettingsService;
import com.jnks.iot.server.service.sync.vc.JnksIotAbstractVersionControlSettingsService;

/**
 * {@link JnksIotAutoCommitSettingsService} 实现，设置键为 {@code autoCommitSettings}。
 */
@Service
public class DefaultJnksIotAutoCommitSettingsService extends JnksIotAbstractVersionControlSettingsService<AutoCommitSettings> implements JnksIotAutoCommitSettingsService {

    public static final String SETTINGS_KEY = "autoCommitSettings";

    public DefaultJnksIotAutoCommitSettingsService(AdminSettingsService adminSettingsService, JnksIotTransactionalCache<TenantId, AutoCommitSettings> cache) {
        super(adminSettingsService, cache, AutoCommitSettings.class, SETTINGS_KEY);
    }

}
