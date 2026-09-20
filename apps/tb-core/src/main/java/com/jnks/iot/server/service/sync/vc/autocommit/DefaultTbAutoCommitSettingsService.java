package com.jnks.iot.server.service.sync.vc.autocommit;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.TbTransactionalCache;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.vc.AutoCommitSettings;
import com.jnks.iot.server.dao.settings.AdminSettingsService;
import com.jnks.iot.server.service.sync.vc.TbAbstractVersionControlSettingsService;

/**
 * {@link TbAutoCommitSettingsService} 实现，设置键为 {@code autoCommitSettings}。
 */
@Service
public class DefaultTbAutoCommitSettingsService extends TbAbstractVersionControlSettingsService<AutoCommitSettings> implements TbAutoCommitSettingsService {

    public static final String SETTINGS_KEY = "autoCommitSettings";

    public DefaultTbAutoCommitSettingsService(AdminSettingsService adminSettingsService, TbTransactionalCache<TenantId, AutoCommitSettings> cache) {
        super(adminSettingsService, cache, AutoCommitSettings.class, SETTINGS_KEY);
    }

}
