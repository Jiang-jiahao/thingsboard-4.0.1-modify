package org.thingsboard.server.service.sync.vc.autocommit;

import org.springframework.stereotype.Service;
import org.thingsboard.server.cache.TbTransactionalCache;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.vc.AutoCommitSettings;
import org.thingsboard.server.dao.settings.AdminSettingsService;
import org.thingsboard.server.service.sync.vc.TbAbstractVersionControlSettingsService;

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
