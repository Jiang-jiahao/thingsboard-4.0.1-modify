package org.thingsboard.server.service.sync.vc.autocommit;

import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.vc.AutoCommitSettings;

/**
 * 租户自动提交（auto-commit）设置的读写接口。
 */
public interface TbAutoCommitSettingsService {

    /** 读取租户 auto-commit 设置。 */
    AutoCommitSettings get(TenantId tenantId);

    AutoCommitSettings save(TenantId tenantId, AutoCommitSettings settings);

    boolean delete(TenantId tenantId);

}
