package org.thingsboard.server.service.sync.vc.repository;

import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.vc.RepositorySettings;

/**
 * 租户 Git 仓库连接设置（URI、分支、认证）的读写接口。
 */
public interface TbRepositorySettingsService {

    /**
     * 更新时回填未提交的密码或私钥。
     */
    RepositorySettings restore(TenantId tenantId, RepositorySettings versionControlSettings);

    RepositorySettings get(TenantId tenantId);

    RepositorySettings save(TenantId tenantId, RepositorySettings versionControlSettings);

    boolean delete(TenantId tenantId);

}
