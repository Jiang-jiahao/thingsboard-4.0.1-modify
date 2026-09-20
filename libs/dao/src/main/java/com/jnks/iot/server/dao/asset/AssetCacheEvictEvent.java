package com.jnks.iot.server.dao.asset;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@RequiredArgsConstructor
class AssetCacheEvictEvent {

    private final TenantId tenantId;
    private final String newName;
    private final String oldName;

}
