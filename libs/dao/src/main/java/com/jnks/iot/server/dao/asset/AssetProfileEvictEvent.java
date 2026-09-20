package com.jnks.iot.server.dao.asset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class AssetProfileEvictEvent {

    private final TenantId tenantId;
    private final String newName;
    private final String oldName;
    private final AssetProfileId assetProfileId;
    private final boolean defaultProfile;
    private AssetProfile savedAssetProfile;

}
