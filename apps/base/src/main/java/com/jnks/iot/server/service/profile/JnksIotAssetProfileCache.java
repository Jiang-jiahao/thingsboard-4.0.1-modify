package com.jnks.iot.server.service.profile;

import com.jnks.iot.rule.engine.api.RuleEngineAssetProfileCache;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.TenantId;

public interface JnksIotAssetProfileCache extends RuleEngineAssetProfileCache {

    void evict(TenantId tenantId, AssetProfileId id);

    void evict(TenantId tenantId, AssetId id);

    AssetProfile find(AssetProfileId assetProfileId);

    AssetProfile findOrCreateAssetProfile(TenantId tenantId, String assetType);
}
