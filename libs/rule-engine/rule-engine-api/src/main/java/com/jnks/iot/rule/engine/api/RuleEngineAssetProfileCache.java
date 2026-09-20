package com.jnks.iot.rule.engine.api;

import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by ashvayka on 02.04.18.
 */
public interface RuleEngineAssetProfileCache {

    AssetProfile get(TenantId tenantId, AssetProfileId assetProfileId);

    AssetProfile get(TenantId tenantId, AssetId assetId);

    void addListener(TenantId tenantId, EntityId listenerId, Consumer<AssetProfile> profileListener, BiConsumer<AssetId, AssetProfile> assetlistener);

    void removeListener(TenantId tenantId, EntityId listenerId);

}
