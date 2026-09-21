package com.jnks.iot.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link Asset} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 将客户、资产 Profile 替换为 externalId。
 */
@Service
public class AssetExportService extends BaseEntityExportService<AssetId, Asset, EntityExportData<Asset>> {

    /**
     * 将客户与资产 Profile ID 替换为 externalId。
     */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, Asset asset, EntityExportData<Asset> exportData) {
        asset.setCustomerId(getExternalIdOrElseInternal(ctx, asset.getCustomerId()));
        asset.setAssetProfileId(getExternalIdOrElseInternal(ctx, asset.getAssetProfileId()));
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.ASSET);
    }

}
