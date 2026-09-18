package org.thingsboard.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

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
