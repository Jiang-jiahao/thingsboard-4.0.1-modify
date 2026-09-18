package org.thingsboard.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.AssetProfile;
import org.thingsboard.server.common.data.id.AssetProfileId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link AssetProfile} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 将默认仪表板、默认规则链替换为 externalId。
 */
@Service
public class AssetProfileExportService extends BaseEntityExportService<AssetProfileId, AssetProfile, EntityExportData<AssetProfile>> {

    /**
     * 将默认仪表板与规则链 ID 替换为 externalId。
     */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, AssetProfile assetProfile, EntityExportData<AssetProfile> exportData) {
        assetProfile.setDefaultDashboardId(getExternalIdOrElseInternal(ctx, assetProfile.getDefaultDashboardId()));
        assetProfile.setDefaultRuleChainId(getExternalIdOrElseInternal(ctx, assetProfile.getDefaultRuleChainId()));
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.ASSET_PROFILE);
    }

}
