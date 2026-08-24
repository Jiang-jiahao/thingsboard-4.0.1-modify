/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.dao.asset.AssetService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link Asset} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 还原客户与资产 Profile 内部 ID；最终轮次导入计算字段。空客户 UID 比较时视为未分配。
 */
@Service
@TbCoreComponent
@RequiredArgsConstructor
public class AssetImportService extends BaseEntityImportService<AssetId, Asset, EntityExportData<Asset>> {

    private final AssetService assetService;

    /** 绑定租户，并将客户 externalId 映射为内部 ID。 */
    @Override
    protected void setOwner(TenantId tenantId, Asset asset, IdProvider idProvider) {
        asset.setTenantId(tenantId);
        asset.setCustomerId(idProvider.getInternalId(asset.getCustomerId()));
    }

    /** 映射资产 Profile 为当前环境内部 ID。 */
    @Override
    protected Asset prepare(EntitiesImportCtx ctx, Asset asset, Asset old, EntityExportData<Asset> exportData, IdProvider idProvider) {
        asset.setAssetProfileId(idProvider.getInternalId(asset.getAssetProfileId()));
        return asset;
    }

    /** 保存资产，并在最终导入轮次写入计算字段。 */
    @Override
    protected Asset saveOrUpdate(EntitiesImportCtx ctx, Asset asset, EntityExportData<Asset> exportData, IdProvider idProvider) {
        Asset savedAsset = assetService.saveAsset(asset);
        if (ctx.isFinalImportAttempt() || ctx.getCurrentImportResult().isUpdatedAllExternalIds()) {
            importCalculatedFields(ctx, savedAsset, exportData, idProvider);
        }
        return savedAsset;
    }

    @Override
    protected Asset deepCopy(Asset asset) {
        return new Asset(asset);
    }

    @Override
    protected void cleanupForComparison(Asset e) {
        super.cleanupForComparison(e);
        if (e.getCustomerId() != null && e.getCustomerId().isNullUid()) {
            e.setCustomerId(null);
        }
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.ASSET;
    }

}
