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
package org.thingsboard.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link Asset} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 将客户、资产 Profile 替换为 externalId。
 */
@Service
@TbCoreComponent
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
