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
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.dao.device.DeviceProfileService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link DeviceProfile} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 还原默认规则链、Edge 规则链、仪表板内部 ID；固件/软件沿用已有 Profile，比较时忽略二者以免误更新。
 */
@Service
@TbCoreComponent
@RequiredArgsConstructor
public class DeviceProfileImportService extends BaseEntityImportService<DeviceProfileId, DeviceProfile, EntityExportData<DeviceProfile>> {

    private final DeviceProfileService deviceProfileService;

    /** 模板覆盖：仅绑定租户。 */
    @Override
    protected void setOwner(TenantId tenantId, DeviceProfile deviceProfile, IdProvider idProvider) {
        deviceProfile.setTenantId(tenantId);
    }

    /**
     * 映射默认规则链/仪表板，并保留已有固件、软件 ID。
     */
    @Override
    protected DeviceProfile prepare(EntitiesImportCtx ctx, DeviceProfile deviceProfile, DeviceProfile old, EntityExportData<DeviceProfile> exportData, IdProvider idProvider) {
        deviceProfile.setDefaultRuleChainId(idProvider.getInternalId(deviceProfile.getDefaultRuleChainId()));
        deviceProfile.setDefaultEdgeRuleChainId(idProvider.getInternalId(deviceProfile.getDefaultEdgeRuleChainId()));
        deviceProfile.setDefaultDashboardId(idProvider.getInternalId(deviceProfile.getDefaultDashboardId()));
        deviceProfile.setFirmwareId(getOldEntityField(old, DeviceProfile::getFirmwareId));
        deviceProfile.setSoftwareId(getOldEntityField(old, DeviceProfile::getSoftwareId));
        return deviceProfile;
    }

    /** 保存设备 Profile，并在最终导入轮次写入计算字段。 */
    @Override
    protected DeviceProfile saveOrUpdate(EntitiesImportCtx ctx, DeviceProfile deviceProfile, EntityExportData<DeviceProfile> exportData, IdProvider idProvider) {
        DeviceProfile saved = deviceProfileService.saveDeviceProfile(deviceProfile);
        if (ctx.isFinalImportAttempt() || ctx.getCurrentImportResult().isUpdatedAllExternalIds()) {
            importCalculatedFields(ctx, saved, exportData, idProvider);
        }
        return saved;
    }

    @Override
    protected void onEntitySaved(User user, DeviceProfile savedDeviceProfile, DeviceProfile oldDeviceProfile) {
        logEntityActionService.logEntityAction(savedDeviceProfile.getTenantId(), savedDeviceProfile.getId(), savedDeviceProfile,
                null, oldDeviceProfile == null ? ActionType.ADDED : ActionType.UPDATED, user);
    }

    @Override
    protected DeviceProfile deepCopy(DeviceProfile deviceProfile) {
        return new DeviceProfile(deviceProfile);
    }

    /** 比较时忽略固件/软件 ID，避免资源未纳入版本控制时产生虚假差异。 */
    @Override
    protected void cleanupForComparison(DeviceProfile deviceProfile) {
        super.cleanupForComparison(deviceProfile);
        deviceProfile.setFirmwareId(null);
        deviceProfile.setSoftwareId(null);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.DEVICE_PROFILE;
    }

}
