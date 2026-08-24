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
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.ie.DeviceExportData;
import org.thingsboard.server.dao.device.DeviceCredentialsService;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link Device} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 还原客户与设备 Profile 内部 ID；固件/软件 ID 沿用已有设备。开启保存凭证时用 {@code saveDeviceWithCredentials}，
 * 实体未改动时仍可单独更新凭证。
 */
@Service
@TbCoreComponent
@RequiredArgsConstructor
public class DeviceImportService extends BaseEntityImportService<DeviceId, Device, DeviceExportData> {

    private final DeviceService deviceService;
    private final DeviceCredentialsService credentialsService;

    /** 绑定租户，并将客户 externalId 映射为内部 ID。 */
    @Override
    protected void setOwner(TenantId tenantId, Device device, IdProvider idProvider) {
        device.setTenantId(tenantId);
        device.setCustomerId(idProvider.getInternalId(device.getCustomerId()));
    }

    /**
     * 映射设备 Profile，并保留已有固件/软件资源 ID。
     */
    @Override
    protected Device prepare(EntitiesImportCtx ctx, Device device, Device old, DeviceExportData exportData, IdProvider idProvider) {
        device.setDeviceProfileId(idProvider.getInternalId(device.getDeviceProfileId()));
        device.setFirmwareId(getOldEntityField(old, Device::getFirmwareId));
        device.setSoftwareId(getOldEntityField(old, Device::getSoftwareId));
        return device;
    }

    @Override
    protected Device deepCopy(Device d) {
        return new Device(d);
    }

    @Override
    protected void cleanupForComparison(Device e) {
        super.cleanupForComparison(e);
        if (e.getCustomerId() != null && e.getCustomerId().isNullUid()) {
            e.setCustomerId(null);
        }
    }

    /**
     * 按设置带凭证保存设备，并在最终导入轮次写入计算字段。
     */
    @Override
    protected Device saveOrUpdate(EntitiesImportCtx ctx, Device device, DeviceExportData exportData, IdProvider idProvider) {
        Device savedDevice;
        if (exportData.getCredentials() != null && ctx.isSaveCredentials()) {
            exportData.getCredentials().setId(null);
            exportData.getCredentials().setDeviceId(null);
            savedDevice = deviceService.saveDeviceWithCredentials(device, exportData.getCredentials());
        } else {
            savedDevice = deviceService.saveDevice(device);
        }
        if (ctx.isFinalImportAttempt() || ctx.getCurrentImportResult().isUpdatedAllExternalIds()) {
            importCalculatedFields(ctx, savedDevice, exportData, idProvider);
        }
        return savedDevice;
    }

    /**
     * 实体本身未变更时，仍可比较并更新设备凭证。
     */
    @Override
    protected boolean updateRelatedEntitiesIfUnmodified(EntitiesImportCtx ctx, Device prepared, DeviceExportData exportData, IdProvider idProvider) {
        boolean updated = super.updateRelatedEntitiesIfUnmodified(ctx, prepared, exportData, idProvider);
        var credentials = exportData.getCredentials();
        if (credentials != null && ctx.isSaveCredentials()) {
            var existing = credentialsService.findDeviceCredentialsByDeviceId(ctx.getTenantId(), prepared.getId());
            credentials.setId(existing.getId());
            credentials.setDeviceId(prepared.getId());
            if (!existing.equals(credentials)) {
                credentialsService.updateDeviceCredentials(ctx.getTenantId(), credentials);
                updated = true;
            }
        }
        return updated;
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.DEVICE;
    }

}
