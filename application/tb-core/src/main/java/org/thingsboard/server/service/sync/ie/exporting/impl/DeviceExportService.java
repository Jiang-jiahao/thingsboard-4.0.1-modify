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

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.sync.ie.DeviceExportData;
import org.thingsboard.server.dao.device.DeviceCredentialsService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link Device} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 将客户、设备 Profile 替换为 externalId；在开启 {@code exportCredentials} 时附带设备凭证（去掉凭证 ID 与设备 ID）。
 */
@Service
@TbCoreComponent
@RequiredArgsConstructor
public class DeviceExportService extends BaseEntityExportService<DeviceId, Device, DeviceExportData> {

    private final DeviceCredentialsService deviceCredentialsService;

    /**
     * 替换客户与 Profile 的 ID；按设置导出设备凭证。
     */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, Device device, DeviceExportData exportData) {
        device.setCustomerId(getExternalIdOrElseInternal(ctx, device.getCustomerId()));
        device.setDeviceProfileId(getExternalIdOrElseInternal(ctx, device.getDeviceProfileId()));
        if (ctx.getSettings().isExportCredentials()) {
            var credentials = deviceCredentialsService.findDeviceCredentialsByDeviceId(ctx.getTenantId(), device.getId());
            credentials.setId(null);
            credentials.setDeviceId(null);
            exportData.setCredentials(credentials);
        }
    }

    @Override
    protected DeviceExportData newExportData() {
        return new DeviceExportData();
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.DEVICE);
    }

}
