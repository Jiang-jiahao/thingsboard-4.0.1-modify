package org.thingsboard.server.service.sync.ie.exporting.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.sync.ie.DeviceExportData;
import org.thingsboard.server.dao.device.DeviceCredentialsService;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link Device} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 将客户、设备 Profile 替换为 externalId；在开启 {@code exportCredentials} 时附带设备凭证（去掉凭证 ID 与设备 ID）。
 */
@Service
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
