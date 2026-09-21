package com.jnks.iot.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.ie.DeviceExportData;
import com.jnks.iot.server.dao.device.DeviceCredentialsService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link Device} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 还原客户与设备 Profile 内部 ID；固件/软件 ID 沿用已有设备。开启保存凭证时用 {@code saveDeviceWithCredentials}，
 * 实体未改动时仍可单独更新凭证。
 */
@Service
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
