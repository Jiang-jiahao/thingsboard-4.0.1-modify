package com.jnks.iot.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link DeviceProfile} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 将默认仪表板、默认规则链替换为 externalId。
 */
@Service
public class DeviceProfileExportService extends BaseEntityExportService<DeviceProfileId, DeviceProfile, EntityExportData<DeviceProfile>> {

    /**
     * 将默认仪表板与规则链 ID 替换为 externalId。
     */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, DeviceProfile deviceProfile, EntityExportData<DeviceProfile> exportData) {
        deviceProfile.setDefaultDashboardId(getExternalIdOrElseInternal(ctx, deviceProfile.getDefaultDashboardId()));
        deviceProfile.setDefaultRuleChainId(getExternalIdOrElseInternal(ctx, deviceProfile.getDefaultRuleChainId()));
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.DEVICE_PROFILE);
    }

}
