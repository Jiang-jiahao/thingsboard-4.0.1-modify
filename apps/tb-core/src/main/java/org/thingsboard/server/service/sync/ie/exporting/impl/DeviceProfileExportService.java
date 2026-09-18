package org.thingsboard.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

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
