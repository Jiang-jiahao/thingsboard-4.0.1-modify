package org.thingsboard.server.service.sync.ie.exporting.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.WidgetTypeId;
import org.thingsboard.server.common.data.sync.ie.WidgetTypeExportData;
import org.thingsboard.server.common.data.widget.WidgetTypeDetails;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link WidgetTypeDetails} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 禁止导出系统级（无租户）部件类型，仅允许租户自定义部件。
 */
@Service
@RequiredArgsConstructor
public class WidgetTypeExportService extends BaseEntityExportService<WidgetTypeId, WidgetTypeDetails, WidgetTypeExportData> {

    /**
     * 系统级部件类型不允许导出。
     */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, WidgetTypeDetails widgetTypeDetails, WidgetTypeExportData exportData) {
        if (widgetTypeDetails.getTenantId() == null || widgetTypeDetails.getTenantId().isNullUid()) {
            throw new IllegalArgumentException("Export of system Widget Type is not allowed");
        }
    }

    @Override
    protected WidgetTypeExportData newExportData() {
        return new WidgetTypeExportData();
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.WIDGET_TYPE);
    }

}