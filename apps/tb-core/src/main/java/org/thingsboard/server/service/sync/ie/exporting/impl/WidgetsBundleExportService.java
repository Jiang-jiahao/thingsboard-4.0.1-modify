package org.thingsboard.server.service.sync.ie.exporting.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.WidgetsBundleId;
import org.thingsboard.server.common.data.sync.ie.WidgetsBundleExportData;
import org.thingsboard.server.common.data.widget.WidgetsBundle;
import org.thingsboard.server.dao.widget.WidgetTypeService;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.List;
import java.util.Set;

/**
 * 针对 {@link WidgetsBundle} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 禁止导出系统级部件包；额外导出包内部件的 FQN 列表，导入时用于重建包与部件的关联。
 */
@Service
@RequiredArgsConstructor
public class WidgetsBundleExportService extends BaseEntityExportService<WidgetsBundleId, WidgetsBundle, WidgetsBundleExportData> {

    private final WidgetTypeService widgetTypeService;

    /**
     * 系统级部件包不允许导出；写入包内部件 FQN 列表。
     */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, WidgetsBundle widgetsBundle, WidgetsBundleExportData exportData) {
        if (widgetsBundle.getTenantId() == null || widgetsBundle.getTenantId().isNullUid()) {
            throw new IllegalArgumentException("Export of system Widget Bundles is not allowed");
        }

        List<String> fqns = widgetTypeService.findWidgetFqnsByWidgetsBundleId(ctx.getTenantId(), widgetsBundle.getId());
        exportData.setFqns(fqns);
    }

    @Override
    protected WidgetsBundleExportData newExportData() {
        return new WidgetsBundleExportData();
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.WIDGETS_BUNDLE);
    }

}
