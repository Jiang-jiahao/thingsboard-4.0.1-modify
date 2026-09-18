package org.thingsboard.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.WidgetTypeId;
import org.thingsboard.server.common.data.sync.ie.WidgetTypeExportData;
import org.thingsboard.server.common.data.widget.WidgetTypeDetails;
import org.thingsboard.server.dao.widget.WidgetTypeService;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link WidgetTypeDetails} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 无额外关联映射；{@link #compare} 始终返回 true，即每次导入都覆盖保存。
 */
@Service
@RequiredArgsConstructor
public class WidgetTypeImportService extends BaseEntityImportService<WidgetTypeId, WidgetTypeDetails, WidgetTypeExportData> {

    private final WidgetTypeService widgetTypeService;

    @Override
    protected void setOwner(TenantId tenantId, WidgetTypeDetails widgetsBundle, IdProvider idProvider) {
        widgetsBundle.setTenantId(tenantId);
    }

    /** 模板覆盖：部件类型无需额外关联映射。 */
    @Override
    protected WidgetTypeDetails prepare(EntitiesImportCtx ctx, WidgetTypeDetails widgetTypeDetails, WidgetTypeDetails old, WidgetTypeExportData exportData, IdProvider idProvider) {
        return widgetTypeDetails;
    }

    /** 保存部件类型。 */
    @Override
    protected WidgetTypeDetails saveOrUpdate(EntitiesImportCtx ctx, WidgetTypeDetails widgetsBundle, WidgetTypeExportData exportData, IdProvider idProvider) {
        return widgetTypeService.saveWidgetType(widgetsBundle);
    }

    /** 始终视为有变更，每次导入都覆盖保存。 */
    @Override
    protected boolean compare(EntitiesImportCtx ctx, WidgetTypeExportData exportData, WidgetTypeDetails prepared, WidgetTypeDetails existing) {
        return true;
    }

    @Override
    protected WidgetTypeDetails deepCopy(WidgetTypeDetails widgetsBundle) {
        return new WidgetTypeDetails(widgetsBundle);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.WIDGET_TYPE;
    }

}
