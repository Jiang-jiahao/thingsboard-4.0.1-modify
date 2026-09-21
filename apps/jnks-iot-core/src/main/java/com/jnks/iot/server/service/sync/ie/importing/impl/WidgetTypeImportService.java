package com.jnks.iot.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.WidgetTypeId;
import com.jnks.iot.server.common.data.sync.ie.WidgetTypeExportData;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.dao.widget.WidgetTypeService;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

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
