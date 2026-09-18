package org.thingsboard.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.TbResource;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.TbResourceId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link TbResource} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 清空 preview（导入时重新生成），不额外处理关联实体。
 */
@Service
public class ResourceExportService extends BaseEntityExportService<TbResourceId, TbResource, EntityExportData<TbResource>> {

    /**
     * 调用基类附加数据后清空 preview，避免把生成图写入 Git。
     */
    @Override
    protected void setAdditionalExportData(EntitiesExportCtx<?> ctx, TbResource resource, EntityExportData<TbResource> exportData) throws ThingsboardException {
        super.setAdditionalExportData(ctx, resource, exportData);
        resource.setPreview(null); // will be generated on import
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.TB_RESOURCE);
    }

}
