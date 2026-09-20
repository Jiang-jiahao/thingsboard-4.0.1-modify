package com.jnks.iot.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link EntityView} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 将关联实体与客户替换为 externalId。
 */
@Service
public class EntityViewExportService extends BaseEntityExportService<EntityViewId, EntityView, EntityExportData<EntityView>> {

    /** 将关联实体与客户 ID 替换为 externalId。 */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, EntityView entityView, EntityExportData<EntityView> exportData) {
        entityView.setEntityId(getExternalIdOrElseInternal(ctx, entityView.getEntityId()));
        entityView.setCustomerId(getExternalIdOrElseInternal(ctx, entityView.getCustomerId()));
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.ENTITY_VIEW);
    }

}
