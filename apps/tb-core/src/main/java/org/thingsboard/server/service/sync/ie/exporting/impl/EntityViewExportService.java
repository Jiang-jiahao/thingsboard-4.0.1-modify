package org.thingsboard.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.EntityView;
import org.thingsboard.server.common.data.id.EntityViewId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

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
