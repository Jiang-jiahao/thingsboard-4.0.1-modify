package com.jnks.iot.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.dao.entityview.EntityViewService;
import com.jnks.iot.server.service.entitiy.entityview.TbEntityViewService;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link EntityView} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 还原客户与目标实体内部 ID；保存后通过 {@code TbEntityViewService} 同步视图属性。
 */
@Service
@RequiredArgsConstructor
public class EntityViewImportService extends BaseEntityImportService<EntityViewId, EntityView, EntityExportData<EntityView>> {

    private final EntityViewService entityViewService;

    @Lazy
    @Autowired
    private TbEntityViewService tbEntityViewService;

    /** 绑定租户，并将客户 externalId 映射为内部 ID。 */
    @Override
    protected void setOwner(TenantId tenantId, EntityView entityView, IdProvider idProvider) {
        entityView.setTenantId(tenantId);
        entityView.setCustomerId(idProvider.getInternalId(entityView.getCustomerId()));
    }

    /** 映射目标实体为当前环境内部 ID。 */
    @Override
    protected EntityView prepare(EntitiesImportCtx ctx, EntityView entityView, EntityView old, EntityExportData<EntityView> exportData, IdProvider idProvider) {
        entityView.setEntityId(idProvider.getInternalId(entityView.getEntityId()));
        return entityView;
    }

    /** 保存视图实体。 */
    @Override
    protected EntityView saveOrUpdate(EntitiesImportCtx ctx, EntityView entityView, EntityExportData<EntityView> exportData, IdProvider idProvider) {
        return entityViewService.saveEntityView(entityView);
    }

    /** 保存后同步实体视图属性，再记审计日志。 */
    @Override
    protected void onEntitySaved(User user, EntityView savedEntityView, EntityView oldEntityView) throws JnksIotException {
        tbEntityViewService.updateEntityViewAttributes(user.getTenantId(), savedEntityView, oldEntityView, user);
        super.onEntitySaved(user, savedEntityView, oldEntityView);
    }

    @Override
    protected EntityView deepCopy(EntityView entityView) {
        return new EntityView(entityView);
    }

    @Override
    protected void cleanupForComparison(EntityView e) {
        super.cleanupForComparison(e);
        if (e.getCustomerId() != null && e.getCustomerId().isNullUid()) {
            e.setCustomerId(null);
        }
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.ENTITY_VIEW;
    }

}
