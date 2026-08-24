/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.EntityView;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EntityViewId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.dao.entityview.EntityViewService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.entityview.TbEntityViewService;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link EntityView} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 还原客户与目标实体内部 ID；保存后通过 {@code TbEntityViewService} 同步视图属性。
 */
@Service
@TbCoreComponent
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
    protected void onEntitySaved(User user, EntityView savedEntityView, EntityView oldEntityView) throws ThingsboardException {
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
