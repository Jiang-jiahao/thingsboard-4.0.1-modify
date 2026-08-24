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
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ResourceType;
import org.thingsboard.server.common.data.TbResource;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.TbResourceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.dao.resource.ImageService;
import org.thingsboard.server.dao.resource.ResourceService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link TbResource} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 可按资源类型+key 兜底匹配；图片走 {@code ImageService}，其它资源走 {@code ResourceService}。
 * {@link #compare} 始终返回 true；保存后通知集群资源变更。
 */
@Service
@TbCoreComponent
@RequiredArgsConstructor
public class ResourceImportService extends BaseEntityImportService<TbResourceId, TbResource, EntityExportData<TbResource>> {

    private final ResourceService resourceService;
    private final ImageService imageService;

    @Override
    protected void setOwner(TenantId tenantId, TbResource resource, IdProvider idProvider) {
        resource.setTenantId(tenantId);
    }

    /** 模板覆盖：资源无额外关联 ID 需要映射。 */
    @Override
    protected TbResource prepare(EntitiesImportCtx ctx, TbResource resource, TbResource oldResource, EntityExportData<TbResource> exportData, IdProvider idProvider) {
        return resource;
    }

    /**
     * 基类匹配失败且允许按名称查找时，按资源类型与 key 查找已有资源。
     */
    @Override
    protected TbResource findExistingEntity(EntitiesImportCtx ctx, TbResource resource, IdProvider idProvider) {
        TbResource existingResource = super.findExistingEntity(ctx, resource, idProvider);
        if (existingResource == null && ctx.isFindExistingByName()) {
            existingResource = resourceService.findResourceByTenantIdAndKey(ctx.getTenantId(), resource.getResourceType(), resource.getResourceKey());
        }
        return existingResource;
    }

    /** 始终视为有变更，每次导入都覆盖保存。 */
    @Override
    protected boolean compare(EntitiesImportCtx ctx, EntityExportData<TbResource> exportData, TbResource prepared, TbResource existing) {
        return true;
    }

    @Override
    protected TbResource deepCopy(TbResource resource) {
        return new TbResource(resource);
    }

    /**
     * 图片走 ImageService；其它资源保存后清空 data/preview 以减小返回体。
     */
    @Override
    protected TbResource saveOrUpdate(EntitiesImportCtx ctx, TbResource resource, EntityExportData<TbResource> exportData, IdProvider idProvider) {
        if (resource.getResourceType() == ResourceType.IMAGE) {
            return new TbResource(imageService.saveImage(resource));
        } else {
            resource = resourceService.saveResource(resource);
            resource.setData(null);
            resource.setPreview(null);
            return resource;
        }
    }

    /** 记审计日志后广播资源变更，供集群其它节点刷新。 */
    @Override
    protected void onEntitySaved(User user, TbResource savedResource, TbResource oldResource) throws ThingsboardException {
        super.onEntitySaved(user, savedResource, oldResource);
        clusterService.onResourceChange(savedResource, null);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.TB_RESOURCE;
    }

}
