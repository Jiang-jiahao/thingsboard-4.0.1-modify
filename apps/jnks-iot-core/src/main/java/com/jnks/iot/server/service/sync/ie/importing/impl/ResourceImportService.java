package com.jnks.iot.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.dao.resource.ImageService;
import com.jnks.iot.server.dao.resource.ResourceService;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link JnksIotResource} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 可按资源类型+key 兜底匹配；图片走 {@code ImageService}，其它资源走 {@code ResourceService}。
 * {@link #compare} 始终返回 true；保存后通知集群资源变更。
 */
@Service
@RequiredArgsConstructor
public class ResourceImportService extends BaseEntityImportService<JnksIotResourceId, JnksIotResource, EntityExportData<JnksIotResource>> {

    private final ResourceService resourceService;
    private final ImageService imageService;

    @Override
    protected void setOwner(TenantId tenantId, JnksIotResource resource, IdProvider idProvider) {
        resource.setTenantId(tenantId);
    }

    /** 模板覆盖：资源无额外关联 ID 需要映射。 */
    @Override
    protected JnksIotResource prepare(EntitiesImportCtx ctx, JnksIotResource resource, JnksIotResource oldResource, EntityExportData<JnksIotResource> exportData, IdProvider idProvider) {
        return resource;
    }

    /**
     * 基类匹配失败且允许按名称查找时，按资源类型与 key 查找已有资源。
     */
    @Override
    protected JnksIotResource findExistingEntity(EntitiesImportCtx ctx, JnksIotResource resource, IdProvider idProvider) {
        JnksIotResource existingResource = super.findExistingEntity(ctx, resource, idProvider);
        if (existingResource == null && ctx.isFindExistingByName()) {
            existingResource = resourceService.findResourceByTenantIdAndKey(ctx.getTenantId(), resource.getResourceType(), resource.getResourceKey());
        }
        return existingResource;
    }

    /** 始终视为有变更，每次导入都覆盖保存。 */
    @Override
    protected boolean compare(EntitiesImportCtx ctx, EntityExportData<JnksIotResource> exportData, JnksIotResource prepared, JnksIotResource existing) {
        return true;
    }

    @Override
    protected JnksIotResource deepCopy(JnksIotResource resource) {
        return new JnksIotResource(resource);
    }

    /**
     * 图片走 ImageService；其它资源保存后清空 data/preview 以减小返回体。
     */
    @Override
    protected JnksIotResource saveOrUpdate(EntitiesImportCtx ctx, JnksIotResource resource, EntityExportData<JnksIotResource> exportData, IdProvider idProvider) {
        if (resource.getResourceType() == ResourceType.IMAGE) {
            return new JnksIotResource(imageService.saveImage(resource));
        } else {
            resource = resourceService.saveResource(resource);
            resource.setData(null);
            resource.setPreview(null);
            return resource;
        }
    }

    /** 记审计日志后广播资源变更，供集群其它节点刷新。 */
    @Override
    protected void onEntitySaved(User user, JnksIotResource savedResource, JnksIotResource oldResource) throws JnksIotException {
        super.onEntitySaved(user, savedResource, oldResource);
        clusterService.onResourceChange(savedResource, null);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.JNKS_IOT_RESOURCE;
    }

}
