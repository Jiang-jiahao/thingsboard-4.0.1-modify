package com.jnks.iot.server.service.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ResourceExportData;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.JnksIotResourceDeleteResult;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.lwm2m.LwM2mObject;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.dao.resource.ImageService;
import com.jnks.iot.server.dao.resource.ResourceService;
import com.jnks.iot.server.service.entitiy.AbstractJnksIotEntityService;
import com.jnks.iot.server.service.security.model.SecurityUser;
import com.jnks.iot.server.service.security.permission.AccessControlService;
import com.jnks.iot.server.service.security.permission.Operation;
import com.jnks.iot.server.service.security.permission.Resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.jnks.iot.server.dao.device.DeviceServiceImpl.INCORRECT_TENANT_ID;
import static com.jnks.iot.server.dao.service.Validator.validateId;
import static com.jnks.iot.server.utils.LwM2mObjectModelUtils.toLwM2mObject;
import static com.jnks.iot.server.utils.LwM2mObjectModelUtils.toLwm2mResource;

/**
 * {@link JnksIotResourceService} 默认实现。
 * <p>
 * 处理非图片资源的增删与 LwM2M 模型查询；仪表板/部件导出导入时校验 {@link AccessControlService} 权限，
 * 图片委托 {@link JnksIotImageService}。
 *
 * @see JnksIotResourceService
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DefaultJnksIotResourceService extends AbstractJnksIotEntityService implements JnksIotResourceService {

    private final ResourceService resourceService;
    private final ImageService imageService;
    private final JnksIotImageService jnksIotImageService;
    private final AccessControlService accessControlService;

    /**
     * 保存非图片资源；LwM2M 模型会先解析对象结构。
     */
    @Override
    public JnksIotResourceInfo save(JnksIotResource resource, SecurityUser user) throws JnksIotException {
        if (resource.getResourceType() == ResourceType.IMAGE) {
            throw new IllegalArgumentException("Image resource type is not supported");
        }
        ActionType actionType = resource.getId() == null ? ActionType.ADDED : ActionType.UPDATED;
        TenantId tenantId = resource.getTenantId();
        try {
            if (ResourceType.LWM2M_MODEL.equals(resource.getResourceType())) {
                toLwm2mResource(resource);
            } else if (resource.getResourceKey() == null) {
                resource.setResourceKey(resource.getFileName());
            }
            JnksIotResourceInfo savedResource = new JnksIotResourceInfo(resourceService.saveResource(resource));
            logEntityActionService.logEntityAction(tenantId, savedResource.getId(), savedResource, actionType, user);
            return savedResource;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.JNKS_IOT_RESOURCE), new JnksIotResourceInfo(resource), actionType, user, e);
            throw e;
        }
    }

    /**
     * 删除非图片资源并记录审计。
     */
    @Override
    public JnksIotResourceDeleteResult delete(JnksIotResourceInfo jnksIotResource, boolean force, User user) {
        if (jnksIotResource.getResourceType() == ResourceType.IMAGE) {
            throw new IllegalArgumentException("Image resource type is not supported");
        }
        ActionType actionType = ActionType.DELETED;
        JnksIotResourceId resourceId = jnksIotResource.getId();
        TenantId tenantId = jnksIotResource.getTenantId();
        try {
            JnksIotResourceDeleteResult result = resourceService.deleteResource(tenantId, resourceId, force);
            if (result.isSuccess()) {
                logEntityActionService.logEntityAction(tenantId, resourceId, jnksIotResource, actionType, user, resourceId.toString());
            }

            return result;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.JNKS_IOT_RESOURCE),
                    actionType, user, e, resourceId.toString());
            throw e;
        }
    }

    /**
     * 按 objectId 查询 LwM2M 模型并按名称或 id 排序。
     */
    @Override
    public List<LwM2mObject> findLwM2mObject(TenantId tenantId, String sortOrder, String sortProperty, String[] objectIds) {
        log.trace("Executing findByTenantId [{}]", tenantId);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        List<JnksIotResource> resources = resourceService.findTenantResourcesByResourceTypeAndObjectIds(tenantId, ResourceType.LWM2M_MODEL,
                objectIds);
        return resources.stream()
                .flatMap(s -> Stream.ofNullable(toLwM2mObject(s, false)))
                .sorted(getComparator(sortProperty, sortOrder))
                .collect(Collectors.toList());
    }

    /**
     * 分页查询租户 LwM2M 模型。
     */
    @Override
    public List<LwM2mObject> findLwM2mObjectPage(TenantId tenantId, String sortProperty, String sortOrder, PageLink pageLink) {
        log.trace("Executing findByTenantId [{}]", tenantId);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        PageData<JnksIotResource> resourcePageData = resourceService.findTenantResourcesByResourceTypeAndPageLink(tenantId, ResourceType.LWM2M_MODEL, pageLink);
        return resourcePageData.getData().stream()
                .flatMap(s -> Stream.ofNullable(toLwM2mObject(s, false)))
                .sorted(getComparator(sortProperty, sortOrder))
                .collect(Collectors.toList());
    }

    /**
     * 导出仪表板使用的图片与其它资源。
     */
    @Override
    public List<ResourceExportData> exportResources(Dashboard dashboard, SecurityUser user) throws JnksIotException {
        return exportResources(() -> imageService.getUsedImages(dashboard), () -> resourceService.getUsedResources(user.getTenantId(), dashboard), user);
    }

    /**
     * 导出部件类型使用的图片与其它资源。
     */
    @Override
    public List<ResourceExportData> exportResources(WidgetTypeDetails widgetTypeDetails, SecurityUser user) throws JnksIotException {
        return exportResources(() -> imageService.getUsedImages(widgetTypeDetails), () -> resourceService.getUsedResources(user.getTenantId(), widgetTypeDetails), user);
    }

    /**
     * 逐条导入资源并回写新链接。
     */
    @Override
    public void importResources(List<ResourceExportData> resources, SecurityUser user) throws Exception {
        for (ResourceExportData resourceData : resources) {
            JnksIotResourceInfo resourceInfo;
            if (resourceData.getType() == ResourceType.IMAGE) {
                resourceInfo = jnksIotImageService.importImage(resourceData, true, user);
            } else {
                resourceInfo = importResource(resourceData, user);
            }
            resourceData.setNewLink(resourceInfo.getLink());
        }
    }

    private <T> List<ResourceExportData> exportResources(Supplier<Collection<JnksIotResourceInfo>> imagesProcessor,
                                                         Supplier<Collection<JnksIotResourceInfo>> resourcesProcessor,
                                                         SecurityUser user) throws JnksIotException {
        List<JnksIotResourceInfo> resources = new ArrayList<>();
        resources.addAll(imagesProcessor.get());
        resources.addAll(resourcesProcessor.get());
        for (JnksIotResourceInfo resourceInfo : resources) {
            accessControlService.checkPermission(user, Resource.JNKS_IOT_RESOURCE, Operation.READ, resourceInfo.getId(), resourceInfo);
        }

        return resourceService.exportResources(user.getTenantId(), resources);
    }

    private JnksIotResourceInfo importResource(ResourceExportData resourceData, SecurityUser user) throws JnksIotException {
        JnksIotResource resource = resourceService.toResource(user.getTenantId(), resourceData);
        if (resource.getData() != null) {
            accessControlService.checkPermission(user, Resource.JNKS_IOT_RESOURCE, Operation.CREATE, null, resource);
            return save(resource, user);
        } else {
            accessControlService.checkPermission(user, Resource.JNKS_IOT_RESOURCE, Operation.READ, resource.getId(), resource);
            return resource;
        }
    }

    private Comparator<? super LwM2mObject> getComparator(String sortProperty, String sortOrder) {
        Comparator<LwM2mObject> comparator;
        if ("name".equals(sortProperty)) {
            comparator = Comparator.comparing(LwM2mObject::getName);
        } else {
            comparator = Comparator.comparingLong(LwM2mObject::getId);
        }
        return "DESC".equals(sortOrder) ? comparator.reversed() : comparator;
    }

}
