package com.jnks.iot.server.dao.resource;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.ResourceExportData;
import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.TbResource;
import com.jnks.iot.server.common.data.TbResourceDeleteResult;
import com.jnks.iot.server.common.data.TbResourceInfo;
import com.jnks.iot.server.common.data.TbResourceInfoFilter;
import com.jnks.iot.server.common.data.id.TbResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.Collection;
import java.util.List;

public interface ResourceService extends EntityDaoService {

    TbResource saveResource(TbResource resource);

    TbResource saveResource(TbResource resource, boolean doValidate);

    TbResource findResourceByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey);

    TbResource findResourceById(TenantId tenantId, TbResourceId resourceId);

    byte[] getResourceData(TenantId tenantId, TbResourceId resourceId);

    ResourceExportData exportResource(TbResourceInfo resourceInfo);

    List<ResourceExportData> exportResources(TenantId tenantId, Collection<TbResourceInfo> resources);

    TbResource toResource(TenantId tenantId, ResourceExportData exportData);

    void importResources(TenantId tenantId, List<ResourceExportData> resources);

    TbResourceInfo findResourceInfoById(TenantId tenantId, TbResourceId resourceId);

    TbResourceInfo findResourceInfoByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey);

    PageData<TbResource> findAllTenantResources(TenantId tenantId, PageLink pageLink);

    ListenableFuture<TbResourceInfo> findResourceInfoByIdAsync(TenantId tenantId, TbResourceId resourceId);

    PageData<TbResourceInfo> findAllTenantResourcesByTenantId(TbResourceInfoFilter filter, PageLink pageLink);

    PageData<TbResourceInfo> findTenantResourcesByTenantId(TbResourceInfoFilter filter, PageLink pageLink);

    List<TbResource> findTenantResourcesByResourceTypeAndObjectIds(TenantId tenantId, ResourceType lwm2mModel, String[] objectIds);

    PageData<TbResource> findTenantResourcesByResourceTypeAndPageLink(TenantId tenantId, ResourceType lwm2mModel, PageLink pageLink);

    TbResourceDeleteResult deleteResource(TenantId tenantId, TbResourceId resourceId, boolean force);

    void deleteResourcesByTenantId(TenantId tenantId);

    long sumDataSizeByTenantId(TenantId tenantId);

    String calculateEtag(byte[] data);

    TbResourceInfo findSystemOrTenantResourceByEtag(TenantId tenantId, ResourceType resourceType, String etag);

    boolean updateResourcesUsage(TenantId tenantId, Dashboard dashboard);

    boolean updateResourcesUsage(TenantId tenantId, WidgetTypeDetails widgetTypeDetails);

    Collection<TbResourceInfo> getUsedResources(TenantId tenantId, Dashboard dashboard);

    Collection<TbResourceInfo> getUsedResources(TenantId tenantId, WidgetTypeDetails widgetTypeDetails);

    TbResource createOrUpdateSystemResource(ResourceType resourceType, ResourceSubType resourceSubType, String resourceKey, byte[] data);

}
