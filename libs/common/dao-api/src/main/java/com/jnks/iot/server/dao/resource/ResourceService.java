package com.jnks.iot.server.dao.resource;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.ResourceExportData;
import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.JnksIotResourceDeleteResult;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;
import com.jnks.iot.server.common.data.JnksIotResourceInfoFilter;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.Collection;
import java.util.List;

public interface ResourceService extends EntityDaoService {

    JnksIotResource saveResource(JnksIotResource resource);

    JnksIotResource saveResource(JnksIotResource resource, boolean doValidate);

    JnksIotResource findResourceByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey);

    JnksIotResource findResourceById(TenantId tenantId, JnksIotResourceId resourceId);

    byte[] getResourceData(TenantId tenantId, JnksIotResourceId resourceId);

    ResourceExportData exportResource(JnksIotResourceInfo resourceInfo);

    List<ResourceExportData> exportResources(TenantId tenantId, Collection<JnksIotResourceInfo> resources);

    JnksIotResource toResource(TenantId tenantId, ResourceExportData exportData);

    void importResources(TenantId tenantId, List<ResourceExportData> resources);

    JnksIotResourceInfo findResourceInfoById(TenantId tenantId, JnksIotResourceId resourceId);

    JnksIotResourceInfo findResourceInfoByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey);

    PageData<JnksIotResource> findAllTenantResources(TenantId tenantId, PageLink pageLink);

    ListenableFuture<JnksIotResourceInfo> findResourceInfoByIdAsync(TenantId tenantId, JnksIotResourceId resourceId);

    PageData<JnksIotResourceInfo> findAllTenantResourcesByTenantId(JnksIotResourceInfoFilter filter, PageLink pageLink);

    PageData<JnksIotResourceInfo> findTenantResourcesByTenantId(JnksIotResourceInfoFilter filter, PageLink pageLink);

    List<JnksIotResource> findTenantResourcesByResourceTypeAndObjectIds(TenantId tenantId, ResourceType lwm2mModel, String[] objectIds);

    PageData<JnksIotResource> findTenantResourcesByResourceTypeAndPageLink(TenantId tenantId, ResourceType lwm2mModel, PageLink pageLink);

    JnksIotResourceDeleteResult deleteResource(TenantId tenantId, JnksIotResourceId resourceId, boolean force);

    void deleteResourcesByTenantId(TenantId tenantId);

    long sumDataSizeByTenantId(TenantId tenantId);

    String calculateEtag(byte[] data);

    JnksIotResourceInfo findSystemOrTenantResourceByEtag(TenantId tenantId, ResourceType resourceType, String etag);

    boolean updateResourcesUsage(TenantId tenantId, Dashboard dashboard);

    boolean updateResourcesUsage(TenantId tenantId, WidgetTypeDetails widgetTypeDetails);

    Collection<JnksIotResourceInfo> getUsedResources(TenantId tenantId, Dashboard dashboard);

    Collection<JnksIotResourceInfo> getUsedResources(TenantId tenantId, WidgetTypeDetails widgetTypeDetails);

    JnksIotResource createOrUpdateSystemResource(ResourceType resourceType, ResourceSubType resourceSubType, String resourceKey, byte[] data);

}
