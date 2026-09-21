package com.jnks.iot.server.dao.resource;

import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ExportableEntityDao;
import com.jnks.iot.server.dao.TenantEntityWithDataDao;

import java.util.List;

public interface JnksIotResourceDao extends Dao<JnksIotResource>, TenantEntityWithDataDao, ExportableEntityDao<JnksIotResourceId, JnksIotResource> {

    JnksIotResource findResourceByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceId);

    PageData<JnksIotResource> findAllByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<JnksIotResource> findResourcesByTenantIdAndResourceType(TenantId tenantId,
                                                                ResourceType resourceType,
                                                                ResourceSubType resourceSubType,
                                                                PageLink pageLink);

    List<JnksIotResource> findResourcesByTenantIdAndResourceType(TenantId tenantId,
                                                            ResourceType resourceType,
                                                            ResourceSubType resourceSubType,
                                                            String[] objectIds,
                                                            String searchText);

    byte[] getResourceData(TenantId tenantId, JnksIotResourceId resourceId);

    byte[] getResourcePreview(TenantId tenantId, JnksIotResourceId resourceId);

    long getResourceSize(TenantId tenantId, JnksIotResourceId resourceId);

}
