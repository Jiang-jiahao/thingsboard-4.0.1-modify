package com.jnks.iot.server.dao.resource;

import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.TbResource;
import com.jnks.iot.server.common.data.id.TbResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ExportableEntityDao;
import com.jnks.iot.server.dao.TenantEntityWithDataDao;

import java.util.List;

public interface TbResourceDao extends Dao<TbResource>, TenantEntityWithDataDao, ExportableEntityDao<TbResourceId, TbResource> {

    TbResource findResourceByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceId);

    PageData<TbResource> findAllByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<TbResource> findResourcesByTenantIdAndResourceType(TenantId tenantId,
                                                                ResourceType resourceType,
                                                                ResourceSubType resourceSubType,
                                                                PageLink pageLink);

    List<TbResource> findResourcesByTenantIdAndResourceType(TenantId tenantId,
                                                            ResourceType resourceType,
                                                            ResourceSubType resourceSubType,
                                                            String[] objectIds,
                                                            String searchText);

    byte[] getResourceData(TenantId tenantId, TbResourceId resourceId);

    byte[] getResourcePreview(TenantId tenantId, TbResourceId resourceId);

    long getResourceSize(TenantId tenantId, TbResourceId resourceId);

}
