package com.jnks.iot.server.dao.resource;

import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;
import com.jnks.iot.server.common.data.JnksIotResourceInfoFilter;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

import java.util.List;
import java.util.Set;

public interface JnksIotResourceInfoDao extends Dao<JnksIotResourceInfo> {

    PageData<JnksIotResourceInfo> findAllTenantResourcesByTenantId(JnksIotResourceInfoFilter filter, PageLink pageLink);

    PageData<JnksIotResourceInfo> findTenantResourcesByTenantId(JnksIotResourceInfoFilter filter, PageLink pageLink);

    JnksIotResourceInfo findByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey);

    boolean existsByTenantIdAndResourceTypeAndResourceKey(TenantId tenantId, ResourceType resourceType, String resourceKey);

    Set<String> findKeysByTenantIdAndResourceTypeAndResourceKeyPrefix(TenantId tenantId, ResourceType resourceType, String prefix);

    List<JnksIotResourceInfo> findByTenantIdAndEtagAndKeyStartingWith(TenantId tenantId, String etag, String query);

    JnksIotResourceInfo findSystemOrTenantResourceByEtag(TenantId tenantId, ResourceType resourceType, String etag);

    boolean existsByPublicResourceKey(ResourceType resourceType, String publicResourceKey);

    JnksIotResourceInfo findPublicResourceByKey(ResourceType resourceType, String publicResourceKey);

}
