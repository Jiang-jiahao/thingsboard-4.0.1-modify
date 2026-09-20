package com.jnks.iot.server.dao.tenant;

import com.jnks.iot.server.common.data.EntityInfo;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

import java.util.List;
import java.util.UUID;

public interface TenantProfileDao extends Dao<TenantProfile> {

    EntityInfo findTenantProfileInfoById(TenantId tenantId, UUID tenantProfileId);

    TenantProfile save(TenantId tenantId, TenantProfile tenantProfile);

    PageData<TenantProfile> findTenantProfiles(TenantId tenantId, PageLink pageLink);

    PageData<EntityInfo> findTenantProfileInfos(TenantId tenantId, PageLink pageLink);

    TenantProfile findDefaultTenantProfile(TenantId tenantId);

    EntityInfo findDefaultTenantProfileInfo(TenantId tenantId);

    List<TenantProfile> findTenantProfilesByIds(TenantId tenantId, UUID[] ids);

}
