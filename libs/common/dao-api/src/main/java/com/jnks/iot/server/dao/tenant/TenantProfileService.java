package com.jnks.iot.server.dao.tenant;

import com.jnks.iot.server.common.data.EntityInfo;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;
import java.util.UUID;

public interface TenantProfileService extends EntityDaoService {

    TenantProfile findTenantProfileById(TenantId tenantId, TenantProfileId tenantProfileId);

    EntityInfo findTenantProfileInfoById(TenantId tenantId, TenantProfileId tenantProfileId);

    TenantProfile saveTenantProfile(TenantId tenantId, TenantProfile tenantProfile);

    void deleteTenantProfile(TenantId tenantId, TenantProfileId tenantProfileId);

    PageData<TenantProfile> findTenantProfiles(TenantId tenantId, PageLink pageLink);

    PageData<EntityInfo> findTenantProfileInfos(TenantId tenantId, PageLink pageLink);

    TenantProfile findOrCreateDefaultTenantProfile(TenantId tenantId);

    TenantProfile findDefaultTenantProfile(TenantId tenantId);

    EntityInfo findDefaultTenantProfileInfo(TenantId tenantId);

    boolean setDefaultTenantProfile(TenantId tenantId, TenantProfileId tenantProfileId);

    void deleteTenantProfiles(TenantId tenantId);

    List<TenantProfile> findTenantProfilesByIds(TenantId tenantId, UUID[] ids);

}
