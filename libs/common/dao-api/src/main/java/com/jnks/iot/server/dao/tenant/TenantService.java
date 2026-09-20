package com.jnks.iot.server.dao.tenant;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.TenantInfo;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;
import java.util.function.Consumer;

public interface TenantService extends EntityDaoService {

    Tenant findTenantById(TenantId tenantId);

    TenantInfo findTenantInfoById(TenantId tenantId);

    ListenableFuture<Tenant> findTenantByIdAsync(TenantId callerId, TenantId tenantId);

    Tenant saveTenant(Tenant tenant);

    Tenant saveTenant(Tenant tenant, Consumer<TenantId> defaultEntitiesCreator);

    boolean tenantExists(TenantId tenantId);

    void deleteTenant(TenantId tenantId);

    PageData<Tenant> findTenants(PageLink pageLink);

    PageData<TenantInfo> findTenantInfos(PageLink pageLink);

    List<TenantId> findTenantIdsByTenantProfileId(TenantProfileId tenantProfileId);

    void deleteTenants();

    PageData<TenantId> findTenantsIds(PageLink pageLink);
}
