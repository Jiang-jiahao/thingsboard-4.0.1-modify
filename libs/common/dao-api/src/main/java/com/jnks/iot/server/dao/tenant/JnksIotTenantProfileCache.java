package com.jnks.iot.server.dao.tenant;

import com.jnks.iot.server.common.data.SystemParams;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;

import java.util.function.Consumer;

public interface JnksIotTenantProfileCache {

    TenantProfile get(TenantId tenantId);

    TenantProfile get(TenantProfileId tenantProfileId);

    void put(TenantProfile profile);

    void evict(TenantProfileId id);

    void evict(TenantId id);

    void addListener(TenantId tenantId, EntityId listenerId, Consumer<TenantProfile> profileListener);

    void removeListener(TenantId tenantId, EntityId listenerId);

}
