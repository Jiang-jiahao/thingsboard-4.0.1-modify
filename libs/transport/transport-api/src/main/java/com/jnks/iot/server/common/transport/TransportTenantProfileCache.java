package com.jnks.iot.server.common.transport;

import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;
import com.jnks.iot.server.common.transport.profile.TenantProfileUpdateResult;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.Set;

public interface TransportTenantProfileCache {

    TenantProfile get(TenantId tenantId);

    TenantProfileUpdateResult put(TransportProtos.TenantProfileProto proto);

    boolean put(TenantId tenantId, TenantProfileId profileId);

    Set<TenantId> remove(TenantProfileId profileId);

}
