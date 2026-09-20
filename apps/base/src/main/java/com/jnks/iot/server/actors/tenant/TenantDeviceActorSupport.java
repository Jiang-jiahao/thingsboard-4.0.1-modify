package com.jnks.iot.server.actors.tenant;

import com.jnks.iot.server.actors.TbActorCtx;
import com.jnks.iot.server.actors.TbActorRef;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;

/**
 * Provides device actor creation for tenant actor without hard dependency on tb-core module.
 */
public interface TenantDeviceActorSupport {

    TbActorRef getOrCreateDeviceActor(TbActorCtx ctx, TenantId tenantId, DeviceId deviceId);

}
