package com.jnks.iot.server.actors.tenant;

import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorRef;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;

/**
 * Provides device actor creation for tenant actor without hard dependency on jnks-iot-core module.
 */
public interface TenantDeviceActorSupport {

    JnksIotActorRef getOrCreateDeviceActor(JnksIotActorCtx ctx, TenantId tenantId, DeviceId deviceId);

}
